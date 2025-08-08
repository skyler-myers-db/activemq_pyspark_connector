"""
PySpark DataSource implementation for streaming ActiveMQ messages using STOMP protocol.

This module provides a streaming interface for consuming messages from ActiveMQ brokers
with optimized memory management, automatic reconnection, and parallel processing.

Classes:
    ActiveMQPartition: Represents a partition of ActiveMQ messages for parallel processing.
    ActiveMQStreamReader: PySpark DataSourceStreamReader and STOMP ConnectionListener implementation.
    ActiveMQDataSource: PySpark DataSource entry point for ActiveMQ integration (streaming only).

Key Features:
    - Memory: Bounded buffer (15,000 msgs) via deque + dict hybrid
    - Fault tolerance: Auto-reconnect with exponential backoff (unlimited attempts)
    - Performance: O(1) message append and offset lookups
    - Monitoring: Lightweight stdlib logging for connection events and errors
    - TTL protection: 200ms heartbeats + manual keepalive loop

Configuration Options:
    - hosts_and_ports: List of broker endpoints (default: [('localhost', 61613)])
    - queues: List of queue names to subscribe to
    - username/password: Authentication credentials (default: admin/password)
    - heartbeats: Heartbeat interval in milliseconds (default: 200)
    - enable_manual_keepalive: Enable client-side keepalive (default: true)
    - manual_keepalive_interval_ms: Interval for keepalive loop (default: 200)

Schema:
    - offset: Integer message offset for ordering
    - frameCmd: STOMP frame command
    - frameHeaders: Message headers as string
    - frameBody: Message payload
    - messageError: Error information if processing fails

Usage:
    df = spark.readStream.format("activemq").option("queues", "['queue1']").load()

Dependencies:
    - pyspark: Core Spark functionality
    - stomp.py: STOMP protocol implementation
    - logging: Standard library logging
"""

import time
import sys
import json
from typing import Iterator, Final
from ast import literal_eval
from collections import deque
from threading import Lock, Event, Thread
import logging
import stomp

_log = logging.getLogger(__name__)
# Ensure this module logs to stderr if no handlers are configured (common on Databricks)
if not _log.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setLevel(logging.WARNING)
    _formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    _handler.setFormatter(_formatter)
    _log.addHandler(_handler)
    _log.setLevel(logging.WARNING)
    # Allow messages to propagate to root handlers managed by Spark/Databricks
    _log.propagate = True


def _safe_frame_str(frame: object) -> str:
    """Return a robust string representation of a STOMP frame for debugging/printing."""
    cmd = getattr(frame, "cmd", None)
    headers = getattr(frame, "headers", None)
    body = getattr(frame, "body", None)

    # Headers string
    if isinstance(headers, dict):
        try:
            headers_str = json.dumps(headers, separators=(",", ":"))
        except (TypeError, ValueError):
            headers_str = str(headers)
    else:
        headers_str = str(headers)

    # Body string
    if isinstance(body, (bytes, bytearray)):
        try:
            body_str = body.decode("utf-8", errors="replace")
        except UnicodeDecodeError:
            body_str = "<unprintable body>"
    else:
        body_str = str(body)

    return f"cmd={cmd} headers={headers_str} body={body_str}"


from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


class ActiveMQPartition(InputPartition):
    """Represents a partition of ActiveMQ messages for parallel processing."""

    def __init__(self, start_offset: int, end_offset: int, messages: list[tuple]):
        partition_info = {
            "start_offset": start_offset,
            "end_offset": end_offset,
            "message_count": len(messages),
        }
        super().__init__(partition_info)
        self.start_offset: int = start_offset
        self.end_offset: int = end_offset
        self._messages: list[tuple] = messages

    def read(self) -> Iterator[tuple]:
        """Required interface method for reading partition data."""
        for message in self._messages:
            yield message

    def __getstate__(self) -> dict:
        # Partition only carries simple, picklable data
        return {k: v for k, v in self.__dict__.items() if k != "_lock"}

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)


class ActiveMQStreamReader(DataSourceStreamReader, stomp.ConnectionListener):
    """
    PySpark DataSource stream reader for ActiveMQ using STOMP protocol.

    Implements hybrid deque/dict memory management for optimal performance:
    - Deque: Bounded memory with automatic eviction (O(1) append)
    - Dict: Fast offset-based lookups for partition creation (O(1) access)

    Features TTL protection via 200ms heartbeats and exponential backoff reconnection.
    """

    MAX_MESSAGES_BUFFER: Final[int] = 15_000
    HEARTBEAT_INTERVAL: Final[int] = 200
    RECONNECT_DELAY: Final[int] = 2
    # 0 => unlimited reconnect attempts
    MAX_RECONNECT_ATTEMPTS: Final[int] = 0

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        stomp.ConnectionListener.__init__(self)
        self._schema: StructType = schema
        self._options: dict[str, str] = options
        self._conn: stomp.Connection12 | None = None
        self._heartbeats: int = int(
            self._options.get("heartbeats", self.HEARTBEAT_INTERVAL)
        )

        self._broker_hosts_and_ports: list[tuple[str, int]] = literal_eval(
            self._options.get("hosts_and_ports", "[('localhost', 61613)]")
        )
        self._broker_queues: list[str] = literal_eval(self._options["queues"])
        self._username: str = self._options.get("username", "admin")
        self._password: str = self._options.get("password", "password")
        # Manual keepalive settings (client-side only)
        self._enable_manual_keepalive: bool = str(
            self._options.get("enable_manual_keepalive", "true")
        ).lower() in {"1", "true", "yes", "y"}
        self._manual_keepalive_interval_ms: int = int(
            self._options.get("manual_keepalive_interval_ms", "200")
        )
        # Debug/health controls
        self._debug_print_frames: bool = str(
            self._options.get("debug_print_frames", "false")
        ).lower() in {"1", "true", "yes", "y"}
        self._health_log_interval_sec: int = int(
            self._options.get("health_log_interval_sec", "0")
        )
        self._current_offset: int = 0
        self._last_committed_offset: int = 0
        self._messages_deque: deque[tuple] = deque(maxlen=self.MAX_MESSAGES_BUFFER)
        self._messages_dict: dict[int, tuple] = {}
        self._lock: Lock = Lock()
        self._shutdown_event: Event = Event()
        self._reconnect_count: int = 0
        self._keepalive_thread: Thread | None = None
        self._keepalive_running: Event = Event()
        self._health_thread: Thread | None = None
        self._health_running: Event = Event()
        self._last_heartbeat_ts: float = 0.0
        self._last_message_ts: float = 0.0
        self._connect_and_subscribe()

    def _connect_and_subscribe(self) -> None:
        """Connects to the broker and subscribes to queues with comprehensive error handling."""
        try:
            if self._conn and self._conn.is_connected():
                return

            if not self._conn:
                self._conn = stomp.Connection12(
                    host_and_ports=self._broker_hosts_and_ports,
                    heartbeats=(self._heartbeats, self._heartbeats),
                    keepalive=True,
                )
                self._conn.set_listener("ActiveMQReaderListener", self)

            _log.info("Attempting to connect to broker…")
            self._conn.connect(
                self._username,
                self._password,
                wait=True,
                headers={
                    "heart-beat": f"{self._heartbeats},{self._heartbeats}",
                    "accept-version": "1.2",
                },
            )

            for idx, queue in enumerate(self._broker_queues, start=1):
                _log.info("Subscribing to queue '%s' with id '%s'", queue, idx)
                self._conn.subscribe(
                    destination=queue,
                    id=str(idx),
                    ack="auto",
                )
            self._reconnect_count = 0

            # Start/ensure keepalive thread is running
            if self._enable_manual_keepalive and (
                self._keepalive_thread is None or not self._keepalive_thread.is_alive()
            ):
                self._start_keepalive_loop()
            # Start health reporter if enabled
            if self._health_log_interval_sec > 0 and (
                self._health_thread is None or not self._health_thread.is_alive()
            ):
                self._start_health_loop()

        except (stomp.exception.ConnectFailedException, ConnectionError) as e:
            _log.error("Failed to connect to broker: %s", e)
            raise
        except (OSError, IOError) as e:
            _log.error("Network error during connection: %s", e)
            raise

    def __getstate__(self) -> dict:
        """Prepare object for serialization by removing non-serializable objects."""
        state: dict = self.__dict__.copy()
        # Remove all threading primitives and connection objects that are not picklable
        non_serializable = [
            "_lock",
            "_conn",
            "_shutdown_event",
            "_keepalive_thread",
            "_keepalive_running",
            "_health_thread",
            "_health_running",
        ]
        for key in non_serializable:
            if key in state:
                del state[key]
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore object after deserialization and re-establish connection."""
        self.__dict__.update(state)
        self._lock = Lock()
        self._conn = None
        self._shutdown_event = Event()
        self._keepalive_thread = None
        self._keepalive_running = Event()
        self._last_committed_offset = getattr(self, "_last_committed_offset", 0)
        self._health_thread = None
        self._health_running = Event()
        self._last_heartbeat_ts = getattr(self, "_last_heartbeat_ts", 0.0)
        self._last_message_ts = getattr(self, "_last_message_ts", 0.0)
        self._reconnect_count = 0
        self._connect_and_subscribe()

    def on_connected(self, frame: stomp.utils.Frame) -> None:
        """Handle successful broker connection."""
        # Print once with timestamp for visibility on the driver (rare event)
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(
            f"[{ts}] on_connected: {_safe_frame_str(frame)}",
            file=sys.stderr,
            flush=True,
        )
        server_info = frame.headers.get("server", "unknown")
        server_hb = frame.headers.get("heart-beat", "unknown")
        _log.info("Connected to broker: %s", server_info)
        _log.info(
            "Heartbeat negotiation -> client: %sms, server advertises: %s",
            self._heartbeats,
            server_hb,
        )

    def on_heartbeat(self) -> None:
        """Called by stomp.py when a heartbeat is received from the broker."""
        # Record timestamp for health reporting
        self._last_heartbeat_ts = time.time()

    # Keep silent to minimize overhead at 200ms cadence; enable if needed:
    # _log.debug("Received heartbeat from broker")

    def on_heartbeat_timeout(self) -> None:
        """Called when broker heartbeats are not received within the expected window."""
        _log.warning("Heartbeat timeout waiting for broker heartbeats")
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(
            f"[{ts}] heartbeat_timeout: no broker heartbeat within expected window",
            file=sys.stderr,
            flush=True,
        )
        # Proactive reconnect to avoid broker-side TTL disconnects
        try:
            if self._conn and self._conn.is_connected():
                self._conn.disconnect()
        except (
            stomp.exception.ConnectFailedException,
            ConnectionError,
            OSError,
            IOError,
        ) as e:
            _log.warning("Disconnect on heartbeat timeout failed: %s", e)
        # Reset reconnect state and attempt immediate reconnect
        self._reconnect_count = 0
        self._connect_and_subscribe()

    # -------------------- Manual keepalive loop --------------------
    def _start_keepalive_loop(self) -> None:
        """Start a lightweight loop that sends benign frames to keep the TCP connection active."""
        self._keepalive_running.set()
        self._keepalive_thread = Thread(
            target=self._keepalive_loop, name="amq-keepalive", daemon=True
        )
        self._keepalive_thread.start()

    def _keepalive_loop(self) -> None:
        interval = max(100, self._manual_keepalive_interval_ms)
        while not self._shutdown_event.is_set() and self._keepalive_running.is_set():
            try:
                # Send a BEGIN/ABORT transaction pair as a harmless no-op if connected
                if self._conn and self._conn.is_connected():
                    tx_id = f"ka-{time.time_ns()}"
                    try:
                        self._conn.begin(transaction=tx_id)
                        self._conn.abort(transaction=tx_id)
                    except (
                        stomp.exception.ConnectFailedException,
                        ConnectionError,
                        OSError,
                        IOError,
                    ) as e:
                        _log.warning("Keepalive send failed: %s", e)
                # Sleep in small chunks to be interruptible
                slept = 0
                while (
                    slept < interval
                    and not self._shutdown_event.is_set()
                    and self._keepalive_running.is_set()
                ):
                    time.sleep(0.05)
                    slept += 50
            except (OSError, IOError, ConnectionError) as e:
                _log.warning("Keepalive loop error: %s", e)
                time.sleep(0.5)

    # -------------------- Health reporter loop --------------------
    def _start_health_loop(self) -> None:
        """Start a loop that periodically prints a concise health summary."""
        if self._health_log_interval_sec <= 0:
            return
        self._health_running.set()
        self._health_thread = Thread(
            target=self._health_loop, name="amq-health", daemon=True
        )
        self._health_thread.start()

    def _health_loop(self) -> None:
        interval = float(max(1, int(self._health_log_interval_sec)))
        while not self._shutdown_event.is_set() and self._health_running.is_set():
            try:
                now = time.time()
                since_hb = (
                    (now - self._last_heartbeat_ts) if self._last_heartbeat_ts else None
                )
                since_msg = (
                    (now - self._last_message_ts) if self._last_message_ts else None
                )
                conn_state = (
                    "connected"
                    if (self._conn and self._conn.is_connected())
                    else "disconnected"
                )
                ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
                hb_s = f"{since_hb:.3f}s" if since_hb is not None else "n/a"
                msg_s = f"{since_msg:.3f}s" if since_msg is not None else "n/a"
                print(
                    f"[{ts}] health: hb_since={hb_s} msg_since={msg_s} state={conn_state} queued={len(self._messages_deque)}",
                    file=sys.stderr,
                    flush=True,
                )
                # Sleep in small chunks to be interruptible
                slept = 0.0
                while (
                    slept < interval
                    and not self._shutdown_event.is_set()
                    and self._health_running.is_set()
                ):
                    time.sleep(0.2)
                    slept += 0.2
            except (OSError, IOError, ConnectionError, RuntimeError, ValueError) as e:
                _log.debug("Health loop transient error: %s", e)
                time.sleep(1)

    def on_message(self, frame: stomp.utils.Frame) -> None:
        """Process incoming ActiveMQ messages with hybrid storage approach."""
        # Update last message timestamp and optionally print full frame when debug enabled
        self._last_message_ts = time.time()
        # Intentionally avoid per-message prints to prevent stderr flooding
        if self._shutdown_event.is_set():
            return

        try:
            # Serialize headers/body to strings to reduce object overhead and match schema
            if hasattr(frame, "headers") and isinstance(frame.headers, dict):
                headers_str = json.dumps(frame.headers, separators=(",", ":"))
            else:
                headers_str = str(getattr(frame, "headers", {}))

            body_val = getattr(frame, "body", "")
            if isinstance(body_val, (bytes, bytearray)):
                try:
                    body_str = body_val.decode("utf-8", errors="replace")
                except UnicodeDecodeError:
                    body_str = str(body_val)
            else:
                body_str = str(body_val)

            message_tuple: tuple = (
                self._current_offset,
                frame.cmd,
                headers_str,
                body_str,
                None,
            )
        except (AttributeError, KeyError) as e:
            message_tuple = (
                self._current_offset,
                frame.cmd if hasattr(frame, "cmd") else "ERROR",
                str(getattr(frame, "headers", {})),
                str(getattr(frame, "body", "")),
                f"Message processing error: {str(e)}",
            )

        with self._lock:
            if len(self._messages_deque) == self.MAX_MESSAGES_BUFFER:
                evicted_message = self._messages_deque[0]
                evicted_offset = evicted_message[0]
                self._messages_dict.pop(evicted_offset, None)

            self._messages_deque.append(message_tuple)
            self._messages_dict[self._current_offset] = message_tuple

        self._current_offset += 1

    def on_error(self, frame: stomp.utils.Frame) -> None:
        """Handle broker error messages."""
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"[{ts}] on_error: {_safe_frame_str(frame)}", file=sys.stderr, flush=True)
        _log.error("Received broker error from broker: %s", _safe_frame_str(frame))

    def on_disconnected(self) -> None:
        """Handle broker disconnection with exponential backoff reconnection strategy."""
        if self._shutdown_event.is_set():
            return
        _log.error("Disconnected from broker. Attempting to reconnect…")
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(
            f"[{ts}] disconnected: attempting to reconnect", file=sys.stderr, flush=True
        )

        while not self._shutdown_event.is_set() and (
            self.MAX_RECONNECT_ATTEMPTS == 0
            or self._reconnect_count < self.MAX_RECONNECT_ATTEMPTS
        ):
            try:
                self._reconnect_count += 1
                delay = min(
                    self.RECONNECT_DELAY * (2 ** (self._reconnect_count - 1)), 60
                )
                _log.info(
                    "Reconnect attempt %s/%s in %ss",
                    self._reconnect_count,
                    self.MAX_RECONNECT_ATTEMPTS,
                    delay,
                )

                if not self._shutdown_event.wait(delay):
                    self._connect_and_subscribe()
                    if self._conn and self._conn.is_connected():
                        _log.info("Reconnected to broker")
                        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        print(
                            f"[{ts}] reconnected: broker connection restored",
                            file=sys.stderr,
                            flush=True,
                        )
                        return

            except (stomp.exception.ConnectFailedException, ConnectionError) as e:
                _log.warning(
                    "Reconnect attempt %s failed: %s", self._reconnect_count, e
                )
            except (OSError, IOError) as e:
                _log.warning("Network error during reconnect: %s", e)

        if self.MAX_RECONNECT_ATTEMPTS and (
            self._reconnect_count >= self.MAX_RECONNECT_ATTEMPTS
        ):
            _log.error(
                "Failed to reconnect after %s attempts", self.MAX_RECONNECT_ATTEMPTS
            )
            self._shutdown_event.set()

    def initialOffset(self) -> dict[str, int]:
        """Return the initial offset for streaming."""
        return {"offset": 0}

    def latestOffset(self) -> dict[str, int]:
        """Return the latest available offset."""
        with self._lock:
            return {"offset": self._current_offset}

    def partitions(
        self, start: dict[str, int], end: dict[str, int]
    ) -> list[ActiveMQPartition]:
        """Create partitions for parallel processing of message batches."""
        start_offset: int = start.get("offset", 0)
        end_offset: int = end.get("offset", self._current_offset)

        if end_offset <= start_offset:
            _log.debug("No new messages, returning empty partition")
            return [ActiveMQPartition(start_offset, start_offset, [])]

        with self._lock:
            partition_data = [
                self._messages_dict[offset]
                for offset in range(start_offset, end_offset)
                if offset in self._messages_dict
            ]

        if len(partition_data) <= 100:
            partitions = [ActiveMQPartition(start_offset, end_offset, partition_data)]
        else:
            num_partitions = min(8, len(partition_data) // 50)
            chunk_size = len(partition_data) // num_partitions
            partitions = []

            for i in range(num_partitions):
                chunk_start = i * chunk_size
                chunk_end = (
                    chunk_start + chunk_size
                    if i < num_partitions - 1
                    else len(partition_data)
                )
                chunk_data = partition_data[chunk_start:chunk_end]

                if chunk_data:
                    chunk_start_offset = chunk_data[0][0]
                    chunk_end_offset = chunk_data[-1][0] + 1
                    partitions.append(
                        ActiveMQPartition(
                            chunk_start_offset,
                            chunk_end_offset,
                            chunk_data,
                        )
                    )

        return partitions

    def commit(self, end: dict[str, int]) -> None:
        """Remove committed messages from hybrid storage structures (O(delta))."""
        commit_offset: int = end.get("offset", 0)
        with self._lock:
            start = self._last_committed_offset
            if commit_offset <= start:
                return
            # Drop dict entries in the committed range
            for off in range(start, commit_offset):
                self._messages_dict.pop(off, None)
            # Incrementally pop from the left of the deque
            while self._messages_deque and self._messages_deque[0][0] < commit_offset:
                self._messages_deque.popleft()
            self._last_committed_offset = commit_offset

    def read(self, partition: InputPartition) -> Iterator[tuple]:
        """Read data from the partition using the restored interface."""
        if isinstance(partition, ActiveMQPartition):
            return partition.read()
        raise TypeError(f"Expected ActiveMQPartition, got {type(partition)}")

    def schema(self) -> StructType:
        """Return the schema for the data source."""
        return self._schema

    def stop(self) -> None:
        """Stop the reader and cleanup all resources."""
        _log.info("ActiveMQ Stream Reader stopping…")
        # Stop keepalive loop
        self._keepalive_running.clear()
        if self._keepalive_thread and self._keepalive_thread.is_alive():
            # Give it a moment to exit
            time.sleep(0.1)
            try:
                self._keepalive_thread.join(timeout=1.0)
            except RuntimeError:
                pass
        # Stop health loop
        self._health_running.clear()
        if self._health_thread and self._health_thread.is_alive():
            time.sleep(0.1)
            try:
                self._health_thread.join(timeout=1.0)
            except RuntimeError:
                pass
        if self._conn and self._conn.is_connected():
            self._conn.disconnect()

        with self._lock:
            self._messages_dict.clear()
            self._messages_deque.clear()


class ActiveMQDataSource(DataSource):
    """
    PySpark DataSource implementation for ActiveMQ message streaming.

    Provides streaming access to ActiveMQ queues using STOMP protocol
    with optimized memory management and TTL protection.
    """

    @classmethod
    def name(cls) -> str:
        """Return the data source name for PySpark registration."""
        return "activemq"

    def schema(self) -> StructType:
        """Define the schema for ActiveMQ message data."""
        return StructType(
            [
                StructField("offset", IntegerType(), nullable=False),
                StructField("frameCmd", StringType(), nullable=True),
                StructField("frameHeaders", StringType(), nullable=True),
                StructField("frameBody", StringType(), nullable=True),
                StructField("messageError", StringType(), nullable=True),
            ]
        )

    def reader(self, schema: StructType) -> ActiveMQStreamReader:
        """Batch reading is not supported for ActiveMQ data source."""
        raise NotImplementedError(
            "ERROR: Batch queries are not supported for ActiveMQDataSource."
        )

    def streamReader(self, schema: StructType) -> ActiveMQStreamReader:
        """Create a stream reader for real-time ActiveMQ message processing."""
        return ActiveMQStreamReader(schema, self.options)
