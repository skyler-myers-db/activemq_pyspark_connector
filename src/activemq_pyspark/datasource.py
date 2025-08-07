"""
PySpark DataSource implementation for streaming ActiveMQ messages using STOMP protocol.

This module provides a comprehensive streaming interface for consuming messages from
ActiveMQ brokers with optimized memory management, automatic reconnection, and parallel
processing capabilities.

Classes:
    ActiveMQPartition: Represents a partition of ActiveMQ messages for parallel processing.
        Handles message batches with thread-safe serialization/deserialization.

    ActiveMQStreamReader: Main stream reader implementing PySpark's DataSourceStreamReader
        and STOMP ConnectionListener interfaces. Features:
        - Hybrid deque/dict memory management for bounded memory usage
        - TTL protection via configurable heartbeats (default 2s)
        - Exponential backoff reconnection strategy
        - Thread-safe message buffering with configurable limits
        - Automatic offset tracking and partition creation

    ActiveMQDataSource: PySpark DataSource entry point for ActiveMQ integration.
        Registers as 'activemq' data source and provides streaming capabilities only.

Key Features:
    - Memory Management: Bounded buffer (15,000 messages) with automatic eviction
    - Fault Tolerance: Auto-reconnection with exponential backoff up to 5 attempts
    - Performance: O(1) message append and offset-based lookups
    - Monitoring: EST timestamp logging for connection events and errors
    - Serialization: Safe pickle support for Spark executor distribution

Configuration Options:
    - hosts_and_ports: List of broker endpoints (default: [('localhost', 61613)])
    - queues: List of queue names to subscribe to
    - username/password: Authentication credentials (default: admin/password)
    - heartbeats: Heartbeat interval in milliseconds (default: 2000)

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
    - pytz: Timezone handling for logging
"""

from typing import Iterator
from ast import literal_eval
from collections import deque
from threading import Lock, Event
from datetime import datetime
from pytz import timezone
import stomp  # type: ignore

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


class ActiveMQPartition(InputPartition):
    """Represents a partition of ActiveMQ messages for parallel processing."""

    def __init__(
        self, start_offset: int, end_offset: int, messages: list[tuple], lock: Lock
    ):
        partition_info = {
            "start_offset": start_offset,
            "end_offset": end_offset,
            "message_count": len(messages),
        }
        super().__init__(partition_info)
        self.start_offset: int = start_offset
        self.end_offset: int = end_offset
        self._messages: list[tuple] = messages
        self._lock: Lock = lock

    def read(self) -> Iterator[tuple]:
        """Required interface method for reading partition data."""
        for message in self._messages:
            yield message

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        if "_lock" in state:
            del state["_lock"]
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._lock = Lock()


class ActiveMQStreamReader(DataSourceStreamReader, stomp.ConnectionListener):
    """
    PySpark DataSource stream reader for ActiveMQ using STOMP protocol.

    Implements hybrid deque/dict memory management for optimal performance:
    - Deque: Bounded memory with automatic eviction (O(1) append)
    - Dict: Fast offset-based lookups for partition creation (O(1) access)

    Features TTL protection via 2-second heartbeats and exponential backoff reconnection.
    """

    MAX_MESSAGES_BUFFER: int = 15000
    HEARTBEAT_INTERVAL: int = 2000
    RECONNECT_DELAY: int = 2
    MAX_RECONNECT_ATTEMPTS: int = 5

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

        self._current_offset: int = 0
        self._messages_deque: deque[tuple] = deque(maxlen=self.MAX_MESSAGES_BUFFER)
        self._messages_dict: dict[int, tuple] = {}
        self._lock: Lock = Lock()
        self._shutdown_event: Event = Event()
        self._reconnect_count: int = 0
        self._connect_and_subscribe()

    def _get_est_timestamp(self) -> str:
        """Get current EST timestamp for logging."""
        est = timezone("America/New_York")
        return datetime.now(est).strftime("%Y-%m-%d %H:%M:%S EST")

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

            print(
                f"{self._get_est_timestamp()}: INFO: Attempting to connect to broker..."
            )
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
                print(
                    f"{self._get_est_timestamp()}: INFO: Attempting to subscribe to queue: '{queue}' with ID: '{idx}'"
                )
                self._conn.subscribe(
                    destination=queue,
                    id=str(idx),
                    ack="auto",
                )
            self._reconnect_count = 0

        except (stomp.exception.ConnectFailedException, ConnectionError) as e:
            print(
                f"{self._get_est_timestamp()}: ERROR: Failed to connect to broker: {e}"
            )
            raise
        except (OSError, IOError) as e:
            print(
                f"{self._get_est_timestamp()}: ERROR: Network error during connection: {e}"
            )
            raise

    def __getstate__(self) -> dict:
        """Prepare object for serialization by removing non-serializable objects."""
        state: dict = self.__dict__.copy()
        non_serializable = ["_lock", "_conn", "_shutdown_event"]
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
        self._reconnect_count = 0
        self._connect_and_subscribe()

    def on_connected(self, frame: stomp.utils.Frame) -> None:
        """Handle successful broker connection."""
        server_info = frame.headers.get("server", "unknown")
        print(
            f"{self._get_est_timestamp()}: SUCCESS: Connected to broker: {server_info}"
        )

    def on_message(self, frame: stomp.utils.Frame) -> None:
        """Process incoming ActiveMQ messages with hybrid storage approach."""
        if self._shutdown_event.is_set():
            return

        try:
            message_tuple: tuple = (
                self._current_offset,
                frame.cmd,
                frame.headers,
                frame.body,
                None,
            )
        except (AttributeError, KeyError) as e:
            message_tuple = (
                self._current_offset,
                frame.cmd if hasattr(frame, "cmd") else "ERROR",
                frame.headers if hasattr(frame, "headers") else {},
                frame.body if hasattr(frame, "body") else "",
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
        error_msg = frame.body if frame.body else "Unknown error"
        print(f"{self._get_est_timestamp()}: ERROR: Received broker error: {error_msg}")

    def on_disconnected(self) -> None:
        """Handle broker disconnection with exponential backoff reconnection strategy."""
        if self._shutdown_event.is_set():
            return

        print(
            f"{self._get_est_timestamp()}: ERROR: Disconnected from broker. Attempting to reconnect..."
        )

        while (
            not self._shutdown_event.is_set()
            and self._reconnect_count < self.MAX_RECONNECT_ATTEMPTS
        ):
            try:
                self._reconnect_count += 1
                delay = min(
                    self.RECONNECT_DELAY * (2 ** (self._reconnect_count - 1)), 60
                )
                print(
                    f"{self._get_est_timestamp()}: Reconnect attempt {self._reconnect_count}/{self.MAX_RECONNECT_ATTEMPTS} in {delay}s"
                )

                if not self._shutdown_event.wait(delay):
                    self._connect_and_subscribe()
                    if self._conn and self._conn.is_connected():
                        print(
                            f"{self._get_est_timestamp()}: SUCCESS: Reconnected to broker"
                        )
                        return

            except (stomp.exception.ConnectFailedException, ConnectionError) as e:
                print(
                    f"{self._get_est_timestamp()}: Reconnect attempt {self._reconnect_count} failed: {e}"
                )
            except (OSError, IOError) as e:
                print(
                    f"{self._get_est_timestamp()}: Network error during reconnect: {e}"
                )

        if self._reconnect_count >= self.MAX_RECONNECT_ATTEMPTS:
            print(
                f"{self._get_est_timestamp()}: ERROR: Failed to reconnect after {self.MAX_RECONNECT_ATTEMPTS} attempts"
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
            print(
                f"{self._get_est_timestamp()}: INFO: No new messages, returning empty partition."
            )
            return [ActiveMQPartition(start_offset, start_offset, [], self._lock)]

        with self._lock:
            partition_data = [
                self._messages_dict[offset]
                for offset in range(start_offset, end_offset)
                if offset in self._messages_dict
            ]

        if len(partition_data) <= 100:
            partitions = [
                ActiveMQPartition(start_offset, end_offset, partition_data, self._lock)
            ]
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
                            chunk_start_offset, chunk_end_offset, chunk_data, self._lock
                        )
                    )

        print(
            f"{self._get_est_timestamp()}: INFO: Created {len(partitions)} partition(s) with {len(partition_data)} messages (offsets {start_offset} to {end_offset})"
        )
        return partitions

    def commit(self, end: dict[str, int]) -> None:
        """Remove committed messages from hybrid storage structures."""
        commit_offset: int = end.get("offset", 0)

        with self._lock:
            committed_offsets = [
                offset for offset in self._messages_dict if offset < commit_offset
            ]
            for offset in committed_offsets:
                self._messages_dict.pop(offset, None)

            remaining_messages = [
                msg for msg in self._messages_deque if msg[0] >= commit_offset
            ]
            self._messages_deque.clear()
            self._messages_deque.extend(remaining_messages)

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
        print(f"{self._get_est_timestamp()}: ActiveMQ Stream Reader stopping...")

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

    def reader(self, schema: StructType):
        """Batch reading is not supported for ActiveMQ data source."""
        raise NotImplementedError(
            "ERROR: Batch queries are not supported for ActiveMQDataSource."
        )

    def streamReader(self, schema: StructType) -> ActiveMQStreamReader:
        """Create a stream reader for real-time ActiveMQ message processing."""
        return ActiveMQStreamReader(schema, self.options)
