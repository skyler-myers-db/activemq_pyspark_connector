import stomp
from typing import Iterator
from ast import literal_eval
from collections import deque
from threading import Lock, Event

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


class ActiveMQPartition(InputPartition):
    def __init__(self, start_offset, end_offset, messages, lock):
        # Pass a meaningful value to the parent constructor
        partition_info = {
            "start_offset": start_offset,
            "end_offset": end_offset,
            "message_count": len(messages),
        }
        super().__init__(partition_info)
        self.start_offset = start_offset
        self.end_offset = end_offset
        self._messages = messages  # list of tuples: (offset, frameCmd, frameHeaders, frameBody, error)
        self._lock = lock

    def read(self):
        for message in self._messages:
            yield message  # (offset, frameCmd, frameHeaders, frameBody, error)

    def __getstate__(self):
        state = self.__dict__.copy()
        if "_lock" in state:
            del state["_lock"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = Lock()


class ActiveMQStreamReader(DataSourceStreamReader, stomp.ConnectionListener):
    # Constants for better memory management
    MAX_MESSAGES_BUFFER = 10000  # Limit message buffer size
    HEARTBEAT_INTERVAL = 3000  # 3 seconds - more frequent than TTL
    RECONNECT_DELAY = 2  # seconds
    MAX_RECONNECT_ATTEMPTS = 5

    def __init__(self, schema: StructType, options: dict[str, str]):
        stomp.ConnectionListener.__init__(self)
        self._schema: StructType = schema
        self._options: dict[str, str] = options
        self._conn: stomp.Connection12 | None = None
        self._heartbeats: int = int(
            self._options.get("heartbeats", self.HEARTBEAT_INTERVAL)
        )

        self._broker_hosts_and_ports: str = literal_eval(
            self._options.get("hosts_and_ports", "[('localhost', 61613)]")
        )
        self._broker_queues: list[str] = literal_eval(self._options["queues"])

        self._username: str = self._options.get("username", "admin")
        self._password: str = self._options.get("password", "password")

        self._current_offset: int = 0
        # Use deque for better memory management - automatically bounds size
        self._messages: deque = deque(maxlen=self.MAX_MESSAGES_BUFFER)
        self._offset_to_index: dict[int, int] = {}  # Maps offset to deque index
        self._lock: Lock = Lock()
        self._shutdown_event: Event = Event()
        self._reconnect_count: int = 0
        self._connect_and_subscribe()

    def _connect_and_subscribe(self):
        """Connects to the broker and subscribes to queues with better error handling."""
        try:
            if self._conn and self._conn.is_connected():
                return  # Already connected

            if not self._conn:
                self._conn = stomp.Connection12(
                    host_and_ports=self._broker_hosts_and_ports,
                    heartbeats=(self._heartbeats, self._heartbeats),
                    keepalive=True,  # Enable TCP keepalive
                )
                self._conn.set_listener("ActiveMQReaderListener", self)

            print("INFO: Attempting to connect to broker...")
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
                    f"INFO: Attempting to subscribe to queue: '{queue}' with ID: '{idx}'"
                )
                self._conn.subscribe(
                    destination=queue,
                    id=str(idx),
                    ack="auto",
                )
            self._reconnect_count = 0  # Reset counter on successful connection

        except (stomp.exception.ConnectFailedException, ConnectionError) as e:
            print(f"ERROR: Failed to connect to broker: {e}")
            raise
        except (OSError, IOError) as e:
            print(f"ERROR: Network error during connection: {e}")
            raise

    def __getstate__(self):
        state: dict = self.__dict__.copy()
        # Remove non-serializable objects
        non_serializable = ["_lock", "_conn", "_shutdown_event"]
        for key in non_serializable:
            if key in state:
                del state[key]
        return state

    def __setstate__(self, state):
        """Ensure connection is re-established after deserialization on the driver."""
        self.__dict__.update(state)
        self._lock: Lock = Lock()
        self._conn: stomp.Connection12 | None = None
        self._shutdown_event: Event = Event()
        self._reconnect_count: int = 0
        self._connect_and_subscribe()

    def on_connected(self, frame: stomp.utils.Frame):
        print(f"SUCCESS: Connected to broker: {frame.headers.get('server', 'unknown')}")

    def on_message(self, frame: stomp.utils.Frame):
        if self._shutdown_event.is_set():
            return

        try:
            # Keep ALL message data as originally designed
            message_tuple = (
                self._current_offset,
                frame.cmd,
                frame.headers,
                frame.body,
                None,
            )
        except (AttributeError, KeyError) as e:
            # More specific exception handling for error case
            message_tuple = (
                self._current_offset,
                frame.cmd if hasattr(frame, "cmd") else "ERROR",
                frame.headers if hasattr(frame, "headers") else {},
                frame.body if hasattr(frame, "body") else "",
                f"Message processing error: {str(e)}",
            )

        with self._lock:
            # Use deque for better memory management
            if len(self._messages) >= self.MAX_MESSAGES_BUFFER:
                # Remove oldest message and its offset mapping
                if self._messages:
                    oldest_offset = self._messages[0][0]
                    self._offset_to_index.pop(oldest_offset, None)

            self._messages.append(message_tuple)
            self._offset_to_index[self._current_offset] = len(self._messages) - 1

        self._current_offset += 1

    def on_error(self, frame: stomp.utils.Frame):
        error_msg = frame.body if frame.body else "Unknown error"
        print(f"ERROR: Received broker error: {error_msg}")

    def on_disconnected(self):
        """Improved reconnection logic with exponential backoff."""
        if self._shutdown_event.is_set():
            return

        print("WARNING: Disconnected from broker. Attempting to reconnect...")

        while (
            not self._shutdown_event.is_set()
            and self._reconnect_count < self.MAX_RECONNECT_ATTEMPTS
        ):
            try:
                self._reconnect_count += 1
                # Exponential backoff: 2, 4, 8, 16, 32 seconds
                delay = min(
                    self.RECONNECT_DELAY * (2 ** (self._reconnect_count - 1)), 60
                )
                print(
                    f"Reconnect attempt {self._reconnect_count}/{self.MAX_RECONNECT_ATTEMPTS} in {delay}s"
                )

                if not self._shutdown_event.wait(delay):  # Interruptible sleep
                    self._connect_and_subscribe()
                    if self._conn and self._conn.is_connected():
                        print("SUCCESS: Reconnected to broker")
                        return

            except (stomp.exception.ConnectFailedException, ConnectionError) as e:
                print(f"Reconnect attempt {self._reconnect_count} failed: {e}")
            except (OSError, IOError) as e:
                print(f"Network error during reconnect: {e}")

        if self._reconnect_count >= self.MAX_RECONNECT_ATTEMPTS:
            print(
                f"ERROR: Failed to reconnect after {self.MAX_RECONNECT_ATTEMPTS} attempts"
            )
            self._shutdown_event.set()

    def initialOffset(self) -> dict:
        return {"offset": 0}

    def latestOffset(self) -> dict:
        with self._lock:
            return {"offset": self._current_offset}

    def partitions(self, start: dict, end: dict):
        start_offset: int = start.get("offset", 0)
        end_offset: int = end.get("offset", self._current_offset)

        if end_offset <= start_offset:
            print("INFO: No new messages, returning empty partition.")
            return [ActiveMQPartition(start_offset, start_offset, [], self._lock)]

        # Efficiently collect messages within the offset range
        with self._lock:
            partition_data: list[tuple[int, str, str, str, str | None]] = []

            # Convert deque to list for efficient access if needed
            messages_list = list(self._messages)

            for message_tuple in messages_list:
                offset = message_tuple[0]
                if start_offset <= offset < end_offset:
                    partition_data.append(message_tuple)

        print(
            f"INFO: Created partition with {len(partition_data)} messages (offsets {start_offset} to {end_offset})"
        )
        return [ActiveMQPartition(start_offset, end_offset, partition_data, self._lock)]

    def commit(self, end: dict):
        """Optimized commit with better memory management."""
        commit_offset: int = end.get("offset", 0)

        with self._lock:
            # Remove committed messages from deque
            while self._messages and self._messages[0][0] < commit_offset:
                removed_message = self._messages.popleft()
                removed_offset = removed_message[0]
                self._offset_to_index.pop(removed_offset, None)

            # Update remaining indices in the mapping
            self._offset_to_index.clear()
            for idx, message_tuple in enumerate(self._messages):
                self._offset_to_index[message_tuple[0]] = idx

        # Suggest garbage collection after commit to manage memory
        if commit_offset % 1000 == 0:  # Every 1000 commits
            import gc

            gc.collect()

    def read(self, partition: InputPartition) -> Iterator[tuple]:
        return partition.read()

    def schema(self) -> StructType:
        return self._schema

    def stop(self):
        """Graceful shutdown with proper cleanup."""
        print("INFO: Stopping ActiveMQ stream reader...")
        self._shutdown_event.set()

        if self._conn:
            try:
                # Unsubscribe from all queues
                for idx, _ in enumerate(self._broker_queues, start=1):
                    try:
                        self._conn.unsubscribe(id=str(idx))
                    except (stomp.exception.StompException, ConnectionError):
                        pass  # Best effort cleanup

                self._conn.disconnect()
                print("INFO: Disconnected from ActiveMQ broker")
            except (stomp.exception.StompException, ConnectionError, OSError) as e:
                print(f"WARNING: Error during cleanup: {e}")

        # Clear message buffer
        with self._lock:
            self._messages.clear()
            self._offset_to_index.clear()


class ActiveMQDataSource(DataSource):
    @classmethod
    def name(cls):
        return "activemq"

    def schema(self):
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
        raise NotImplementedError(
            "ERROR: ----------------Batch queries are not supported for ActiveMQDataSource.----------------"
        )

    def streamReader(self, schema: StructType):
        return ActiveMQStreamReader(schema, self.options)
