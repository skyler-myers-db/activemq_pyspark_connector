import stomp
from time import sleep
from datetime import datetime
from pytz import timezone
from typing import Iterator, Final
from ast import literal_eval
from collections import deque
from threading import Lock

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    VariantType,
)


class ActiveMQPartition(InputPartition):
    def __init__(self, start_offset, end_offset, messages, lock):
        self.start_offset = start_offset
        self.end_offset = end_offset
        self._messages = messages  # list of tuples: (offset, queue, mesage, ts, error)
        self._lock = lock

    def read(self):
        for message in self._messages:
            yield message  # (offset, queue, message, ts, error)

    def __getstate__(self):
        state = self.__dict__.copy()
        if "_lock" in state:
            del state["_lock"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = Lock()


class ActiveMQStreamReader(DataSourceStreamReader, stomp.ConnectionListener):
    def __init__(self, schema: StructType, options: dict[str, str]):
        stomp.ConnectionListener.__init__(self)
        self._schema: StructType = schema
        self._options: dict[str, str] = options
        self._conn: stomp.Connection12 | None = None
        self._heartbeats: int = int(self._options.get("heartbeats", 5_000))

        self._broker_hosts_and_ports: str = literal_eval(
            self._options.get("hosts_and_ports", "[('localhost', 61613)]")
        )
        self._broker_queues: list[str] = literal_eval(self._options["queues"])

        self._username: str = self._options.get("username", "admin")
        self._password: str = self._options.get("password", "password")

        self._current_offset: int = 0
        self._messages: deque = deque()  # will hold tuples: (offset, queue, message, recieved_ts, error)
        self._lock: Lock = Lock()
        self._connect_and_subscribe()

    def _connect_and_subscribe(self):
        """Connects to the broker and subscribes to queues."""
        if not self._conn:
            self._conn = stomp.Connection12(
                host_and_ports=self._broker_hosts_and_ports,
                heartbeats=(self._heartbeats, self._heartbeats),
            )
            self._conn.set_listener("ActiveMQReaderListener", self)

        print("INFO: Attempting to connect to broker...")
        self._conn.connect(self._username, self._password, wait=True)
        for idx, queue in enumerate(self._broker_queues, start=1):
            print(f"INFO: Attempting to subscribe to queue: '{queue}' with ID: '{idx}'")
            self._conn.subscribe(
                destination=queue,
                id=str(idx),
                ack="auto",
            )

    def __getstate__(self):
        state: dict = self.__dict__.copy()
        if "_lock" in state:
            del state["_lock"]
        if "_conn" in state:
            del state["_conn"]
        return state

    def __setstate__(self, state):
        """Ensure connection is re-established after deserialization on the driver."""
        self.__dict__.update(state)
        self._lock: Lock = Lock()
        self._conn: stomp.Connection12 | None = None
        self._connect_and_subscribe()

    def on_connected(self, frame: stomp.utils.Frame):
        print(
            f"SUCCESS: on_connected: ----------------Connected to broker: '{frame}'----------------\n"
        )

    def on_message(self, frame: stomp.utils.Frame):
        try:
            with self._lock:
                self._messages.append(
                    (self._current_offset, frame.cmd, frame.headers, frame.body, None)
                )
        except Exception as e:
            # If an error occurs during message processing, capture error message.
            with self._lock:
                self._messages.append(
                    (self._current_offset, frame.cmd, frame.headers, frame.body, str(e))
                )
        self._current_offset += 1

    def on_error(self, frame: stomp.utils.Frame):
        print(
            f"ERROR: on_error: ----------------Recieved an error: '{frame}'----------------"
        )

    def on_disconnected(self):
        """Called by stomp.py when the connection is lost."""
        print(
            "ERROR: on_disconnected: ----------------Disconnected from broker. Attempting to reconnect...----------------"
        )
        while not self._conn.is_connected():
            try:
                sleep(5)
                self._connect_and_subscribe()
            except Exception as e:
                print(
                    f"ERROR: on_disconnected: ----------------Reconnect failed: {e}----------------"
                )

    def initialOffset(self) -> dict:
        return {"offset": 0}

    def latestOffset(self) -> dict:
        with self._lock:
            if not self._messages:
                return {"offset": 0}
            return {"offset": self._current_offset}

    def partitions(self, start: dict, end: dict):
        est = timezone("America/New_York")
        current_est: datetime = datetime.now(est)
        start_offset: int = start.get("offset", 0)
        end_offset: int = end.get("offset", self._current_offset)
        # print(
        #     f"{current_est}: Creating partition: start_offset={start_offset}, end_offset={end_offset}"
        # )

        if end_offset < start_offset:
            print(
                "WARNING: ----------------No new messages, returning dummy partition.----------------"
            )
            return [ActiveMQPartition(start_offset, start_offset + 1, [], self._lock)]

        # Filter the messages on the driver BEFORE creating the partition object.
        with self._lock:
            partition_data: list = [
                record
                for record in self._messages
                if start_offset <= record[0] < end_offset
            ]

        # Now pass the much smaller, self-contained list to the partition.
        return [ActiveMQPartition(start_offset, end_offset, partition_data, self._lock)]

    def commit(self, end: dict):
        commit_offset: int = end.get("offset", 0)
        with self._lock:
            while self._messages and self._messages[0][0] < commit_offset:
                self._messages.popleft()

    def read(self, partition: InputPartition) -> Iterator[tuple]:
        return partition.read()

    def schema(self) -> StructType:
        return self._schema

    def stop(self):
        if self._conn:
            self._conn.disconnect()


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
