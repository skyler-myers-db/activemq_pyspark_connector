import threading
from datetime import datetime
import stomp
from typing import Iterator, Tuple, List, Dict, Final

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    VariantType,
)
from ast import literal_eval


class ActiveMQPartition(InputPartition):
    def __init__(self, start_offset, end_offset, messages, lock):
        self.start_offset = start_offset
        self.end_offset = end_offset
        self._messages = messages  # list of tuples: (offset, queue, mesage, ts, error)
        self._lock = lock

    def read(self):
        with self._lock:
            partition_data = [
                record
                for record in self._messages
                if self.start_offset <= record[0] < self.end_offset
            ]
        for message in partition_data:
            yield message  # (offset, queue, message, ts, error)

    def __getstate__(self):
        state = self.__dict__.copy()
        if "_lock" in state:
            del state["_lock"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = threading.Lock()


# Custom DataSourceStreamReader
class ActiveMQStreamReader(DataSourceStreamReader, stomp.ConnectionListener):
    def __init__(self, schema: StructType, options: Dict[str, str]):
        stomp.ConnectionListener.__init__(self)
        self._schema: str = schema
        self._options: Dict[str, str] = options
        self._message: None = None  # satisfy Spark's expectations

        # Get connection parameters.
        self._broker_hosts_and_ports: str = literal_eval(
            self._options.get("hps", [("localhost", 61613)])
        )
        self._broker_queues: List[str] = literal_eval(self._options["queues"])

        self._username: str = self._options.get("username", "admin")
        self._password: str = self._options.get("password", "password")

        self._current_offset: int = 0
        self._messages: List = (
            []
        )  # will hold tuples: (offset, queue, message, recieved_ts, error)
        self._lock = threading.Lock()
        self._conn = stomp.Connection12(
            host_and_ports=self._broker_hosts_and_ports, heartbeats=(4_000, 4_000)
        )
        self._conn.set_listener("ActiveMQReaderListener", self)
        self._conn.connect(self._username, self._password, wait=True)
        for idx, queue in enumerate(self._broker_queues, start=1):
            print(f"Attempting to connect to queue: '{queue}' with ID: '{id}'")
            self._conn.subscribe(
                destination=queue,
                id=str(idx),
                ack="auto",
            )

    def __getstate__(self):
        state = self.__dict__.copy()
        if "_lock" in state:
            del state["_lock"]
        if "_conn" in state:
            del state["_conn"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = threading.Lock()
        self._conn = None

    def on_connected(self, frame: stomp.utils.Frame):
        print("Connected to broker")

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
        print(f"Recieved an error: {frame.body}")

    def on_disconnected(self):
        print("Disconnected")

    def initialOffset(self) -> dict:
        return {"offset": 0}

    def latestOffset(self) -> dict:
        with self._lock:
            if not self._messages:
                return {"offset": 0}
            return {"offset": self._current_offset}

    def partitions(self, start: dict, end: dict):
        start_offset: int = start.get("offset", 0)
        end_offset: int = end.get("offset", self._current_offset)
        print(
            f"Creating partition: start_offset={start_offset}, end_offset={end_offset}"
        )

        if end_offset < start_offset:
            print("No new messages, returning dummy partition.")
            return [ActiveMQPartition(start_offset, start_offset + 1, [], self._lock)]

        return [ActiveMQPartition(start_offset, end_offset, self._messages, self._lock)]

    def commit(self, end: dict):
        commit_offset: int = end.get("offset", 0)
        with self._lock:
            self._messages = [
                (off, cmd, headers, body, err)
                for (off, cmd, headers, body, err) in self._messages
                if off >= commit_offset
            ]

    def read(self, partition: InputPartition) -> Iterator[Tuple]:
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
            "Batch queries are not supported for ActiveMQDataSource"
        )

    def streamReader(self, schema: StructType):
        return ActiveMQStreamReader(schema, self.options)
