import ast
import collections
import threading
from dataclasses import dataclass
from typing import Any, Deque, Dict, Iterator, List, Optional, Tuple, Type
from loguru import logger

import stomp
from stomp.exception import ConnectFailedException
from stomp.utils import Frame

from pyspark.sql.datasource import DataSource, InputPartition, DataSourceStreamReader
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)


@dataclass
class StompMessage:
    """Represents the data from a single STOMP frame, matching the desired schema."""

    offset: int
    cmd: Optional[str]
    headers: str  # Headers are stored as a string representation of the dict
    body: Optional[str]
    error: Optional[str] = None

    def to_row(self) -> Tuple[int, Optional[str], str, Optional[str], Optional[str]]:
        """Converts the dataclass to a tuple for Spark Row creation."""
        return (self.offset, self.cmd, self.headers, self.body, self.error)


class ActiveMQPartitionReader(InputPartition):
    """A reader for a specific partition of ActiveMQ messages."""

    def __init__(self, messages: List[StompMessage]):
        """Initializes the partition reader with a list of messages."""
        self._messages: List[StompMessage] = messages

    def read(
        self,
    ) -> Iterator[Tuple[int, Optional[str], str, Optional[str], Optional[str]]]:
        """Yields each message in the partition as a Spark-compatible tuple."""
        logger.info(f"Reading partition with {len(self._messages)} messages.")
        for msg in self._messages:
            yield msg.to_row()


class ActiveMQStreamReader(DataSourceStreamReader, stomp.ConnectionListener):
    """A stream reader for ActiveMQ that runs on the Spark driver."""

    def __init__(self, schema: StructType, options: Dict[str, str]):
        """Initializes the stream reader, parses options, and connects to the broker."""
        super().__init__()
        self._schema: StructType = schema
        self._options: Dict[str, str] = options
        self._messages: Deque[StompMessage] = collections.deque()
        self._lock: threading.Lock = threading.Lock()
        self._conn: Optional[stomp.Connection] = None
        self._current_offset: int = 0
        self._hosts_and_ports: List[Tuple[str, int]]
        self._queues: List[str]
        self._username: Optional[str]
        self._password: Optional[str]
        self._parse_options()
        self._connect_and_subscribe()

    def _parse_options(self) -> None:
        """Parses and validates user-provided options using ast.literal_eval."""
        try:
            hps_str: str = self._options.get(
                "hosts_and_ports", "[('localhost', 61616)]"
            )
            self._hosts_and_ports = ast.literal_eval(hps_str)
            queues_str: str = self._options["queues"]
            self._queues: List[str] = ast.literal_eval(queues_str)
            self._username: Optional[str] = self._options.get("username")
            self._password: Optional[str] = self._options.get("password")
        except KeyError as e:
            logger.error(f"FATAL: Missing required option: {e}")
            raise ValueError(f"Missing required option: {e}") from e
        except (ValueError, SyntaxError) as e:
            logger.error(f"FATAL: Invalid format for options: {e}")
            raise ValueError(
                "Options must be valid string representations of Python literals."
            ) from e

    def _connect_and_subscribe(self) -> None:
        """Establishes a connection to the broker and subscribes to queues."""
        logger.info(f"Initializing STOMP connection to hosts: {self._hosts_and_ports}")
        self._conn: stomp.Connection = stomp.Connection(
            host_and_ports=self._hosts_and_ports, heartbeats=(10_000, 10_000)
        )
        self._conn.set_listener("ActiveMQReaderListener", self)
        try:
            self._conn.connect(self._username, self._password, wait=True)
        except ConnectFailedException as e:
            logger.error(
                f"FATAL: Failed to connect to ActiveMQ broker: {e}", exc_info=True
            )
            raise

    def on_connected(self, frame: Frame) -> None:
        """Called by stomp.py when a connection is established."""
        logger.info(f"Successfully connected to broker. Frame: {frame.cmd}")
        if self._conn and self._conn.is_connected():
            for idx, queue in enumerate(self._queues, start=1):
                sub_id: str = str(idx)
                self._conn.subscribe(
                    destination=queue, id=sub_id, ack="client-individual"
                )

    def on_message(self, frame: Frame) -> None:
        """Called by stomp.py when a message is received."""
        try:
            with self._lock:
                body_content: Optional[str] = (
                    frame.body if frame.body is not None else None
                )
                msg: StompMessage = StompMessage(
                    offset=self._current_offset,
                    cmd=frame.cmd,
                    headers=str(frame.headers),
                    body=body_content,
                )
                self._messages.append(msg)
        except Exception as e:
            logger.error(
                f"Error processing received message: {frame.headers}", exc_info=True
            )
            with self._lock:
                error_msg: StompMessage = StompMessage(
                    offset=self._current_offset,
                    cmd=frame.cmd,
                    headers=str(frame.headers),
                    body=frame.body,
                    error=str(e),
                )
                self._messages.append(error_msg)
        finally:
            with self._lock:
                self._current_offset += 1

    def on_error(self, frame: Frame) -> None:
        """Called by stomp.py on a protocol error."""
        logger.error(f"Received a STOMP protocol error: {frame.body}")

    def on_disconnected(self) -> None:
        """Called by stomp.py when the connection is lost."""
        logger.warning("Disconnected from broker.")

    def initialOffset(self) -> Dict[str, Any]:
        """Returns the initial offset for the stream."""
        return {"offset": 0}

    def latestOffset(self) -> Dict[str, Any]:
        """Returns the latest available offset."""
        return {"offset": self._current_offset}

    def partitions(
        self, start: Dict[str, Any], end: Dict[str, Any]
    ) -> List[InputPartition]:
        """Creates partitions for a micro-batch."""
        start_offset: int = start.get("offset", 0)
        end_offset: int = end.get("offset", 0)
        with self._lock:
            partition_messages: List[StompMessage] = [
                msg for msg in self._messages if start_offset <= msg.offset < end_offset
            ]
        return [ActiveMQPartitionReader(partition_messages)]

    def commit(self, end: Dict[str, Any]) -> None:
        """Commits the processed batch."""
        commit_offset: int = end.get("offset", 0)
        # Acknowledge messages on the broker
        pass

    def read(self, partition: InputPartition) -> Iterator[Any]:
        """Delegates reading to the partition reader instance."""
        assert isinstance(partition, ActiveMQPartitionReader)
        return partition.read()

    def stop(self) -> None:
        """Stops the reader and disconnects from the broker."""
        if self._conn and self._conn.is_connected():
            self._conn.disconnect()

    def __getstate__(self) -> Dict[str, Any]:
        """Prepares the object for serialization."""
        state: Dict[str, Any] = self.__dict__.copy()
        if "_lock" in state:
            del state["_lock"]
        if "_conn" in state:
            del state["_conn"]
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        """Restores the object after serialization."""
        self.__dict__.update(state)
        self._lock: threading.Lock = threading.Lock()
        self._conn: Optional[stomp.Connection] = None


class ActiveMQDataSource(DataSource):
    """A PySpark DataSource for ActiveMQ."""

    @classmethod
    def name(cls: Type["ActiveMQDataSource"]) -> str:
        """The name of the data source."""
        return "activemq"

    def schema(self) -> StructType:
        """Defines the schema of the data, restoring your original fields."""
        return StructType(
            [
                StructField("offset", IntegerType(), nullable=False),
                StructField("frameCmd", StringType(), nullable=True),
                StructField("frameHeaders", StringType(), nullable=True),
                StructField("frameBody", StringType(), nullable=True),
                StructField("messageError", StringType(), nullable=True),
            ]
        )

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        """Creates the stream reader instance for Spark."""
        return ActiveMQStreamReader(schema, self.options)
