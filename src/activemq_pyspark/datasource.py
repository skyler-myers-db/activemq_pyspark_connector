from __future__ import annotations

"""ActiveMQ Structured Streaming source for Apache Spark on Databricks.

This reader consumes messages from ActiveMQ (STOMP 1.2) and surfaces them to
Spark as a micro‑batch stream with *at‑least‑once* guarantees. All variables
(including locals) are type‑hinted; logging uses the standard library to remain
picklable across Spark driver ↔︎ executor boundaries.
"""

###############################################################################
# Standard library
###############################################################################

import json
import logging
import threading
from ast import literal_eval
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Deque, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

###############################################################################
# Third‑party
###############################################################################

import stomp  # type: ignore
from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

###############################################################################
# Logging – stdlib only (picklable)
###############################################################################

logger: logging.Logger = logging.getLogger(__name__)
if not logger.handlers:  # Databricks initializes loggers late; be defensive.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
logger.debug("Module imported – logging configured")

###############################################################################
# Data model
###############################################################################


@dataclass(frozen=True, slots=True)
class ActiveMQRecord:
    """In‑memory representation of one STOMP frame."""

    offset: int
    frame_cmd: str
    headers: Mapping[str, str]
    body: str
    error: Optional[str]
    received_ts: datetime

    def to_row(self) -> Tuple[int, str, str, str, Optional[str]]:
        """Return the 5‑tuple expected by the downstream Spark schema."""
        return (
            self.offset,
            self.frame_cmd,
            json.dumps(dict(self.headers)),
            self.body,
            self.error,
        )


###############################################################################
# Partition (executors)
###############################################################################


class ActiveMQPartition(InputPartition):
    """Spark *InputPartition* backed by a shared in‑process deque."""

    def __init__(
        self,
        start_offset: int,
        end_offset: int,
        shared_buffer: Deque[ActiveMQRecord],
        lock: threading.Lock,
    ) -> None:
        self.start_offset: int = start_offset
        self.end_offset: int = end_offset
        self._buffer: Deque[ActiveMQRecord] = shared_buffer
        self._lock: threading.Lock = lock

    # ------------------------------------------------------------------ #
    # Spark executor callback
    # ------------------------------------------------------------------ #

    def read(self) -> Iterator[Tuple[int, str, str, str, Optional[str]]]:
        with self._lock:
            for rec in list(self._buffer):
                if self.start_offset <= rec.offset < self.end_offset:
                    yield rec.to_row()

    # ------------------------------------------------------------------ #
    # Pickling helpers – avoid serializing locks across processes
    # ------------------------------------------------------------------ #

    def __getstate__(self):  # noqa: D401
        state: Dict[str, object] = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: Dict[str, object]) -> None:  # noqa: D401
        self.__dict__.update(state)
        self._lock = threading.Lock()


###############################################################################
# Stream reader (driver)
###############################################################################


class ActiveMQStreamReader(DataSourceStreamReader, stomp.ConnectionListener):
    """Structured Streaming reader with *client‑individual* ACK semantics."""

    def __init__(self, schema: StructType, options: Dict[str, str]):
        super().__init__()
        stomp.ConnectionListener.__init__(self)

        # ------------------------------------------------------------------ #
        # Parse options – always type‑annotate locals
        # ------------------------------------------------------------------ #
        self._schema: StructType = schema
        self._options: Dict[str, str] = options

        hosts: List[Tuple[str, int]] = literal_eval(
            self._options.get("hosts", "[('localhost', 61613)]")
        )
        queues: List[str] = literal_eval(
            self._options.get("queues", "['/queue/default']")
        )

        self._username: str = self._options.get("username", "admin")
        self._password: str = self._options.get("password", "password")
        self._ack_mode: str = self._options.get("ack", "client-individual")

        hb_out: int = int(self._options.get("heartbeat_out_ms", "4000"))
        hb_in: int = int(self._options.get("heartbeat_in_ms", "4000"))

        # Shared state guarded by _lock
        self._lock: threading.Lock = threading.Lock()
        self._buffer: Deque[ActiveMQRecord] = deque()
        self._pending_acks: Dict[int, Tuple[str, str]] = {}
        self._offset: int = 0

        # Connection
        self._conn: stomp.Connection12 = stomp.Connection12(
            hosts, heartbeats=(hb_out, hb_in)
        )
        self._conn.set_listener("spark‑activemq", self)

        self._connect_and_subscribe(queues)
        logger.info("ActiveMQStreamReader initialized – hosts=%s queues=%s", hosts, queues)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _connect_and_subscribe(self, queues: List[str]) -> None:
        self._conn.connect(self._username, self._password, wait=True)
        for idx, q in enumerate(queues, start=1):
            self._conn.subscribe(destination=q, id=str(idx), ack=self._ack_mode)
            logger.debug("Subscribed to %s (id=%s, ack=%s)", q, idx, self._ack_mode)

    # ------------------------------------------------------------------ #
    # STOMP callbacks
    # ------------------------------------------------------------------ #

    def on_connected(self, frame: stomp.utils.Frame):  # noqa: D401
        logger.info("Connected – broker=%s", frame.headers.get("server", "unknown"))

    def on_message(self, frame: stomp.utils.Frame):  # noqa: D401
        frame_cmd: str = frame.cmd
        try:
            rec: ActiveMQRecord = ActiveMQRecord(
                offset=self._offset,
                frame_cmd=frame_cmd,
                headers=frame.headers,
                body=frame.body,
                error=None,
                received_ts=datetime.now(timezone.utc),
            )
            with self._lock:
                self._buffer.append(rec)
                self._pending_acks[self._offset] = (
                    frame.headers.get("message-id"),
                    frame.headers.get("subscription"),
                )
                self._offset += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to process frame – cmd=%s", frame_cmd)
            bad: ActiveMQRecord = ActiveMQRecord(
                offset=self._offset,
                frame_cmd=frame_cmd,
                headers=frame.headers,
                body=frame.body,
                error=str(exc),
                received_ts=datetime.now(timezone.utc),
            )
            with self._lock:
                self._buffer.append(bad)
                self._offset += 1

    def on_error(self, frame: stomp.utils.Frame):  # noqa: D401
        logger.error("Broker error – %s", frame.body)

    def on_disconnected(self):  # noqa: D401
        logger.warning("Disconnected – attempting automatic reconnect …")
        try:
            self._connect_and_subscribe(
                literal_eval(self._options.get("queues", "['/queue/default']"))
            )
        except Exception:  # noqa: BLE001
            logger.critical("Reconnection failed – stream will terminate")
            raise

    # ------------------------------------------------------------------ #
    # Spark DataSourceStreamReader interface
    # ------------------------------------------------------------------ #

    def initialOffset(self) -> Dict[str, int]:  # type: ignore[override]
        return {"offset": 0}

    def latestOffset(self) -> Dict[str, int]:  # type: ignore[override]
        with self._lock:
            return {"offset": self._offset}

    def partitions(self, start: Dict[str, int], end: Dict[str, int]):  # type: ignore[override]
        start_off: int = start.get("offset", 0)
        end_off: int = end.get("offset", self._offset)
        logger.debug("Planning partition – %s → %s", start_off, end_off)

        if end_off <= start_off:
            return [ActiveMQPartition(start_off, start_off, deque(), self._lock)]

        return [ActiveMQPartition(start_off, end_off, self._buffer, self._lock)]

    def commit(self, end: Dict[str, int]):  # type: ignore[override]
        commit_off: int = end.get("offset", 0)
        with self._lock:
            while self._buffer and self._buffer[0].offset < commit_off:
                old: ActiveMQRecord = self._buffer.popleft()
                msg_id, subscription = self._pending_acks.pop(old.offset, (None, None))
                if msg_id and subscription:
                    self._conn.ack(id=msg_id, subscription=subscription)
        logger.debug("Committed up to offset %s", commit_off)

    def read(
        self, partition: InputPartition
    ) -> Iterator[Tuple[int, str, str, str, Optional[str]]]:  # type: ignore[override]
        return partition.read()

    def schema(self) -> StructType:  # type: ignore[override]
        return self._schema

    def stop(self):  # type: ignore[override]
        logger.info("Stopping reader – disconnecting from broker…")
        try:
            self._conn.disconnect()
        finally:
            logger.info("Disconnected")


###############################################################################
# DataSource entry‑point
###############################################################################


class ActiveMQDataSource(DataSource):
    """Register this source as `format('activemq')`."""

    @classmethod
    def name(cls) -> str:  # noqa: D401
        return "activemq"

    def schema(self) -> StructType:  # noqa: D401
        """Fixed schema matching the original definition provided by the user."""
        return StructType(
            [
                StructField("offset", IntegerType(), False),
                StructField("frameCmd", StringType(), True),
                StructField("frameHeaders", StringType(), True),
                StructField("frameBody", StringType(), True),
                StructField("messageError", StringType(), True),
            ]
        )

    def reader(self, schema: StructType):  # noqa: D401
        raise NotImplementedError("Batch reads are not supported for ActiveMQ")

    def streamReader(self, schema: StructType):  # noqa: D401
        return ActiveMQStreamReader(schema, self.options)
