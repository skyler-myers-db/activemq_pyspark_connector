from __future__ import annotations

"""ActiveMQ Structured Streaming source for Apache Spark on Databricks.

This module implements a **DataSource V2** reader that consumes messages from
ActiveMQ via the STOMP 1.2 protocol and exposes them to Spark as a micro‑batch
stream. It is designed for **enterprise** usage: typed, well‑documented, fully
observable, and robust to broker/network failures.

Usage example
-------------
```python
(spark.readStream
     .format("activemq")
     .option("hosts", "[('mq1.acme.com', 61613), ('mq2.acme.com', 61613)]")
     .option("queues", "['/queue/orders', '/queue/payments']")
     .option("username", "svc_reader")
     .option("password", dbutils.secrets.get(scope="mq", key="svc_reader_pwd"))
     .load()
     .writeStream
     .trigger(processingTime="5 seconds")
     .option("checkpointLocation", "/mnt/checkpoints/orders")
     .toTable("raw.orders"))
```

Options
~~~~~~~
``hosts``               – Python literal list of ``(host, port)`` tuples.  
``queues``              – Python literal list of destinations (queues/topics).  
``username``/``password`` – Broker credentials.  
``ack``                 – STOMP ack mode (default ``client-individual``).  
``heartbeat_out_ms``/``heartbeat_in_ms`` – Custom heart‑beat intervals.

All other parameters are passed through to :pyclass:`stomp.Connection12`.
"""

###############################################################################
# Standard library
###############################################################################

import json
import threading
from ast import literal_eval
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    Deque,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
)

###############################################################################
# Third‑party
###############################################################################

import stomp  # type: ignore
from loguru import logger
from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

__all__ = [
    "ActiveMQDataSource",
]

###############################################################################
# Data model
###############################################################################


@dataclass(frozen=True, slots=True)
class ActiveMQRecord:
    """Canonical representation of a single STOMP frame."""

    offset: int
    destination: str
    headers: Mapping[str, str]
    body: str
    error: Optional[str]
    received_ts: datetime

    # Convenience converter – used when yielding to Spark executors.
    def as_row(self) -> Tuple[int, str, str, str, Optional[str]]:
        return (
            self.offset,
            self.destination,
            json.dumps(dict(self.headers)),
            self.body,
            self.error,
        )


###############################################################################
# Partition – lives on the executors
###############################################################################


class ActiveMQPartition(InputPartition):
    """Represents a slice of offsets for one micro‑batch."""

    def __init__(
        self,
        start_offset: int,
        end_offset: int,
        shared_buffer: Deque[ActiveMQRecord],
        lock: threading.Lock,
    ) -> None:
        self._start_offset: int = start_offset
        self._end_offset: int = end_offset
        self._buf: Deque[ActiveMQRecord] = shared_buffer
        self._lock: threading.Lock = lock

    # ------------------------------------------------------------------ #

    def read(self) -> Iterator[Tuple[int, str, str, str, Optional[str]]]:
        """Iterates over records in the offset range [start, end)."""
        with self._lock:
            for rec in list(self._buf):
                if self._start_offset <= rec.offset < self._end_offset:
                    yield rec.as_row()

    # ------------------------------------------------------------------ #
    # Pickling helpers – Spark serialises the partition to executors.
    # ------------------------------------------------------------------ #

    def __getstate__(self):  # noqa: D401
        state: Dict[str, object] = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: Dict[str, object]) -> None:  # noqa: D401
        self.__dict__.update(state)
        self._lock = threading.Lock()


###############################################################################
# Stream reader – lives on the driver only
###############################################################################


class ActiveMQStreamReader(DataSourceStreamReader, stomp.ConnectionListener):
    """Implements Spark’s micro‑batch callbacks + STOMP listener hooks."""

    #######################################
    # Construction
    #######################################

    def __init__(self, schema: StructType, options: Dict[str, str]):
        super().__init__()
        stomp.ConnectionListener.__init__(self)

        self._schema: StructType = schema
        self._options: Dict[str, str] = options

        # Parse connection options – fallbacks intentionally match ActiveMQ 5.
        hosts: List[Tuple[str, int]] = literal_eval(
            self._options.get("hosts", "[('localhost', 61613)]")
        )
        self._queues: List[str] = literal_eval(
            self._options.get("queues", "['/queue/default']")
        )

        self._username: str = self._options.get("username", "admin")
        self._password: str = self._options.get("password", "password")
        self._ack_mode: str = self._options.get("ack", "client-individual")

        heartbeat_out: int = int(self._options.get("heartbeat_out_ms", "4000"))
        heartbeat_in: int = int(self._options.get("heartbeat_in_ms", "4000"))

        # Runtime state – guarded by _lock.
        self._lock: threading.Lock = threading.Lock()
        self._records: Deque[ActiveMQRecord] = deque()
        self._pending_acks: Dict[int, Tuple[str, str]] = {}
        self._offset: int = 0

        # Establish connection.
        self._conn: stomp.Connection12 = stomp.Connection12(
            hosts,
            heartbeats=(heartbeat_out, heartbeat_in),
        )
        self._conn.set_listener("spark-activemq", self)
        self._connect_and_subscribe()

        logger.info(
            "ActiveMQStreamReader started – hosts={} queues={} ack={}", hosts, self._queues, self._ack_mode
        )

    #######################################
    # Internal helpers
    #######################################

    def _connect_and_subscribe(self) -> None:
        """Connect and subscribe to all requested destinations."""
        self._conn.connect(self._username, self._password, wait=True)
        for idx, dest in enumerate(self._queues, start=1):
            self._conn.subscribe(destination=dest, id=str(idx), ack=self._ack_mode)
            logger.debug("Subscribed to {} (id={})", dest, idx)

    #######################################
    # STOMP callbacks
    #######################################

    def on_connected(self, frame: stomp.utils.Frame):  # noqa: D401
        logger.info("Connected – broker={}", frame.headers.get("server", "unknown"))

    def on_message(self, frame: stomp.utils.Frame):  # noqa: D401
        dest: str = frame.headers.get("destination", "unknown")
        try:
            rec = ActiveMQRecord(
                offset=self._offset,
                destination=dest,
                headers=frame.headers,
                body=frame.body,
                error=None,
                received_ts=datetime.now(timezone.utc),
            )
            with self._lock:
                self._records.append(rec)
                self._pending_acks[self._offset] = (
                    frame.headers.get("message-id"),
                    frame.headers.get("subscription"),
                )
                self._offset += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error handling message from {}", dest)
            bad = ActiveMQRecord(
                offset=self._offset,
                destination=dest,
                headers=frame.headers,
                body=frame.body,
                error=str(exc),
                received_ts=datetime.now(timezone.utc),
            )
            with self._lock:
                self._records.append(bad)
                self._offset += 1

    def on_error(self, frame: stomp.utils.Frame):  # noqa: D401
        logger.error("Broker‑level error: {}", frame.body)

    def on_disconnected(self):  # noqa: D401
        logger.warning("Disconnected – attempting reconnection …")
        try:
            self._connect_and_subscribe()
        except Exception:  # noqa: BLE001
            logger.critical("Automatic reconnection failed – stream will stop")
            raise

    #######################################
    # Spark DataSourceStreamReader API
    #######################################

    def initialOffset(self) -> Dict[str, int]:  # type: ignore[override]
        return {"offset": 0}

    def latestOffset(self) -> Dict[str, int]:  # type: ignore[override]
        with self._lock:
            return {"offset": self._offset}

    def partitions(self, start: Dict[str, int], end: Dict[str, int]):  # type: ignore[override]
        start_off: int = start.get("offset", 0)
        end_off: int = end.get("offset", self._offset)
        logger.debug("Plan partition – {} -> {} ({} buffered)", start_off, end_off, len(self._records))

        if end_off <= start_off:
            return [ActiveMQPartition(start_off, start_off, deque(), self._lock)]

        return [ActiveMQPartition(start_off, end_off, self._records, self._lock)]

    def commit(self, end: Dict[str, int]):  # type: ignore[override]
        commit_off: int = end.get("offset", 0)
        with self._lock:
            while self._records and self._records[0].offset < commit_off:
                old = self._records.popleft()
                msg_id, sub = self._pending_acks.pop(old.offset, (None, None))
                if msg_id and sub:
                    self._conn.ack(id=msg_id, subscription=sub)
        logger.debug("Committed up to offset {} ({} pending)", commit_off, len(self._pending_acks))

    def read(
        self, partition: InputPartition
    ) -> Iterator[Tuple[int, str, str, str, Optional[str]]]:  # type: ignore[override]
        return partition.read()

    def schema(self) -> StructType:  # type: ignore[override]
        return self._schema

    def stop(self):  # type: ignore[override]
        logger.info("Stopping – disconnecting from broker …")
        try:
            self._conn.disconnect()
        finally:
            logger.info("Disconnected from ActiveMQ")


###############################################################################
# DataSource entry‑point
###############################################################################


class ActiveMQDataSource(DataSource):
    """Spark entry‑point – refer to this format as ``activemq``."""

    ######################################################################
    # Required class methods
    ######################################################################

    @classmethod
    def name(cls) -> str:  # noqa: D401
        return "activemq"

    def schema(self) -> StructType:  # noqa: D401
        return StructType(
            [
                StructField("offset", IntegerType(), nullable=False),
                StructField("destination", StringType(), nullable=False),
                StructField("headers", StringType(), nullable=True),
                StructField("body", StringType(), nullable=True),
                StructField("error", StringType(), nullable=True),
            ]
        )

    ######################################################################
    # Batch interfaces are intentionally unsupported.
    ######################################################################

    def reader(self, schema: StructType):  # noqa: D401
        raise NotImplementedError("Batch reads are not supported for ActiveMQ")

    def streamReader(self, schema: StructType):  # noqa: D401
        return ActiveMQStreamReader(schema, self.options)
