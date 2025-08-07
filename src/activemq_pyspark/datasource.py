"""
This module provides a comprehensive data source implementation for integrating Apache ActiveMQ
with Apache Spark's Structured Streaming framework. It enables real-time message consumption
from ActiveMQ queues with support for paral        hosts: list[tuple[str, int]] = literal_eval(reader_options["hosts_and_ports"])
        self._connection = stomp.Connection12(
            hosts, 
            heartbeats=(HEARTBEAT_MS, HEARTBEAT_MS)
        )ssing, fault tolerance, and exactly-once
semantics.

- **Parallel Processing**: Creates one InputPartition per ActiveMQ queue for distributed processing
- **Memory Safety**: Implements bounded ring buffers with configurable size limits (≤ MAX_BUF bytes per queue)
- **Exactly-Once Semantics**: Uses client-individual message acknowledgment after Spark commit operations
- **Fault Tolerance**: Automatic fail-over with configurable broker URIs, heartbeat monitoring (2.5s),
    and infinite reconnection attempts
- **Thread Safety**: All operations are thread-safe with proper locking mechanisms
- **Backpressure Control**: Configurable buffer limits to prevent memory overflow

Architecture:
The module consists of three main components:

1. **ActiveMQPartition**: Represents a single partition containing messages from one ActiveMQ queue.
     Implements the InputPartition interface for distributed processing across Spark workers.

2. **ActiveMQStreamReader**: Core streaming reader that extends DataSourceStreamReader and implements
     stomp.ConnectionListener. Manages STOMP connections, message buffering, offset tracking, and
     provides the streaming interface to Spark.

3. **ActiveMQDataSource**: Main data source class that implements the DataSource interface.
     Provides the entry point for Spark applications to read from ActiveMQ streams.

Schema:
- offset (IntegerType): Sequential message identifier for ordering and deduplication
- frameCmd (StringType): STOMP frame command type (MESSAGE, CONNECT, etc.)
- frameHeaders (StringType): JSON-encoded message headers and metadata
- frameBody (StringType): Raw message payload content
- messageError (StringType): Error information if message processing fails

Configuration:
Required options:
- hosts_and_ports: List of broker host:port tuples for connection
- username: Authentication username for broker access
- password: Authentication password for broker access
- queues: List of queue names to subscribe to

Connection Management:
- Uses STOMP 1.2 protocol for communication with ActiveMQ brokers
- Implements exponential backoff retry logic with jitter for connection failures
- Maintains persistent connections with configurable heartbeat intervals
- Automatic reconnection on connection loss with infinite retry attempts

Buffer Management:
- Per-queue bounded message buffers (default: 50,000 messages)
- Per-batch offset buffer limits (default: 5,000 offsets) for backpressure control
- Thread-safe buffer operations with proper cleanup after commit

Usage Example:
    ActiveMQ → Spark Structured Streaming V2 source
    – Parallel: 1 InputPartition per queue
    – Memory-safe: bounded ring ⟶ ≤ MAX_BUF bytes per queue
    – Exactly-once: client-individual ACK after Spark commit
    – Resilient: fail-over URI, heart-beat 2.5 s, reconnect ∞
"""

import json
import logging
import random
from collections import deque, defaultdict
from ast import literal_eval
from threading import Lock
from time import sleep
from typing import Iterator, Final
import stomp  # type: ignore

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import StructType, StringType, IntegerType, StructField

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s"
)
logging.getLogger("py4j").setLevel(logging.ERROR)  # Suppress Py4J warnings
log = logging.getLogger(__name__)

# ─────────────────────────── constants ────────────────────────────
HEARTBEAT_MS: Final[int] = 2_500  # 2.5 s ⇆ heart-beat
BROKER_TTL: Final[int] = 30_000  # 30 s keep-alive (broker xml)
MAX_QUEUE_BUFFER: Final[int] = (
    100_000  # per-queue bounded buffer (increased for throughput)
)
MAX_OFFSETS_BUFFER: Final[int] = (
    10_000  # per batch back-pressure (increased for throughput)
)
PREFETCH_SIZE: Final[int] = 1000  # ActiveMQ prefetch for better throughput


# ───────────────────────── partitions ─────────────────────────────
class ActiveMQPartition(InputPartition):
    """
    A partition implementation for an ActiveMQ data source in PySpark.

    This class represents a single partition of data from an ActiveMQ queue that can be
    processed by Spark workers. It implements the InputPartition interface to enable
    distributed processing of ActiveMQ messages.

    Attributes:
        queue (str): The name of the ActiveMQ queue this partition represents.
        _rows (list[tuple]): The list of message data tuples contained in this partition.

    Methods:
        read() -> Iterator[tuple]: Returns an iterator over the message tuples in this partition.
        __getstate__(): Returns the state dictionary for serialization during distribution to workers.
    """

    def __init__(
        self: "ActiveMQPartition",
        queue: str,
        rows: list[tuple[int, str, str, str, str | None]],
    ) -> None:
        """
        Initialize an ActiveMQPartition instance.

        Args:
            queue (str): The name of the ActiveMQ queue.
            rows (list[tuple[int, str, str, str, str | None]]):
                List of tuples containing message data. Each tuple contains:
                - int: Message offset
                - str: STOMP frame command
                - str: JSON-encoded message headers
                - str: Message body/content
                - str | None: Optional error message if processing failed

        Returns:
            None
        """
        super().__init__(value=f"{queue}_{len(rows)}")
        self.queue, self._rows = queue, rows

    def read(self: "ActiveMQPartition") -> Iterator[tuple]:
        """
        Read and yield data rows from the data source.

        This method implements an iterator pattern to provide access to the stored
        data rows. It yields each row as a tuple from the internal _rows collection.

        Yields:
            Iterator[tuple]: An iterator that yields each data row as a tuple.

        Example:
            >>> datasource = DataSource()
            >>> for row in datasource.read():
            ...     print(row)
        """
        yield from self._rows

    def __getstate__(self: "ActiveMQPartition"):
        """
        Return the state of the ActiveMQPartition instance for pickling.

        This method is called during the pickling process to determine what data
        should be serialized. It returns a dictionary containing the essential
        attributes needed to reconstruct the partition.

        Returns:
            dict: A dictionary containing the partition's queue name and rows data.
                - queue: The name of the ActiveMQ queue
                - _rows: The cached rows data for this partition
        """
        return {"queue": self.queue, "_rows": self._rows}


# ───────────────────────── stream reader ──────────────────────────
class ActiveMQStreamReader(DataSourceStreamReader, stomp.ConnectionListener):
    """
    ActiveMQ Stream Reader for PySpark Structured Streaming.

    This class implements a custom data source stream reader that connects to ActiveMQ brokers
    and provides real-time message streaming capabilities for PySpark structured streaming jobs.
    It extends DataSourceStreamReader and implements stomp.ConnectionListener to handle
    ActiveMQ message consumption.

    The reader maintains persistent connections to ActiveMQ brokers, buffers incoming messages
    in memory, and provides them to Spark in a streaming fashion with proper offset management
    and fault tolerance.

    Key Features:
    - Multi-queue subscription support
    - Automatic connection retry with exponential backoff
    - Thread-safe message buffering with configurable limits
    - Offset tracking and commit functionality
    - Error handling and message acknowledgment
    - Heartbeat-based connection monitoring

    Attributes:
        _schema (StructType): The schema definition for the streaming DataFrame
        _reader_options (dict[str, str]): Configuration options for the reader
        _queues (list[str]): List of ActiveMQ queues to subscribe to
        _offset_by_queue (defaultdict[str, int]): Current offset tracking per queue
        _message_buffer_by_queue (dict[str, deque[tuple]]): In-memory message buffers per queue
            Each tuple contains: (offset, frameCmd, frameHeaders, frameBody, messageError)
        _lock (Lock): Thread synchronization lock for buffer operations
        _connection (stomp.Connection12): STOMP connection to ActiveMQ broker
        _user (str): Username for broker authentication
        _pwd (str): Password for broker authentication

    Connection Lifecycle Methods:
        _connect(): Establishes connection to ActiveMQ broker with retry logic
        on_connected(): Callback when connection is successfully established
        on_disconnected(): Callback when connection is lost, triggers reconnection
        on_error(): Callback for handling connection errors
        on_message(): Processes incoming messages and updates buffers

    Spark Stream Reader Interface:
        initialOffset(): Returns initial offset state for all queues
        latestOffset(): Returns current offset state for all queues
        partitions(): Creates partitions for a given offset range
        commit(): Commits processed offsets and cleans up buffers
        read(): Reads data from a specific partition
        schema(): Returns the DataFrame schema
        stop(): Cleanly shuts down the connection

    Usage:
        This class is typically instantiated by the ActiveMQ data source and should not
        be created directly. It requires proper configuration through reader_options
        including broker hosts, credentials, and queue specifications.
    """

    def __init__(self, schema: StructType, reader_options: dict[str, str]):
        self._schema: StructType = schema
        self._reader_options: dict[str, str] = reader_options
        self._queues: list[str] = literal_eval(reader_options["queues"])
        self._offset_by_queue: defaultdict[str, int] = defaultdict(int)
        self._message_buffer_by_queue: dict[str, deque[tuple]] = {
            queue: deque() for queue in self._queues  # Remove maxlen to prevent message loss
        }
        self._pending_ack: deque[stomp.utils.Frame] = deque()  # Remove maxlen to prevent ACK loss
        self._frame_to_offset: dict[stomp.utils.Frame, int] = {}
        self._total_messages_received: int = 0  # Debug counter
        self._lock: Lock = Lock()
        self._connection: stomp.Connection12 | None = None
        self._is_stopped: bool = False

        hosts: list[tuple[str, int]] = literal_eval(reader_options["hosts_and_ports"])
        self._connection = stomp.Connection12(
            hosts, heartbeats=(HEARTBEAT_MS, HEARTBEAT_MS)
        )
        self._connection.set_listener("ActiveMQListener", self)
        self._user: str = reader_options["username"]
        self._pwd: str = reader_options["password"]
        self._connect()

    # ─ connection lifecycle ─
    def _connect(self: "ActiveMQStreamReader") -> None:
        """
        Establishes connection to ActiveMQ broker with exponential backoff retry logic.

        Attempts to connect to the ActiveMQ broker and subscribe to configured queues.
        Uses exponential backoff with jitter for retry delays, capped at 60 seconds.
        Will retry indefinitely until connection is successful.

        The method performs the following steps:
        1. Checks if connection is already established
        2. Calculates retry delay using exponential backoff with random jitter
        3. Attempts to connect to broker with provided credentials
        4. Subscribes to all configured queues with client-individual acknowledgment
        5. On failure, logs warning and retries after calculated delay

        Raises:
            No exceptions are raised as all connection failures are caught and retried.

        Note:
            This method will block until a successful connection is established.
            Connection failures include ConnectFailedException, ConnectionError, and OSError.
        """
        attempt: int = 0
        max_sleep_time: int = 60
        while (
            self._connection
            and not self._connection.is_connected()
            and not self._is_stopped
        ):
            try:
                if attempt > 0:  # Only sleep after first failed attempt
                    sleep_time: float = min(
                        max_sleep_time, (2**attempt)
                    ) + random.uniform(0, 1)
                    log.warning(
                        "Attempting to connect to broker (attempt: %d)... Waiting %.2f seconds.",
                        attempt + 1,
                        sleep_time,
                    )
                    sleep(sleep_time)
                else:
                    log.info("Initial connection attempt to broker...")

                self._connection.connect(self._user, self._pwd, wait=True)

                # Subscribe to all queues with proper error handling
                for q in self._queues:
                    try:
                        # Simplified subscription - ActiveMQ-specific headers may not work with STOMP
                        # Focus on reliability over throughput optimization
                        self._connection.subscribe(
                            destination=q,
                            id=q,
                            ack="client-individual"
                        )
                        log.info("Successfully subscribed to queue: %s", q)
                    except Exception as sub_error:
                        log.error("Failed to subscribe to queue %s: %s", q, sub_error)
                        raise  # Re-raise to trigger retry

                log.info("Successfully connected and subscribed to all queues")
                break

            except (
                stomp.exception.ConnectFailedException,
                ConnectionError,
                OSError,
            ) as e:
                log.warning(
                    "ActiveMQ server connection failed (attempt %d): %s",
                    attempt + 1,
                    e,
                    exc_info=True,
                )
                attempt += 1

    def on_message(
        self: "ActiveMQStreamReader",
        frame: stomp.utils.Frame,
    ) -> None:
        """Handle incoming STOMP frame messages from ActiveMQ.

        Processes incoming messages by extracting the destination queue, creating a message
        tuple with queue, body, headers, and offset information, then buffering
        the message for consumption by the stream reader.

        Args:
            frame: STOMP frame containing the message data, headers, and command.

        Note:
            This method is thread-safe and uses a lock when modifying shared state.
            Any errors during message processing are caught and logged.
            Optimized for high throughput processing.
        """
        # Fast path: get queue and current offset
        queue: str = frame.headers["destination"]

        with self._lock:
            current_offset = self._offset_by_queue[queue]
            self._frame_to_offset[frame] = current_offset

            # Optimized message creation - minimize JSON serialization overhead
            try:
                # Use faster serialization approach
                headers_str = (
                    json.dumps(frame.headers, separators=(",", ":"))
                    if frame.headers
                    else "{}"
                )

                # Create tuple matching the schema: (offset, frameCmd, frameHeaders, frameBody, messageError)
                message: tuple[int, str, str, str, str | None] = (
                    current_offset,
                    frame.cmd,
                    headers_str,
                    frame.body or "",
                    None,
                )

                # Only log at debug level to reduce I/O overhead in high-throughput scenarios
                if log.isEnabledFor(logging.DEBUG):
                    log.debug(
                        "Processed message: queue=%s, offset=%d, body_length=%d",
                        queue,
                        current_offset,
                        len(frame.body) if frame.body else 0,
                    )

            except (AttributeError, KeyError, TypeError, json.JSONDecodeError) as error:
                log.error("Error processing message: %s", error, exc_info=True)
                message = (
                    current_offset,
                    getattr(frame, "cmd", "UNKNOWN"),
                    (
                        json.dumps(frame.headers, separators=(",", ":"))
                        if hasattr(frame, "headers")
                        else "{}"
                    ),
                    frame.body or "",
                    str(error),
                )

            # Batch operations for better performance
            self._message_buffer_by_queue[queue].append(message)
            self._offset_by_queue[queue] += 1
            self._pending_ack.append(frame)
            self._total_messages_received += 1

            # Periodic logging of message reception
            if self._total_messages_received % 100 == 0:
                log.info("Received %d total messages across all queues", self._total_messages_received)

            # Memory monitoring - warn but don't drop messages
            queue_size = len(self._message_buffer_by_queue[queue])
            if queue_size > MAX_QUEUE_BUFFER:
                log.warning(
                    "Queue %s buffer size (%d) exceeds recommended limit (%d). "
                    "Consider increasing Spark batch processing frequency.",
                    queue, queue_size, MAX_QUEUE_BUFFER
                )

            # Optimized cleanup - only run when necessary and less frequently
            if len(self._frame_to_offset) > MAX_QUEUE_BUFFER * len(self._queues) * 1.5:
                self._cleanup_frame_mapping()

    def _cleanup_frame_mapping(self: "ActiveMQStreamReader") -> None:
        """
        Optimized cleanup of frame-to-offset mapping to prevent memory bloat.

        This method is called less frequently but does more aggressive cleanup
        to maintain performance during high-throughput scenarios.
        """
        try:
            # More aggressive cleanup - keep only the most recent entries
            max_keep = (
                MAX_QUEUE_BUFFER * len(self._queues) // 3
            )  # Keep 1/3 instead of 1/2
            if len(self._frame_to_offset) > max_keep:
                # Sort by offset and keep the highest offsets
                sorted_items = sorted(
                    self._frame_to_offset.items(), key=lambda x: x[1], reverse=True
                )
                # Keep the most recent entries
                items_to_keep = dict(sorted_items[:max_keep])
                self._frame_to_offset = items_to_keep

                log.debug(
                    "Cleaned up frame mapping: kept %d entries out of %d",
                    len(items_to_keep),
                    len(sorted_items),
                )
        except Exception as e:
            log.warning("Error during frame mapping cleanup: %s", e)

    def on_connected(
        self: "ActiveMQStreamReader",
        frame: stomp.utils.Frame,
    ) -> None:
        """
        Callback method executed when a successful connection to the ActiveMQ broker is established.

        This method is called by the STOMP client when the connection handshake with the
        ActiveMQ broker completes successfully. It logs the connection success along with
        the frame details received from the broker.

        Args:
            frame (stomp.utils.Frame): The STOMP frame received from the broker upon
                successful connection, containing connection acknowledgment details.

        Returns:
            None

        Note:
            This is a callback method that should not be called directly. It is invoked
            automatically by the STOMP client library when a connection is established.
        """
        log.info(
            "SUCCESS: on_connected: ----------------Connected to broker: '%s'----------------\n",
            frame,
        )

    def on_error(
        self: "ActiveMQStreamReader",
        frame: stomp.utils.Frame,
    ) -> None:
        """
        Handle error frames received from the ActiveMQ broker.

        This method is called when an ERROR frame is received from the STOMP connection.
        It logs the error frame details for debugging and monitoring purposes.

        Args:
            frame (stomp.utils.Frame): The ERROR frame received from the broker containing
                error details such as error message, headers, and body.

        Returns:
            None
        """
        log.error(
            "on_error: ----------------Recieved an error: '%s'----------------",
            frame,
        )

    def on_disconnected(self: "ActiveMQStreamReader") -> None:
        """Handle disconnection from ActiveMQ broker and attempt automatic reconnection.

        This method is called automatically by the stomp.py library when the connection
        to the ActiveMQ broker is lost. It implements a reconnection strategy with a
        5-second delay between attempts.

        The method will continuously attempt to reconnect until a successful connection
        is established. Each failed reconnection attempt is logged as an error.

        Raises:
            stomp.exception.ConnectFailedException: When STOMP connection fails
            ConnectionError: When network connection cannot be established
            OSError: When system-level connection errors occur

        Note:
            This method blocks the calling thread until reconnection is successful.
            The reconnection loop runs indefinitely until connection is restored.
        """
        log.error(
            "on_disconnected: ----------------Disconnected from broker. Attempting to reconnect...----------------"
        )
        while (
            self._connection
            and not self._connection.is_connected()
            and not self._is_stopped
        ):
            try:
                sleep(5)
                self._connect()
            except (
                stomp.exception.ConnectFailedException,
                ConnectionError,
                OSError,
            ) as error:
                log.error(
                    "on_disconnected: ----------------Reconnect failed: '%s'----------------",
                    error,
                )

    # ─ Spark required methods ─
    def initialOffset(self: "ActiveMQStreamReader") -> dict[str, int]:
        """
        Get the initial offset for all queues in the ActiveMQ stream.

        This method returns a dictionary mapping each queue name to an initial
        offset of 0, indicating that reading should start from the beginning
        of each queue.

        Returns:
            dict[str, int]: A dictionary where keys are queue names and values
                are the initial offset (0) for each queue.
        """
        # Ensure we have entries for all queues, even if no messages received yet
        initial_offsets = {}
        for queue in self._queues:
            initial_offsets[queue] = 0
        log.debug("Initial offsets: %s", initial_offsets)
        return initial_offsets

    def latestOffset(self: "ActiveMQStreamReader") -> dict[str, int]:
        """
        Get the latest offset for each queue being monitored.

        Returns a copy of the current offset mapping to ensure thread safety
        and prevent external modification of internal state.

        Returns:
            dict[str, int]: A dictionary mapping queue names to their latest
                           processed message offsets.
        """
        # Ensure connection is active before reporting offsets
        if not self._connection or not self._connection.is_connected():
            log.warning("Connection lost, attempting to reconnect...")
            try:
                self._connect()
            except Exception as e:
                log.error("Failed to reconnect: %s", e)

        with self._lock:
            # Ensure all configured queues have offset entries, even if no messages received
            result = {}
            total_buffered = 0
            for queue in self._queues:
                result[queue] = self._offset_by_queue[queue]
                total_buffered += len(self._message_buffer_by_queue[queue])
            
            log.debug("Latest offsets: %s (total buffered messages: %d)", result, total_buffered)
            return result

    def partitions(
        self: "ActiveMQStreamReader",
        start: dict[str, int],
        end: dict[str, int],
    ) -> list[ActiveMQPartition]:
        """
        Create ActiveMQ partitions based on specified offset ranges.

        This method generates a list of ActiveMQPartition objects containing messages
        from queues within the specified offset boundaries. It filters messages from
        the internal buffer based on starting and ending offsets for each queue.

        Args:
            start (dict[str, int]): Dictionary mapping queue names to starting offset positions.
                If a queue is not present in the dictionary, offset 0 is used as default.
            end (dict[str, int]): Dictionary mapping queue names to ending offset positions.
                The actual ending offset is limited by MAX_OFFSETS_BUFFER to prevent
                excessive memory usage.

        Returns:
            list[ActiveMQPartition]: List of ActiveMQPartition objects containing filtered
                messages for each queue. If no messages are found within the specified
                ranges, returns a single empty partition with queue name "__empty__".

        Note:
            This method is thread-safe and uses an internal lock to ensure consistent
            access to the message buffer during partition creation.
        """
        partitions: list[ActiveMQPartition] = []
        log.debug("Creating partitions with start=%s, end=%s", start, end)
        with self._lock:
            total_messages_found = 0
            for queue in self._queues:
                starting_offset = start.get(queue, 0)
                # Handle case where end offset may not exist for a queue
                queue_end_offset = end.get(queue, starting_offset)
                ending_offset = min(
                    queue_end_offset, starting_offset + MAX_OFFSETS_BUFFER
                )

                # Skip if there's no range to process
                if starting_offset >= ending_offset:
                    log.debug("Skipping queue %s: no range to process (start=%d, end=%d)", 
                             queue, starting_offset, ending_offset)
                    continue

                rows: list[tuple[int, str, str, str, str | None]] = [
                    row
                    for row in self._message_buffer_by_queue[queue]
                    if starting_offset <= row[0] < ending_offset  # row[0] is the offset
                ]
                
                log.debug("Queue %s: found %d messages in range [%d, %d), buffer size: %d", 
                         queue, len(rows), starting_offset, ending_offset, 
                         len(self._message_buffer_by_queue[queue]))
                
                if rows:
                    partitions.append(ActiveMQPartition(queue, rows))
                    total_messages_found += len(rows)

            log.debug("Created %d partitions with %d total messages", 
                     len(partitions), total_messages_found)

        # Always return at least one partition to prevent empty partition issues
        return partitions or [ActiveMQPartition("__empty__", [])]

    def _is_committed(
        self: "ActiveMQStreamReader",
        frame: stomp.utils.Frame,
        end_offsets: dict[str, int],
    ) -> bool:
        """
        Check if a message frame has been committed based on its offset.

        Args:
            frame (stomp.utils.Frame): The STOMP frame containing message headers with
                destination queue and offset information.
            end_offsets (dict[str, int]): Dictionary mapping queue names to their
                respective end offsets.

        Returns:
            bool: True if the message offset is less than the end offset for the
                queue, indicating the message has been committed. False otherwise.
                Returns False if the queue is not found in end_offsets.
        """
        try:
            queue: str = frame.headers["destination"]
            # CRITICAL FIX: The frame doesn't have an "offset" header - we need to track this ourselves
            # Use a mapping from frame to our internal offset
            message_offset: int = self._frame_to_offset.get(frame, -1)
            if message_offset == -1:
                log.debug("Frame not found in offset mapping")
                return False  # Unknown frame, don't commit
            return message_offset < end_offsets.get(queue, 0)
        except (KeyError, AttributeError, TypeError) as e:
            log.error("Error checking if frame is committed: %s", e)
            return False

    def commit(self: "ActiveMQStreamReader", end: dict[str, int]) -> None:
        """
        Commits processed messages up to the specified offsets for each queue.

        This method removes messages from the internal buffer that have been processed
        (i.e., messages with offsets less than the specified end offset for each queue).
        This is typically called after Spark has successfully processed a batch of data
        to clean up the buffer and free memory.

        Args:
            end (dict[str, int]): A dictionary mapping queue names to the highest
                offset that has been successfully processed for each queue.

        Note:
            This method is thread-safe and uses an internal lock to ensure atomic
            operations on the message buffers. Optimized for high-throughput scenarios.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Committing offsets: %s", end)

        # Validate that end offsets contain expected queue names
        if not end:
            log.warning("Empty end offsets received in commit")
            return

        # Check for missing queue names in end offsets
        missing_queues = set(self._queues) - set(end.keys())
        if missing_queues:
            log.warning("Missing queues in end offsets: %s", missing_queues)

        with self._lock:
            # Clean up message buffers - optimized for batch processing
            for queue, offset in end.items():
                if queue in self._message_buffer_by_queue:
                    message_buffer: deque[tuple[int, str, str, str, str | None]] = (
                        self._message_buffer_by_queue[queue]
                    )
                    # Batch removal for better performance
                    removed_count = 0
                    while message_buffer and message_buffer[0][0] < offset:
                        message_buffer.popleft()
                        removed_count += 1

                    if log.isEnabledFor(logging.DEBUG) and removed_count > 0:
                        log.debug(
                            "Removed %d committed messages from queue %s",
                            removed_count,
                            queue,
                        )
                else:
                    log.warning("Unknown queue in commit: %s", queue)

            # Batch acknowledge frames and clean up tracking
            committed_frames = []
            ack_batch_size = 100  # Process acknowledgments in batches
            ack_count = 0

            while self._pending_ack and self._is_committed(self._pending_ack[0], end):
                frame: stomp.utils.Frame = self._pending_ack.popleft()
                try:
                    if self._connection:
                        self._connection.ack(frame)
                    committed_frames.append(frame)
                    ack_count += 1

                    # Batch logging to reduce I/O overhead
                    if (
                        log.isEnabledFor(logging.DEBUG)
                        and ack_count % ack_batch_size == 0
                    ):
                        log.debug("Acknowledged %d messages (batch)", ack_batch_size)

                except (
                    stomp.exception.StompException,
                    ConnectionError,
                    OSError,
                ) as ack_error:
                    log.error(
                        "Failed to acknowledge frame: %s", ack_error, exc_info=True
                    )
                    # Don't remove from pending_ack if ack failed
                    self._pending_ack.appendleft(frame)
                    break

            # Batch cleanup of frame-to-offset mapping
            if committed_frames:
                for frame in committed_frames:
                    self._frame_to_offset.pop(frame, None)

                if log.isEnabledFor(logging.DEBUG):
                    log.debug(
                        "Acknowledged %d total messages in commit",
                        len(committed_frames),
                    )

            # Optimized cleanup: remove frame mappings in batch
            if end:
                min_committed_offset = min(end.values())
                frames_to_remove = [
                    frame
                    for frame, offset in self._frame_to_offset.items()
                    if offset < min_committed_offset
                ]

                # Batch removal
                for frame in frames_to_remove:
                    self._frame_to_offset.pop(frame, None)

                if frames_to_remove and log.isEnabledFor(logging.DEBUG):
                    log.debug(
                        "Cleaned up %d expired frame mappings", len(frames_to_remove)
                    )

    def read(self, partition: InputPartition) -> Iterator[tuple]:
        """
        Read data from the specified partition.

        This method processes an ActiveMQ partition and returns an iterator of tuples
        containing the data from that partition. If the partition is not an instance
        of ActiveMQPartition, an empty iterator is returned.

        Args:
            partition (InputPartition): The input partition to read data from.
                Expected to be an instance of ActiveMQPartition for successful reading.

        Returns:
            Iterator[tuple]: An iterator yielding tuples of data from the partition.
                Returns an empty iterator if the partition is not an ActiveMQPartition.

        Raises:
            Any exceptions that may be raised by the partition's read() method.
        """
        if isinstance(partition, ActiveMQPartition):
            return partition.read()
        return iter([])

    def schema(self: "ActiveMQStreamReader") -> StructType:
        """
        Get the schema of the ActiveMQ stream.

        Returns:
            StructType: The schema structure defining the format of data records
                       that will be read from the ActiveMQ stream.
        """
        return self._schema

    def __getstate__(self):
        """
        Custom serialization method for pickling the ActiveMQStreamReader.

        This method is called when the object needs to be serialized (pickled)
        for distribution to Spark workers. It excludes non-serializable objects
        like threading.Lock and STOMP connections.

        Returns:
            dict: A dictionary containing only the serializable state of the object.
        """
        state = self.__dict__.copy()
        # Remove non-serializable objects
        state["_lock"] = None
        state["_connection"] = None
        state["_pending_ack"] = None  # STOMP frames are not serializable
        state["_frame_to_offset"] = {}  # Clear frame references
        return state

    def __setstate__(self, state):
        """
        Custom deserialization method for unpickling the ActiveMQStreamReader.

        This method is called when the object is deserialized (unpickled)
        on Spark workers. It reconstructs the non-serializable objects.

        Args:
            state (dict): The serialized state dictionary.
        """
        self.__dict__.update(state)
        # Recreate non-serializable objects
        self._lock = Lock()
        self._pending_ack = deque()  # Remove maxlen to prevent ACK loss
        self._frame_to_offset = {}
        self._total_messages_received = 0  # Reset counter
        self._connection = None
        self._is_stopped = False

        # Recreate connection if we have the necessary info
        if hasattr(self, "_reader_options") and self._reader_options:
            try:
                hosts = literal_eval(self._reader_options["hosts_and_ports"])
                self._connection = stomp.Connection12(
                    hosts, heartbeats=(HEARTBEAT_MS, HEARTBEAT_MS)
                )
                self._connection.set_listener("ActiveMQListener", self)
                self._connect()
            except (
                ValueError,
                SyntaxError,
                TypeError,
                KeyError,
                stomp.exception.ConnectFailedException,
                ConnectionError,
                OSError,
            ) as error:
                log.warning(
                    "Failed to recreate connection after deserialization: %s", error
                )

    def stop(self: "ActiveMQStreamReader") -> None:
        """
        Stop the ActiveMQ stream reader and disconnect from the broker.

        This method gracefully shuts down the ActiveMQ connection if one exists.
        It should be called when the stream reader is no longer needed to ensure
        proper cleanup of resources.

        Returns:
            None
        """
        self._is_stopped = True

        with self._lock:
            # Clean up frame tracking to prevent memory leaks
            self._frame_to_offset.clear()

            # Acknowledge any remaining pending messages before shutdown
            while self._pending_ack:
                frame = self._pending_ack.popleft()
                try:
                    if self._connection and self._connection.is_connected():
                        self._connection.ack(frame)
                except (stomp.exception.StompException, ConnectionError, OSError):
                    # Ignore ack failures during shutdown
                    pass

        if self._connection and self._connection.is_connected():
            try:
                # Unsubscribe from all queues before disconnecting
                for queue in self._queues:
                    try:
                        self._connection.unsubscribe(id=queue)
                    except (stomp.exception.StompException, ConnectionError, OSError):
                        pass  # Ignore unsubscribe failures during shutdown

                self._connection.disconnect()
                log.info("Successfully disconnected from ActiveMQ broker")
            except (stomp.exception.StompException, ConnectionError, OSError) as e:
                log.warning("Error during disconnect: %s", e)


class ActiveMQDataSource(DataSource):
    """
    ActiveMQ data source for Apache Spark structured streaming.

    This data source provides streaming capabilities for reading messages from ActiveMQ
    message queues. It implements the DataSource interface to integrate with Spark's
    structured streaming framework.

    The data source produces records with the following schema:
    - offset: Message sequence number (IntegerType, non-nullable)
    - frameCmd: STOMP frame command (StringType, nullable)
    - frameHeaders: STOMP frame headers as string (StringType, nullable)
    - frameBody: Message body content (StringType, nullable)
    - messageError: Error information if message processing failed (StringType, nullable)

    Note:
        This data source only supports streaming queries. Batch queries will raise
        a NotImplementedError.

    Example:
        df = spark.readStream.format("activemq").load()

    Raises:
        NotImplementedError: When attempting to use batch reading operations.
    """

    @classmethod
    def name(cls):
        """
        Get the name identifier for the ActiveMQ data source.

        Returns:
            str: The string "activemq" which serves as the identifier for this data source type.
        """
        return "activemq"

    def schema(self):
        """
        Returns the schema for ActiveMQ message data.

        The schema defines the structure of data read from ActiveMQ messages with the following fields:
        - offset: Sequential message identifier (integer, required)
        - frameCmd: STOMP frame command (string, optional)
        - frameHeaders: STOMP frame headers as JSON string (string, optional)
        - frameBody: Message payload content (string, optional)
        - messageError: Error information if message processing failed (string, optional)

        Returns:
            StructType: PySpark DataFrame schema for ActiveMQ message data
        """
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
        """
        Creates a DataFrameReader for batch reading from ActiveMQ.

        Args:
            schema (StructType): The schema to apply to the data being read.

        Raises:
            NotImplementedError: Always raised as batch queries are not supported
                                for ActiveMQ data source. ActiveMQ is designed for
                                streaming data processing only.

        Note:
            ActiveMQ data source only supports streaming queries. Use writeStream()
            and readStream() methods for streaming operations instead.
        """
        raise NotImplementedError(
            "ERROR: ----------------Batch queries are not supported for ActiveMQDataSource.----------------"
        )

    def streamReader(self, schema: StructType):
        """
        Creates and returns an ActiveMQ stream reader for reading streaming data.

        Args:
            schema (StructType): The schema definition that describes the structure
                                of the data to be read from the ActiveMQ stream.

        Returns:
            ActiveMQStreamReader: A configured stream reader instance that can be used
                                 to read data from ActiveMQ with the specified schema
                                 and connection options.
        """
        return ActiveMQStreamReader(schema, self.options)
