# PySpark Streaming Connector for ActiveMQ

A production-ready, fault-tolerant PySpark Structured Streaming data source for reading from ActiveMQ queues using the STOMP protocol.

This connector is designed for use in Databricks environments and follows best practices for custom PySpark data sources.

## Features

-   **Guaranteed Delivery**: Uses `client-individual` acknowledgement to ensure messages are not lost if a job fails.
-   **Fault-Tolerant Connections**: Automatically handles disconnects and attempts to reconnect to the broker.
-   **Robust Configuration**: Clear and explicit options for connection and authentication.
-   **Packaged for Production**: Designed to be installed as a standard Python wheel on a Databricks cluster.

## Installation

This package is intended to be installed as a library on a Databricks cluster.

1.  Build the wheel file from the source code (see Development section).
2.  In Databricks, navigate to your cluster's configuration page.
3.  Go to the **Libraries** tab and click **Install New**.
4.  Select **Python Whl** as the library type and upload the `.whl` file from the `dist/` directory.

## Usage

Once the library is installed on your cluster, you must first register the data source with your SparkSession. Then, you can use it in your streaming queries.

```python
from activemq_pyspark import ActiveMQDataSource

# Assuming 'spark' is your active SparkSession
spark: SparkSession = ...

# 1. Register the custom data source
spark.dataSource.register(ActiveMQDataSource)

# 2. Define your connection parameters as a (str, int) tuple and topics/queues as a list of strings
HOSTS_AND_PORTS = [
    ("host1", 61616),
    ("host2", 61616),
]

QUEUES = ["topic1", "topic2"]
USERNAME = dbutils.secrets.get(scope="my-scope", key="activemq-username")
PASSWORD = dbutils.secrets.get(scope="my-scope", key="activemq-password")

# 3. Configure and run the stream
df = (
    spark.readStream.format("activemq")
    .option("hosts_and_ports", HOSTS_AND_PORTS)
    .option("queues", QUEUES)
    .option("username", USERNAME)
    .option("password", PASSWORD)
    .load()
)

# Your stream is now ready for processing
query = (
    df.writeStream
      .format("delta")
      .option("checkpointLocation", "/path/to/your/checkpoint")
      .toTable("my_target_delta_table")
)
```

### Configuration Options

| Option              | Type                               | Required | Description                                                                                             |
| ------------------- | ---------------------------------- | -------- | ------------------------------------------------------------------------------------------------------- |
| `hosts_and_ports`   | `list[tuple[str, int]]`       | Yes      | A string representation of a list of `(host, port)` tuples for the ActiveMQ brokers.                    |
| `queues`            | `list[str]`                   | Yes      | A string representation of a list of queue names to subscribe to.                                       |
| `username`          | `str`                              | No       | The username for authentication.                                                                        |
| `password`          | `str`                              | No       | The password for authentication.                                                                        |
| `heartbeats`          | `int`                              | No       | How often (in milliseconds) to send client heartbeats to the server                                                                   |

### Output Schema

The streaming DataFrame will have the following schema:

| Column         | Type      | Description                                           |
| -------------- | --------- | ----------------------------------------------------- |
| `offset`       | `IntegerType` | A unique, incrementing ID for each message.           |
| `frameCmd`     | `StringType`  | The STOMP command of the frame (e.g., 'MESSAGE').     |
| `frameHeaders` | `StringType`  | A string representation of the message headers.       |
| `frameBody`    | `StringType`  | The body of the message.                              |
| `messageError` | `StringType`  | Populated with an error message if processing failed. |

## Development

To build the project from source, you will need Python 3.8+ and the `build` package.

1.  Clone the repository.
2.  Install the build tool: `pip install build`
3.  Run the build command from the project root: `python -m build`
4.  The distributable wheel file will be located in the `dist/` directory.
