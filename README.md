![Build](https://github.com/kcmhub/kcm-kafka-connect-adls-sink/actions/workflows/ci.yml/badge.svg)
![Release](https://img.shields.io/github/v/release/kcmhub/kcm-kafka-connect-adls-sink?color=blue)
![License](https://img.shields.io/github/license/kcmhub/kcm-kafka-connect-adls-sink)
![Java](https://img.shields.io/badge/Java-11-blue)
![Kafka Connect](https://img.shields.io/badge/Kafka%20Connect-3.x-orange)

# kcm-kafka-connect-adls-sink

Kafka Connect **Sink Connector** that writes records from Kafka topics to **Azure Data Lake Storage Gen2 (ADLS Gen2)** using **SAS authentication**.

It supports Avro (and other schemaful formats via Kafka Connect converters), writes one file per **topic / partition / starting offset**, and optionally compresses output using **GZIP**.

---

## 🚀 Features

* Kafka **Sink Connector** for ADLS Gen2 (DFS endpoint)
* Authentication with **SAS token** (no managed identity required)
* One output file per **topic / partition / start offset**
* **Optional GZIP compression** (`.log` or `.log.gz`)
* **Configurable batch size** via `flush.max.records`
* Simple line-based text output (each Kafka record → one line)
* Handles **Avro / Struct / schemaful** records via Kafka Connect converters

---

## 🧩 Architecture

The connector is a standard Kafka Connect **sink plugin**:

1. Kafka Connect worker reads records from Kafka topics.
2. Converters (e.g. AvroConverter, JsonConverter) deserialize bytes into `(Schema, Object)`.
3. `kcm-kafka-connect-adls-sink`:

   * formats the record value (Struct / Map / primitive) into JSON-like text,
   * buffers records per **topic-partition**,
   * writes a file to ADLS Gen2 when `flush.max.records` is reached (or on task stop).

Output files are stored under a configurable base path, partitioned by **date**:

```text
<base-path>/date=YYYYMMDD/<topic>-p<partition>-o<startOffset>.log[.gz]
```

Example:

```text
kafka-export/date=20251214/my-topic-p0-o1000.log.gz
kafka-export/date=20251214/my-topic-p1-o2000.log.gz
```

---

## ✅ Requirements

* Java **11**+
* Apache Kafka & Kafka Connect **3.x** (or compatible)
* Azure Storage account with **ADLS Gen2 enabled (HNS)**
* A **SAS token** with at least `rw` permissions on the target filesystem/container

---

## 📦 Installation

1. **Build the connector:**

```bash
mvn clean package
```

This will produce a fat jar, for example:

```text
target/kafka-connect-adls-sink-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

2. **Copy the jar to your Kafka Connect plugin path:**

```bash
mkdir -p /opt/kafka/plugins/kcm-kafka-connect-adls-sink
cp target/*jar-with-dependencies.jar /opt/kafka/plugins/kcm-kafka-connect-adls-sink/
```

3. **Configure the worker** to use that plugin path, e.g. in `connect-distributed.properties`:

```properties
plugin.path=/opt/kafka/plugins
```

4. Restart your Kafka Connect worker.

---

## ⚙️ Configuration

These are the main connector configuration properties:

| Name                | Type    | Required | Default        | Description                                                          |
| ------------------- | ------- | -------- |----------------| -------------------------------------------------------------------- |
| `connector.class`   | string  | yes      |                | Must be `io.kcmhub.kafka.connect.adls.AdlsSinkConnector`. |
| `tasks.max`         | int     | yes      |                | Max number of tasks to run.                                          |
| `topics`            | string  | yes      |                | Comma-separated list of topics to consume from.                      |
| `adls.account.name` | string  | yes      |                | Azure Storage account name (e.g. `dxxxxxxadl01`).                    |
| `adls.filesystem`   | string  | yes      |                | ADLS Gen2 filesystem/container (e.g. `kafka-poc`).                   |
| `adls.base.path`    | string  | no       | `kafka-export` | Base path inside the filesystem.                                     |
| `adls.sas.token`    | string  | yes      |                | SAS token **without** the leading `?`.                               |
| `flush.max.records` | int     | no       | `500`          | Maximum number of records per ADLS file per topic-partition.         |
| `compress.gzip`     | boolean | no       | `false`        | If `true`, files are compressed with GZIP (`.log.gz`).               |
| `adls.retry.max.attempts` | int | no | `3`            | Maximum number of retries for ADLS operations (Azure SDK retry policy). Set to `0` to disable retries. |
| `flush.interval.ms` | long | no | `0` | If > 0, flush buffers at least every N milliseconds even if `flush.max.records` is not reached. |

---

## Auth failures

If ADLS returns an **authentication/authorization error** (typically HTTP **401/403**, e.g. expired/invalid SAS token), the task throws a **non-retriable** error and Kafka Connect will mark the task as **FAILED**.

For transient network/server errors, the task throws a **RetriableException** so Kafka Connect can retry.

---

## 🔁 Converters & Avro Support

This connector relies on Kafka Connect **converters** to provide `(Schema, Value)` pairs.

Typical Avro setup (with Confluent Schema Registry):

```properties
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry:8081
```

The connector will:

* detect **Struct** values and serialize them to JSON-like text,
* handle Maps, Lists, and primitives,
* fall back to `value.toString()` if no schema is present.

Each Kafka record becomes one line in the output file.

---

## 📝 Example Connector Configuration

`adls-sink-connector.json`:

```json
{
  "name": "adls-gen2-sink",
  "config": {
    "connector.class": "io.kcmhub.kafka.connect.adls.AdlsSinkConnector",
    "tasks.max": "2",

    "topics": "my-avro-topic",

    "adls.account.name": "dxxxxxxadl01",
    "adls.filesystem": "kafka-poc",
    "adls.base.path": "kafka-export",
    "adls.sas.token": "si=...&sv=...&sr=c&sig=...",

    "flush.max.records": "500",
    "compress.gzip": "true"
  }
}
```

Create the connector via Kafka Connect REST API:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @adls-sink-connector.json \
  http://localhost:8083/connectors
```

---

## 📂 Output Layout

For a topic `my-avro-topic` with partitions 0 and 1, you will get files such as:

```text
kafka-export/
└── date=20251214/
    ├── my-avro-topic-p0-o1000.log.gz
    ├── my-avro-topic-p0-o1500.log.gz
    ├── my-avro-topic-p1-o2000.log.gz
    └── ...
```

Each file contains up to `flush.max.records` lines, one per Kafka record.

---

## 🧪 Development

* Build and run unit tests:

```bash
mvn clean test
```

* Build the fat jar:

```bash
mvn clean package
```

You can then deploy the jar into a local Kafka Connect standalone worker for quick testing.

---

## 📜 License

This project is licensed under the **Apache 2.0 License** – see the [LICENSE](./LICENSE) file for details.
