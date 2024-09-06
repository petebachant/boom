# BOOM (Burst & Outburst Observations Monitor)

## Description

BOOM is multiple things at once:
- A real-time processing system, receiving alert packets from multiple surveys (for now, only ZTF), enriching them (with spatial crossmatches and ML classifiers) and storing them in a database.
- A real-time filtering system, allowing users to pre-define filters to run in real-time as new alerts come in.
- An archive, allowing users to query the database for alerts and lightcurves, allowing them to filter and retrieve data after the fact.
- An API, allowing the archive to be queried programmatically, and allowing users to interact with all the collections/catalogs stored in the database.

BOOM's nightly processing pipeline is highly modular and multi-language, with modules for:
- Receiving alerts from multiple surveys (for now, only ZTF), running spatial crossmatches and storing them in the database (implemented in `Rust`).
- Running ML classifiers on the alerts and storing the results in the database (implemented in `Python`).
- Running user-defined filters on the alerts and storing the results in the database (implemented in `Python` and/or `Rust`).

**Note: The API and filtering system(s) are still under development.**

## Installation

For now we support running boom on MacOS and Linux. You'll need:

-  For the Database & Cache/Task Queue:
    - `Redis` or `Valkey` (a Redis-compatible open-source in-memory key-value store). *We recommend using Valkey*.
    - `MongoDB` (a document-oriented NoSQL database).
-  For the real-time processing system:
    - `Rust` (a systems programming language), any version `>= 1.55.0` should work.
    - `Python` (a high-level programming language), any version `>= 3.10` should work.

To run `Valkey` and `MongoDB` in docker, we provide a `docker-compose.yaml` file in the repository. You can start both services with:
```bash
docker-compose up -d
```

## Usage

- First, make sure that `Redis`/`Valkey` and `MongoDB` are running. You can easily run them in docker with the command above. Then, you can start the **fake** kafka consumer (that reads alerts from a file instead of a kafka topic) with:
    ```bash
    cargo run --bin fake_kafka_consumer <date_in_YYYMMDD_format>
    ```
    Where `<date_in_YYYMMDD_format>` is the date of the alerts you want to read. We suggest using a night with a very small number of alerts to just get the code running, like `20240617` for example. The script will take care of downloading the alerts from the ZTF IPAC server, and writing them to `data/alerts/ztf/YYYYMMDD/*.avro`.
    This will start reading alerts from the file `data/alerts.json` and sending them to the `Valkey` instance's `alertpacketqueue` list.

- Next, you can start the real-time processing system with:
    ```bash
    cargo run --bin alert_worker --release
    ```
    This will start reading alerts from the `Valkey` instance's `alertpacketqueue` list, an process the alerts. You can start multiple instances of this worker to parallelize the processing (in another terminal for example). At the end of each alert processing, the `candid` (unique identifier of an alert) will be stored in the `Valkey` instance's `alertclassifierqueue` list.

- Finally, you can start the real-time ML worker with:
    ```bash
    pip install -r requirements.txt
    python py_workers/ml_worker.py
    ```
    This will start reading alerts from the `Valkey` instance's `alertclassifierqueue` list, and process the alerts with the ML classifiers. You can start multiple instances of this worker to parallelize the processing (in another terminal for example). Essentially, this gets up to 1000 `candid`s at once, grabs the alerts from the DB, and runs the ML classifiers on them, in batches of up to 1000 alerts. Then, it stores the results in the DB.

## Contributing

We welcome contributions! Please read the [CONTRIBUTING.md](CONTRIBUTING.md) file for more information.

## TODOs:
- [ ] Add unit tests for the `process_alert` function.
- [ ] Keep developing the Python-based ML worker to include more models (like all the `acai` models + `btsbot` to start with), and add unit tests for it.
- [ ] Add the API and filtering system(s) to the repository, with the ability to run user-defined filters in real-time.
- [ ] Think about the multi-survey support, and how to handle alerts from multiple surveys in the same system and let the filters run across all of them.
- [ ] Add CI/CD to the repository, with GitHub Actions.
- [ ] Create docker images for the `alert_worker` and `ml_worker` binaries, as lightweight as possible, with the number of workers configurable via environment variables.
- [ ] Add all the kube manifests to deploy the whole system on a kubernetes cluster, with autoscaling of the workers when the `Valkey` queues get too long.
- [ ] Improve the performance of **EVERYTHING**, by profiling the code and adding benchmarks to the repository.

**All of the steps above do not need to be done in order, and can be done in parallel.**

## Implementation details:

### Generating Rust types from the ZTF Avro schema:

#### Dealing with Avro & Rust structs:

It can get pretty painful in Rust to work with Avro schemas, and more specifically to have to write Rust structs that match them. To make this easier, we use the super useful `rsgen-avro` crate, which allows us to generate Rust structs from Avro schemas. First, install it as a binary with:
```bash
cargo install rsgen-avro --features="build-cli"
```
Then, you can generate the Rust structs from the Avro schema with:
```bash
rsgen-avro "schema/ztf/*.avsc" -
```
This will output the Rust structs to the standard output, which you can then copy-paste in a lib file in the `src` directory, so you can use them in your code.
We already ran it for the ZTF Avro schema (so no need to do it again), and the corresponding Rust structs are in the `src/types.rs` file. We only slightly modified the `Alert` struct to add methods to create an alert from the bytes of an Avro record (`from_avro_bytes`).

#### Dealing with Rust structs and MongoDB BSON documents:

We could in theory just query MongoDB in a way that allows us to get Rust structs out of it, and also do the same when writing to the database. However under the hood the `mongodb` crate just serializes back and forth, and since Rust structs can't just "remove" their fields that are null (in a way, they need to enforce a schema), we would end up with a lot of null fields in the database. To avoid this, we use the `bson` crate to serialize the Rust structs to BSON documents, sanitize them (remove the null fields and such), and then write them to the database. When querying the DB, both bson documents or Rust structs can be returned, it depends on the use case.

#### Why still using Python for some parts of the pipeline?

For everything ML-related, it's not that easy to just take anyone's model (that was 99% of the time trained with a Python library) and just run it in Rust. We could try (and successfully did for a handful of models) converting them to ONNX, and then running them with `tch-rs` (a Rust wrapper around the `libtorch` C++ library). However, this proved to be a pain on a lot of systems. Thanks to the fact that we use something like `Redis`/`Valkey` as a cache/task queue, we can pretty much send data between whatever language we want, and have the ML models run in Python, and the rest of the pipeline run in Rust. It will also come in handy for the filtering pipeline, as we can leverage any of Python's libraries to do some fancy & complex computation that would be a pain for your average astronomer to write in Rust. So for now, we limit the Rust code to all of the "core" parts of the pipeline where performance is key, and use Python where we can affort to lose a bit of performance for more flexibility.

#### Why `Redis`/`Valkey` as a cache/task queue?

There are multiple answers to this:
- Because it is well maintained, well documented, both on the server side and the clients to interact with the software from any programming language.
- Because it is fast, and can handle a lot of data in memory.
- Because it is easy to use, and can be used as a cache, a task queue, a message broker, and more. It's a piece of software we can reuse to solve multiple problems in a large system.
- Because other task queue systems like `Celery`, `Dask`, `RabbitMQ`, `Kafka`, etc. are either too complex, too slow, too hard to maintain, have memory leaks, poor support across multiple programming languages, or all of the above. `Redis`/`Valkey` is simple and fast, and allows us to build the rest of the system on top of it. However the system is designed in a way that we can swap `Redis`/`Valkey` for another task queue system if we ever need to, with minimal changes to the code.

#### Why `MongoDB` as a database?

MongoDB proved to be a great choice for another broker that `BOOM` is heavily inspired from: `Kowalski`. Mongo has great support across multiple programming languages, is highly flexible, has a rich query language that we can build complex pipelines with (perfect for filtering alerts), and is easy to maintain. It is also fast, and can handle a lot of data. We could have gone with `PostgreSQL`, but we would have lost some flexibility, and we would have had to enforce a schema on the data, which is not ideal for an alert stream that can have a lot of different fields. With MongoDB, we do not have to enforce a schema or run database migrations whenever we want to add another astronomical catalog to crossmatch with the alerts.
