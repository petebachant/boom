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

## Installation

For now we support running boom on MacOS and Linux. You'll need:

-  For the Database & Cache/Task Queue:
    - `Redis` or `Valkey` (a Redis-compatible open-source in-memory key-value store). *We recommend using Valkey*.
    - `MongoDB` (a document-oriented NoSQL database).
-  For the real-time processing system:
    - `Rust` (a systems programming language), any version `>= 1.55.0` should work.
    - `Python` (a high-level programming language), any version `>= 3.10` should work.

**The API and filtering system(s) are still under development.**

#### Comments/Suggestions:

- For `Redis` or `Valkey`, you can use a local installation or a docker container. Here's how to start a `Valkey` docker container:
    ```bash
    docker run -p 6379:6379 --rm valkey/valkey:7.2.6
    ```
    To run it detached, just add the `-d` flag:
    ```bash
    docker run -p 6379:6379 --rm -d valkey/valkey:7.2.6
    ```

- For `MongoDB`, you can use a local installation or a docker container. Here's how to start a `MongoDB` docker container:
    ```bash
    docker run -p 27017:27017 --rm mongo:5.0.3
    ```
    To run it detached, just add the `-d` flag:
    ```bash
    docker run -p 27017:27017 --rm -d mongo:5.0.3
    ```

**Note:** Do keep in mind that these will not have a persistent volume, so you'll lose all data when the container is stopped. To add a persistent volume (and handling multiple docker containers in general), we recommend using `docker-compose`.

## Usage

- First, make sure that `Redis`/`Valkey` and `MongoDB` are running. Then, you can start the **fake** kafka consumer (that reads alerts from a file instead of a kafka topic) with:
    ```bash
    cargo run --bin fake_kafka_consumer --release
    ```
    This will start reading alerts from the file `data/alerts.json` and sending them to the `Valkey` instance's `alertpacketqueue` list.

- Next, you can start the real-time processing system with:
    ```bash
    cargo run --bin alert_worker --release
    ```
    This will start reading alerts from the `Valkey` instance's `alertpacketqueue` list, an process the alerts. You can start multiple instances of this worker to parallelize the processing (in another terminal for example). At the end of each alert processing, the `candid` (unique identifier of an alert) will be stored in the `Valkey` instance's `alertclassifierqueue` list.

- Finally, you can start the real-time ML worker with:
    ```bash
    python3 -m pip install -r requirements.txt
    python3 -m src.ml_worker
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
