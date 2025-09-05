# EventStore Example Application

Demonstration for running applications on top of
[go-eventstore](https://github.com/global-soft-ba/go-eventstore).

### Architecture of this app

- Golang
- Docker
- PostgreSQL

### Building and running
To build and run the example application with logs, you need to have Docker installed on your machine:

```bash
make build && make start && make logs
```

### Testing
To run the tests, you can use the following command:

```bash
make tests
```

### Stopping
To stop the application, you can use the following command:
```bash
make stop
```

---

## API Documentation
This application uses Swagger for API documentation. Once the application is running, you can access the Swagger UI by clicking the link below:

[Swagger UI - API Documentation](http://localhost:8080/swagger/index.html)

---

## Project Organization

This project follows domain-driven design (DDD) and hexagonal architecture principles. It is structured into ports and adapters, separating core business logic from infrastructure concerns. This allows easier testing, maintainability, and extensibility.

Directory structure is following:

```plaintext
├── config/              # Environment config files
├── core/                # Core application logic
├── docker/              # Docker and containerization config
├── docs/                # API documentation (Swagger)
├── infrastructure/      # Technology-specific adapters (DB, env, etc.)
├── presentation/        # REST API and supporting layers
├── testdata/            # Fixtures and test utilities
├── main.go              # Application entrypoint
├── Makefile             # Build and development commands
├── go.mod, go.sum       # Go module configuration
└── README.md            # Project documentation
```

---

### `core/`
This is the center of application which contains business logic layer. It’s decoupled from infrastructure and external tools.

#### `application/`

Contains high-level orchestration and use case logic.

- `service/`: Application services for each domain area.
    - `event/`, `item/`, `projection/`, `seeder/`: Business workflows.
- `repository/`: Interfaces and concrete implementations for data access.
    - `IReadRepository.go`, `IWriteRepository.go`: Abstractions.
    - `eventstore/`: Implementations for event sourcing pattern.
- `projection/`: Projection handlers that update read models in response to events.

#### `domain/`

Defines core domain models and events.

- `model/item/`: Item aggregate and domain events like `Event:Created`, `Event:Renamed`, etc.

#### `port/`

Defines ports (interfaces) for driving and driven adapters.

- `service/`: Ports for application services.
- `readmodel/` & `writemodel/`: Ports for read/write persistence layers.
- `environment/`: Port for environment configuration.

---

### `infrastructure/`

Technology-specific code that implements interfaces defined in `core/port`.

- `environment/`: Reads runtime environment configs.
- `persistence/`: DB connection setup and table definitions.
    - `PgxClient.go`, `postgres/migration/`: SQL migrations.
- `readmodel/` and `writemodel/`: Implements read/write model adapters.
    - `Adapter.go`: Concrete implementations for `Port.go`
    - `mapper/`: Translates between DB schema and domain models
    - `queries/`: Query builders

---

### `presentation/`

Handles incoming requests and HTTP responses.

- `rest/`: REST controllers (`Item.go`, `Event.go`, etc.).
- `response/`: HTTP response structures.
- `validator/`: Validation logic.
- `extractor/`: Helpers for extracting data from requests.
- `paginator/`: Pagination and sorting utilities.
- `test/`: Controller-level test helpers or mock tools.

---

### `docker/`

Docker setup for local development and testing.

- `Dockerfile`, `docker-compose.yml`: Service definitions.
- `postgresql/`: Custom PostgreSQL init script.
- `tools/`: Build scripts (e.g., `build.sh`)

---

### `config/`

Contains environment variable files:

- `local.env`
- `test.env`

Used for local development and automated tests.

---

### `docs/`

Swagger documentation:

- `swagger.yaml`, `swagger.json`
- `docs.go`: Boilerplate for embedding docs in Go

---

### `testdata/`

Test fixtures and data generators.

- `item/`: Item-related test data.
- `tenant/`: Tenant-related test data.

---

### Entry Point

- `main.go`: Bootstraps the application (wiring services, adapters, REST).

---

## Workflow Example: Item Projection

This section describes how the `ItemProjection` works within the application, including its interaction with the event store and how it updates the read model.

### 1. Item and Event Definitions

The `core/domain/model/item.go` file contains the definition of the `Item` struct, which represents an event store aggregate with all its fields.

Same directory contains event definitions related to items, such as `Created`, `Renamed`, and `Deleted`. These events are used to track changes to items over time.

Event handlers that are responsible to mutate the state of the `Item` aggregate in the memory (not saving them) should be contained here.

### 2. Saving Events to the Event Store

The `core/application/repository/eventstore/` directory contains the implementation of the event store save methods. We use these methods to persist single or multiple `Item` aggregates and their associated events to the event store.

Example of usage could be found in the `core/application/service/item/` directory, where the `SaveItem` method is called after changes are performed on the `Item` aggregate.

### 3. Item Projection Overview

The `core/application/projection/item/` directory contains the `ItemProjection` struct, which is responsible for processing events related to items.

Event store library detects events related to items and processes them through the `ItemProjection`. The `ItemProjection` listens to events like `Created`, `Renamed`, and `Deleted` and updates the read model accordingly.

The `Execute` method processes a batch of events. It groups events by tenant and processes them in a transaction.

### 4. Updating the Read Model

The `ItemProjection` updates the read model by calling methods on the `writeModel.Port` interface. This interface is implemented by the persistence layer, which interacts with the database to update the state of items.

Persistence layer adapter is responsible for translating the events into database operations, ensuring that the read model reflects the current state of items. It can be found in the `infrastructure/persistence/writemodel/` directory.

For this example we are using postgres, but persistence layer can be easily extended to support other databases or storage systems, as long it implements the `writeModel.Port` interface.

### 5. Using the Read model

The read model is used by the REST API controllers to serve item data to clients. The `presentation/rest/item.go` file contains the controller that handles HTTP requests related to items.

When a client requests item data, the controller retrieves the current state from the read model, which has been updated by the `ItemProjection` based on the events processed from the event store.

Instead of Read model we could load events and aggregates directly from the event store. Downside of this approach are performance issues, as loading and processing events for each request can be slow, especially with large datasets. The read model provides a more efficient way to query and retrieve data.

### 6. Rebuilding the Read Model

If the read model needs to be rebuilt, the `ItemProjection` can replay all events from the event store. This is useful for recovering from data corruption or when changing the read model structure.

Read model method is exposed through the rest API, allowing clients to trigger a rebuild if necessary.
