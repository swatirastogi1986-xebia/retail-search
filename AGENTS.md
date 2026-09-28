# AGENTS.md — Product & Inventory Service

## 1. Stack

| Technology | Role |
|---|---|
| Java 21 (LTS) | Primary language; use virtual threads (Project Loom) for I/O-bound paths |
| Spring Boot 3.3.x | Application framework; web, data, actuator, validation modules |
| Spring Data JPA / Hibernate 6 | ORM for PostgreSQL writes and transactional reads |
| Spring Data Elasticsearch 5.x | Elasticsearch repository abstraction and index management |
| Spring Data Redis | Cache-aside pattern; Lettuce driver with connection pooling |
| Spring Kafka | Kafka producer integration; transactional outbox event publishing |
| PostgreSQL 15 | Primary RDBMS; source of truth for catalog and inventory records |
| Elasticsearch 8.x | Full-text and faceted search over product and inventory data |
| Redis 7 | Read-through cache for catalog and inventory hot paths |
| Apache Kafka | Domain event streaming (product.created, product.updated, inventory.changed) |
| Debezium (Outbox CDC) | Captures outbox table changes and relays events to Kafka reliably |
| Flyway | Database schema versioning and migration |
| MapStruct 1.5 | Compile-time DTO ↔ domain model mapping |
| OpenTelemetry Java Agent | Distributed tracing and metrics export |
| Micrometer + Prometheus | Application metrics exposition via `/actuator/prometheus` |
| Testcontainers | Integration tests against real PostgreSQL, Redis, Elasticsearch, Kafka |
| JUnit 5 + Mockito | Unit and slice testing |
| ArchUnit | Enforce layered architecture rules in tests |
| Docker + Docker Compose | Local development environment |
| Kubernetes | Production orchestration; Deployment, Service, HPA, ConfigMap, Secret |
| Google Cloud Storage (GCS) | Product image and binary asset storage via `google-cloud-storage` SDK |
| Lombok | Boilerplate reduction (builders, loggers); no field injection |
| SpringDoc OpenAPI 3 | Auto-generated API documentation at `/swagger-ui.html` |

---

## 2. Project Structure

```
product-inventory-service/
├── AGENTS.md                          # This file
├── tasks.md                           # Agent-generated task checklist (created before coding)
├── README.md                          # Human-readable setup and usage guide
├── pom.xml                            # Maven multi-module root POM
├── .env.example                       # Environment variable template (never commit .env)
├── .gitignore
├── checkstyle.xml                     # Google Java Style checkstyle rules
├── docker/
│   ├── Dockerfile                     # Multi-stage production image
│   ├── docker-compose.yml             # Full local stack (app + all dependencies)
│   ├── docker-compose.test.yml        # Testcontainers-compatible override for CI
│   └── init/
│       └── postgres-init.sql          # DB user/extension bootstrap (uuid-ossp, pg_trgm)
├── k8s/
│   ├── deployment.yaml                # Kubernetes Deployment with resource limits
│   ├── service.yaml                   # ClusterIP Service
│   ├── hpa.yaml                       # HorizontalPodAutoscaler
│   ├── configmap.yaml                 # Non-secret config
│   └── secret.yaml                    # Secret template (values injected by CI)
├── scripts/
│   ├── run-local.sh                   # Start docker-compose stack
│   └── seed-data.sh                   # Load sample catalog and inventory data
└── src/
    ├── main/
    │   ├── java/com/company/productinventory/
    │   │   ├── ProductInventoryApplication.java   # @SpringBootApplication entry point
    │   │   ├── config/
    │   │   │   ├── AsyncConfig.java               # Virtual thread executor configuration
    │   │   │   ├── CacheConfig.java               # Redis CacheManager, TTL per cache name
    │   │   │   ├── ElasticsearchConfig.java        # ES client, index settings, analyzers
    │   │   │   ├── FlywayConfig.java               # Flyway baseline and callback config
    │   │   │   ├── GcsConfig.java                  # Google Cloud Storage client bean
    │   │   │   ├── KafkaProducerConfig.java        # Producer factory, transactional template
    │   │   │   ├── OpenApiConfig.java              # SpringDoc metadata and security schemes
    │   │   │   ├── OpenTelemetryConfig.java        # OTEL SDK resource attributes
    │   │   │   ├── PersistenceConfig.java          # DataSource, JPA properties, auditing
    │   │   │   └── SecurityConfig.java             # Spring Security; JWT/API-key filter chain
    │   │   ├── domain/
    │   │   │   ├── product/
    │   │   │   │   ├── Product.java                # @Entity: SKU, name, barcode, category, attributes
    │   │   │   │   ├── ProductAttribute.java       # @Embeddable: key-value attribute pairs
    │   │   │   │   ├── ProductStatus.java          # Enum: ACTIVE, INACTIVE, DISCONTINUED
    │   │   │   │   └── Category.java               # @Entity: hierarchical category tree
    │   │   │   └── inventory/
    │   │   │       ├── InventoryRecord.java        # @Entity: SKU + location composite key
    │   │   │       ├── InventoryLocation.java      # @Entity: store/warehouse location
    │   │   │       ├── StockStatus.java            # Enum: ON_HAND, RESERVED, IN_TRANSIT, AVAILABLE
    │   │   │       └── InventoryReservation.java   # @Entity: reservation lifecycle record
    │   │   ├── repository/
    │   │   │   ├── product/
    │   │   │   │   ├── ProductRepository.java      # JpaRepository + custom JPQL queries
    │   │   │   │   └── CategoryRepository.java
    │   │   │   └── inventory/
    │   │   │       ├── InventoryRepository.java    # JpaRepository; lock-aware reservation queries
    │   │   │       └── InventoryReservationRepository.java
    │   │   ├── search/
    │   │   │   ├── document/
    │   │   │   │   ├── ProductDocument.java        # @Document: ES product index schema
    │   │   │   │   └── InventoryDocument.java      # @Document: ES inventory index schema
    │   │   │   ├── repository/
    │   │   │   │   ├── ProductSearchRepository.java   # ElasticsearchRepository
    │   │   │   │   └── InventorySearchRepository.java
    │   │   │   └── service/
    │   │   │       └── ProductSearchService.java   # Full-text + faceted query builder
    │   │   ├── service/
    │   │   │   ├── product/
    │   │   │   │   ├── ProductService.java         # Catalog CRUD, audit logging, cache eviction
    │   │   │   │   └── ProductImageService.java    # GCS upload/download/delete orchestration
    │   │   │   └── inventory/
    │   │   │       ├── InventoryService.java       # Inventory queries, cache-aside reads
    │   │   │       └── InventoryReservationService.java  # Reserve/release with optimistic lock
    │   │   ├── outbox/
    │   │   │   ├── OutboxEvent.java               # @Entity: outbox table record
    │   │   │   ├── OutboxRepository.java           # JpaRepository for outbox table
    │   │   │   └── OutboxEventPublisher.java       # Writes to outbox within same transaction
    │   │   ├── event/
    │   │   │   ├── ProductCreatedEvent.java        # Domain event payload (record)
    │   │   │   ├── ProductUpdatedEvent.java
    │   │   │   └── InventoryChangedEvent.java
    │   │   ├── api/
    │   │   │   ├── product/
    │   │   │   │   ├── ProductController.java      # REST: /api/v1/products
    │   │   │   │   ├── ProductRequest.java         # Validated request DTO
    │   │   │   │   └── ProductResponse.java        # Response DTO (no entity leakage)
    │   │   │   ├── inventory/
    │   │   │   │   ├── InventoryController.java    # REST: /api/v1/inventory
    │   │   │   │   ├── ReservationRequest.java
    │   │   │   │   └── InventoryResponse.java
    │   │   │   └── search/
    │   │   │       ├── SearchController.java       # REST: /api/v1/search
    │   │   │       └── SearchRequest.java          # Facet + full-text query params
    │   │   ├── mapper/
    │   │   │   ├── ProductMapper.java              # MapStruct: Product ↔ DTO ↔ Document
    │   │   │   └── InventoryMapper.java
    │   │   ├── audit/
    │   │   │   ├── AuditLog.java                  # @Entity: admin write audit trail
    │   │   │   ├── AuditLogRepository.java
    │   │   │   └── AuditAspect.java               # @Around AOP aspect for admin mutations
    │   │   ├── cache/
    │   │   │   └── CacheNames.java                # Constants: PRODUCT_BY_SKU, INVENTORY_BY_SKU, etc.
    │   │   ├── exception/
    │   │   │   ├── GlobalExceptionHandler.java    # @RestControllerAdvice; RFC 7807 ProblemDetail
    │   │   │   ├── ProductNotFoundException.java
    │   │   │   ├── InsufficientStockException.java
    │   │   │   └── DuplicateSkuException.java
    │   │   └── health/
    │   │       ├── ElasticsearchHealthIndicator.java  # Custom ES health check
    │   │       └── KafkaHealthIndicator.java
    │   └── resources/
    │       ├── application.yml                    # Base configuration
    │       ├── application-local.yml              # Local overrides
    │       ├── application-test.yml               # Test profile (Testcontainers URLs)
    │       ├── application-prod.yml               # Production profile (env var references)
    │       ├── db/migration/
    │       │   ├── V1__create_product_tables.sql
    │       │   ├── V2__create_inventory_tables.sql
    │       │   ├── V3__create_outbox_table.sql
    │       │   ├── V4__create_audit_log_table.sql
    │       │   └── V5__add_indexes_and_constraints.sql
    │       └── elasticsearch/
    │           ├── product-index-settings.json    # Analyzer, tokenizer, mapping config
    │           └── inventory-index-settings.json
    └── test/
        ├── java/com/company/productinventory/
        │   ├── unit/
        │   │   ├── service/
        │   │   │   ├── ProductServiceTest.java
        │   │   │   ├── InventoryServiceTest.java
        │   │   │   └── InventoryReservationServiceTest.java
        │   │   ├── outbox/
        │   │   │   └── OutboxEventPublisherTest.java
        │   │   ├── mapper/
        │   │   │   └── ProductMapperTest.java
        │   │   └── arch/
        │   │       └── ArchitectureTest.java      # ArchUnit layering rules
        │   ├── integration/
        │   │   ├── BaseIntegrationTest.java        # Shared Testcontainers setup
        │   │   ├── product/
        │   │   │   ├── ProductRepositoryIT.java
        │   │   │   └── ProductControllerIT.java    # @SpringBootTest + MockMvc
        │   │   ├── inventory/
        │   │   │   ├── InventoryRepositoryIT.java
        │   │   │   └── InventoryReservationIT.java # Concurrency + locking tests
        │   │   ├── search/
        │   │   │   └── ProductSearchIT.java        # ES index/query integration test
        │   │   ├── cache/
        │   │   │   └── CacheAsideIT.java           # Redis cache hit/miss verification
        │   │   └── outbox/
        │   │       └── OutboxCdcIT.java            # Debezium relay end-to-end test
        │   └── contract/
        │       └── ProductInventoryContractTest.java  # Spring Cloud Contract or Pact consumer tests
        └── resources/
            ├── application-test.yml
            └── testdata/
                ├── products.json
                └── inventory.json
```

---

## 3. Required Workflow

The agent **must** follow these steps in order. Do not skip or reorder.

### Step 1 — Read All Specifications
- Read every `.md` file in the repository root and any `docs/` directory.
- Read all OpenAPI spec files if present.
- Identify all domain entities, API contracts, event schemas, and business rules before writing any code.

### Step 2 — Create `tasks.md`
- Create `tasks.md` in the repository root before writing any implementation code.
- Break work into atomic, checkable tasks grouped by layer: Database → Domain → Repository → Service → Outbox → API → Search → Cache → Tests → Docker → CI.
- Each task must have a checkbox: `- [ ] Task description`.
- Update each checkbox to `- [x]` as the task is completed.

### Step 3 — Implement Database Layer First
- Write all Flyway migrations in `src/main/resources/db/migration/` before creating JPA entities.
- Migration filenames must follow the pattern `V{n}__{description}.sql` exactly.
- Enable `uuid-ossp` and `pg_trgm` extensions in `V1`.
- Use `UUID` as primary key type for all entities.
- Add `created_at`, `updated_at` (managed by `@EntityListeners(AuditingEntityListener.class)`), and `version` (optimistic lock) columns to every mutable entity.

### Step 4 — Implement Domain and Repository Layer
- Implement JPA entities strictly matching the migration schema.
- No business logic in entities beyond invariant guards (e.g., quantity cannot go negative).
- Repositories expose only the query methods required by services; no open-ended `findAll()` without pagination.

### Step 5 — Implement Service Layer
- Services own all business logic and transaction boundaries (`@Transactional`).
- `InventoryReservationService` must use `SELECT ... FOR UPDATE` (via `@Lock(LockModeType.PESSIMISTIC_WRITE)`) for reservation and release operations.
- Every admin write operation must call `AuditAspect` (via AOP — do not inline audit calls in service methods).
- After every successful write, publish to the outbox table within the **same** transaction; never publish directly to Kafka from a service method.

### Step 6 — Implement Outbox and Event Layer
- `OutboxEvent` entity stores: `id` (UUID), `aggregateType`, `aggregateId`, `eventType`, `payload` (JSONB), `createdAt`, `processed` (boolean).
- Debezium monitors the `outbox_events` table via CDC; the agent must provide a `debezium-connector-config.json` in `docker/`.
- Event payload records (`ProductCre