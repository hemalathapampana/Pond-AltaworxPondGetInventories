## AltaworxPondGetInventories Lambda — Flow & Operations Guide

### Overview

The `AltaworxPondGetInventories` Lambda synchronizes inventory data from the Pond API into the database. It supports two modes:

- **Initialization mode**: Seeds a sync session by truncating staging, determining total pages per Service Provider (SP), recording pages to process, and enqueuing one SQS message per page.
- **Processing mode**: Processes a single page for a specific SP by fetching inventories from Pond, bulk-loading to staging, and emitting a progress SQS message for downstream processing.


## Architecture & Scheduling

### Trigger

- **AWS EventBridge** schedule: daily at 09:00 UTC
  - Cron: `0 9 * * ? *`
  - Example runs: Fri, 19 Sep 2025 09:00 UTC; Sat, 20 Sep 2025 09:00 UTC
- **SQS**: The Lambda also consumes SQS messages (both page seed messages and progress messages routing info).

### Queues

- Get-Inventories queue: provided by env var `POND_GET_INVENTORIES_QUEUE_URL`
- Process-Staged-Inventories queue: provided by env var `POND_PROCESS_STAGED_INVENTORIES_QUEUE_URL`


## Environment Variables

- `POND_GET_INVENTORIES_QUEUE_URL` (key: `PondHelper.CommonString.POND_GET_INVENTORIES_QUEUE_URL_VARIABLE_KEY`)
- `POND_PROCESS_STAGED_INVENTORIES_QUEUE_URL` (key: `PondHelper.CommonString.POND_PROCESS_STAGED_INVENTORIES_QUEUE_URL_VARIABLE_KEY`)
- `POND_GET_INVENTORY_ENDPOINT` (key: `PondHelper.CommonString.POND_GET_INVENTORY_ENDPOINT_VARIABLE_KEY`)
- `PAGE_SIZE` (key: `PondHelper.CommonString.PAGE_SIZE`, default: `PondHelper.CommonConfig.DEFAULT_PAGE_SIZE`, typically 10)


## SQS Message Schemas

### Page seed / processing messages (sent to Get-Inventories queue)

- Attributes:
  - `SERVICE_PROVIDER_ID` (number, required for processing; missing/≤0 triggers initialization)
  - `PAGE_NUMBER` (number, 0-based; required for processing)

### Progress messages (sent to Process-Staged-Inventories queue)

- Attributes:
  - `SERVICE_PROVIDER_ID` (number)
  - `PAGE_NUMBER` (number)
  - `IS_SUCCESSFUL` (boolean as string, e.g., "true"/"false")


## High-Level Flow

### Main entry point

1. Receive `SQSEvent` and `ILambdaContext`.
2. Initialize Lambda base context and environment configuration.
3. For each SQS record:
   - Parse attributes via `GetMessageValues()`.
   - If `ServiceProviderId` is missing or ≤ 0 → `InitializeSyncInventoryProcess()`.
   - Else → `ProcessSyncPageByServiceProviderId()`.
4. Handle errors with logging and cleanup.

### Initialization flow (no `ServiceProviderId`)

1. `TruncateStagingTables`.
2. `GetAllServiceProviderIds(IntegrationType.Pond)`.
3. For each SP:
   - `GetPondAuthentication`.
   - `TryGetTotalPageCount` using Pond API and configured `PAGE_SIZE`.
   - `LoadPagesToProcessTable(serviceProviderId, totalPages)`.
   - `InitGetInventoryPages(serviceProviderId, page)` for each page `[0..totalPages)` to enqueue a page message.

### Processing flow (has `ServiceProviderId`)

1. `GetPondAuthentication`.
2. Create `PondApiService`.
3. `SyncInventory(context, sqsValues, retryPolicy, pondApiService)`:
   - `GetSinglePageListFromPondAPIAsync` (offset = `pageNumber * pageSize`).
   - `LoadInventoryToStagingTable` via bulk copy.
   - `CheckSyncInventoryStepProgress` to enqueue downstream progress message.


## Detailed Functions & Methods

### FunctionHandler (Main Entry Point)

```csharp
Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
```

- **Purpose**: Orchestrates inventory synchronization by consuming SQS events.
- **Inputs**:
  - `SQSEvent sqsEvent`: Batch of SQS records.
  - `ILambdaContext context`: AWS Lambda execution context for logging, tracing, and timeouts.
- **Behavior**:
  - Initializes base context via `BaseAmopFunctionHandler()` and loads settings via `TryGetAllEnvironmentVariables()`.
  - Validates SQS trigger payloads.
  - For each record:
    - Logs diagnostics and parses attributes with `GetMessageValues()` into `SqsValues`.
    - Routes to `InitializeSyncInventoryProcess()` if `ServiceProviderId` missing/≤0; otherwise `ProcessSyncPageByServiceProviderId()`.
  - Catches and logs exceptions; always calls `CleanUp()`.
- **Outputs/Side-effects**: SQS enqueues (initialization or progress), DB staging resets, logs.
- **Error handling**: Try/catch around per-message processing; failures logged with correlation to SQS `MessageId`.
- **Idempotency**: Initialization truncates staging; page processing writes to staging and emits progress; downstream idempotency expected.


### InitializeSyncInventoryProcess (Initialization Mode)

```csharp
Task InitializeSyncInventoryProcess(AmopLambdaContext context, ServiceProviderRepository serviceProviderRepository)
```

- **Purpose**: Seed a fresh run by discovering pages per SP and enqueueing work.
- **Inputs**:
  - `AmopLambdaContext context`: Shared runtime context.
  - `ServiceProviderRepository serviceProviderRepository`: Data access for SP enumeration.
- **Behavior**:
  - `pondRepository.TruncateStagingTables()` to reset all staging.
  - Enumerate SPs with `GetAllServiceProviderIds(IntegrationType.Pond)`.
  - For each SP:
    - Resolve credentials via `pondRepository.GetPondAuthentication()`.
    - Get `totalPages` via `pondApiService.TryGetTotalPageCount(PondGetInventoryEndpoint, pageSize)`.
    - `LoadPagesToProcessTable(context, serviceProviderId, totalPages)` into `POND_GET_INVENTORIES_PAGE_TO_PROCESS`.
    - For each page in `[0, totalPages)` → `InitGetInventoryPages(context, serviceProviderId, page)` to enqueue SQS messages.
- **Outputs/Side-effects**: Truncation of staging tables; SQS page messages; page tracking entries in DB.
- **Error handling**: Logs failures per SP; skips SPs with missing credentials or page count fetch errors.
- **Idempotency**: Truncation ensures clean slate; re-running reseeds page markers and messages.


### ProcessSyncPageByServiceProviderId (Processing Mode)

```csharp
Task ProcessSyncPageByServiceProviderId(AmopLambdaContext context, SqsValues sqsValues)
```

- **Purpose**: Process a single page for the given SP.
- **Inputs**:
  - `AmopLambdaContext context`: Runtime context.
  - `SqsValues sqsValues`: Contains `ServiceProviderId`, `PageNumber`, and optionally `IsSuccessful`.
- **Behavior**:
  - Retrieve credentials via `pondRepository.GetPondAuthentication()`.
  - Instantiate `PondApiService` with auth and HTTP client.
  - Execute `SyncInventory(context, sqsValues, sqlTransientRetryPolicy, pondApiService)`.
- **Outputs/Side-effects**: Bulk inserts into `PondInventoryStaging`; emits progress SQS message.
- **Error handling**: Retry transient SQL using `RetryPolicyHelper`; HTTP retries via Polly inside `PondApiService`.
- **Idempotency**: Bulk load may insert duplicates if run multiple times; downstream merge should de-duplicate based on natural keys.


### SyncInventory

```csharp
Task SyncInventory(AmopLambdaContext context, SqsValues sqsValues, IAsyncPolicy sqlTransientRetryPolicy, PondApiService pondApiService)
```

- **Purpose**: Orchestrate fetch, stage, and signal for one page.
- **Inputs**: `context`, `sqsValues` (SP + page), SQL retry policy, `pondApiService`.
- **Behavior**:
  - Calls `GetSinglePageListFromPondAPIAsync<PondInventoryItem, PondInventoryListResponse>` with calculated offset and count.
  - `LoadInventoryToStagingTable` using `SqlBulkCopy` under `sqlTransientRetryPolicy`.
  - `CheckSyncInventoryStepProgress` to emit progress with `IS_SUCCESSFUL` set according to the page outcome.
- **Failure paths**:
  - If API returns non-2xx or throws, mark `IS_SUCCESSFUL=false` and skip/empty load.
  - If bulk copy fails, log and still emit failure progress.


### GetSinglePageListFromPondAPIAsync

```csharp
Task<List<PondInventoryItem>> GetSinglePageListFromPondAPIAsync<PondInventoryItem, PondInventoryListResponse>(
    PondApiService pondApiService,
    string endpoint,
    int pageNumber,
    int pageSize)
```

- **Purpose**: Fetch one page of inventories from Pond.
- **Inputs**: `endpoint` (from `POND_GET_INVENTORY_ENDPOINT`), `pageNumber`, `pageSize`.
- **Behavior**:
  - Compute `offset = pageNumber * pageSize`.
  - Call `pondApiService.GetPondListAsync<PondInventoryListResponse>(HttpClientSingleton.Instance, endpoint, offset, pageSize)`.
  - Extract list from response via `response => response.Elements`.
- **Outputs**: List of `PondInventoryItem`.
- **Errors**: HTTP exceptions and non-2xx handled via Polly; errors bubble up to be recorded as page failures.


### LoadInventoryToStagingTable

```csharp
Task LoadInventoryToStagingTable(IEnumerable<PondInventoryItem> items, int serviceProviderId)
```

- **Purpose**: Bulk insert page results into staging table.
- **Schema** (DataTable columns):
  - `Id`, `Name`, `Distributor_Id`, `Parent_Inventory_Id`, `Ocs_Group_Id`, `Type`, `Status`, `CreatedDate`, `ServiceProviderId`.
- **Behavior**:
  - Map `items` to a `DataTable` with the above schema.
  - Execute `SqlBulkCopy` into `DatabaseTableNames.PondInventoryStaging`.
- **Errors**: Retries on transient SQL failures via `RetryPolicyHelper`. Logs and rethrows/records failures per page.


### LoadPagesToProcessTable

```csharp
Task LoadPagesToProcessTable(AmopLambdaContext context, int serviceProviderId, int totalPages)
```

- **Purpose**: Seed page markers so downstream can track per-page status.
- **Behavior**:
  - Build `DataTable` with rows: `{ PageNumber, ServiceProviderId }` for all pages `[0..totalPages)`.
  - Bulk copy into `DatabaseTableNames.POND_GET_INVENTORIES_PAGE_TO_PROCESS`.
- **Notes**: This table enables progress tracking and resumability by downstream components.


### InitGetInventoryPages

```csharp
Task InitGetInventoryPages(AmopLambdaContext context, int serviceProviderId, int pageNumber)
```

- **Purpose**: Enqueue one page-processing SQS message per discovered page.
- **Behavior**:
  - Publish to `GetInventoriesQueueURL` with attributes:
    - `SERVICE_PROVIDER_ID = serviceProviderId`
    - `PAGE_NUMBER = pageNumber`
- **Errors**: Logs failures to enqueue; those pages will be retried on next schedule if not processed.


### CheckSyncInventoryStepProgress

```csharp
Task CheckSyncInventoryStepProgress(AmopLambdaContext context, int serviceProviderId, int pageNumber, bool isSuccessful)
```

- **Purpose**: Notify downstream that a page has completed (success or failure).
- **Behavior**:
  - Publish to `ProcessStagedInventoriesQueueURL` with attributes:
    - `SERVICE_PROVIDER_ID`
    - `PAGE_NUMBER`
    - `IS_SUCCESSFUL`
- **Downstream**: A separate processor updates DB page status (e.g., `UpdateInventoriesPageStatusAndCheckSyncProgress`).


### GetMessageValues

```csharp
SqsValues GetMessageValues(SQSMessage message)
```

- **Purpose**: Parse message attributes into a strongly typed structure.
- **Attributes parsed** (via `SQSMessageKeyConstant`):
  - `SERVICE_PROVIDER_ID`
  - `PAGE_NUMBER`
  - `IS_SUCCESSFUL` (optional; used by downstream progress chain if present)
- **Validation**: If `ServiceProviderId` is missing or ≤ 0, the caller treats it as initialization mode.


### TryGetAllEnvironmentVariables

```csharp
bool TryGetAllEnvironmentVariables(out EnvironmentConfig config)
```

- **Purpose**: Load and validate all required environment variables.
- **Expected keys**:
  - `POND_GET_INVENTORIES_QUEUE_URL`
  - `POND_PROCESS_STAGED_INVENTORIES_QUEUE_URL`
  - `POND_GET_INVENTORY_ENDPOINT`
  - `PAGE_SIZE` (optional; default used if not set)
- **Behavior**: Reads via `EnvironmentRepository`; applies defaults and logs configuration.


### InitializeRepositories

```csharp
void InitializeRepositories(string centralDbConnectionString)
```

- **Purpose**: Construct repository and service instances needed by the Lambda.
- **Constructs**:
  - `PondRepository`
  - `ServiceProviderRepository`
  - `SqsService`
  - Any DB access helpers used by bulk copy and stored procedures


## Data Flow Summary

- **Initialization**: Truncate staging → compute total pages per SP → seed page markers → enqueue page messages.
- **Fetch**: For each page message, call Pond API using `offset = pageNumber * pageSize`, `count = pageSize`.
- **Stage**: Bulk insert page items to `PondInventoryStaging`.
- **Advance**: Emit progress messages for downstream to merge staged data into final tables and to update page status.


## External Integrations

### Authentication & Credentials

- Credentials are retrieved from DB via stored procedure `GET_POND_AUTHENTICATION` and materialized as `PondAuthentication`.
- Usage for list endpoints: HTTP header `x-api-key: <APIKey>`, header `Accept: application/json`.
- Token (if required by other endpoints) is available via `TokenValue`.

### Pond API Endpoints

- Base URL: `https://www.mydashboard.pondmobile.com/`
- Production base: `https://www.mydashboard.pondmobile.com/ds/u/distributorPPUService/v1`
- Sandbox base: `https://www.mydashboard.pondmobile.com/ds/u/distributorPPUService/v1`
- Inventory list path: `inventory/list`

Request pattern:

```text
GET {ProductionBase}/{DistributorId}/inventory/list?offset={offset}&count={pageSize}
Headers:
  Accept: application/json
  x-api-key: <APIKey>
Body: none
```

Example curl:

```bash
curl -s -X GET \
  "https://www.mydashboard.pondmobile.com/ds/u/distributorPPUService/v1/{DistributorId}/inventory/list?offset=0&count=10" \
  -H "Accept: application/json" \
  -H "x-api-key: 8de5bfa4-8c8f-4495-85ab-c90d6b0d1ca7"
```

Note: Replace `{DistributorId}` with the authenticated distributor identifier. Never hard-code secrets; use the DB-provided credentials at runtime.


## Database Objects

- `DatabaseTableNames.PondInventoryStaging`:
  - Bulk-loaded staging table for raw inventory rows from Pond.
  - Expected columns: `Id`, `Name`, `Distributor_Id`, `Parent_Inventory_Id`, `Ocs_Group_Id`, `Type`, `Status`, `CreatedDate`, `ServiceProviderId`.

- `DatabaseTableNames.POND_GET_INVENTORIES_PAGE_TO_PROCESS`:
  - Per-page tracking for the current run, with `PageNumber` and `ServiceProviderId`.
  - Updated by downstream processors to reflect completion per page.

- Stored procedures (downstream usage examples):
  - `UPDATE_POND_INVENTORY_FROM_STAGING` (merge staged into final AMOP tables)
  - `POND_TRUNCATE_STAGING` (called during initialize)
  - `GET_POND_AUTHENTICATION` (resolve API credentials)


## Error Handling & Retry

- **HTTP Retries (Polly)**:
  - Attempts: `CommonConstants.NUMBER_OF_RETRIES` (config-driven)
  - Backoff: exponential (delay = `API_ERROR_DELAY_IN_SECONDS^attempt` seconds)
  - Retries on exceptions and non-2xx; logs request/response diagnostics without leaking secrets.

- **SQL Retries**:
  - Transient failures retried via `RetryPolicyHelper` around `SqlBulkCopy` and DB interactions.

- **Page Failure**:
  - On API or DB failure, emit progress with `IS_SUCCESSFUL=false`.
  - No automatic re-enqueue by this Lambda; rely on next scheduled run or downstream policies.


## Operational Runbook

- **Manual/Default Invocation**:
  - Invoke Lambda with an empty SQS event or without `SERVICE_PROVIDER_ID` to trigger initialization flow.

- **Pagination Mechanics**:
  - `offset = pageNumber * pageSize`
  - `count = pageSize`

- **Monitoring**:
  - CloudWatch Logs: per-message diagnostics, API call results, bulk copy status.
  - SQS DLQ (if configured): monitor for failed publications.
  - DB: page-to-process table status via downstream repository methods.

- **Common Issues**:
  - Credential failures: ensure `GET_POND_AUTHENTICATION` returns valid `APIKey`/`DistributorId`.
  - Network egress blocked: verify VPC/Subnet/SG allow outbound to Pond API.
  - Page size misconfiguration: ensure `PAGE_SIZE` aligns with API limits and performance requirements.


## Security & Compliance

- Store secrets only in secure stores (DB via stored procedures, AWS Secrets Manager), never in code or logs.
- Mask credentials in logs; do not emit `APIKey`, `TokenValue`, or passwords.
- Use least-privilege IAM for SQS publish/consume and DB access.


## Reference: Example Credentials Payload (Do Not Hard-Code)

These values are typically sourced at runtime from DB. Example values were provided for documentation; treat them as secrets and do not commit to public repositories.

- `APIKey`: `8de5bfa4-8c8f-4495-85ab-c90d6b0d1ca7`
- `Username`: `person@altaworx.com`
- `EncodedPassword`: `M2YxMjUzNzYtNzljZi00N2VlLTk4NTEtNjQyY2MyZWVjNmU4`
- `TokenValue`: `eyJvcmciOiI2Mjg2MWUxZmY4YjU3ZDAwMDEzNmI1NjkiLCJpZCI6IjU1M2MzYWUwMGU3NjRlMjM4MzYxOWY3OWY4N2I3YWZlIiwiaCI6Im11cm11cjY0In0`


## Appendix: Sequence Summaries

### Initialization Sequence

- EventBridge triggers Lambda
- `FunctionHandler` detects no `ServiceProviderId`
- `TruncateStagingTables`
- For each SP:
  - `GetPondAuthentication`
  - `TryGetTotalPageCount`
  - `LoadPagesToProcessTable`
  - Loop pages → `InitGetInventoryPages`

### Processing Sequence

- SQS page message consumed
- `GetPondAuthentication`
- Instantiate `PondApiService`
- `SyncInventory`
  - `GetSinglePageListFromPondAPIAsync`
  - `LoadInventoryToStagingTable`
  - `CheckSyncInventoryStepProgress`


## Quick Reference: Key Classes & Utilities

- `AwsFunctionBase`: logging, config, DB connections, bulk copy helpers, cleanup.
- `PondRepository`: CRUD for Pond sync (auth retrieval, staging ops, progress tracking).
- `PondApiService`: request construction, list pagination helpers with retry.
- `ServiceProviderRepository`: enumerate SPs and associated metadata.
- `EnvironmentRepository`: read environment variables.
- `SqsService`: publish SQS messages with attributes.
- `RetryPolicyHelper`: Polly-based transient retry policies for SQL and HTTP.
- `HttpClientSingleton` and `HttpRequestFactory`: HTTP client reuse and request creation.


## Validation Checklist (Per-Deployment)

- EventBridge rule present with cron `0 9 * * ? *` in UTC.
- SQS queues exist and IAM allows publish/consume.
- DB connectivity configured (VPC, SG, subnet, secret/connection string).
- `POND_GET_INVENTORY_ENDPOINT` set (e.g., `inventory/list`).
- `PAGE_SIZE` configured (default 10) if override desired.
- Outbound access to `mydashboard.pondmobile.com` allowed.