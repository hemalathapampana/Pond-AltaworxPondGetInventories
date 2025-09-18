### AltaworxPondGetInventories Lambda Flow Documentation

## Overview
The `AltaworxPondGetInventories` Lambda function synchronizes inventory data from the Pond API into the database. It can initialize an inventory sync session (seeding page work) and then process inventory pages in batches via SQS messages.

## HIGH-LEVEL FLOW (Sequential Function Flow)

### Main Entry Point
- `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
  - Receives SQS event and Lambda context
  - Initializes base function handler
  - Iterates through SQS records and routes per-message

### Initialization Flow (ServiceProviderId not supplied in SQS message)
- `InitializeSyncInventoryProcess`
  - `TruncateStagingTables` (staging reset)
  - `GetAllServiceProviderIds(IntegrationType.Pond)`
  - For each Service Provider (SP):
    - `GetPondAuthentication`
    - `TryGetTotalPageCount` via Pond API (`PondGetInventoryEndpoint`, `PageSize`)
    - `LoadPagesToProcessTable` (seed page markers)
    - `InitGetInventoryPages` (enqueue one SQS message per page)
- Shape

### Processing Flow (ServiceProviderId supplied in SQS message)
- `ProcessSyncPageByServiceProviderId`
  - `GetPondAuthentication`
  - Instantiate `PondApiService`
  - `SyncInventory`
    - `GetSinglePageListFromPondAPIAsync` (paged fetch)
    - `LoadInventoryToStagingTable` (bulk copy to staging)
    - `CheckSyncInventoryStepProgress` (emit progress/completion message)
- Shape

## LOW-LEVEL FLOW (Detailed Method Explanations)

### FunctionHandler (Main Entry Point)
- Input: `SQSEvent sqsEvent`, `ILambdaContext context`
- Purpose: Processes SQS messages to orchestrate inventory synchronization

What happens:
- Initializes `AmopLambdaContext` via `BaseAmopFunctionHandler()`
- Reads environment variables via `TryGetAllEnvironmentVariables()`:
  - `POND_GET_INVENTORIES_QUEUE_URL` (`PondHelper.CommonString.POND_GET_INVENTORIES_QUEUE_URL_VARIABLE_KEY`)
  - `POND_PROCESS_STAGED_INVENTORIES_QUEUE_URL` (`PondHelper.CommonString.POND_PROCESS_STAGED_INVENTORIES_QUEUE_URL_VARIABLE_KEY`)
  - `POND_GET_INVENTORY_ENDPOINT` (`PondHelper.CommonString.POND_GET_INVENTORY_ENDPOINT_VARIABLE_KEY`)
  - `PAGE_SIZE` (`PondHelper.CommonString.PAGE_SIZE`, default `PondHelper.CommonConfig.DEFAULT_PAGE_SIZE`)
- Ensures SQS trigger validity and iterates each record
- For each record:
  - Logs diagnostics
  - Parses attributes with `GetMessageValues()`
    - `ServiceProviderId` (required for processing mode)
    - `PageNumber` (required for processing mode)
    - `IsSuccessful` (used in downstream stage processing queue)
  - If `ServiceProviderId <= 0` or missing: routes to `InitializeSyncInventoryProcess()`
  - Else: routes to `ProcessSyncPageByServiceProviderId()`
- Handles exceptions and calls `CleanUp()`

### InitializeSyncInventoryProcess (Initialization Mode)
- Input: `AmopLambdaContext context`, `ServiceProviderRepository serviceProviderRepository`
- Purpose: Seeds the sync process and fans out processing across pages

What happens:
- Reset staging via `pondRepository.TruncateStagingTables`
- Retrieve all Pond service provider IDs via `GetAllServiceProviderIds`
- For each `serviceProviderId`:
  - Retrieve auth via `pondRepository.GetPondAuthentication`
  - Call API once to get total page count via `pondApiService.TryGetTotalPageCount<T>` using `PondGetInventoryEndpoint`
  - `LoadPagesToProcessTable(context, serviceProviderId, totalPages)` seeds DB page markers (`POND_GET_INVENTORIES_PAGE_TO_PROCESS`)
  - For `page` in `[0, totalPages)`:
    - `InitGetInventoryPages(context, serviceProviderId, page)` enqueues SQS message to `GetInventoriesQueueURL`
- Shape

### ProcessSyncPageByServiceProviderId (Processing Mode)
- Input: `AmopLambdaContext context`, `SqsValues sqsValues`
- Purpose: Pulls one page of inventories from Pond and loads to staging

What happens:
- Retrieve auth via `pondRepository.GetPondAuthentication`
- Create `PondApiService`
- `SyncInventory(context, sqsValues, sqlTransientRetryPolicy, pondApiService)`
  - Calls `GetSinglePageListFromPondAPIAsync<PondInventoryItem, PondInventoryListResponse>`:
    - Calculates `offset = pageNumber * PageSize`
    - Fetches from Pond via `GetPondListAsync<PondInventoryListResponse>(HttpClientSingleton.Instance, PondGetInventoryEndpoint, offset, PageSize)`
    - Extracts list via `response => response.Elements`
    - `LoadInventoryToStagingTable` builds a `DataTable` and executes `SqlBulkCopy` to `PondInventoryStaging`
    - On each page, calls `CheckSyncInventoryStepProgress` with `IsSuccessful`, which emits an SQS message to `ProcessStagedInventoriesQueueURL` for downstream processing
- Shape

## Utility Functions
- `GetMessageValues`
  - Parses SQS attributes from the incoming message into `SqsValues`
  - Attributes used (via `SQSMessageKeyConstant`):
    - `SERVICE_PROVIDER_ID`
    - `PAGE_NUMBER`
    - `IS_SUCCESSFUL` (used for progress/completion signaling downstream)

- `TryGetAllEnvironmentVariables`
  - Reads Lambda, API, and sync configuration from environment variables:
    - `POND_GET_INVENTORIES_QUEUE_URL`
    - `POND_PROCESS_STAGED_INVENTORIES_QUEUE_URL`
    - `POND_GET_INVENTORY_ENDPOINT`
    - `PAGE_SIZE`

- `InitializeRepositories`
  - Instantiates `PondRepository` and `ServiceProviderRepository` using `CentralDbConnectionString`

- `LoadInventoryToStagingTable`
  - Shapes DataTable schema with columns: `Id`, `Name`, `Distributor_Id`, `Parent_Inventory_Id`, `Ocs_Group_Id`, `Type`, `Status`, `CreatedDate`, `ServiceProviderId`
  - Executes `SqlBulkCopy` into `DatabaseTableNames.PondInventoryStaging`

- `LoadPagesToProcessTable`
  - Builds DataTable with `PageNumber` and `ServiceProviderId` for all pages
  - Executes `SqlBulkCopy` into `DatabaseTableNames.POND_GET_INVENTORIES_PAGE_TO_PROCESS`

- `InitGetInventoryPages`
  - Sends an SQS message per page to `GetInventoriesQueueURL` with attributes:
    - `SERVICE_PROVIDER_ID`
    - `PAGE_NUMBER`

- `CheckSyncInventoryStepProgress`
  - Sends an SQS message to `ProcessStagedInventoriesQueueURL` with attributes:
    - `SERVICE_PROVIDER_ID`
    - `PAGE_NUMBER`
    - `IS_SUCCESSFUL`

- `SyncInventory`
  - Orchestrates single-page fetch, staging load, and progress signaling using retry policy

## Key Dependencies and Integrations
- `AwsFunctionBase`: logging, config, DB connections, bulk copy, cleanup
- `PondRepository`: DB CRUD for Pond sync, staging, and progress tracking
- `PondApiService`: list API calls, query param construction, request building
- `ServiceProviderRepository`: service provider enumeration and metadata
- `EnvironmentRepository`: environment variable access
- `SqsService`: SQS message publishing
- `RetryPolicyHelper`: SQL transient retry policy
- `HttpClientSingleton` and `HttpRequestFactory`: HTTP client and request construction
- Shape

## Data Flow Summary
- Initialization: seed page markers per service provider and enqueue SQS messages per page
- Fetch: pull one page of inventories from Pond using `offset = pageNumber * pageSize`
- Stage: bulk insert to `PondInventoryStaging`
- Advance: emit progress messages to `ProcessStagedInventoriesQueueURL` for downstream processing
- Note: Page-to-process tracking is staged into `POND_GET_INVENTORIES_PAGE_TO_PROCESS`; downstream components can update progress via repository methods (e.g., `UpdateInventoriesPageStatusAndCheckSyncProgress`) as applicable
- Shape