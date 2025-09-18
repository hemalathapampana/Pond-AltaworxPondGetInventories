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
    - `pondRepository.GetPondAuthentication`
    - `pondApiService.TryGetTotalPageCount<T>` via Pond API (`PondGetInventoryEndpoint`, `PageSize`)
    - `LoadPagesToProcessTable` (seed page markers)
    - `InitGetInventoryPages` (enqueue one SQS message per page)
- Shape

### Processing Flow (ServiceProviderId supplied in SQS message)
- `ProcessSyncPageByServiceProviderId`
  - `pondRepository.GetPondAuthentication`
  - Instantiate `PondApiService`
  - `SyncInventory`
    - `GetSinglePageListFromPondAPIAsync` (paged fetch)
    - `LoadInventoryToStagingTable` (bulk copy to staging)
    - `CheckSyncInventoryStepProgress` (emit progress/completion message to downstream processor)
- Shape

## LOW-LEVEL FLOW (Detailed Method Explanations)

### FunctionHandler (Main Entry Point)
- Input: `SQSEvent sqsEvent`, `ILambdaContext context`
- Purpose: Processes SQS messages to orchestrate inventory synchronization

What happens:
- Initializes `AmopLambdaContext` via `BaseAmopFunctionHandler()`
- Calls `TryGetAllEnvironmentVariables()` to load configuration
- Ensures SQS trigger validity and iterates each record
- For each record:
  - Logs diagnostics
  - Parses attributes with `GetMessageValues()` including `ServiceProviderId`, `PageNumber`
  - If `ServiceProviderId <= 0` or missing: routes to `InitializeSyncInventoryProcess()`
  - Else: routes to `ProcessSyncPageByServiceProviderId()`
- Handles exceptions and calls `CleanUp()`

### TryGetAllEnvironmentVariables (Configuration)
- Input: `AmopLambdaContext lambdaContext`
- Purpose: Reads Lambda, API, and sync configuration from environment variables

What happens:
- Lambda related configurations:
  - `GetInventoriesQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, _environmentRepo, PondHelper.CommonString.POND_GET_INVENTORIES_QUEUE_URL_VARIABLE_KEY)`
  - `ProcessStagedInventoriesQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, _environmentRepo, PondHelper.CommonString.POND_PROCESS_STAGED_INVENTORIES_QUEUE_URL_VARIABLE_KEY)`
- API related configurations:
  - `PondGetInventoryEndpoint = GetStringValueFromEnvironmentVariable(lambdaContext.Context, _environmentRepo, PondHelper.CommonString.POND_GET_INVENTORY_ENDPOINT_VARIABLE_KEY)`
- Sync logic related configurations:
  - `PageSize = GetIntValueFromEnvironmentVariable(lambdaContext, _environmentRepo, PondHelper.CommonString.PAGE_SIZE, PondHelper.CommonConfig.DEFAULT_PAGE_SIZE)`
- Shape

### InitializeSyncInventoryProcess (Initialization Mode)
- Input: `AmopLambdaContext context`, `ServiceProviderRepository serviceProviderRepository`
- Purpose: Seeds the sync process and fans out processing across pages

What happens:
- Reset staging via `pondRepository.TruncateStagingTables`
- Retrieve all Pond service provider IDs via `serviceProviderRepository.GetAllServiceProviderIds(IntegrationType.Pond)`
- For each `serviceProviderId`:
  - Retrieve auth via `pondRepository.GetPondAuthentication(ParameterizedLog(context), context.Base64Service, serviceProviderId)`
  - Call API once to get total page count via `pondApiService.TryGetTotalPageCount<PondBillingGroupItem>(ParameterizedLog(context), PondGetInventoryEndpoint, PageSize)`
  - `LoadPagesToProcessTable(context, serviceProviderId, totalPages)` seeds DB page markers (`DatabaseTableNames.POND_GET_INVENTORIES_PAGE_TO_PROCESS`)
  - For `page` in `[0, totalPages)`:
    - `InitGetInventoryPages(context, serviceProviderId, page)` enqueues SQS message to `GetInventoriesQueueURL` with attributes `SERVICE_PROVIDER_ID`, `PAGE_NUMBER`
- Shape

### ProcessSyncPageByServiceProviderId (Processing Mode)
- Input: `AmopLambdaContext context`, `SqsValues sqsValues`
- Purpose: Pulls one page of inventories from Pond and loads to staging

What happens:
- Retrieve auth via `pondRepository.GetPondAuthentication(ParameterizedLog(context), context.Base64Service, sqsValues.ServiceProviderId)`
- Create `PondApiService`
- `SyncInventory(context, sqsValues, sqlTransientRetryPolicy, pondApiService)`
  - Calls `GetSinglePageListFromPondAPIAsync<PondInventoryItem, PondInventoryListResponse>(...)`:
    - Calculates `offset = pageNumber * PageSize`
    - Fetches from Pond via `pondApiService.GetPondListAsync<PondInventoryListResponse>(HttpClientSingleton.Instance, PondGetInventoryEndpoint, offset, PageSize)`
    - Extracts list via `(response) => response.Elements`
    - `LoadInventoryToStagingTable` builds a `DataTable` and executes `SqlBulkCopy` to `DatabaseTableNames.PondInventoryStaging`
    - For each page processed, `CheckSyncInventoryStepProgress(context, serviceProviderId, pageNumber, isSuccess)` sends a message to `ProcessStagedInventoriesQueueURL`
- Shape

### GetPondListAsync (API Fetch Core)
- Input: `HttpClient httpClient`, `string endpoint`, `int offset`, `int pageSize`
- Purpose: Calls Pond list endpoint with pagination

What happens:
- Determines base URI from `PondAuthentication` and environment (prod/sandbox)
- Builds query string with `BuildQueryParamGetInventoryList(offset, pageSize)` ⇒ `offset`, `count`
- URL: `{baseUri}/{DistributorId}/{endpoint}?{queryString}`
- Builds request message: `BuildRequestMessage(apiUrl, GET)`
- Sends request and deserializes into `T` (e.g., `PondInventoryListResponse`); logs error body on non-2xx
- Shape

### UpdateInventoriesPageStatusAndCheckSyncProgress (Progress Tracking)
- Input: `Action<string, string> logFunction`, `int serviceProviderId`, `int pageNumber`, `bool isSuccessful`
- Purpose: Updates page sync status in DB and returns remaining or status marker

What happens:
- Executes stored procedure `POND_UPDATE_INVENTORIES_PAGE_SYNC_PROGRESS` with parameters `ServiceProviderId`, `PageNumber`, `IsSuccessful`
- Returns `int` (e.g., remaining pages or a status code; default `-1` on failure)
- Note: This method is typically invoked by the downstream processor that receives messages from `ProcessStagedInventoriesQueueURL` after staging work completes
- Shape

## Utility Functions
- `GetMessageValues`
  - Parses SQS attributes into `SqsValues` for `SERVICE_PROVIDER_ID`, `PAGE_NUMBER`
- `LoadInventoryToStagingTable`
  - Shapes `DataTable` with columns: `Id`, `Name`, `Distributor_Id`, `Parent_Inventory_Id`, `Ocs_Group_Id`, `Type`, `Status`, `CreatedDate`, `ServiceProviderId`
  - Executes `SqlBulkCopy` into `DatabaseTableNames.PondInventoryStaging`
- `LoadPagesToProcessTable`
  - Builds `DataTable` with `PageNumber`, `ServiceProviderId` for all pages; bulk inserts into `DatabaseTableNames.POND_GET_INVENTORIES_PAGE_TO_PROCESS`
- `InitGetInventoryPages`
  - Sends SQS message to `GetInventoriesQueueURL` per page with attributes `SERVICE_PROVIDER_ID`, `PAGE_NUMBER`
- `CheckSyncInventoryStepProgress`
  - Sends SQS message to `ProcessStagedInventoriesQueueURL` with attributes `SERVICE_PROVIDER_ID`, `PAGE_NUMBER`, `IS_SUCCESSFUL`
- Shape

## Key Dependencies and Integrations
- `AwsFunctionBase`: logging, config, DB connections, bulk copy, cleanup
- `PondRepository`: auth retrieval, staging management, progress tracking
- `PondApiService`: list API, token/URL composition, request construction
- `ServiceProviderRepository`: provider enumeration and integration scoping
- `EnvironmentRepository`: environment variable access
- `SqsService`: SQS message publishing
- `RetryPolicyHelper` / `ISyncPolicy`: SQL transient retry policy
- `HttpClientSingleton` and `HttpRequestFactory`: HTTP client and request construction
- Shape

## Data Flow Summary
- Initialization: seed page markers per service provider and enqueue SQS messages per page
- Fetch: pull one page of inventories from Pond using `offset = pageNumber * pageSize`
- Stage: bulk insert to `PondInventoryStaging`
- Advance: emit progress messages to `ProcessStagedInventoriesQueueURL`
- Track: downstream processing can call `UpdateInventoriesPageStatusAndCheckSyncProgress` to mark page completion
- Shape