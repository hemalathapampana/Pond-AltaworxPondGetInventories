### AltaworxPondGetInventories Lambda Flow Documentation

## Overview

The AltaworxPondGetInventories Lambda function synchronizes inventory data from the Pond API into the database. It can initialize an inventory sync session and then process inventory pages in batches using SQS messages.

## HIGH-LEVEL FLOW (Sequential Function Flow)

### Main Entry Point (AltaworxPondGetInventories → 09:00 UTC → 02:30 PM IST daily)

1) FunctionHandler (Entry point)
   - Receives SQS event and Lambda context
   - Initializes base function handler
   - Iterates through SQS records (if any)

2) Initialization Flow (InitializeProcessing = implicit when no ServiceProviderId in message or no SQS event)
   - StartInventorySyncInitialization
   - GetServiceProviderIds
   - GetTotalPagesPerServiceProvider
   - LoadPagesToProcessTable
   - SendPageMessagesToQueue

3) Processing Flow (InitializeProcessing = false; ServiceProviderId present)
   - ProcessInventoryListPage
   - GetPondAuthenticationInformation
   - GetPondInventoryListAsync (paged)
   - SqlBulkCopy (stage records)
   - SendProgressMessageToQueue (page-level success/failure)

4) Utility Functions
   - GetMessageQueueValues
   - InitInventoryDataTable
   - AddInventoryToDataRow
   - GetSqlRetryPolicy
   - SqlBulkCopy

Shape

## LOW-LEVEL FLOW (Detailed Method Explanations)

### FunctionHandler (Main Entry Point)

- Input: SQSEvent sqsEvent, ILambdaContext context
- Purpose: Processes SQS messages to orchestrate inventory synchronization

What happens:
- Initializes KeySys/AMOP Lambda context via base handler
- Reads environment variables (queue URLs, endpoint name, `PAGE_SIZE`)
- If SQS has records: iterate per record; else run initialization path
- For each record:
  - Logs diagnostics
  - Parses attributes with GetMessageQueueValues() (e.g., ServiceProviderId, PageNumber)
  - Routes to StartInventorySyncInitialization() if ServiceProviderId missing; otherwise ProcessInventoryListPage()
- Handles exceptions and calls CleanUp()

### StartInventorySyncInitialization (Initialization Mode)

- Input: AmopLambdaContext context
- Purpose: Seeds the sync process and fans out processing across pages per service provider

What happens:
- Truncate staging tables: executes `POND_TRUNCATE_STAGING`
- GetServiceProviderIds(): retrieves all Pond service providers eligible for sync
- For each ServiceProviderId:
  - GetPondAuthenticationInformation(): fetches Pond credentials/URLs
  - GetTotalPagesPerServiceProvider(): calls first-page list to compute total pages (`ceil(Total / pageSize)`)
  - LoadPagesToProcessTable(): bulk inserts [ServiceProviderId, PageNumber] rows into `POND_GET_INVENTORIES_PAGE_TO_PROCESS`
  - SendPageMessagesToQueue(): enqueues one message per page with attributes `SERVICE_PROVIDER_ID`, `PAGE_NUMBER`

### ProcessInventoryListPage (Processing Mode)

- Input: AmopLambdaContext context, SqsValues sqsValues
- Purpose: Pulls one page of inventory from Pond and stages to DB

What happens:
- Authentication
  - GetPondAuthenticationInformation() from DB
  - Instantiate Pond API service
- API Fetch
  - GetPondInventoryListAsync(pageNumber, pageSize)
  - Endpoint query params: `offset = pageNumber * pageSize`, `count = pageSize`
  - Returns inventory list (elements)
- Transform & Stage
  - Build DataTable via InitInventoryDataTable()
  - For each item, map fields using AddInventoryToDataRow()
  - SqlBulkCopy() into `PondInventoryStaging`
- Progress & Downstream Processing
  - SendProgressMessageToQueue(): SQS to downstream consumer with `IS_SUCCESSFUL`
  - Downstream processor merges staged data to final tables and updates page sync status via DB stored procedures

Shape

## Supporting Functions

- GetMessageQueueValues: Parses SQS attributes (`SERVICE_PROVIDER_ID`, `PAGE_NUMBER`)
- InitInventoryDataTable: Shapes staging schema (Id, Name, DistributorId, ParentInventoryId, OcsGroupId, Type, Status, CreatedDate, ServiceProviderId)
- AddInventoryToDataRow: Safe mapping and type conversions
- GetSqlRetryPolicy: SQL transient retry wrapper
- SqlBulkCopy: Re-usable bulk copy into staging tables
- SendPageMessagesToQueue & SendProgressMessageToQueue: Workflow fan-out and lifecycle messaging

Shape

## Key Dependencies and Integrations

- AwsFunctionBase: logging, config, DB connections, bulk copy, cleanup
- PondRepository: auth retrieval, staging maintenance, page progress stored procedures
- PondApiService: inventory list API calls and page computations
- ServiceProviderRepository: provider enumeration for Pond integration

Shape

## Data Flow Summary

- Initialization: truncate staging; enumerate providers; compute total pages; enqueue one message per page; record pages in `POND_GET_INVENTORIES_PAGE_TO_PROCESS`
- Fetch: pull one page of inventory per message
- Stage: bulk insert to `PondInventoryStaging`
- Advance: send page completion message with `IS_SUCCESSFUL` for downstream merge and progress tracking
- Merge (downstream): merge from staging into canonical tables using stored procedures; update page sync progress

## Scheduling

- Trigger: AWS EventBridge
- Cron: `0 9 * * ? *`
- Time zone: UTC
- Frequency: Daily at 09:00 UTC (02:30 PM IST)

## Messaging

- Get Inventories Queue (seed pages): attributes `SERVICE_PROVIDER_ID`, `PAGE_NUMBER`
- Process Staged Inventories Queue (progress): attributes `SERVICE_PROVIDER_ID`, `PAGE_NUMBER`, `IS_SUCCESSFUL`

## API Request (Inventory List)

- ProductionURL: `https://www.mydashboard.pondmobile.com/ds/u/distributorPPUService/v1`
- Endpoint: `inventory/list`
- Full URL: `{ProductionURL}/{DistributorId}/inventory/list?offset={offset}&count={pageSize}`
- Headers: `Accept: application/json`, `x-api-key: <APIKey>`
- Request body: none (GET)

