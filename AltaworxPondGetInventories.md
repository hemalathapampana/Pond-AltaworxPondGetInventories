### AltaworxPondGetInventories — Integration & Operations Guide

#### 1) Triggers & Scheduling
- **Publisher of initial SQS messages**: This Lambda, when invoked without `ServiceProviderId` or with an empty SQS event, enumerates total pages and enqueues one SQS message per page to the Get-Inventories SQS queue.
- **EventBridge schedule**: Triggered by AWS EventBridge.
  - **Cron**: `0 9 * * ? *`
  - **Time zone**: UTC
  - **Frequency**: Daily at 09:00 UTC
  - **Next runs (examples)**: Fri, 19 Sep 2025 09:00 UTC; Sat, 20 Sep 2025 09:00 UTC (and daily thereafter)

#### 2) Message Handling
- **SQS message attributes (seed/page messages)**:
  - `SERVICE_PROVIDER_ID`
  - `PAGE_NUMBER`
- **SQS message attributes (progress messages)**:
  - `SERVICE_PROVIDER_ID`
  - `PAGE_NUMBER`
  - `IS_SUCCESSFUL`
- **Continuation for pagination**: One SQS message per page; each message processes exactly one page.
- **Manual/default invocation**: If the event has no `SERVICE_PROVIDER_ID`, the Lambda initializes a full run: truncates staging, computes total pages, and enqueues page messages.

#### 3) Batch & Pagination
- **Configured API page size**: Default 10; can be overridden via env var `PAGE_SIZE`.
- **Pagination mechanics**: `offset = pageNumber * pageSize`, `count = pageSize`.
- **Completion determination**: Each page emits a progress message; DB page status is updated by the downstream processor, which determines when all pages are complete.

#### 4) Integration Details (Authentication)
- **Credential source**: DB stored procedure `GET_POND_AUTHENTICATION`; decoded into `PondAuthentication`.
- **Usage**:
  - List endpoints: header `x-api-key: <APIKey>` plus `Accept: application/json`.
  - Token (if needed by other endpoints) supplied via `TokenValue`.

#### 5) Data Handling & Staging
- **Staging tables**:
  - `PondInventoryStaging` (bulk-inserts inventory page results)
  - `POND_GET_INVENTORIES_PAGE_TO_PROCESS` (pages to process)
- **Clearing staging**: At start of initialize run via `POND_TRUNCATE_STAGING`.
- **Final sync to AMOP 2.0 tables**: Performed by downstream “process staged inventories” flow using stored procedures (e.g., `UPDATE_POND_INVENTORY_FROM_STAGING`) after all page data is staged.

#### 6) Error Handling & Retry
- **HTTP retries (Polly)**:
  - Attempts: Config-driven (`CommonConstants.NUMBER_OF_RETRIES`)
  - Backoff: Exponential, delay = `API_ERROR_DELAY_IN_SECONDS^attempt` seconds
  - Retries on exceptions and non-2xx responses; logs details.
- **API failures**: Logged; page marked unsuccessful via progress message. Data load for that page is skipped or empty.
- **Re-enqueue of incomplete jobs**: Not handled by this Lambda; retried on next scheduled run or by downstream processor policy.

#### 7) Failed/Unprocessed Records
- **Validation failures**: This Lambda stages raw inventory items; validation/merging is downstream.
- **Failure logging**: CloudWatch logs; page-level success/failure emitted via progress SQS and recorded in DB by downstream flow.
- **Retry policy**: Via daily schedule or downstream logic; no automatic per-record retry here.

#### 8) Cleanup Processes
- **Retention (DaysToKeep)**: Not implemented in this Lambda.
- **Cleanup batch size (RecordsPerCycle)**: Not implemented in this Lambda.
- **Cleanup logging**: Not applicable.

#### 9) Notifications & Reporting
- **Notifications**: None in this Lambda beyond CloudWatch logs.
- **Sync summary reports**: Not produced here. Progress captured via SQS and DB flags by the downstream processor.

#### 10) External Dependencies (Prerequisites)
- **Environment variables**:
  - `POND_GET_INVENTORIES_QUEUE_URL_VARIABLE_KEY`
  - `POND_PROCESS_STAGED_INVENTORIES_QUEUE_URL_VARIABLE_KEY`
  - `POND_GET_INVENTORY_ENDPOINT_VARIABLE_KEY`
  - `PAGE_SIZE` (optional; default 10)
- **Infrastructure**:
  - EventBridge rule with cron `0 9 * * ? *` (UTC) targeting this Lambda
  - SQS queues for “get inventories” and “process staged inventories”
  - DB connectivity for credentials and staging/final merge
  - Outbound access to Pond API
- **Credentials and API info (as provided)**:
  - **BaseUrl**: `https://www.mydashboard.pondmobile.com/`
  - **ProductionURL**: `https://www.mydashboard.pondmobile.com/ds/u/distributorPPUService/v1`
  - **SandboxURL**: `https://www.mydashboard.pondmobile.com/ds/u/distributorPPUService/v1`
  - **APIKey**: `8de5bfa4-8c8f-4495-85ab-c90d6b0d1ca7`
  - **Username**: `person@altaworx.com`
  - **EncodedPassword**: `M2YxMjUzNzYtNzljZi00N2VlLTk4NTEtNjQyY2MyZWVjNmU4`
  - **TokenValue**: `eyJvcmciOiI2Mjg2MWUxZmY4YjU3ZDAwMDEzNmI1NjkiLCJpZCI6IjU1M2MzYWUwMGU3NjRlMjM4MzYxOWY3OWY4N2I3YWZlIiwiaCI6Im11cm11cjY0In0`

### API Request Details (Inventory List)
- **Endpoint**: `inventory/list`
- **Production GET URL**: `{ProductionURL}/{DistributorId}/inventory/list?offset={offset}&count={pageSize}`
- **Headers**:
  - `Accept: application/json`
  - `x-api-key: <APIKey>`
- **Request body**: None (GET)

```bash
curl -s -X GET \
  "https://www.mydashboard.pondmobile.com/ds/u/distributorPPUService/v1/{DistributorId}/inventory/list?offset=0&count=10" \
  -H "Accept: application/json" \
  -H "x-api-key: 8de5bfa4-8c8f-4495-85ab-c90d6b0d1ca7"
```

