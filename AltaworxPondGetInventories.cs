using System.Data;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Services.SQS;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Helpers.Pond;
using Amop.Core.Models;
using Amop.Core.Models.Pond;
using Amop.Core.Repositories;
using Amop.Core.Repositories.Environment;
using Amop.Core.Repositories.Pond;
using Amop.Core.Services.Http;
using Amop.Core.Services.Pond;
using Microsoft.Data.SqlClient;
using Polly;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxPondGetInventories;

public class Function : AwsFunctionBase
{
    private int PageSize;
    // SQS Queue URL that is connected to the AltaworxPondGetDevices lambda
    private string? GetInventoriesQueueURL;
    private string? ProcessStagedInventoriesQueueURL;
    private string? PondGetInventoryEndpoint;
    protected PondRepository pondRepository;
    protected SqsService sqsService = new SqsService();
    protected ServiceProviderRepository serviceProviderRepository;
    private readonly HttpRequestFactory _httpRequestFactory = new HttpRequestFactory();
    private readonly EnvironmentRepository _environmentRepo = new EnvironmentRepository();
    public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
    {
        AmopLambdaContext? lambdaContext = null;
        try
        {
            lambdaContext = BaseAmopFunctionHandler(context);
            ArgumentNullException.ThrowIfNull(lambdaContext);

            InitializeRepositories(lambdaContext);

            TryGetAllEnvironmentVariables(lambdaContext);

            await ProcessEventAsync(lambdaContext, sqsEvent);
        }
        catch (Exception ex)
        {
            if (lambdaContext == null)
            {
                context.Logger.Log(CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }
            else
            {
                LogInfo(lambdaContext, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }
        }

        base.CleanUp(lambdaContext);
    }

    protected void InitializeRepositories(AmopLambdaContext lambdaContext)
    {
        pondRepository = new PondRepository(lambdaContext.CentralDbConnectionString);
        serviceProviderRepository = new ServiceProviderRepository(lambdaContext.CentralDbConnectionString);
    }

    protected void TryGetAllEnvironmentVariables(AmopLambdaContext lambdaContext)
    {
        // Lambda related configurations
        GetInventoriesQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, _environmentRepo, PondHelper.CommonString.POND_GET_INVENTORIES_QUEUE_URL_VARIABLE_KEY);
        ProcessStagedInventoriesQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, _environmentRepo, PondHelper.CommonString.POND_PROCESS_STAGED_INVENTORIES_QUEUE_URL_VARIABLE_KEY);
        // API related configurations
        PondGetInventoryEndpoint = GetStringValueFromEnvironmentVariable(lambdaContext.Context, _environmentRepo, PondHelper.CommonString.POND_GET_INVENTORY_ENDPOINT_VARIABLE_KEY);
        // Sync logic related configurations
        PageSize = GetIntValueFromEnvironmentVariable(lambdaContext, _environmentRepo,
            PondHelper.CommonString.PAGE_SIZE,
            PondHelper.CommonConfig.DEFAULT_PAGE_SIZE);
    }

    private SqsValues GetMessageValues(AmopLambdaContext context, SQSEvent.SQSMessage message)
    {
        return new SqsValues(context, message);
    }

    private async Task ProcessEventAsync(AmopLambdaContext context, SQSEvent sqsEvent)
    {
        LogInfo(context, CommonConstants.SUB);
        if (sqsEvent?.Records != null)
        {
            var processedRecordCount = sqsEvent.Records.Count;
            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.BEGINNING_PROCESS, processedRecordCount));
            foreach (var record in sqsEvent.Records)
            {
                LogInfo(context, CommonConstants.INFO, $"MessageId: {record.MessageId}");
                var sqsValues = GetMessageValues(context, record);
                if (sqsValues.ServiceProviderId <= 0)
                {
                    // No service provider id provided -> Initialize sync process using sqs message
                    await InitializeSyncInventoryProcess(context, serviceProviderRepository);
                }
                else
                {
                    // Run for the current service provider id (specified in the SQS Message)
                    await ProcessSyncPageByServiceProviderId(context, sqsValues);
                }
            }
        }
        else
        {
            await InitializeSyncInventoryProcess(context, serviceProviderRepository);
        }
    }

    protected async Task InitializeSyncInventoryProcess(AmopLambdaContext context, ServiceProviderRepository serviceProviderRepository)
    {
        // Clean staging table
        var errorMessages = new List<string>();
        var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(context.logger, errorMessages);
        sqlTransientRetryPolicy.Execute(() => pondRepository.TruncateStagingTables(ParameterizedLog(context)));
        var serviceProviderIds = serviceProviderRepository.GetAllServiceProviderIds(ParameterizedLog(context), IntegrationType.Pond);
        if (serviceProviderIds?.Count > 0)
        {
            foreach (var serviceProviderId in serviceProviderIds)
            {
                var pondAuth = pondRepository.GetPondAuthentication(ParameterizedLog(context), context.Base64Service, serviceProviderId);
                if (pondAuth == null)
                {
                    LogInfo(context, CommonConstants.ERROR, string.Format(LogCommonStrings.SERVICE_PROVIDER_NO_AUTH_INFO, serviceProviderId));
                    continue;
                }

                var pondApiService = new PondApiService(pondAuth, _httpRequestFactory, context.IsProduction);
                // Call API once to get total pages
                var totalPages = await pondApiService.TryGetTotalPageCount<PondBillingGroupItem>(ParameterizedLog(context), PondGetInventoryEndpoint, PageSize);
                if (totalPages > 0)
                {
                    LoadPagesToProcessTable(context, serviceProviderId, totalPages);
                    // Page number start from 0 since the API need to be query by offset, which start by 0
                    // (first item of page = offset + pageNumber * pageSize)
                    for (var i = 0; i < totalPages; i++)
                    {
                        await InitGetInventoryPages(context, serviceProviderId, i);
                    }
                }
            }
        }
        else
        {
            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.NO_SERVICE_PROVIDER_FOUND, CommonConstants.POND_CARRIER_NAME));
        }
    }

    protected async Task ProcessSyncPageByServiceProviderId(AmopLambdaContext context, SqsValues sqsValues)
    {
        try
        {
            var errorMessages = new List<string>();
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(context.logger, errorMessages);
            var pondAuth = pondRepository.GetPondAuthentication(ParameterizedLog(context), context.Base64Service, sqsValues.ServiceProviderId);
            if (pondAuth == null)
            {
                LogInfo(context, CommonConstants.ERROR, string.Format(LogCommonStrings.SERVICE_PROVIDER_NO_AUTH_INFO, sqsValues.ServiceProviderId));
                return;
            }

            var httpRequestFactory = new HttpRequestFactory();
            var pondApiService = new PondApiService(pondAuth, httpRequestFactory, context.IsProduction);
            await SyncInventory(context, sqsValues, sqlTransientRetryPolicy, pondApiService);
        }
        catch (Exception ex)
        {
            LogInfo(context, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
        }
    }

    protected async Task SyncInventory(AmopLambdaContext context, SqsValues sqsValues, ISyncPolicy syncPolicy, PondApiService pondApiService)
    {
        await pondApiService.GetSinglePageListFromPondAPIAsync<PondInventoryItem, PondInventoryListResponse>(ParameterizedLog(context), syncPolicy, sqsValues.PageNumber, PageSize,
            (offset, pageSize) => pondApiService.GetPondListAsync<PondInventoryListResponse>(HttpClientSingleton.Instance, PondGetInventoryEndpoint, offset, pageSize),
            (response) => response.Elements,
            (response) => LoadInventoryToStagingTable(context, response, sqsValues.ServiceProviderId),
            async (pageNumber, isSuccess) => await CheckSyncInventoryStepProgress(context, sqsValues.ServiceProviderId, pageNumber, isSuccess));
    }

    protected static void LoadInventoryToStagingTable(AmopLambdaContext context, List<PondInventoryItem> pondInventoryList, int serviceProviderId)
    {
        LogInfo(context, CommonConstants.SUB);
        var pondInventoryTable = new DataTable();
        pondInventoryTable.Columns.Add(CommonColumnNames.Id, typeof(int));
        pondInventoryTable.Columns.Add(CommonColumnNames.Name, typeof(string));
        pondInventoryTable.Columns.Add(CommonColumnNames.Distributor_Id, typeof(int));
        pondInventoryTable.Columns.Add(CommonColumnNames.Parent_Inventory_Id, typeof(int));
        pondInventoryTable.Columns.Add(CommonColumnNames.Ocs_Group_Id, typeof(int));
        pondInventoryTable.Columns.Add(CommonColumnNames.Type, typeof(string));
        pondInventoryTable.Columns.Add(CommonColumnNames.Status, typeof(string));
        pondInventoryTable.Columns.Add(CommonColumnNames.CreatedDate, typeof(DateTime));
        pondInventoryTable.Columns.Add(CommonColumnNames.ServiceProviderId, typeof(int));

        foreach (var pondInventory in pondInventoryList)
        {
            var pondInventoryRow = pondInventoryTable.NewRow();
            pondInventoryRow[CommonColumnNames.Id] = pondInventory.Id;
            pondInventoryRow[CommonColumnNames.Name] = pondInventory.Name;
            pondInventoryRow[CommonColumnNames.Distributor_Id] = pondInventory.DistributorId;
            pondInventoryRow[CommonColumnNames.Parent_Inventory_Id] = pondInventory.ParentInventoryId;
            pondInventoryRow[CommonColumnNames.Ocs_Group_Id] = pondInventory.OcsGroupId;
            pondInventoryRow[CommonColumnNames.Type] = pondInventory.Type;
            pondInventoryRow[CommonColumnNames.Status] = pondInventory.Status;
            pondInventoryRow[CommonColumnNames.CreatedDate] = DateTime.UtcNow;
            pondInventoryRow[CommonColumnNames.ServiceProviderId] = serviceProviderId;
            pondInventoryTable.Rows.Add(pondInventoryRow);
        }

        List<SqlBulkCopyColumnMapping> columnMappings = SQLBulkCopyHelper.AutoMapColumns(pondInventoryTable);
        SqlBulkCopy(context, context.CentralDbConnectionString, pondInventoryTable, DatabaseTableNames.PondInventoryStaging, columnMappings);
    }

    protected static void LoadPagesToProcessTable(AmopLambdaContext context, int serviceProviderId, int totalPages)
    {
        LogInfo(context, CommonConstants.SUB);
        var pondInventoryTable = new DataTable();
        pondInventoryTable.Columns.Add(CommonColumnNames.PageNumber, typeof(int));
        pondInventoryTable.Columns.Add(CommonColumnNames.ServiceProviderId, typeof(int));

        for (var i = 0; i < totalPages; i++)
        {
            var pondInventoryRow = pondInventoryTable.NewRow();
            pondInventoryRow[CommonColumnNames.PageNumber] = i;
            pondInventoryRow[CommonColumnNames.ServiceProviderId] = serviceProviderId;
            pondInventoryTable.Rows.Add(pondInventoryRow);
        }

        List<SqlBulkCopyColumnMapping> columnMappings = SQLBulkCopyHelper.AutoMapColumns(pondInventoryTable);
        SqlBulkCopy(context, context.CentralDbConnectionString, pondInventoryTable, DatabaseTableNames.POND_GET_INVENTORIES_PAGE_TO_PROCESS, columnMappings);
    }

    private async Task InitGetInventoryPages(AmopLambdaContext context, int serviceProviderId, int currentPage)
    {
        // Insert all the pages to table
        var attributes = new Dictionary<string, string>()
        {
            {SQSMessageKeyConstant.SERVICE_PROVIDER_ID, serviceProviderId.ToString()},
            {SQSMessageKeyConstant.PAGE_NUMBER, currentPage.ToString()},
        };
        await sqsService.SendSQSMessage(ParameterizedLog(context), AwsCredentials(context), GetInventoriesQueueURL, attributes);
    }

    private async Task CheckSyncInventoryStepProgress(AmopLambdaContext context, int serviceProviderId, int currentPage, bool isSuccess)
    {
        var attributes = new Dictionary<string, string>()
        {
            {SQSMessageKeyConstant.SERVICE_PROVIDER_ID, serviceProviderId.ToString()},
            {SQSMessageKeyConstant.PAGE_NUMBER, currentPage.ToString()},
            {SQSMessageKeyConstant.IS_SUCCESSFUL, isSuccess.ToString()},
        };

        await sqsService.SendSQSMessage(ParameterizedLog(context), AwsCredentials(context), ProcessStagedInventoriesQueueURL, attributes);
    }
}using System;

public class Class1
{
	public Class1()
	{
	}
}
