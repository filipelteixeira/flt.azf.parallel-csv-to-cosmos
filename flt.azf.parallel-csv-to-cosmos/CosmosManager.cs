using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace flt.azf.parallel_csv_to_cosmos;

public class CosmosManager
{
    private readonly ILogger log;

    private readonly string endpointUrl;
    private readonly string authorizationKey;
    private readonly string databaseName;
    private readonly string containerName;

    public CosmosManager(ILogger log, string endpointUrl, string authorizationKey, string databaseName, string containerName)
    {
        this.log = log;
        this.endpointUrl = endpointUrl;
        this.authorizationKey = authorizationKey;
        this.databaseName = databaseName;
        this.containerName = containerName;
    }

    public bool UploadData(List<DataModel> data)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();
        long startMilliseconds = stopwatch.ElapsedMilliseconds;

        log.LogInformation($"[CosmosManager.UploadData] Connecting to CosmosDB");

        var cosmosClient = new CosmosClient(endpointUrl, authorizationKey, new CosmosClientOptions() { AllowBulkExecution = true });
        var database = cosmosClient.GetDatabase(databaseName);

        var container = database.GetContainer(containerName);

        log.LogInformation($"[CosmosManager.UploadData] Loading tasks for uploading {data.Count} records");

        var tasks = new List<Task>();

        foreach(var item in data)
        {
            tasks.Add(container.UpsertItemAsync(item));
        }

        log.LogInformation($"[CosmosManager.UploadData] Waiting for tasks");
        Task.WhenAll(tasks.ToArray()).Wait();

        stopwatch.Stop();
        TimeSpan ts = stopwatch.Elapsed;
        log.LogInformation($"[CosmosManager.UploadData] Added {data.Count} records to database in {ts.TotalMilliseconds}ms");
        return true;
    }
}
