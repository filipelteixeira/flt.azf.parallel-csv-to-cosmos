using flt.azf.parallel_csv_to_cosmos.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace flt.azf.parallel_csv_to_cosmos.Services;

public class CosmosService
{
    private readonly ILogger log;

    private static readonly string authorizationKey = Environment.GetEnvironmentVariable("CosmosAuthorizationKey", EnvironmentVariableTarget.Process);
    private static readonly string endpointUrl = Environment.GetEnvironmentVariable("CosmosUrl", EnvironmentVariableTarget.Process);
    private static readonly string databaseName = Environment.GetEnvironmentVariable("CosmosDatabaseName", EnvironmentVariableTarget.Process);
    private static readonly string containerName = Environment.GetEnvironmentVariable("CosmosContainerName", EnvironmentVariableTarget.Process);

    private CosmosClient _client;
    private Database _database;
    private Container _container;

    public CosmosService(ILogger log)
    {
        this.log = log;

        // Init Cosmos connection
        log.LogInformation($"[CosmosService] Connecting to CosmosDB");
        _client = new CosmosClient(endpointUrl, authorizationKey, new CosmosClientOptions() { AllowBulkExecution = true });
        _database = _client.GetDatabase(databaseName);
        _container = _database.GetContainer(containerName);
    }

    public async Task<bool> UploadData(List<DataModel> data)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();

        log.LogInformation($"[CosmosService.UploadData] Loading tasks for uploading {data.Count} records");

        int batchSize = 100;
        int itemCount = 0;
        for(int batch = 0; batchSize * batch < data.Count; batch++)
        {
            var tasks = new List<Task<bool>>();
            for (int entry = batch * batchSize; entry < ((batch + 1) * batchSize) && entry < data.Count; entry++)
            {
                var task = UpsertItemAsync(data[entry]);
                tasks.Add(task);
            }

            var results = await Task.WhenAll(tasks.ToArray());
            itemCount += results.Where(result => true).Count();
        }

        stopwatch.Stop();
        TimeSpan ts = stopwatch.Elapsed;
        log.LogInformation($"[CosmosService.UploadData] Succesfuly added {itemCount}/{data.Count} records to database in {ts.TotalMilliseconds}ms");
        if(itemCount != data.Count)
        {
            log.LogWarning($"[CosmosService.UpsertItemAsync] Failed to insert {data.Count-itemCount} items.");
        }
        return true;
    }

    private async Task<bool> UpsertItemAsync(DataModel item)
    {
        try
        {
            var response = await _container.UpsertItemAsync(item);
            return response?.StatusCode == System.Net.HttpStatusCode.Created;
        }
        catch (Exception)
        {
            // log.LogError($"[CosmosService.UpsertItemAsync] Error on upserting item: {ex.Message[..100]}...");
            // May include item in retry list
            return false;
        }
    }
}
