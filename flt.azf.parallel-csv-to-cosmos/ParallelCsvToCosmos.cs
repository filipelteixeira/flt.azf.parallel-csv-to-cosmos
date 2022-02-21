using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace flt.azf.parallel_csv_to_cosmos
{
    public static class ParallelCsvToCosmos
    {

        private static readonly string ProcessFileName = ConfigurationManager.AppSettings["ProcessFileName"];

        private static readonly string StorageConnectionString = ConfigurationManager.AppSettings["StorageConnectionString"];
        private static readonly string StorageContainerName = ConfigurationManager.AppSettings["StorageContainerName"];

        private static readonly string CosmosAuthorizationKey = ConfigurationManager.AppSettings["CosmosAuthorizationKey"];
        private static readonly string CosmosUrl = ConfigurationManager.AppSettings["CosmosUrl"];
        private static readonly string CosmosDatabaseName = ConfigurationManager.AppSettings["CosmosDatabaseName"];
        private static readonly string CosmosContainerName = ConfigurationManager.AppSettings["CosmosContainerName"];

        [FunctionName("ParallelCsvToCosmos")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger log)
        {
            log.LogInformation($"[ParallelCsvToCosmos] Starting orchestration");

            Stopwatch stopwatch = Stopwatch.StartNew();
            long startMilliseconds = stopwatch.ElapsedMilliseconds;

            log.LogInformation($"[ParallelCsvToCosmos] Processing CSV file");

            var filenames = new StorageManager(log).ProcessCsv(ProcessFileName, StorageConnectionString, StorageContainerName);

            log.LogInformation($"[ParallelCsvToCosmos] Csv file processed, generated {filenames.Count} splits");

            log.LogInformation($"[ParallelCsvToCosmos] Initiating workers");

            // Start workers with multiple files
            var parallelTasks = new List<Task<bool>>();
            for (int i = 0; i < filenames.Count; i++)
            {
                Task<bool> task = context.CallActivityAsync<bool>("ParallelCsvToCosmos_ProcessData", filenames[i]);
                parallelTasks.Add(task);
            }

            await Task.WhenAll(parallelTasks);

            log.LogInformation($"[ParallelCsvToCosmos] Finishing orchestration");

            stopwatch.Stop();
            TimeSpan ts = stopwatch.Elapsed;
            log.LogInformation($"[ParallelCsvToCosmos] Processing data took '{ts.TotalMilliseconds}ms'.");

            return filenames;
        }

        [FunctionName("ParallelCsvToCosmos_ProcessData")]
        public static bool ProcessData([ActivityTrigger] string filename, ILogger log)
        {
            log.LogInformation($"[ParallelCsvToCosmos_ProcessData] Starting process on {filename}.");

            Stopwatch stopwatch = Stopwatch.StartNew();
            long startMilliseconds = stopwatch.ElapsedMilliseconds;

            var storageManager = new StorageManager(log);
            var cosmosManager = new CosmosManager(log, CosmosUrl, CosmosAuthorizationKey, CosmosDatabaseName, CosmosContainerName);

            cosmosManager.UploadData(storageManager.TransformCsv(filename, StorageConnectionString, StorageContainerName));

            stopwatch.Stop();
            TimeSpan ts = stopwatch.Elapsed;
            log.LogInformation($"[ProcessData] Processing file {filename} took '{ts.TotalMilliseconds}ms'.");

            return true;
        }

        [FunctionName("ParallelCsvToCosmos_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("ParallelCsvToCosmos", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}