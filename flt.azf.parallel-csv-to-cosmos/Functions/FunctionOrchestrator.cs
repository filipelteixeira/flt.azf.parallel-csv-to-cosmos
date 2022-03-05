using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace flt.azf.parallel_csv_to_cosmos
{
    public static class FunctionOrchestrator
    {
        [FunctionName("ParallelCsvToCosmosOrchestrator")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger log)
        {
            try
            {
                log.LogInformation($"[ParallelCsvToCosmosOrchestrator] Starting orchestration {DateTime.Now}");

                log.LogInformation($"[ParallelCsvToCosmosOrchestrator] Processing CSV file");

                var filenames = await context.CallActivityAsync<List<string>>(
                    "ProcessFile",
                    Environment.GetEnvironmentVariable("ProcessFileName", EnvironmentVariableTarget.Process));

                log.LogInformation($"[ParallelCsvToCosmosOrchestrator] Initiating workers");

                // Start workers with multiple files
                int batchSize = 5;
                for (int batchIndex = 0; (batchIndex * batchSize) < filenames.Count; batchIndex++)
                {
                    log.LogInformation($"[ParallelCsvToCosmosOrchestrator] Running batch: {batchIndex + 1}/{filenames.Count/batchSize}");

                    var parallelTasks = new List<Task<bool>>();
                    for (int fileIndex = batchIndex * batchSize; fileIndex < ((batchIndex+1) * batchSize) && fileIndex < filenames.Count; fileIndex++)
                    {
                        Task<bool> task = context.CallActivityAsync<bool>("ProcessData", filenames[fileIndex]);
                        parallelTasks.Add(task);
                    }

                    var results = await Task.WhenAll(parallelTasks);
                }

                log.LogInformation($"[ParallelCsvToCosmosOrchestrator] Finishing orchestration {DateTime.Now}");

                return filenames;
            }
            catch (Exception ex)
            {
                log.LogError($"[ParallelCsvToCosmosOrchestrator] Failed orchestration execution with error: {ex.Message}");
                return null;
            }
        }
    }
}