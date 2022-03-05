using System;
using System.Diagnostics;
using System.Threading.Tasks;
using flt.azf.parallel_csv_to_cosmos.Services;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace flt.azf.parallel_csv_to_cosmos.Functions
{
    public static class ProcessData
    {
        [FunctionName("ProcessData")]
        public static async Task<bool> RunLogic([ActivityTrigger] string filename, ILogger log)
        {
            try
            {
                log.LogInformation($"[ProcessData] Starting process on {filename}.");

                Stopwatch stopwatch = Stopwatch.StartNew();

                var storageManager = new StorageService(log);
                var cosmosManager = new CosmosService(log);

                await cosmosManager.UploadData(await storageManager.TransformCsv(filename));

                stopwatch.Stop();
                TimeSpan ts = stopwatch.Elapsed;
                log.LogInformation($"[ProcessData] Processing data from file {filename} took '{ts.TotalMilliseconds}ms'.");

            }
            catch (Exception ex)
            {
                log.LogError($"[ProcessData] Error on processing {filename}.", ex);
                return false;
            }

            return true;
        }
    }
}
