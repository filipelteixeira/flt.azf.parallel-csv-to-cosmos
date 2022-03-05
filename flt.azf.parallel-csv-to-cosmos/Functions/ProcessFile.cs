using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using flt.azf.parallel_csv_to_cosmos.Services;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace flt.azf.parallel_csv_to_cosmos.Functions
{
    public static class ProcessFile
    {
        [FunctionName("ProcessFile")]
        public static async Task<List<string>> RunLogic([ActivityTrigger] string filename, ILogger log)
        {
            try
            {
                log.LogInformation($"[ProcessFile] Starting process on {filename}.");
                Stopwatch stopwatch = Stopwatch.StartNew();

                var filenames = await new StorageService(log).ProcessCsv(filename);

                log.LogInformation($"[ProcessFile] Csv file processed, generated {filenames.Count} splits");

                stopwatch.Stop();
                TimeSpan ts = stopwatch.Elapsed;
                log.LogInformation($"[ProcessFile] Processing file {filename} took '{ts.TotalMilliseconds}ms'.");

                return filenames;
            }
            catch (Exception ex)
            {
                log.LogError($"[ProcessFile] Error in processing file {filename}.", ex);
                return new List<string>();
            }
        }
    }
}
