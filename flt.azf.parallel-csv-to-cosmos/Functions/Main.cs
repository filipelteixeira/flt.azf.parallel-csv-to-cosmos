using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace flt.azf.parallel_csv_to_cosmos.Functions
{
    public class Main
    {
        [FunctionName("Main_Trigger")]
        public static async Task<HttpResponseMessage> RunMain(
            [HttpTrigger(AuthorizationLevel.Function, methods: "post", Route = "orchestrators/{functionName}/{instanceId}")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            string functionName,
            string instanceId,
            ILogger log)
        {
            log.LogInformation($"[Main] Starting trigger Main with ID:{instanceId} at {DateTime.UtcNow.ToLongTimeString()}");

            // Check if an instance with the specified ID already exists or an existing one stopped running(completed/failed/terminated).
            var existingInstance = await starter.GetStatusAsync(instanceId);
            if (existingInstance == null
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Completed
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Failed
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Terminated)
            {
                // An instance with the specified ID doesn't exist or an existing one stopped running, create one.
                await starter.StartNewAsync(functionName, instanceId);
                log.LogInformation($"[Main] Started orchestration with ID = '{instanceId}'.");
                return starter.CreateCheckStatusResponse(req, instanceId);
            }
            else
            {
                // An instance with the specified ID exists or an existing one still running, don't create one.
                return new HttpResponseMessage(HttpStatusCode.Conflict)
                {
                    Content = new StringContent($"[Main] An instance with ID '{instanceId}' already exists."),
                };
            }
        }

        //[FunctionName("Test_Trigger")]
        //public static async Task RunTest(
        //    [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
        //    [DurableClient] IDurableOrchestrationClient starter,
        //    ILogger log)
        //{
        //    log.LogInformation($"[Test_Trigger] Starting time trigger on Main at {DateTime.UtcNow.ToShortTimeString()}");

        //    //string instanceId = await starter.StartNewAsync("ParallelCsvToCosmosOrchestrator");

        //    log.LogInformation($"[ParallelCsvToCosmos_Trigger] Finished execution");
        //}

        //[FunctionName("Stop_Trigger")]
        //public static async Task RunStop(
        //    [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
        //    [DurableClient] IDurableOrchestrationClient client,
        //    ILogger log)
        //{
        //    try
        //    {
        //        var noFilter = new OrchestrationStatusQueryCondition();
        //        OrchestrationStatusQueryResult result = await client.ListInstancesAsync(
        //            noFilter,
        //            CancellationToken.None);
        //        var instances = result.DurableOrchestrationState.Where(instance => instance.RuntimeStatus != OrchestrationRuntimeStatus.Failed &&
        //                instance.RuntimeStatus != OrchestrationRuntimeStatus.Completed &&
        //                instance.RuntimeStatus != OrchestrationRuntimeStatus.Terminated).ToList();
        //        log.LogError($"[Stop_Trigger] Terminating execution for {instances.Count}");
        //        foreach (DurableOrchestrationStatus instance in instances)
        //        {
        //            log.LogError($"[Stop_Trigger] Terminating execution for {instance.Name}");
        //            await client.TerminateAsync(instance.InstanceId, "called by http trigger");
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        log.LogError($"[Stop_Trigger] Error on terminating {ex.Message}");
        //        throw;
        //    }

        //}
    }
}
