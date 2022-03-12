using Azure.Storage.Blobs;
using flt.azf.parallel_csv_to_cosmos.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace flt.azf.parallel_csv_to_cosmos.Services;

internal class StorageService
{
    private readonly ILogger log;
    private readonly string storageConnectionString = Environment.GetEnvironmentVariable("StorageConnectionString", EnvironmentVariableTarget.Process);
    private readonly string storageContainerName = Environment.GetEnvironmentVariable("StorageContainerName", EnvironmentVariableTarget.Process);

    internal StorageService(ILogger log)
    {
        this.log = log;
    }

    public async Task<List<string>> ProcessCsv(string filename)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();

        log.LogInformation($"[StorageManager.ProcessCsv] Removing old file partitions");

        // Retrieve storage account from connection string.
        BlobContainerClient container = new(storageConnectionString, storageContainerName);

        // for debug only
        //return container.GetBlobs().ToList().Where(x => x.Name.Contains("toprocess_")).Select(x => x.Name).ToList();

        // Clean old partitions
        log.LogInformation($"[StorageManager.ProcessCsv] Removing old file partitions");
        await foreach (var blob in container.GetBlobsAsync())
        {
            if (blob.Name.Contains("toprocess_"))
            {
                container.DeleteBlob(blob.Name);
                log.LogInformation($"[StorageManager.ProcessCsv] Deleted file {blob.Name} to remove duplication");
            }
        }

        // List of new processed csvs
        var filenames = new List<string>();

        // Read CSV line by line and build smaller uploadable csvs
        using (var stream = await container.GetBlobClient(filename).OpenReadAsync())
        {

            log.LogInformation($"[StorageManager.ProcessCsv] Loading file {filename} read stream");

            using StreamReader reader = new(stream);

            string header = reader.ReadLine();

            int fileId = 0;

            while (!reader.EndOfStream)
            {
                Stopwatch fileStopwatch = Stopwatch.StartNew();

                var csv = new StringBuilder();
                csv.AppendLine(header);

                for (int i = 0; i < 10000 && !reader.EndOfStream; i++)
                {
                    csv.AppendLine(reader.ReadLine());
                }

                var csvFilename = "toprocess_" + filename.Split('.')[0] + "_" + fileId++ + ".csv";

                using (var fileStream = new MemoryStream(Encoding.UTF8.GetBytes(csv.ToString())))
                {
                    container.GetBlobClient(csvFilename).Upload(fileStream);
                }

                filenames.Add(csvFilename);

                fileStopwatch.Stop();
                log.LogInformation($"[StorageManager.ProcessCsv] Processed file {csvFilename} in {fileStopwatch.Elapsed.TotalMilliseconds}ms");
            }
        }

        stopwatch.Stop();
        log.LogInformation($"[StorageManager.ProcessCsv] Processed {filenames.Count} partition files in {stopwatch.Elapsed.TotalMilliseconds}ms");

        return filenames;
    }
    public async Task<List<DataModel>> TransformCsv(string filename)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();

        var data = new List<DataModel>();

        // Retrieve storage account from connection string.
        BlobContainerClient container = new(storageConnectionString, storageContainerName);

        // Read CSV line by line and build smaller uploadable csvs
        using (var stream = await container.GetBlobClient(filename).OpenReadAsync())
        {
            using StreamReader reader = new(stream);

            string header = reader.ReadLine();
            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                var values = line.Split(',');
                data.Add(new DataModel()
                {
                    id = Guid.NewGuid().ToString(),
                    Country = values[0],
                    ItemType = values[1],
                    Region = values[2],
                    SalesChannel = values[3]
                });
            }
        }

        stopwatch.Stop();

        return data;
    }
}
