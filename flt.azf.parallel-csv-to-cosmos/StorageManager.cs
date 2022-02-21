using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace flt.azf.parallel_csv_to_cosmos;

internal class StorageManager
{
    private ILogger log;

    internal StorageManager(ILogger log)
    {
        this.log = log;
    }

    public List<string> ProcessCsv(string filename, string connectionString, string containerName)
    {
        var filenames = new List<string>();

        // Retrieve storage account from connection string.
        BlobContainerClient container = new(connectionString, containerName);

        // Clean old partitions
        log.LogInformation($"[StorageManager.ProcessCsv] Removing old file partitions");
        foreach (var blob in container.GetBlobs())
        {
            if (blob.Name.Contains("toprocess_"))
            {
                container.DeleteBlob(blob.Name);
                log.LogInformation($"[StorageManager.ProcessCsv] Deleted file {blob.Name} to remove duplication");
            }
        }

        // Read CSV line by line and build smaller uploadable csvs
        using (var stream = container.GetBlobClient(filename).OpenRead())
        {
            log.LogInformation($"[StorageManager.ProcessCsv] Loading file {filename} read stream");

            using StreamReader reader = new(stream);

            string header = reader.ReadLine();

            int fileId = 0;

            while (!reader.EndOfStream)
            {
                Stopwatch stopwatch = Stopwatch.StartNew();
                long startMilliseconds = stopwatch.ElapsedMilliseconds;

                var csv = new StringBuilder();
                csv.AppendLine(header);

                for (int i = 0; i < 100000 && !reader.EndOfStream; i++)
                {
                    csv.AppendLine(reader.ReadLine());
                }

                var csvFilename = "toprocess_" + filename.Split()[0] + "_" + fileId++ + ".csv";

                File.WriteAllText(csvFilename, csv.ToString());

                container.GetBlobClient(csvFilename).Upload(csvFilename);

                File.Delete(csvFilename);

                filenames.Add(csvFilename);

                stopwatch.Stop();
                TimeSpan ts = stopwatch.Elapsed;
                log.LogInformation($"[StorageManager.ProcessCsv] Processed file {csvFilename} in {ts.TotalMilliseconds}ms");
            }
        }

        log.LogInformation($"[StorageManager.ProcessCsv] Processed {filenames.Count} partition files");

        return filenames;
    }
    public List<DataModel> TransformCsv(string filename, string connectionString, string containerName)
    {
        var data = new List<DataModel>();

        // Retrieve storage account from connection string.
        BlobContainerClient container = new(connectionString, containerName);


        // Read CSV line by line and build smaller uploadable csvs
        using (var stream = container.GetBlobClient(filename).OpenRead())
        {
            using StreamReader reader = new(stream);

            string header = reader.ReadLine();
            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                var values = line.Split(',');
                data.Add(new DataModel()
                {
                    Id = Guid.NewGuid().ToString(),
                    Country = values[0],
                    ItemType = values[1],
                    Region = values[2],
                    SalesChannel = values[3]
                });
            }
        }

        return data;
    }
}
