using System.IO;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Threading.Tasks;

namespace NwNsgProject
{
    public static class Stage1BlobTriggerActivity
    {
        const int MAXDOWNLOADBYTES = 1024000;

        [FunctionName("Stage1BlobTriggerActivity")]
        public static async Task Run(
            [BlobTrigger("%blobContainerNameActivity%/resourceId=/SUBSCRIPTIONS/{subId}/y={blobYear}/m={blobMonth}/d={blobDay}/h={blobHour}/m={blobMinute}/PT1H.json", Connection = "%nsgSourceDataAccount%")]CloudAppendBlob myBlobActivity,
            [Queue("activitystage1", Connection = "AzureWebJobsStorage")] ICollector<Chunk> outputChunksActivity,
            [Table("activitycheckpoints", Connection = "AzureWebJobsStorage")] CloudTable checkpointTableActivity,
            string subId, string blobYear, string blobMonth, string blobDay, string blobHour, string blobMinute,
            TraceWriter log)
        {
            log.Info("starting");
            string nsgSourceDataAccount = Util.GetEnvironmentVariable("nsgSourceDataAccount");
            if (nsgSourceDataAccount.Length == 0)
            {
                log.Error("Value for nsgSourceDataAccount is required.");
                throw new System.ArgumentNullException("nsgSourceDataAccount", "Please provide setting.");
            }

            string blobContainerName = Util.GetEnvironmentVariable("blobContainerNameActivity");
            if (blobContainerName.Length == 0)
            {
                log.Error("Value for blobContainerName is required.");
                throw new System.ArgumentNullException("blobContainerName", "Please provide setting.");
            }

            var blobDetails = new BlobDetailsActivity(subId, blobYear, blobMonth, blobDay, blobHour, blobMinute);

            // get checkpoint
            Checkpoint checkpoint = Checkpoint.GetCheckpointActivity(blobDetails, checkpointTableActivity);
            // break up the block list into 10k chunks

            long blobSize = myBlobActivity.Properties.Length;
            long chunklength = blobSize - checkpoint.StartingByteOffset;
            if(chunklength >10)
            {
                Chunk newchunk = new Chunk
                    {
                        BlobName = blobContainerName + "/" + myBlobActivity.Name,
                        Length = chunklength,
                        LastBlockName = "",
                        Start = checkpoint.StartingByteOffset,
                        BlobAccountConnectionName = nsgSourceDataAccount
                    };

                checkpoint.PutCheckpointActivity(checkpointTableActivity, blobSize);
                outputChunksActivity.Add(newchunk);
            }

        }
    }

}
