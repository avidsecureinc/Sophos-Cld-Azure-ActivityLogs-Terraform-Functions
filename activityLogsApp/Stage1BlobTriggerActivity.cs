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
            [BlobTrigger("%blobContainerNameActivity%/name=AvidActivityLogCollector/resourceId=/SUBSCRIPTIONS/{subId}/y={blobYear}/m={blobMonth}/d={blobDay}/h={blobHour}/m={blobMinute}/PT1H.json", Connection = "%nsgSourceDataAccount%")]CloudAppendBlob myBlobActivity,
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
            log.Info("second check");
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
                log.Info("added chunk");
            }

            log.Info("no chunk");

            /*foreach (var blockListItem in myBlobActivity.DownloadBlockList(BlockListingFilter.Committed))
            {
                if (!foundStartingOffset)
                {
                    if (firstBlockItem)
                    {
                        currentStartingByteOffset += blockListItem.Length;
                        firstBlockItem = false;
                        if (checkpoint.LastBlockName == "")
                        {
                            foundStartingOffset = true;
                        }
                    }
                    else
                    {
                        if (blockListItem.Name == checkpoint.LastBlockName)
                        {
                            foundStartingOffset = true;
                        }
                        currentStartingByteOffset += blockListItem.Length;
                    }
                }
                else
                {
                    // tieOffChunk = add current chunk to the list, initialize next chunk counters
                    // conditions to account for:
                    // 1) current chunk is empty & not the last block (size > 10 I think)
                    //   a) add blockListItem to current chunk
                    //   b) loop
                    // 2) current chunk is empty & last block (size < 10 I think)
                    //   a) do not add blockListItem to current chunk
                    //   b) loop terminates
                    //   c) chunk last added to the list is the last chunk
                    // 3) current chunk is not empty & not the last block
                    //   a) if size of block + size of chunk >10k
                    //     i) add chunk to list  <-- tieOffChunk
                    //     ii) reset chunk counters
                    //   b) add blockListItem to chunk
                    //   c) loop
                    // 4) current chunk is not empty & last block
                    //   a) add chunk to list  <-- tieOffChunk
                    //   b) do not add blockListItem to chunk
                    //   c) loop terminates
                    tieOffChunk = (currentChunkSize != 0) && ((blockListItem.Length < 10) || (currentChunkSize + blockListItem.Length > MAXDOWNLOADBYTES));
                    if (tieOffChunk)
                    {
                        // chunk complete, add it to the list & reset counters
                        chunks.Add(new Chunk
                        {
                            BlobName = blobContainerName + "/" + myBlobActivity.Name,
                            Length = currentChunkSize,
                            LastBlockName = currentChunkLastBlockName,
                            Start = currentStartingByteOffset,
                            BlobAccountConnectionName = nsgSourceDataAccount
                        });
                        currentStartingByteOffset += currentChunkSize; // the next chunk starts at this offset
                        currentChunkSize = 0;
                        tieOffChunk = false;
                    }
                    if (blockListItem.Length > 10)
                    {
                        numberOfBlocks++;
                        sizeOfBlocks += blockListItem.Length;

                        currentChunkSize += blockListItem.Length;
                        currentChunkLastBlockName = blockListItem.Name;
                    }
                }

            }
            if (currentChunkSize != 0)
            {
                // residual chunk
                chunks.Add(new Chunk
                {
                    BlobName = blobContainerName + "/" + myBlobActivity.Name,
                    Length = currentChunkSize,
                    LastBlockName = currentChunkLastBlockName,
                    Start = currentStartingByteOffset,
                    BlobAccountConnectionName = nsgSourceDataAccount
                });
            }

            if (chunks.Count > 0)
            {
                var lastChunk = chunks[chunks.Count - 1];
                checkpoint.PutCheckpoint(checkpointTableActivity, lastChunk.LastBlockName, lastChunk.Start + lastChunk.Length);
            }

            // add the chunks to output queue
            // they are sent automatically by Functions configuration
            foreach (var chunk in chunks)
            {
                outputChunksActivity.Add(chunk);
                log.Info("added chunks");
                if (chunk.Length == 0)
                {
                    log.Error("chunk length is 0");
                }
            }
            */

        }
    }

}
