using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Host.Bindings;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace NwNsgProject
{
    public static class Stage2QueueTriggerActivity
    {
        const int MAX_CHUNK_SIZE = 1024000;

        [FunctionName("Stage2QueueTriggerActivity")]
        public static async Task Run(
            [QueueTrigger("activitystage1", Connection = "AzureWebJobsStorage")]Chunk inputChunk,
            [Queue("activitystage2", Connection = "AzureWebJobsStorage")] ICollector<Chunk> outputQueue,
            Binder binder,
            ILogger log)
        {
            try
            {
                log.LogInformation($"C# Queue trigger function processed: {inputChunk}");

                if (inputChunk.Length < MAX_CHUNK_SIZE)
                {
                    outputQueue.Add(inputChunk);
                    return;
                }

                string nsgSourceDataAccount = Util.GetEnvironmentVariable("nsgSourceDataAccount");
                if (nsgSourceDataAccount.Length == 0)
                {
                    log.LogError("Value for nsgSourceDataAccount is required.");
                    throw new ArgumentNullException("nsgSourceDataAccount", "Please supply in this setting the name of the connection string from which NSG logs should be read.");
                }

                var attributes = new Attribute[]
                {
                    new BlobAttribute(inputChunk.BlobName),
                    new StorageAccountAttribute(nsgSourceDataAccount)
                };

                byte[] nsgMessages = new byte[inputChunk.Length];
                try
                {
                    CloudAppendBlob blob = await binder.BindAsync<CloudAppendBlob>(attributes);
                    await blob.DownloadRangeToByteArrayAsync(nsgMessages, 0, inputChunk.Start, inputChunk.Length);
                }
                catch (Exception ex)
                {
                    log.LogError(string.Format("Error binding blob input: {0}", ex.Message));
                    throw ex;
                }

                int startingByte = 0;
                var chunkCount = 0;

                var newChunk = GetNewChunk(inputChunk, chunkCount++, log, 0);

                //long length = FindNextRecord(nsgMessages, startingByte);
                var nsgMessagesString = System.Text.Encoding.Default.GetString(nsgMessages);
                int endingByte = FindNextRecordRecurse(nsgMessagesString, startingByte, 0, log);
                int length = endingByte - startingByte + 1;

                while (length != 0)
                {
                    if (newChunk.Length + length > MAX_CHUNK_SIZE)
                    {
                        outputQueue.Add(newChunk);

                        newChunk = GetNewChunk(inputChunk, chunkCount++, log, newChunk.Start + newChunk.Length);
                    }

                    newChunk.Length += length;
                    startingByte += length;

                    endingByte = FindNextRecordRecurse(nsgMessagesString, startingByte, 0, log);
                    length = endingByte - startingByte + 1;
                }

                if (newChunk.Length > 0)
                {
                    outputQueue.Add(newChunk);
                    //log.LogInformation($"Chunk starts at {newChunk.Start}, length is {newChunk.Length}");
                }
            }
            catch (Exception e)
            {
                log.LogError(e, "Function Stage2QueueTriggerActivity is failed to process request");
            }

        }

        public static Chunk GetNewChunk(Chunk thisChunk, int index, ILogger log, long start = 0)
        {
            var chunk = new Chunk
            {
                BlobName = thisChunk.BlobName,
                BlobAccountConnectionName = thisChunk.BlobAccountConnectionName,
                LastBlockName = string.Format("{0}-{1}", index, thisChunk.LastBlockName),
                Start = (start == 0 ? thisChunk.Start : start),
                Length = 0
            };

            //log.LogInformation($"new chunk: {chunk.ToString()}");

            return chunk;
        }

        public static long FindNextRecord(byte[] array, long startingByte)
        {
            var arraySize = array.Length;
            var endingByte = startingByte;
            var curlyBraceCount = 0;
            var insideARecord = false;

            for (long index = startingByte; index < arraySize; index++)
            {
                endingByte++;

                if (array[index] == '{')
                {
                    insideARecord = true;
                    curlyBraceCount++;
                }

                curlyBraceCount -= (array[index] == '}' ? 1 : 0);

                if (insideARecord && curlyBraceCount == 0)
                {
                    break;
                }
            }

            return endingByte - startingByte;
        }

        public static int FindNextRecordRecurse(string nsgMessages, int startingByte, int braceCounter, ILogger log)
        {
            if (startingByte == nsgMessages.Length)
            {
                return startingByte - 1;
            }

            int nextBrace = nsgMessages.IndexOfAny(new Char[] { '}', '{' }, startingByte);

            if (nsgMessages[nextBrace] == '{')
            {
                braceCounter++;
                nextBrace = FindNextRecordRecurse(nsgMessages, nextBrace + 1, braceCounter, log);
            }
            else
            {
                braceCounter--;
                if (braceCounter > 0)
                {
                    nextBrace = FindNextRecordRecurse(nsgMessages, nextBrace + 1, braceCounter, log);
                }
            }

            return nextBrace;
        }
    }


}