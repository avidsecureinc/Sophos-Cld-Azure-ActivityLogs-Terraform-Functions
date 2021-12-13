using System;
using Microsoft.Azure.Cosmos.Table;

namespace NwNsgProject
{
    public class Checkpoint : TableEntity
    {

        public string LastBlockName { get; set; }
        public long StartingByteOffset { get; set; }

        public Checkpoint()
        {
        }

        public Checkpoint(string partitionKey, string rowKey, string blockName, long offset)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            LastBlockName = blockName;
            StartingByteOffset = offset;
        }

        public static Checkpoint GetCheckpoint(BlobDetails blobDetails, CloudTable checkpointTable)
        {
            TableOperation operation = TableOperation.Retrieve<Checkpoint>(
                blobDetails.GetPartitionKey(), blobDetails.GetRowKey());
            TableResult result = checkpointTable.Execute(operation);

            Checkpoint checkpoint = (Checkpoint)result.Result;
            if (checkpoint == null)
            {
                checkpoint = new Checkpoint(blobDetails.GetPartitionKey(), blobDetails.GetRowKey(), "", 0);
            }

            return checkpoint;
        }

        public static Checkpoint GetCheckpointActivity(BlobDetailsActivity blobDetails, CloudTable checkpointTable)
        {
            TableOperation operation = TableOperation.Retrieve<Checkpoint>(
                blobDetails.GetPartitionKey(), blobDetails.GetRowKey());
            TableResult result = checkpointTable.Execute(operation);

            Checkpoint checkpoint = (Checkpoint)result.Result;
            if (checkpoint == null)
            {
                checkpoint = new Checkpoint(blobDetails.GetPartitionKey(), blobDetails.GetRowKey(), "", 0);
            }

            return checkpoint;
        }

        public void PutCheckpoint(CloudTable checkpointTable, string lastBlockName, long startingByteOffset)
        {
            LastBlockName = lastBlockName;
            StartingByteOffset = startingByteOffset;

            TableOperation operation = TableOperation.InsertOrReplace(this);
            checkpointTable.Execute(operation);
        }

        public void PutCheckpointActivity(CloudTable checkpointTable, long startingByteOffset)
        {
            StartingByteOffset = startingByteOffset;

            TableOperation operation = TableOperation.InsertOrReplace(this);
            checkpointTable.Execute(operation);
        }
    }
}
