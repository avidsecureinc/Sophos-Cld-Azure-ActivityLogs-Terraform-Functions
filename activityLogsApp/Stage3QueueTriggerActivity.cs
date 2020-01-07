using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Extensions;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Net.Sockets;
using Newtonsoft.Json;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;

namespace NwNsgProject
{
    public static class Stage3QueueTriggerActivity
    {
        [FunctionName("Stage3QueueTriggerActivity")]
        public static async Task Run(
            [QueueTrigger("activitystage2", Connection = "AzureWebJobsStorage")]Chunk inputChunk,
            Binder binder,
            TraceWriter log)
        {
            //log.Info($"C# Queue trigger function processed: {inputChunk}");

            string nsgSourceDataAccount = Util.GetEnvironmentVariable("nsgSourceDataAccount");
            if (nsgSourceDataAccount.Length == 0)
            {
                log.Error("Value for nsgSourceDataAccount is required.");
                throw new ArgumentNullException("nsgSourceDataAccount", "Please supply in this setting the name of the connection string from which NSG logs should be read.");
            }

            var attributes = new Attribute[]
            {
                new BlobAttribute(inputChunk.BlobName),
                new StorageAccountAttribute(nsgSourceDataAccount)
            };

            string nsgMessagesString;
            try
            {
                byte[] nsgMessages = new byte[inputChunk.Length];
                CloudAppendBlob blob = await binder.BindAsync<CloudAppendBlob>(attributes);
                await blob.DownloadRangeToByteArrayAsync(nsgMessages, 0, inputChunk.Start, inputChunk.Length);
                nsgMessagesString = System.Text.Encoding.UTF8.GetString(nsgMessages);
            }
            catch (Exception ex)
            {
                log.Error(string.Format("Error binding blob input: {0}", ex.Message));
                throw ex;
            }

            // skip past the leading comma
            string trimmedMessages = nsgMessagesString.Trim();
            int curlyBrace = trimmedMessages.IndexOf('{');
            string newClientContent = "{\"records\":[";
            newClientContent += trimmedMessages.Substring(curlyBrace);
            newClientContent += "]}";
            newClientContent = newClientContent.Replace(System.Environment.NewLine, ",");
            await SendMessagesDownstream(newClientContent, log);

        }

        public static async Task SendMessagesDownstream(string myMessages, TraceWriter log)
        {
            await obAvidSecure(myMessages, log);
            
        }

        static async Task obAvidSecure(string newClientContent, TraceWriter log)
        {

            string avidAddress = Util.GetEnvironmentVariable("avidActivityAddress");

            if (avidAddress.Length == 0)
            {
                log.Error("Values for splunkAddress and splunkToken are required.");
                return;
            }
            string customerid = Util.GetEnvironmentVariable("customerId");

            var client = new SingleHttpClientInstance();
            try
            {
                ActivityLogsRecords logs = JsonConvert.DeserializeObject<ActivityLogsRecords>(newClientContent);
                logs.uuid = customerid;
                string jsonString = JsonConvert.SerializeObject(logs);
            	log.Info($"sending request to: {avidAddress}");
                log.Info($"activity logs data: {jsonString}");
                HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Post, avidAddress);
                req.Headers.Accept.Clear();
                req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                
                req.Content = new StringContent(jsonString, Encoding.UTF8, "application/json");
                HttpResponseMessage response = await SingleHttpClientInstance.SendToSplunk(req);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new System.Net.Http.HttpRequestException($"StatusCode from Splunk: {response.StatusCode}, and reason: {response.ReasonPhrase}");
                }
            }
            catch (System.Net.Http.HttpRequestException e)
            {
                throw new System.Net.Http.HttpRequestException("Sending to Splunk. Is Splunk service running?", e);
            }
            catch (Exception f)
            {
                log.Info("Error in parsing:", f);
                throw new System.Exception("Sending to Splunk. Unplanned exception.", f);
            }

        }


        

        public class SingleHttpClientInstance
        {
            private static readonly HttpClient HttpClient;

            static SingleHttpClientInstance()
            {
                HttpClient = new HttpClient();
                HttpClient.Timeout = new TimeSpan(0, 1, 0);
            }

            public static async Task<HttpResponseMessage> SendToSplunk(HttpRequestMessage req)
            {
                HttpResponseMessage response = await HttpClient.SendAsync(req);
                return response;
            }

        }      
    }
}
