using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Meo
{
    public static class HandleOrderEvents
    {

        public class Order
        {
            public string Email { get; set; }
            public string PostalCode { get; set; }
            public string OrderDateTime { get; set; }
            public string Total { get; set; }
            public string OrderId { get; set; }
            public string OrderDate { get; set; }
            public string OrderDateHour { get; set; }
        }

        [FunctionName("HandleOrderEvents")]
        public static async Task Run([EventHubTrigger("telemetry", Connection = "openhackhuboqbkgpam2snlc_lab_EVENTHUB")] EventData[] events, 
            [CosmosDB(
                databaseName: "contoso",
                collectionName: "orders-aggregate",
                ConnectionStringSetting = "CosmosConnectionString")]
                IAsyncCollector<Order> ordersOut,
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"C# Event Hub trigger function processed a message: {messageBody}");
                    //await Task.Yield();

                    JObject jOrder = JObject.Parse(messageBody);
                    Order o = new Order()
                    {
                        Email = (string)jOrder["Email"],
                        OrderDate = (string)jOrder["OrderDate"],
                        OrderDateHour = (string)jOrder["OrderDateHour"],
                        OrderDateTime = (string)jOrder["OrderDateTime"],
                        OrderId = (string)jOrder["OrderId"],
                        PostalCode = (string)jOrder["PostalCode"],
                        Total = (string)jOrder["Total"]
                    };
                    await ordersOut.AddAsync(o);
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
