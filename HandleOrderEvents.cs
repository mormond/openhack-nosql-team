using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Meo
{
    public static class HandleOrderEvents
    {
        [FunctionName("HandleOrderEvents")]
        public static async Task Run([EventHubTrigger("telemetry", Connection = "openhackhuboqbkgpam2snlc_lab_EVENTHUB")] EventData[] events, ILogger log)
        {

            string connStr = Environment.GetEnvironmentVariable("CosmosConnectionString");
            string preferredRegion = Environment.GetEnvironmentVariable("PreferredLocation");

            using (CosmosClient cosmosClient = new CosmosClient(
                connStr,
                new CosmosClientOptions()
                {
                    ApplicationRegion = preferredRegion
                }))
            {
                Database db = cosmosClient.GetDatabase("contoso");
                Container ordersContainer = db.GetContainer("orders-aggregate");
                Container orderDetailsContainer = db.GetContainer("orders-details-aggregate");

                var exceptions = new List<Exception>();

                foreach (EventData eventData in events)
                {
                    try
                    {
                        string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                        var tasks = new List<Task>();

                        JObject jOrder = JObject.Parse(messageBody);
                        JArray details = (JArray)jOrder["Details"];

                        string orderDate = ((DateTime)jOrder["OrderDate"]).ToString("O");
                        string orderId = string.Concat((string)jOrder["Email"], orderDate);

                        Order o = new Order()
                        {
                            id = orderId,
                            InsertRegion = preferredRegion,
                            Email = (string)jOrder["Email"],
                            OrderDate = orderDate.Substring(0, 10),
                            OrderDateHour = orderDate.Substring(0, 13),
                            OrderDateTime = orderDate,
                            OrderId = orderId,
                            PostalCode = (string)jOrder["PostalCode"],
                            Total = (decimal)jOrder["Total"]
                        };

                        foreach (JObject jo in details)
                        {
                            int productId = (int)jo["ProductId"];

                            OrderDetails od = new OrderDetails()
                            {
                                id = $"{o.id}-{productId}",
                                InsertRegion = preferredRegion,
                                Email = o.Email,
                                OrderDate = o.OrderDate,
                                OrderDateHour = o.OrderDateHour,
                                OrderDateTime = o.OrderDateTime,
                                OrderId = o.OrderId,
                                PostalCode = o.PostalCode,
                                Total = o.Total,
                                ProductId = productId,
                                Quantity = (int)jo["Quantity"],
                                UnitPrice = (decimal)jo["UnitPrice"]

                            };

                            tasks.Add(orderDetailsContainer.CreateItemAsync(od));
                        }

                        tasks.Add(ordersContainer.CreateItemAsync(o));

                        await Task.WhenAll(tasks);

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
}
