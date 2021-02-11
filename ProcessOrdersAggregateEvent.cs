using System;
using System.Collections.Generic;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;
using System.Linq;

namespace Meo
{
    public static class ProcessOrdersAggregateEvent
    {
        private class Order
        {
            public string id { get; set; }
            public int Count { get; set; }
            public decimal Total { get; set; }

        }

        [FunctionName("ProcessOrdersAggregateEvent")]
        public static async void Run([CosmosDBTrigger(
            databaseName: "contoso",
            collectionName: "orders-aggregate",
            ConnectionStringSetting = "CosmosConnectionString",
            LeaseCollectionName = "leases")]IReadOnlyList<Microsoft.Azure.Documents.Document> input, ILogger log)
        {
            if (input != null && input.Count > 0)
            {
                log.LogInformation("Documents modified " + input.Count);
                log.LogInformation("First document Id " + input[0].Id);

                string connStr = Environment.GetEnvironmentVariable("CosmosConnectionString");
                CosmosClient cosmosClient = new CosmosClient(connStr);
                Microsoft.Azure.Cosmos.Database db = cosmosClient.GetDatabase("contoso");
                Container container = db.GetContainer("orders-for-hour");

                // custQuery is an IEnumerable<IGrouping<string, Customer>>
                var dateQuery =
                    from o in input
                    group o by o.GetPropertyValue<string>("OrderDateHour") into orderDateGroup
                    select orderDateGroup;

                log.LogInformation(dateQuery.Count().ToString());

                foreach (var dq in dateQuery)
                {
                    log.LogInformation("DateQuery: " + dq.Key);

                    var sqlQueryText = $"SELECT * FROM c WHERE c.id = '{dq.Key}'";

                    log.LogInformation("Running query: {0}\n", sqlQueryText);

                    QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
                    FeedIterator<Order> queryResultSetIterator = container.GetItemQueryIterator<Order>(queryDefinition);

                    List<Order> orders = new List<Order>();

                    while (queryResultSetIterator.HasMoreResults)
                    {
                        log.LogInformation("In loop");
                        FeedResponse<Order> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                        foreach (Order order in currentResultSet)
                        {
                            orders.Add(order);
                            log.LogInformation("\tRead {0}\n", order);
                        }
                    }

                    log.LogInformation("Orders Count: " + orders.Count);

                    Order currentOrder = new Order() { id = dq.Key, Count = dq.Count(), Total = dq.Sum(x => x.GetPropertyValue<decimal>("Total")) };

                    if (orders.Count == 0)
                    {
                        log.LogInformation("Addin entry");
                        try
                        {
                            ItemResponse<Order> orderResponse = await container.CreateItemAsync<Order>(currentOrder);
                        }
                        catch (Exception ex)
                        {
                            log.LogInformation("Exception: " + ex.Message);
                        }
                    }
                    else
                    {
                        ItemResponse<Order> orderResponse = await container.ReadItemAsync<Order>(dq.Key, new PartitionKey(dq.Key));
                        int currentCount = orderResponse.Resource.Count;
                        decimal currentTotal = orderResponse.Resource.Total;

                        Order newOrder = new Order() { id = dq.Key, Count = currentCount + currentOrder.Count, Total = currentTotal + currentOrder.Total };
                        orderResponse = await container.ReplaceItemAsync<Order>(newOrder, newOrder.id);

                    }
                }
            }
        }
    }
}
