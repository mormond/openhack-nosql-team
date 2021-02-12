using System;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Microsoft.Azure.Cosmos;

namespace Meo
{
    public static class RefreshRedisCache
    {
        [FunctionName("RefreshRedisCache")]
        public static async void Run([CosmosDBTrigger(
            databaseName: "contoso",
            collectionName: "category",
            ConnectionStringSetting = "CosmosConnectionString",
            LeaseCollectionName = "leases")]IReadOnlyList<Document> input, ILogger log)
        {
            if (input != null && input.Count > 0)
            {
                log.LogInformation("Documents modified " + input.Count);
                log.LogInformation("First document Id " + input[0].Id);

                var connectionString = "contoso-meo.redis.cache.windows.net:6380,password=EIhC70PPwkHZKSY5cc48SAy6YfM4FmahxchOEHKJjwo=,ssl=True,abortConnect=False";
                var redisConnection = ConnectionMultiplexer.Connect(connectionString);
                IDatabase db = redisConnection.GetDatabase();
                db.Execute("flushall");

                string connStr = Environment.GetEnvironmentVariable("CosmosConnectionString");
                CosmosClient cosmosClient = new CosmosClient(connStr);
                Microsoft.Azure.Cosmos.Database cosmosDb = cosmosClient.GetDatabase("contoso");
                Container container = cosmosDb.GetContainer("category");

                var sqlQueryText = $"SELECT c.CategoryId, c.CategoryName, c.Decription, c.Products, c.id FROM c";

                log.LogInformation("Running query: {0}\n", sqlQueryText);

                QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
                FeedIterator<Category> queryResultSetIterator = container.GetItemQueryIterator<Category>(queryDefinition);

                List<Category> categories = new List<Category>();

                while (queryResultSetIterator.HasMoreResults)
                {
                    log.LogInformation("In loop");
                    FeedResponse<Category> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                    foreach (Category cat in currentResultSet)
                    {
                        categories.Add(cat);
                        log.LogInformation("\tRead {0}\n", cat);

                        db.StringSet($"category:{cat.CategoryId}", cat.CategoryName);
                    }
                }

                redisConnection.Dispose();
                redisConnection = null;
            }
        }
    }
}
