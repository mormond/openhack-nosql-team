using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos;

namespace Meo
{
    public static class CreateCollection
    {
        [FunctionName("CreateCollection")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string containerId = req.Query["containerId"];
            string partitionKeyPath = req.Query["partitionKeyPath"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            containerId = containerId ?? data?.containerId;
            partitionKeyPath = partitionKeyPath ?? data?.partitionKeyPath;

            log.LogInformation($"Parsed the following from request: containerId {containerId}, partitionKeyPath {partitionKeyPath}");

            if (string.IsNullOrEmpty(containerId) || string.IsNullOrEmpty(partitionKeyPath))
            {
                return new OkObjectResult("{ \"Response\":\"Please pass both 'containerId' and 'partitionKeyPath'\" }");
            }

            string responseMessage;
            string connStr = Environment.GetEnvironmentVariable("CosmosConnectionString");
            CosmosClient cosmosClient = new CosmosClient(connStr);
            Database db = cosmosClient.GetDatabase("contoso");

            try
            {
                log.LogInformation($"Create container {containerId}");
                ContainerResponse responseCreate = await db.CreateContainerAsync(containerId, partitionKeyPath);
                responseMessage = $"Container {containerId} successfully created.";
            }
            catch (Exception ex)
            {
                responseMessage = "Container creation failed. Does a collection with the same name already exist?";
            }

            return new OkObjectResult($" {{ \"Response\":\"{responseMessage}\" }}");
        }
    }
}
