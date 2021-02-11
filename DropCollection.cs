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
    public static class DropCollection
    {
        [FunctionName("DropCollection")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string containerId = req.Query["containerId"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            containerId = containerId ?? data?.containerId;

            log.LogInformation($"Parsed the following from request: containerId {containerId}");

            if (string.IsNullOrEmpty(containerId))
            {
                return new OkObjectResult("{ \"Response\":\"Please pass a 'containerId'.\" }");
            }

            string responseMessage;
            string connStr = Environment.GetEnvironmentVariable("CosmosConnectionString");
            CosmosClient cosmosClient = new CosmosClient(connStr);
            Database db = cosmosClient.GetDatabase("contoso");
            Container container = db.GetContainer(containerId);

            log.LogInformation($"Delete container {containerId}");
            try
            {
                ContainerResponse responseDel = await container.DeleteContainerAsync();
                responseMessage = $"Container {containerId} deleted.";
            }
            catch (Exception ex)
            {
                responseMessage = "No container with that name exists";
            }

            return new OkObjectResult($" {{ \"Response\":\"{responseMessage}\" }}");
        }
    }
}
