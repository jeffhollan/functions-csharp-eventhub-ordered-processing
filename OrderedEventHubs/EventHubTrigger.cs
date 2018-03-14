using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Polly;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OrderedEventHubs
{
    public static class EventHubTrigger
    {
        private static ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable("Redis"));
        private static IDatabase db = redis.GetDatabase();
        [FunctionName("EventHubTrigger")]
        public static async Task RunAsync([EventHubTrigger(eventHubName: "events", Connection = "EventHub")] EventData[] eventDataSet, TraceWriter log)
        {
            log.Info($"Triggered batch of size {eventDataSet.Length}");

            foreach (var eventData in eventDataSet) {
                var result = await Policy
                .Handle<Exception>()
                .RetryAsync(3, onRetryAsync: async (exception, retryCount, context) =>
                {
                    await db.ListRightPushAsync("events:" + context["partitionKey"], (string)context["counter"] + $"CAUGHT{retryCount}");
                })
                .ExecuteAndCaptureAsync(async () =>
                {
                    if (int.Parse((string)eventData.Properties["counter"]) % 100 == 0)
                    {
                        throw new SystemException("Some Exception");
                    }
                    await db.ListRightPushAsync("events:" + eventData.Properties["partitionKey"], (string)eventData.Properties["counter"]);
                },
                new Dictionary<string, object>() { { "partitionKey", eventData.Properties["partitionKey"] }, { "counter", eventData.Properties["counter"] } }, true);

                if(result.Outcome == OutcomeType.Failure)
                {
                    await db.ListRightPushAsync("events:" + eventData.Properties["partitionKey"], (string)eventData.Properties["counter"] + "FAILED");
                }
            }
        }
    }
}
