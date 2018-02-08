using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace OrderedEventHubs
{
    public static class EventHubTrigger
    {
        private static ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable("Redis"));
        private static IDatabase db = redis.GetDatabase();

        [FunctionName("EventHubTrigger")]
        public static async Task RunAsync([EventHubTrigger("ordered", Connection = "EventHub")] EventData[] eventDataSet, TraceWriter log)
        {
            log.Info($"Triggered batch of size {eventDataSet.Length}");
            foreach (var eventData in eventDataSet) {
                try
                {
                    await db.ListRightPushAsync("events:" + eventData.Properties["partitionKey"], (string)eventData.Properties["counter"]);
                }
                catch
                {
                    // handle event exception
                }
            }
        }
    }
}
