using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Polly;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace OrderedEventHubs
{
    public static class EventHubTrigger
    {
        private static ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable("Redis"));
        private static IDatabase db = redis.GetDatabase();
        private static Policy _circuit;

        [FunctionName("EventHubTrigger")]
        [Disable(typeof(CircuitStatus))]
        public static async Task RunAsync(
            [EventHubTrigger(eventHubName: "events", Connection = "EventHub")] EventData[] eventDataSet, 
            TraceWriter log,
            [Queue("deadletter")] IAsyncCollector<string> queue)
        {
            log.Info($"Triggered batch of size {eventDataSet.Length}");
            foreach (var eventData in eventDataSet) {
                var result = await Policy
                .Handle<Exception>()
                .RetryAsync(3, onRetryAsync: async (exception, retryCount, context) =>
                {
                    await db.ListRightPushAsync("events:" + context["partitionKey"], (string)context["counter"] + $"CAUGHT{retryCount}");
                })
                .WrapAsync(GetCircuit())
                .ExecuteAndCaptureAsync(async () =>
                {
                    if (int.Parse((string)eventData.Properties["counter"]) % 100 == 0)
                    {
                        throw new SystemException("Some Exception");
                    }
                    await db.ListRightPushAsync("events:" + eventData.Properties["partitionKey"], (string)eventData.Properties["counter"]);
                },
                new Dictionary<string, object>() { { "partitionKey", eventData.Properties["partitionKey"] }, { "counter", eventData.Properties["counter"] } });

                if(result.Outcome == OutcomeType.Failure)
                {
                    await db.ListRightPushAsync("events:" + eventData.Properties["partitionKey"], (string)eventData.Properties["counter"] + "FAILED");
                    await queue.AddAsync(Encoding.UTF8.GetString(eventData.Body.Array));
                    await queue.FlushAsync();
                }
            }
        }

        private static Policy GetCircuit()
        {
            if(_circuit == null)
            {
                Action<Exception, TimeSpan> onBreak = (exception, timespan) => {
                    CircuitStatus.Disable();
                    File.SetLastWriteTimeUtc("host.json", DateTime.UtcNow);
                };
                Action onReset = () => {
                    CircuitStatus.Enable();
                    File.SetLastWriteTimeUtc("host.json", DateTime.UtcNow);
                };
                _circuit = Policy
                    .Handle<Exception>()
                    .CircuitBreakerAsync(10, new TimeSpan(0, 5, 0), onBreak, onReset);  
            }
            return _circuit;
        }

        private static class CircuitStatus
        {
            private static bool _status = bool.Parse(db.StringGet("disabled"));
            public static bool IsDisabled() { return _status; }
            public static void Disable() {
                db.StringSet("disabled", "true");
                _status = true;
            }
            public static void Enable() {
                db.StringSet("disabled", "false");
                _status = false;
            }
        }
    }
}
