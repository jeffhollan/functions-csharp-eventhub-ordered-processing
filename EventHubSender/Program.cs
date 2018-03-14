using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;

namespace EventHubSender
{
    class Program
    {
        private static int NUM_OF_EVENTS = 1000;
        private static int PARTITION_KEYS = 10;
        private static EventHubClient ehc = EventHubClient.CreateFromConnectionString(Environment.GetEnvironmentVariable("EventHubEntityConnection"));
        static void Main(string[] args)
        {
            Console.WriteLine($"Sending {NUM_OF_EVENTS} events to {PARTITION_KEYS} partition keys...");

            List<Task> partitionTasks = new List<Task>();
            for(int x = 1; x <= PARTITION_KEYS; x++)
            {
                partitionTasks.Add(SendEventsAsync(NUM_OF_EVENTS, x.ToString()));
            }
            Task.WhenAll(partitionTasks).Wait();
            Console.WriteLine("\n\nSent all events");
            Console.ReadLine();
        }

        static async Task SendEventsAsync(int events, string partitionKey)
        {
            List<EventData> eventBatch = new List<EventData>();
            for (int x = 1; x <= events; x++)
            {
                EventData e = new EventData(Encoding.UTF8.GetBytes(x.ToString()));
                e.Properties.Add("counter", x.ToString());
                e.Properties.Add("partitionKey", partitionKey);
                eventBatch.Add(e);
                if (x % 100 == 0)
                {
                    await ehc.SendAsync(eventBatch, partitionKey);
                    eventBatch = new List<EventData>();
                }
            }
            Console.WriteLine($"Sent all events for partition {partitionKey}");
        }
    }
}
