using System;
using System.Threading;
using Confluent.Kafka;

class Program
{
    private static string bootstrapServers = "localhost:9092";
    private static string groupId = "warehouse-consumers";
    private static string topicNameEvents = "warehouse_events";
    private static string topicNameActivity = "warehouse_activity";
    private static string topicNameReader1 = "Reader1"; // New topic

    static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            // Subscribe to all three topics
            consumer.Subscribe(new[] { topicNameEvents, topicNameActivity, topicNameReader1 });

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // Prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        // Poll with a timeout of 10 seconds
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(10)); 

                        if (consumeResult != null)
                        {
                            if (consumeResult.Topic == topicNameEvents)
                            {
                                Console.WriteLine($"Received message from {topicNameEvents}: {consumeResult.Value}");
                            }
                            else if (consumeResult.Topic == topicNameActivity)
                            {
                                Console.WriteLine($"Received message from {topicNameActivity}: {consumeResult.Value}");
                            }
                            else if (consumeResult.Topic == topicNameReader1) // New condition for Reader1
                            {
                                Console.WriteLine($"Received message from {topicNameReader1}: {consumeResult.Value}");
                            }
                        }
                        else
                        {
                            // Timeout occurred, handle inactivity
                            Console.WriteLine("No new messages received in the last 10 seconds.");
                            // Perform other tasks or check the status of the producer
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException ex)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                Console.WriteLine($"Consumer operation canceled: {ex.Message}");
            }
        }
    }
}
