using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Identity;
using System.Text;
using System.Text.Json;
using DotNetEnv;

namespace EventHubSender;

class Program
{
    private static string fullyQualifiedNamespace = null!;
    private static string eventHubName = null!;

    private static void LoadEnvironmentFile()
    {
        // Try multiple possible locations for .env file
        var possiblePaths = new[]
        {
            ".env", // Current directory
            Path.Combine("..", ".env"), // Parent directory
            Path.Combine("..", "..", ".env"), // Grandparent directory
            Path.Combine("..", "..", "..", ".env"), // Great-grandparent directory
            Path.Combine(Directory.GetCurrentDirectory(), ".env"), // Explicit current directory
            Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", ".env") // Project root
        };

        foreach (var envPath in possiblePaths)
        {
            try
            {
                var fullPath = Path.GetFullPath(envPath);
                if (File.Exists(fullPath))
                {
                    Env.Load(fullPath);
                    Console.WriteLine($"✓ Loaded .env file from: {fullPath}");
                    return;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to check path {envPath}: {ex.Message}");
            }
        }
        
        Console.WriteLine("⚠️  No .env file found. Using system environment variables only.");
    }

    static async Task Main(string[] args)
    {
        // Try to load .env file from multiple possible locations
        LoadEnvironmentFile();

        // Get environment variables
        fullyQualifiedNamespace = Environment.GetEnvironmentVariable("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE") 
            ?? throw new ArgumentException("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE environment variable is required");
        
        eventHubName = Environment.GetEnvironmentVariable("EVENT_HUB_NAME") 
            ?? throw new ArgumentException("EVENT_HUB_NAME environment variable is required");

        Console.WriteLine("Azure Event Hub Sender Starting...");
        Console.WriteLine($"Namespace: {fullyQualifiedNamespace}");
        Console.WriteLine($"Event Hub: {eventHubName}");

        // Use DefaultAzureCredential for authentication
        var credential = new DefaultAzureCredential();

        // Create a producer client that you can use to send events to an event hub
        await using var producerClient = new EventHubProducerClient(fullyQualifiedNamespace, eventHubName, credential);

        try
        {
            Console.WriteLine("Sending events...");
            await SendEvents(producerClient);
            Console.WriteLine("Events sent successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error sending events: {ex.Message}");
            if (ex.InnerException != null)
            {
                Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
            }
        }
    }

    private static async Task SendEvents(EventHubProducerClient producerClient)
    {
        // Create a batch of events 
        using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

        for (int i = 1; i <= 1000; i++)
        {
            var eventData = new
            {
                id = Guid.NewGuid().ToString(),
                message = $"Hello Event Hub! Message {i}",
                timestamp = DateTime.UtcNow,
                sequence = i,
                sender = "dotnet-sender"
            };

            string jsonString = JsonSerializer.Serialize(eventData, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            var eventDataObject = new EventData(Encoding.UTF8.GetBytes(jsonString));
            
            // Add some custom properties
            eventDataObject.Properties.Add("MessageType", "Test");
            eventDataObject.Properties.Add("Sequence", i.ToString());

            // Try to add the event to the batch
            if (!eventBatch.TryAdd(eventDataObject))
            {
                // If it is too large for the batch, send the current batch and create a new one
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine("Batch sent!");
                
                // Create a new batch and add the event that couldn't fit in the previous batch
                using var newBatch = await producerClient.CreateBatchAsync();
                if (!newBatch.TryAdd(eventDataObject))
                {
                    throw new Exception("Event is too large to fit in any batch");
                }
            }

            Console.WriteLine($"Added event {i} to batch");
          
        }

        // Send the last batch of events
        if (eventBatch.Count > 0)
        {
            await producerClient.SendAsync(eventBatch);
            Console.WriteLine("Final batch sent!");
        }
    }
}
