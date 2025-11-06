using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using Azure.Identity;
using System.Text;
using System.Text.Json;
using DotNetEnv;

namespace EventHubReceiver;

class Program
{
    private static string fullyQualifiedNamespace = null!;
    private static string eventHubName = null!;
    private static string? storageAccountName;
    private static string blobContainerName = null!;

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

        storageAccountName = Environment.GetEnvironmentVariable("STORAGE_ACCOUNT_NAME");
        blobContainerName = Environment.GetEnvironmentVariable("BLOB_CONTAINER_NAME") ?? "checkpoints";

        Console.WriteLine("Azure Event Hub Receiver Starting...");
        Console.WriteLine($"Namespace: {fullyQualifiedNamespace}");
        Console.WriteLine($"Event Hub: {eventHubName}");

        if (!string.IsNullOrEmpty(storageAccountName))
        {
            Console.WriteLine("Using EventProcessorClient with checkpointing...");
            await ReceiveEventsWithProcessor();
        }
        else
        {
            Console.WriteLine("Using EventHubConsumerClient (no checkpointing)...");
            await ReceiveEventsWithConsumer();
        }
    }

    private static async Task ReceiveEventsWithProcessor()
    {
        // Use DefaultAzureCredential for authentication
        var credential = new DefaultAzureCredential();

        // Create a blob container client for checkpointing using DefaultAzureCredential
        var storageUri = new Uri($"https://{storageAccountName}.blob.core.windows.net/{blobContainerName}");
        var storageClient = new BlobContainerClient(storageUri, credential);
        await storageClient.CreateIfNotExistsAsync();

        // Create an event processor client to process events
        var processor = new EventProcessorClient(storageClient, EventHubConsumerClient.DefaultConsumerGroupName, 
            fullyQualifiedNamespace, eventHubName, credential);

        // Register handlers for processing events and handling errors
        processor.ProcessEventAsync += ProcessEventHandler;
        processor.ProcessErrorAsync += ProcessErrorHandler;

        try
        {
            // Start the processing
            await processor.StartProcessingAsync();

            Console.WriteLine("Processing events. Press 'q' to quit...");
            while (Console.ReadKey().Key != ConsoleKey.Q) 
            {
                // Keep processing until user presses 'q'
                await Task.Delay(100);
            }

            // Stop the processing
            await processor.StopProcessingAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error in event processing: {ex.Message}");
        }
    }

    private static async Task ReceiveEventsWithConsumer()
    {
        // Use DefaultAzureCredential for authentication
        var credential = new DefaultAzureCredential();
        
        await using var consumerClient = new EventHubConsumerClient(
            EventHubConsumerClient.DefaultConsumerGroupName, 
            fullyQualifiedNamespace, 
            eventHubName,
            credential);

        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.CancelAfter(TimeSpan.FromSeconds(45));

        try
        {
            Console.WriteLine("Reading events from all partitions...");
            
            await foreach (PartitionEvent receivedEvent in consumerClient.ReadEventsAsync(cancellationSource.Token))
            {
                Console.WriteLine($"Event received from partition: {receivedEvent.Partition.PartitionId}");
                Console.WriteLine($"Sequence number: {receivedEvent.Data.SequenceNumber}");
                Console.WriteLine($"Offset: {receivedEvent.Data.OffsetString}");
                Console.WriteLine($"Enqueued time: {receivedEvent.Data.EnqueuedTime}");

                // Process custom properties
                foreach (var property in receivedEvent.Data.Properties)
                {
                    Console.WriteLine($"Property: {property.Key} = {property.Value}");
                }

                // Process the event body
                string eventBody = Encoding.UTF8.GetString(receivedEvent.Data.Body.ToArray());
                Console.WriteLine($"Event body: {eventBody}");

                try
                {
                    var jsonDocument = JsonDocument.Parse(eventBody);
                    Console.WriteLine($"Parsed JSON successfully");
                }
                catch (JsonException)
                {
                    Console.WriteLine("Event body is not valid JSON");
                }

                Console.WriteLine(new string('-', 50));
            }
        }
        catch (TaskCanceledException)
        {
            Console.WriteLine("Event reading has been cancelled.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading events: {ex.Message}");
        }
    }

    private static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
    {
        Console.WriteLine($"Received event from partition: {eventArgs.Partition.PartitionId}");
        Console.WriteLine($"Sequence number: {eventArgs.Data.SequenceNumber}");
        Console.WriteLine($"Offset: {eventArgs.Data.OffsetString}");
        Console.WriteLine($"Enqueued time: {eventArgs.Data.EnqueuedTime}");

        // Process custom properties
        foreach (var property in eventArgs.Data.Properties)
        {
            Console.WriteLine($"Property: {property.Key} = {property.Value}");
        }

        // Process the event body
        string eventBody = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
        Console.WriteLine($"Event body: {eventBody}");

        try
        {
            var jsonDocument = JsonDocument.Parse(eventBody);
            Console.WriteLine($"Parsed JSON successfully");
        }
        catch (JsonException)
        {
            Console.WriteLine("Event body is not valid JSON");
        }

        // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
        await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        Console.WriteLine("Checkpoint updated");
        Console.WriteLine(new string('-', 50));
    }

    private static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
    {
        Console.WriteLine($"Partnership '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
        Console.WriteLine(eventArgs.Exception.Message);
        return Task.CompletedTask;
    }
}
