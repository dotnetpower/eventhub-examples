// Azure Event Hub에서 메시지를 수신하는 .NET 콘솔 애플리케이션
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
    // Event Hub의 전체 주소 (예: namespace.servicebus.windows.net)
    private static string fullyQualifiedNamespace = null!;
    // Event Hub 이름
    private static string eventHubName = null!;
    // Storage Account 이름 (체크포인팅용, 선택 사항)
    private static string? storageAccountName;
    // Blob 컨테이너 이름 (체크포인팅용)
    private static string blobContainerName = null!;

    /// <summary>
    /// 여러 경로에서 .env 파일을 찾아 환경 변수를 로드합니다.
    /// </summary>
    private static void LoadEnvironmentFile()
    {
        // .env 파일을 찾을 수 있는 여러 경로를 정의합니다.
        var possiblePaths = new[]
        {
            ".env", // 현재 디렉터리
            Path.Combine("..", ".env"), // 부모 디렉터리
            Path.Combine("..", "..", ".env"), // 조부모 디렉터리
            Path.Combine("..", "..", "..", ".env"), // 증조부모 디렉터리
            Path.Combine(Directory.GetCurrentDirectory(), ".env"), // 명시적 현재 디렉터리
            Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", ".env") // 프로젝트 루트
        };

        // 각 경로를 순회하면서 .env 파일을 찾습니다.
        foreach (var envPath in possiblePaths)
        {
            try
            {
                var fullPath = Path.GetFullPath(envPath);
                if (File.Exists(fullPath))
                {
                    // .env 파일을 찾으면 로드하고 종료합니다.
                    Env.Load(fullPath);
                    Console.WriteLine($"Loaded .env file from: {fullPath}");
                    return;
                }
            }
            catch (Exception ex)
            {
                // 경로 확인 중 오류가 발생하면 로그를 출력하고 계속 진행합니다.
                Console.WriteLine($"Failed to check path {envPath}: {ex.Message}");
            }
        }
        
        // .env 파일을 찾지 못한 경우 시스템 환경 변수만 사용합니다.
        Console.WriteLine("No .env file found. Using system environment variables only.");
    }

    /// <summary>
    /// 애플리케이션의 진입점 - Event Hub에서 이벤트를 수신합니다.
    /// </summary>
    static async Task Main(string[] args)
    {
        // 여러 경로에서 .env 파일을 찾아 로드를 시도합니다.
        LoadEnvironmentFile();

        // 환경 변수에서 필수 설정을 가져옵니다.
        fullyQualifiedNamespace = Environment.GetEnvironmentVariable("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE") 
            ?? throw new ArgumentException("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE environment variable is required");
        
        eventHubName = Environment.GetEnvironmentVariable("EVENT_HUB_NAME") 
            ?? throw new ArgumentException("EVENT_HUB_NAME environment variable is required");

        // Storage Account 설정 (체크포인팅을 위한 선택 사항)
        storageAccountName = Environment.GetEnvironmentVariable("STORAGE_ACCOUNT_NAME");
        blobContainerName = Environment.GetEnvironmentVariable("BLOB_CONTAINER_NAME") ?? "checkpoints";

        Console.WriteLine("Azure Event Hub Receiver Starting...");
        Console.WriteLine($"Namespace: {fullyQualifiedNamespace}");
        Console.WriteLine($"Event Hub: {eventHubName}");

        // Storage Account가 설정되어 있으면 체크포인팅을 사용하는 EventProcessorClient를 사용합니다.
        // 그렇지 않으면 단순한 EventHubConsumerClient를 사용합니다.
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

    /// <summary>
    /// EventProcessorClient를 사용하여 체크포인팅과 함께 이벤트를 수신합니다.
    /// </summary>
    private static async Task ReceiveEventsWithProcessor()
    {
        // Azure 인증에 DefaultAzureCredential을 사용합니다.
        var credential = new DefaultAzureCredential();

        // 체크포인팅을 위한 Blob 컨테이너 클라이언트를 생성합니다.
        // DefaultAzureCredential을 사용하여 Storage Account에 인증합니다.
        var storageUri = new Uri($"https://{storageAccountName}.blob.core.windows.net/{blobContainerName}");
        var storageClient = new BlobContainerClient(storageUri, credential);
        // 컨테이너가 존재하지 않으면 생성합니다.
        await storageClient.CreateIfNotExistsAsync();

        // Event Processor 클라이언트를 생성합니다.
        // 이 클라이언트는 여러 파티션에서 이벤트를 자동으로 로드 밸런싱하고 체크포인트를 관리합니다.
        var processor = new EventProcessorClient(storageClient, EventHubConsumerClient.DefaultConsumerGroupName, 
            fullyQualifiedNamespace, eventHubName, credential);

        // 이벤트 처리 및 오류 처리를 위한 핸들러를 등록합니다.
        processor.ProcessEventAsync += ProcessEventHandler;
        processor.ProcessErrorAsync += ProcessErrorHandler;

        try
        {
            // 이벤트 처리를 시작합니다.
            await processor.StartProcessingAsync();

            Console.WriteLine("Processing events. Press 'q' to quit...");
            // 사용자가 'q' 키를 누를 때까지 이벤트를 계속 처리합니다.
            while (Console.ReadKey().Key != ConsoleKey.Q) 
            {
                await Task.Delay(100);
            }

            // 이벤트 처리를 중지합니다.
            await processor.StopProcessingAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error in event processing: {ex.Message}");
        }
    }

    /// <summary>
    /// EventHubConsumerClient를 사용하여 체크포인팅 없이 이벤트를 수신합니다.
    /// </summary>
    private static async Task ReceiveEventsWithConsumer()
    {
        // Azure 인증에 DefaultAzureCredential을 사용합니다.
        var credential = new DefaultAzureCredential();
        
        // Consumer 클라이언트를 생성합니다.
        // 기본 컨슈머 그룹($Default)을 사용합니다.
        await using var consumerClient = new EventHubConsumerClient(
            EventHubConsumerClient.DefaultConsumerGroupName, 
            fullyQualifiedNamespace, 
            eventHubName,
            credential);

        // 45초 후 자동으로 취소되도록 CancellationToken을 설정합니다.
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.CancelAfter(TimeSpan.FromSeconds(45));

        try
        {
            Console.WriteLine("Reading events from all partitions...");
            
            // 모든 파티션에서 이벤트를 비동기적으로 읽어옵니다.
            await foreach (PartitionEvent receivedEvent in consumerClient.ReadEventsAsync(cancellationSource.Token))
            {
                // 수신된 이벤트의 메타데이터를 출력합니다.
                Console.WriteLine($"Event received from partition: {receivedEvent.Partition.PartitionId}");
                Console.WriteLine($"Sequence number: {receivedEvent.Data.SequenceNumber}");
                Console.WriteLine($"Offset: {receivedEvent.Data.OffsetString}");
                Console.WriteLine($"Enqueued time: {receivedEvent.Data.EnqueuedTime}");

                // 사용자 정의 속성을 처리합니다.
                foreach (var property in receivedEvent.Data.Properties)
                {
                    Console.WriteLine($"Property: {property.Key} = {property.Value}");
                }

                // 이벤트 본문을 문자열로 변환하여 출력합니다.
                string eventBody = Encoding.UTF8.GetString(receivedEvent.Data.Body.ToArray());
                Console.WriteLine($"Event body: {eventBody}");

                try
                {
                    // JSON 형식인지 확인합니다.
                    var jsonDocument = JsonDocument.Parse(eventBody);
                    Console.WriteLine($"Parsed JSON successfully");
                }
                catch (JsonException)
                {
                    // JSON이 아닌 경우 메시지를 출력합니다.
                    Console.WriteLine("Event body is not valid JSON");
                }

                Console.WriteLine(new string('-', 50));
            }
        }
        catch (TaskCanceledException)
        {
            // 타임아웃이 발생하면 메시지를 출력합니다.
            Console.WriteLine("Event reading has been cancelled.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading events: {ex.Message}");
        }
    }

    /// <summary>
    /// 이벤트가 수신되었을 때 호출되는 핸들러
    /// </summary>
    /// <param name="eventArgs">이벤트 인자 (파티션 정보 및 이벤트 데이터 포함)</param>
    private static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
    {
        // 수신된 이벤트의 메타데이터를 출력합니다.
        Console.WriteLine($"Received event from partition: {eventArgs.Partition.PartitionId}");
        Console.WriteLine($"Sequence number: {eventArgs.Data.SequenceNumber}");
        Console.WriteLine($"Offset: {eventArgs.Data.OffsetString}");
        Console.WriteLine($"Enqueued time: {eventArgs.Data.EnqueuedTime}");

        // 사용자 정의 속성을 처리합니다.
        foreach (var property in eventArgs.Data.Properties)
        {
            Console.WriteLine($"Property: {property.Key} = {property.Value}");
        }

        // 이벤트 본문을 문자열로 변환하여 출력합니다.
        string eventBody = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
        Console.WriteLine($"Event body: {eventBody}");

        try
        {
            // JSON 형식인지 확인합니다.
            var jsonDocument = JsonDocument.Parse(eventBody);
            Console.WriteLine($"Parsed JSON successfully");
        }
        catch (JsonException)
        {
            // JSON이 아닌 경우 메시지를 출력합니다.
            Console.WriteLine("Event body is not valid JSON");
        }

        // Blob Storage에 체크포인트를 업데이트합니다.
        // 이렇게 하면 애플리케이션이 재시작될 때 새 이벤트만 수신할 수 있습니다.
        await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        Console.WriteLine("Checkpoint updated");
        Console.WriteLine(new string('-', 50));
    }

    /// <summary>
    /// 오류가 발생했을 때 호출되는 핸들러
    /// </summary>
    /// <param name="eventArgs">오류 이벤트 인자</param>
    private static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
    {
        // 예상치 못한 예외가 발생했을 때 오류 정보를 출력합니다.
        Console.WriteLine($"Partnership '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
        Console.WriteLine(eventArgs.Exception.Message);
        return Task.CompletedTask;
    }
}
