// Azure Event Hub에 메시지를 전송하는 .NET 콘솔 애플리케이션
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Identity;
using System.Text;
using System.Text.Json;
using DotNetEnv;

namespace EventHubSender;

class Program
{
    // Event Hub의 전체 주소 (예: namespace.servicebus.windows.net)
    private static string fullyQualifiedNamespace = null!;
    // Event Hub 이름
    private static string eventHubName = null!;

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
    /// 애플리케이션의 진입점 - Event Hub에 이벤트를 전송합니다.
    /// </summary>
    static async Task Main(string[] args)
    {
        // 여러 경로에서 .env 파일을 찾아 로드를 시도합니다.
        LoadEnvironmentFile();

        // 환경 변수에서 필수 설정을 가져옵니다.
        // 환경 변수가 없으면 예외를 발생시킵니다.
        fullyQualifiedNamespace = Environment.GetEnvironmentVariable("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE") 
            ?? throw new ArgumentException("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE environment variable is required");
        
        eventHubName = Environment.GetEnvironmentVariable("EVENT_HUB_NAME") 
            ?? throw new ArgumentException("EVENT_HUB_NAME environment variable is required");

        Console.WriteLine("Azure Event Hub Sender Starting...");
        Console.WriteLine($"Namespace: {fullyQualifiedNamespace}");
        Console.WriteLine($"Event Hub: {eventHubName}");

        // Azure 인증에 DefaultAzureCredential을 사용합니다.
        // Azure CLI, Managed Identity, 환경 변수 등 다양한 인증 방법을 자동으로 시도합니다.
        var credential = new DefaultAzureCredential();

        // Event Hub에 이벤트를 전송할 수 있는 Producer 클라이언트를 생성합니다.
        // await using을 사용하면 리소스가 자동으로 해제됩니다.
        await using var producerClient = new EventHubProducerClient(fullyQualifiedNamespace, eventHubName, credential);

        try
        {
            Console.WriteLine("Sending events...");
            // 이벤트 전송 메서드를 호출합니다.
            await SendEvents(producerClient);
            Console.WriteLine("Events sent successfully!");
        }
        catch (Exception ex)
        {
            // 이벤트 전송 중 오류가 발생하면 오류 메시지를 출력합니다.
            Console.WriteLine($"Error sending events: {ex.Message}");
            if (ex.InnerException != null)
            {
                Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
            }
        }
    }

    /// <summary>
    /// Event Hub에 1000개의 이벤트를 배치로 전송합니다.
    /// </summary>
    /// <param name="producerClient">Event Hub Producer 클라이언트</param>
    private static async Task SendEvents(EventHubProducerClient producerClient)
    {
        // 이벤트 배치를 생성합니다.
        // 배치를 사용하면 여러 이벤트를 하나의 요청으로 효율적으로 전송할 수 있습니다.
        using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

        // 1000개의 이벤트를 생성합니다.
        for (int i = 1; i <= 1000; i++)
        {
            // JSON 형식의 이벤트 데이터를 생성합니다.
            var eventData = new
            {
                id = Guid.NewGuid().ToString(),
                message = $"Hello Event Hub! Message {i}",
                timestamp = DateTime.UtcNow,
                sequence = i,
                sender = "dotnet-sender"
            };

            // 객체를 JSON 문자열로 직렬화합니다.
            string jsonString = JsonSerializer.Serialize(eventData, new JsonSerializerOptions
            {
                WriteIndented = true  // 읽기 쉽도록 들여쓰기 적용
            });

            // JSON 문자열을 바이트 배열로 변환하여 EventData 객체를 생성합니다.
            var eventDataObject = new EventData(Encoding.UTF8.GetBytes(jsonString));
            
            // 이벤트에 사용자 정의 속성을 추가합니다.
            // 이 속성들은 메타데이터로 사용되며 본문과 별도로 접근할 수 있습니다.
            eventDataObject.Properties.Add("MessageType", "Test");
            eventDataObject.Properties.Add("Sequence", i.ToString());

            // 이벤트를 배치에 추가를 시도합니다.
            if (!eventBatch.TryAdd(eventDataObject))
            {
                // 배치가 가득 차서 이벤트를 추가할 수 없는 경우
                // 현재 배치를 전송하고 새 배치를 생성합니다.
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine("Batch sent!");
                
                // 새 배치를 생성하고 추가하지 못한 이벤트를 다시 추가합니다.
                using var newBatch = await producerClient.CreateBatchAsync();
                if (!newBatch.TryAdd(eventDataObject))
                {
                    // 이벤트가 너무 커서 어떤 배치에도 들어갈 수 없는 경우 예외를 발생시킵니다.
                    throw new Exception("Event is too large to fit in any batch");
                }
            }

            Console.WriteLine($"Added event {i} to batch");
          
        }

        // 마지막 남은 배치를 전송합니다.
        if (eventBatch.Count > 0)
        {
            await producerClient.SendAsync(eventBatch);
            Console.WriteLine("Final batch sent!");
        }
    }
}
