"""
Azure Event Hub 기본 데모 스크립트
이 스크립트는 Event Hub에 3000개의 이벤트를 배치로 전송하는 기본적인 예제입니다.
"""
import asyncio
import os

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import DefaultAzureCredential
from dotenv import load_dotenv

# .env 파일에서 환경 변수를 로드합니다.
load_dotenv()

async def main():
    """
    메인 함수 - Event Hub에 이벤트를 배치로 전송합니다.
    """
    # Azure 인증을 위한 DefaultAzureCredential 객체 생성
    # Azure CLI, Managed Identity, 환경 변수 등 다양한 인증 방법을 자동으로 시도합니다.
    credential = DefaultAzureCredential()
    
    # 환경 변수에서 Event Hub 네임스페이스와 이벤트 허브 이름을 가져옵니다.
    EVENT_HUB_FULLY_QUALIFIED_NAMESPACE = os.getenv("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE")
    event_hub_name = os.getenv("EVENT_HUB_NAME")
    
    print(f"Using Event Hub: {event_hub_name}")

    # EventHubProducerClient 인스턴스를 생성합니다.
    # 이 클라이언트를 통해 Event Hub에 이벤트를 전송할 수 있습니다.
    producer = EventHubProducerClient(
        fully_qualified_namespace=EVENT_HUB_FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=event_hub_name,
        credential=credential
    )

    # async with 구문을 사용하여 리소스를 자동으로 정리합니다.
    async with producer:
        # 이벤트 배치를 생성합니다.
        # 배치를 사용하면 여러 이벤트를 하나의 요청으로 효율적으로 전송할 수 있습니다.
        event_data_batch = await producer.create_batch()

        # 3000개의 이벤트를 생성하여 배치에 추가합니다.
        for i in range(3000):
            data = f"Event from demo.py - {i}"
            # EventData 객체를 생성하여 배치에 추가
            event_data_batch.add(EventData(data))
            print(f"Added event: {data}")

        # 이벤트 배치를 Event Hub로 전송합니다.
        await producer.send_batch(event_data_batch)
        print("이벤트가 성공적으로 전송되었습니다.")

        # 인증 자격 증명을 종료합니다.
        await credential.close()


if __name__ == "__main__":
    # asyncio를 사용하여 비동기 main 함수를 실행합니다.
    asyncio.run(main())