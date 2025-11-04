import asyncio
import os

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import DefaultAzureCredential
from dotenv import load_dotenv

load_dotenv()

async def main():
    
    # 환경 변수에서 namespace 및 이벤트 허브 이름을 가져옵니다.
    credential = DefaultAzureCredential()
    EVENT_HUB_FULLY_QUALIFIED_NAMESPACE = os.getenv("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE")
    event_hub_name = os.getenv("EVENT_HUB_NAME")
    
    print(f"Using Event Hub: {event_hub_name}")

    # EventHubProducerClient 인스턴스를 생성합니다.
    producer = EventHubProducerClient(
        fully_qualified_namespace=EVENT_HUB_FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=event_hub_name,
        credential=credential
    )

    async with producer:
        # 이벤트 배치를 생성합니다.
        event_data_batch = await producer.create_batch()

        # 이벤트를 배치에 추가합니다.
        for i in range(3000):
            data = f"Event from demo.py - {i}"
            event_data_batch.add(EventData(data))
            print(f"Added event: {data}")

        # 이벤트 배치를 이벤트 허브로 보냅니다.
        await producer.send_batch(event_data_batch)
        print("이벤트가 성공적으로 전송되었습니다.")

        await credential.close()


if __name__ == "__main__":
    asyncio.run(main())