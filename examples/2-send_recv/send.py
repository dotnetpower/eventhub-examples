import asyncio
import os

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import DefaultAzureCredential
from dotenv import load_dotenv

load_dotenv()


async def main():
    credential = DefaultAzureCredential()
    event_hub_namespace = os.getenv("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE")
    event_hub_name = os.getenv("EVENT_HUB_NAME")

    print(f"Using Event Hub: {event_hub_name}")
    print("이벤트 전송 시작... (Ctrl+C로 종료)\n")

    producer = EventHubProducerClient(
        fully_qualified_namespace=event_hub_namespace,
        eventhub_name=event_hub_name,
        credential=credential,
    )

    try:
        async with producer:
            i = 0
            while True:
                data = f"Event from send.py - {i}"
                event = EventData(data)
                await producer.send_event(event)
                print(f"Sent event: {data}")
                i += 1
                await asyncio.sleep(0.5)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n프로그램이 중단되었습니다.")
    finally:
        await credential.close()


if __name__ == "__main__":
    asyncio.run(main())
