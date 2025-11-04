import asyncio
import os

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.aio._eventprocessor.partition_context import PartitionContext # Debugging 을 위해
from azure.eventhub._common import EventData # Debugging 을 위해
from azure.identity.aio import DefaultAzureCredential
from dotenv import load_dotenv

load_dotenv()


async def on_event(partition_context: PartitionContext, event: EventData):
    """이벤트를 수신할 때 호출되는 콜백 함수"""
    print(
        f"Received event from partition {partition_context.partition_id}: "
        f"{event.body_as_str()}"
    )
    await partition_context.update_checkpoint(event)


async def main():
    credential = DefaultAzureCredential()
    event_hub_namespace = os.getenv("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE")
    event_hub_name = os.getenv("EVENT_HUB_NAME")
    consumer_group = os.getenv("CONSUMER_GROUP", "$Default")

    print(f"Using Event Hub: {event_hub_name}")
    print(f"Consumer Group: {consumer_group}")
    print("이벤트 수신 대기 중... (Ctrl+C로 종료)\n")

    consumer = EventHubConsumerClient(
        fully_qualified_namespace=event_hub_namespace,
        eventhub_name=event_hub_name,
        consumer_group=consumer_group,
        credential=credential,
    )

    try:
        async with consumer:
            
            # starting_position 이 없으면 체크포인트 이후 부터 수신된 메시지만 처리, 삭제는 retention 기간 이후 자동 삭제됨.
            await consumer.receive(
                on_event=on_event
            )
            
            # await consumer.receive(
            #     on_event=on_event,
            #     starting_position="-1",  # 최신 이벤트부터 수신
            # )
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n프로그램이 중단되었습니다.")
    finally:
        await credential.close()


if __name__ == "__main__":
    asyncio.run(main())
