"""
Azure Event Hub 이벤트 수신 스크립트
이 스크립트는 Event Hub에서 이벤트를 수신하고 체크포인트를 관리합니다.
"""
import asyncio
import os

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.aio._eventprocessor.partition_context import PartitionContext # Debugging 을 위해
from azure.eventhub._common import EventData # Debugging 을 위해
from azure.identity.aio import DefaultAzureCredential
from dotenv import load_dotenv

# .env 파일에서 환경 변수를 로드합니다.
load_dotenv()


async def on_event(partition_context: PartitionContext, event: EventData):
    """
    이벤트를 수신할 때 호출되는 콜백 함수
    
    Args:
        partition_context: 파티션 컨텍스트 (파티션 ID, 체크포인트 정보 등 포함)
        event: 수신된 이벤트 데이터
    """
    # 수신된 이벤트의 파티션 ID와 본문 내용을 출력합니다.
    print(
        f"Received event from partition {partition_context.partition_id}: "
        f"{event.body_as_str()}"
    )
    
    # 이 이벤트까지 처리했다는 체크포인트를 업데이트합니다.
    # 체크포인트를 저장하면 프로그램 재시작 시 이 지점부터 다시 읽을 수 있습니다.
    await partition_context.update_checkpoint(event)


async def main():
    """
    메인 함수 - Event Hub에서 이벤트를 수신합니다.
    """
    # Azure 인증을 위한 DefaultAzureCredential 객체 생성
    credential = DefaultAzureCredential()
    
    # 환경 변수에서 Event Hub 설정을 가져옵니다.
    event_hub_namespace = os.getenv("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE")
    event_hub_name = os.getenv("EVENT_HUB_NAME")
    # 컨슈머 그룹 설정 (기본값: $Default)
    consumer_group = os.getenv("CONSUMER_GROUP", "$Default")

    print(f"Using Event Hub: {event_hub_name}")
    print(f"Consumer Group: {consumer_group}")
    print("이벤트 수신 대기 중... (Ctrl+C로 종료)\n")

    # EventHubConsumerClient 인스턴스를 생성합니다.
    # 이 클라이언트를 통해 Event Hub에서 이벤트를 수신할 수 있습니다.
    consumer = EventHubConsumerClient(
        fully_qualified_namespace=event_hub_namespace,
        eventhub_name=event_hub_name,
        consumer_group=consumer_group,
        credential=credential,
    )

    try:
        # async with 구문을 사용하여 리소스를 자동으로 정리합니다.
        async with consumer:
            # 이벤트 수신을 시작합니다.
            # starting_position이 없으면 체크포인트 이후부터 수신된 메시지만 처리합니다.
            # 삭제는 retention 기간 이후 자동으로 이루어집니다.
            await consumer.receive(
                on_event=on_event
            )
            
            # 아래 주석 처리된 코드는 최신 이벤트부터 수신하는 예제입니다.
            # await consumer.receive(
            #     on_event=on_event,
            #     starting_position="-1",  # 최신 이벤트부터 수신
            # )
    except (KeyboardInterrupt, asyncio.CancelledError):
        # 사용자가 Ctrl+C를 누르거나 프로그램이 취소될 때 처리
        print("\n프로그램이 중단되었습니다.")
    finally:
        # 항상 인증 자격 증명을 종료합니다.
        await credential.close()


if __name__ == "__main__":
    # asyncio를 사용하여 비동기 main 함수를 실행합니다.
    asyncio.run(main())
