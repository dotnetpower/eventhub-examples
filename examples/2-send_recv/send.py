"""
Azure Event Hub 이벤트 전송 스크립트
이 스크립트는 Event Hub에 이벤트를 개별적으로 연속해서 전송합니다.
0.5초 간격으로 이벤트를 전송하며, Ctrl+C로 중단할 수 있습니다.
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
    메인 함수 - Event Hub에 개별 이벤트를 연속적으로 전송합니다.
    """
    # Azure 인증을 위한 DefaultAzureCredential 객체 생성
    credential = DefaultAzureCredential()
    
    # 환경 변수에서 Event Hub 설정을 가져옵니다.
    event_hub_namespace = os.getenv("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE")
    event_hub_name = os.getenv("EVENT_HUB_NAME")

    print(f"Using Event Hub: {event_hub_name}")
    print("이벤트 전송 시작... (Ctrl+C로 종료)\n")

    # EventHubProducerClient 인스턴스를 생성합니다.
    producer = EventHubProducerClient(
        fully_qualified_namespace=event_hub_namespace,
        eventhub_name=event_hub_name,
        credential=credential,
    )

    try:
        # async with 구문을 사용하여 리소스를 자동으로 정리합니다.
        async with producer:
            i = 0
            # 무한 루프로 이벤트를 계속 전송합니다.
            while True:
                # 이벤트 데이터 생성
                data = f"Event from send.py - {i}"
                event = EventData(data)
                
                # 개별 이벤트를 즉시 전송합니다.
                await producer.send_event(event)
                print(f"Sent event: {data}")
                
                i += 1
                # 0.5초 대기 (이벤트 전송 속도 조절)
                await asyncio.sleep(0.5)
    except (KeyboardInterrupt, asyncio.CancelledError):
        # 사용자가 Ctrl+C를 누르거나 프로그램이 취소될 때 처리
        print("\n프로그램이 중단되었습니다.")
    finally:
        # 항상 인증 자격 증명을 종료합니다.
        await credential.close()


if __name__ == "__main__":
    # asyncio를 사용하여 비동기 main 함수를 실행합니다.
    asyncio.run(main())
