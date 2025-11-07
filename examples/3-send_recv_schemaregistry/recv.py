"""
Azure Event Hub Schema Registry 이벤트 수신 스크립트
이 스크립트는 Schema Registry를 사용하여 Avro 형식으로 인코딩된 이벤트를 수신하고 디코딩합니다.
"""
import asyncio
import os

from azure.eventhub.aio import EventHubConsumerClient
from azure.identity.aio import DefaultAzureCredential
from azure.schemaregistry.aio import SchemaRegistryClient
from azure.schemaregistry.encoder.avroencoder.aio import AvroEncoder
from dotenv import load_dotenv

# .env 파일에서 환경 변수를 로드합니다.
load_dotenv()


async def main():
    """
    메인 함수 - Schema Registry를 사용하여 Event Hub에서 이벤트를 수신하고 디코딩합니다.
    """
    # Azure 인증을 위한 DefaultAzureCredential 객체 생성
    credential = DefaultAzureCredential()
    
    # 환경 변수에서 Event Hub 및 Schema Registry 설정을 가져옵니다.
    event_hub_namespace = os.getenv("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE")
    event_hub_name = os.getenv("EVENT_HUB_NAME")
    # 컨슈머 그룹 설정 (기본값: $Default)
    consumer_group = os.getenv("CONSUMER_GROUP", "$Default")
    schema_registry_endpoint = os.getenv("SCHEMA_REGISTRY_ENDPOINT")
    schema_group = os.getenv("SCHEMA_GROUP")

    print(f"Using Event Hub: {event_hub_name}")
    print(f"Consumer Group: {consumer_group}")
    print(f"Schema Registry: {schema_registry_endpoint}")
    print(f"Schema Group: {schema_group}")
    print("이벤트 수신 대기 중... (Ctrl+C로 종료)\n")

    # Schema Registry 클라이언트를 생성합니다.
    # 스키마를 조회하고 관리합니다.
    schema_registry_client = SchemaRegistryClient(
        fully_qualified_namespace=schema_registry_endpoint,
        credential=credential,
    )

    # Avro 인코더를 생성합니다.
    # Avro 형식으로 인코딩된 데이터를 디코딩합니다.
    avro_encoder = AvroEncoder(
        client=schema_registry_client,
        group_name=schema_group,
        auto_register_schemas=True,  # 필요 시 스키마를 자동으로 등록
    )

    # EventHubConsumerClient 인스턴스를 생성합니다.
    consumer = EventHubConsumerClient(
        fully_qualified_namespace=event_hub_namespace,
        eventhub_name=event_hub_name,
        consumer_group=consumer_group,
        credential=credential,
    )

    async def on_event(partition_context, event):
        """
        이벤트를 수신할 때 호출되는 콜백 함수
        
        Args:
            partition_context: 파티션 컨텍스트 (파티션 ID, 체크포인트 정보 등 포함)
            event: 수신된 이벤트 데이터
        """
        try:
            # Avro 형식으로 인코딩된 이벤트를 디코딩합니다.
            decoded_data = await avro_encoder.decode(event)
            
            # 디코딩된 데이터를 출력합니다.
            print(
                f"Received event from partition {partition_context.partition_id}: "
                f"{decoded_data['name']}, age={decoded_data['age']}, "
                f"message={decoded_data['message']}"
            )
            
            # 이 이벤트까지 처리했다는 체크포인트를 업데이트합니다.
            await partition_context.update_checkpoint(event)
        except Exception as e:
            # 디코딩 중 오류가 발생하면 오류 메시지를 출력합니다.
            print(f"Error decoding event: {e}")

    try:
        # async with 구문을 사용하여 여러 리소스를 동시에 관리합니다.
        async with consumer, avro_encoder:
            # 이벤트 수신을 시작합니다.
            # starting_position="-1"은 최신 이벤트부터 수신한다는 의미입니다.
            await consumer.receive(on_event=on_event, starting_position="-1")
    except (KeyboardInterrupt, asyncio.CancelledError):
        # 사용자가 Ctrl+C를 누르거나 프로그램이 취소될 때 처리
        print("\n프로그램이 중단되었습니다.")
    finally:
        # 항상 인증 자격 증명을 종료합니다.
        await credential.close()


if __name__ == "__main__":
    # asyncio를 사용하여 비동기 main 함수를 실행합니다.
    asyncio.run(main())

