"""
Azure Event Hub Schema Registry 이벤트 전송 스크립트
이 스크립트는 Avro 스키마를 사용하여 구조화된 데이터를 Event Hub에 전송합니다.
Schema Registry를 통해 스키마를 자동으로 등록하고 관리합니다.
"""
import asyncio
import os

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import DefaultAzureCredential
from azure.schemaregistry.aio import SchemaRegistryClient
from azure.schemaregistry.encoder.avroencoder.aio import AvroEncoder
from dotenv import load_dotenv

# .env 파일에서 환경 변수를 로드합니다.
load_dotenv()

# Avro 스키마 정의
# Schema Group에 등록되는 스키마 이름은 example.avro.User입니다.
SCHEMA_STRING = """
{
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "message", "type": "string"}
    ]
}
"""


async def main():
    """
    메인 함수 - Avro 스키마를 사용하여 Event Hub에 이벤트를 전송합니다.
    """
    # Azure 인증을 위한 DefaultAzureCredential 객체 생성
    credential = DefaultAzureCredential()
    
    # 환경 변수에서 Event Hub 및 Schema Registry 설정을 가져옵니다.
    event_hub_namespace = os.getenv("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE")
    event_hub_name = os.getenv("EVENT_HUB_NAME")
    schema_registry_endpoint = os.getenv("SCHEMA_REGISTRY_ENDPOINT")
    schema_group = os.getenv("SCHEMA_GROUP")

    print(f"Using Event Hub: {event_hub_name}")
    print(f"Schema Registry: {schema_registry_endpoint}")
    print(f"Schema Group: {schema_group}")
    print("이벤트 전송 시작... (Ctrl+C로 종료)\n")

    # Schema Registry 클라이언트를 생성합니다.
    # 스키마의 등록 및 조회를 관리합니다.
    schema_registry_client = SchemaRegistryClient(
        fully_qualified_namespace=schema_registry_endpoint,
        credential=credential,
    )

    # Avro 인코더를 생성합니다.
    # 데이터를 Avro 형식으로 직렬화하고 스키마를 자동으로 등록합니다.
    avro_encoder = AvroEncoder(
        client=schema_registry_client,
        group_name=schema_group,
        auto_register_schemas=True,  # 스키마를 자동으로 등록
    )

    # EventHubProducerClient 인스턴스를 생성합니다.
    producer = EventHubProducerClient(
        fully_qualified_namespace=event_hub_namespace,
        eventhub_name=event_hub_name,
        credential=credential,
    )

    try:
        # async with 구문을 사용하여 여러 리소스를 동시에 관리합니다.
        async with producer, avro_encoder:
            i = 0
            # 무한 루프로 이벤트를 계속 전송합니다.
            while True:
                # Avro 스키마에 맞는 데이터 딕셔너리 생성
                data_dict = {
                    "name": f"User-{i}",
                    "age": 20 + (i % 50),  # 20~69 사이의 나이 값 생성
                    "message": f"Hello from send.py - {i}",
                }

                # 데이터를 Avro 형식으로 인코딩합니다.
                # 스키마가 없으면 자동으로 등록됩니다.
                message_content = await avro_encoder.encode(
                    content=data_dict, schema=SCHEMA_STRING
                )

                # EventData 객체를 생성하고 인코딩된 내용을 설정합니다.
                event_data = EventData(body=message_content["content"])
                event_data.content_type = message_content["content_type"]
                
                # 이벤트를 Event Hub로 전송합니다.
                await producer.send_event(event_data)

                print(f"Sent event: {data_dict}")
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

