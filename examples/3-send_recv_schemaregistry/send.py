import asyncio
import os

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import DefaultAzureCredential
from azure.schemaregistry.aio import SchemaRegistryClient
from azure.schemaregistry.encoder.avroencoder.aio import AvroEncoder
from dotenv import load_dotenv

load_dotenv()

# Schema Group 에 Schema name 은 example.avro.User 입니다.
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
    credential = DefaultAzureCredential()
    event_hub_namespace = os.getenv("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE")
    event_hub_name = os.getenv("EVENT_HUB_NAME")
    schema_registry_endpoint = os.getenv("SCHEMA_REGISTRY_ENDPOINT")
    schema_group = os.getenv("SCHEMA_GROUP")

    print(f"Using Event Hub: {event_hub_name}")
    print(f"Schema Registry: {schema_registry_endpoint}")
    print(f"Schema Group: {schema_group}")
    print("이벤트 전송 시작... (Ctrl+C로 종료)\n")

    schema_registry_client = SchemaRegistryClient(
        fully_qualified_namespace=schema_registry_endpoint,
        credential=credential,
    )

    avro_encoder = AvroEncoder(
        client=schema_registry_client,
        group_name=schema_group,
        auto_register_schemas=True,
    )

    producer = EventHubProducerClient(
        fully_qualified_namespace=event_hub_namespace,
        eventhub_name=event_hub_name,
        credential=credential,
    )

    try:
        async with producer, avro_encoder:
            i = 0
            while True:
                data_dict = {
                    "name": f"User-{i}",
                    "age": 20 + (i % 50),
                    "message": f"Hello from send.py - {i}",
                }

                message_content = await avro_encoder.encode(
                    content=data_dict, schema=SCHEMA_STRING
                )

                event_data = EventData(body=message_content["content"])
                event_data.content_type = message_content["content_type"]
                await producer.send_event(event_data)

                print(f"Sent event: {data_dict}")
                i += 1
                await asyncio.sleep(0.5)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n프로그램이 중단되었습니다.")
    finally:
        await credential.close()


if __name__ == "__main__":
    asyncio.run(main())

