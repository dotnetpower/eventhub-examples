import asyncio
import os

from azure.eventhub.aio import EventHubConsumerClient
from azure.identity.aio import DefaultAzureCredential
from azure.schemaregistry.aio import SchemaRegistryClient
from azure.schemaregistry.encoder.avroencoder.aio import AvroEncoder
from dotenv import load_dotenv

load_dotenv()


async def main():
    credential = DefaultAzureCredential()
    event_hub_namespace = os.getenv("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE")
    event_hub_name = os.getenv("EVENT_HUB_NAME")
    consumer_group = os.getenv("CONSUMER_GROUP", "$Default")
    schema_registry_endpoint = os.getenv("SCHEMA_REGISTRY_ENDPOINT")
    schema_group = os.getenv("SCHEMA_GROUP")

    print(f"Using Event Hub: {event_hub_name}")
    print(f"Consumer Group: {consumer_group}")
    print(f"Schema Registry: {schema_registry_endpoint}")
    print(f"Schema Group: {schema_group}")
    print("이벤트 수신 대기 중... (Ctrl+C로 종료)\n")

    schema_registry_client = SchemaRegistryClient(
        fully_qualified_namespace=schema_registry_endpoint,
        credential=credential,
    )

    avro_encoder = AvroEncoder(
        client=schema_registry_client,
        group_name=schema_group,
        auto_register_schemas=True,
    )

    consumer = EventHubConsumerClient(
        fully_qualified_namespace=event_hub_namespace,
        eventhub_name=event_hub_name,
        consumer_group=consumer_group,
        credential=credential,
    )

    async def on_event(partition_context, event):
        try:
            decoded_data = await avro_encoder.decode(event)
            print(
                f"Received event from partition {partition_context.partition_id}: "
                f"{decoded_data['name']}, age={decoded_data['age']}, "
                f"message={decoded_data['message']}"
            )
            await partition_context.update_checkpoint(event)
        except Exception as e:
            print(f"Error decoding event: {e}")

    try:
        async with consumer, avro_encoder:
            await consumer.receive(on_event=on_event, starting_position="-1")
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n프로그램이 중단되었습니다.")
    finally:
        await credential.close()


if __name__ == "__main__":
    asyncio.run(main())

