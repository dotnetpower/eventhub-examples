# Schema Registry를 사용한 Event Hub 예제

Azure Schema Registry를 사용하여 Avro 형식으로 구조화된 데이터를 송수신하는 예제입니다.

## 파일

- `send.py` - Avro 스키마를 사용하여 이벤트 전송
- `recv.py` - Schema Registry를 통해 이벤트 수신 및 디코딩

## 실행 방법

### 이벤트 수신 (터미널 1)

```bash
python recv.py
```

### 이벤트 전송 (터미널 2)

```bash
python send.py
```

## 필요한 패키지

```bash
pip install azure-eventhub azure-identity azure-schemaregistry azure-schemaregistry-avroencoder python-dotenv
```

## 필요한 환경 변수

```
EVENT_HUB_FULLY_QUALIFIED_NAMESPACE=your-namespace.servicebus.windows.net
EVENT_HUB_NAME=your-eventhub-name
CONSUMER_GROUP=$Default
SCHEMA_REGISTRY_ENDPOINT=your-namespace.servicebus.windows.net
SCHEMA_GROUP=your-schema-group
```

## Schema Registry 설정

1. Azure Portal에서 Event Hubs 네임스페이스로 이동
2. 왼쪽 메뉴에서 "Schema Registry" 선택
3. 새 스키마 그룹 생성

## 주요 기능

- Avro 스키마 기반 직렬화/역직렬화
- 자동 스키마 등록
- 스키마 버전 관리
- 타입 안정성 보장
- 효율적인 바이너리 포맷

