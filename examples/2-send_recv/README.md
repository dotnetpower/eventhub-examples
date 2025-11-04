# 이벤트 송수신 예제

Event Hub에 이벤트를 전송하고 수신하는 기본 예제입니다.

## 파일

- `send.py` - 이벤트를 Event Hub로 전송
- `recv.py` - Event Hub에서 이벤트 수신

## 실행 방법

### 이벤트 수신 (터미널 1)

```bash
python recv.py
```

### 이벤트 전송 (터미널 2)

```bash
python send.py
```

## 필요한 환경 변수

```
EVENT_HUB_FULLY_QUALIFIED_NAMESPACE=your-namespace.servicebus.windows.net
EVENT_HUB_NAME=your-eventhub-name
CONSUMER_GROUP=$Default
```

## 주요 기능

- 개별 이벤트 즉시 전송
- 비동기 이벤트 수신
- 체크포인트 관리
- KeyboardInterrupt 처리
