# .NET Event Hub Examples

이 폴더에는 Azure Event Hubs를 사용하는 .NET 예제가 포함되어 있습니다.

## 프로젝트 구조

- **EventHubSender**: Event Hub에 메시지를 전송하는 콘솔 애플리케이션
- **EventHubReceiver**: Event Hub에서 메시지를 수신하는 콘솔 애플리케이션

## 요구 사항

- .NET 8.0 이상
- Azure Event Hubs 네임스페이스 및 이벤트 허브
- (선택사항) Azure Storage Account (체크포인팅을 위한)
- Azure CLI 또는 Azure PowerShell (인증을 위한)

## 인증 방법

이 예제는 **DefaultAzureCredential**을 사용하여 Azure에 인증합니다. 다음 중 하나의 방법으로 인증할 수 있습니다:

### 1. Azure CLI 로그인 (권장)
```bash
az login
```

### 2. Service Principal 환경변수
```bash
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
export AZURE_TENANT_ID="your-tenant-id"
```

### 3. Managed Identity (Azure VM/Container에서 실행 시)
별도 설정 불필요 - 자동으로 사용됩니다.

## 환경 변수 설정

두 가지 방법으로 환경 변수를 설정할 수 있습니다:

### 방법 1: .env 파일 사용 (권장)

1. 프로젝트 루트에 `.env` 파일을 생성합니다:
   ```bash
   cd /home/dotnetpower/dev/eventhub-examples
   cp .env.example .env
   ```

2. `.env` 파일을 편집하여 실제 값을 입력합니다:
   ```bash
   # Azure Event Hub Configuration
   EVENT_HUB_FULLY_QUALIFIED_NAMESPACE=your-namespace.servicebus.windows.net
   EVENT_HUB_NAME=your-event-hub-name
   
   # Storage Account for Checkpointing (선택사항)
   STORAGE_ACCOUNT_NAME=your-storage-account-name
   BLOB_CONTAINER_NAME=checkpoints
   ```

**참고**: 프로그램은 여러 위치에서 `.env` 파일을 자동으로 찾습니다:
- 현재 디렉터리
- 부모 디렉터리들
- 프로젝트 루트 디렉터리

### 방법 2: 환경 변수 직접 설정

```bash
# Event Hub 설정 (필수)
export EVENT_HUB_FULLY_QUALIFIED_NAMESPACE="your-namespace.servicebus.windows.net"
export EVENT_HUB_NAME="your-event-hub-name"

# 체크포인팅을 사용하려면 (Receiver에서만, 선택사항):
export STORAGE_ACCOUNT_NAME="your-storage-account-name"
export BLOB_CONTAINER_NAME="checkpoints"
```

## Azure 권한 설정

Event Hub와 Storage Account에 다음 역할을 할당해야 합니다:

### Event Hub 권한
- **Azure Event Hubs Data Owner** (또는)
- **Azure Event Hubs Data Sender** (Sender용)
- **Azure Event Hubs Data Receiver** (Receiver용)

### Storage Account 권한 (체크포인팅용)
- **Storage Blob Data Contributor**

권한 할당 예시:
```bash
# Event Hub 권한 할당
az role assignment create \
  --assignee "your-user-or-service-principal" \
  --role "Azure Event Hubs Data Owner" \
  --scope "/subscriptions/your-subscription-id/resourceGroups/your-rg/providers/Microsoft.EventHub/namespaces/your-namespace"

# Storage Account 권한 할당
az role assignment create \
  --assignee "your-user-or-service-principal" \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/your-subscription-id/resourceGroups/your-rg/providers/Microsoft.Storage/storageAccounts/your-storage-account"
```

## 빌드 및 실행

### Sender 실행

```bash
cd EventHubSender
dotnet run
```

### Receiver 실행

```bash
cd EventHubReceiver
dotnet run
```

## VS Code 디버깅

이 프로젝트는 VS Code에서 디버깅을 위한 구성이 포함되어 있습니다.

### 디버깅 구성
- **.NET: EventHub Sender**: EventHubSender 프로젝트 디버깅
- **.NET: EventHub Receiver**: EventHubReceiver 프로젝트 디버깅

### 사용 방법
1. VS Code에서 프로젝트 루트 폴더를 엽니다
2. 환경 변수를 설정합니다 (터미널에서):
   ```bash
   export EVENT_HUB_FULLY_QUALIFIED_NAMESPACE="your-namespace.servicebus.windows.net"
   export EVENT_HUB_NAME="your-event-hub-name"
   # 체크포인팅 사용 시:
   export STORAGE_ACCOUNT_NAME="your-storage-account"
   export BLOB_CONTAINER_NAME="checkpoints"
   ```
3. `F5`를 누르거나 Run and Debug 패널에서 구성을 선택합니다
4. 브레이크포인트를 설정하여 디버깅할 수 있습니다

### 빌드 태스크
- **build-sender**: EventHubSender 빌드
- **build-receiver**: EventHubReceiver 빌드  
- **build-all-dotnet**: 모든 .NET 프로젝트 빌드
- **watch-sender**: EventHubSender 파일 변경 감시 및 자동 실행
- **watch-receiver**: EventHubReceiver 파일 변경 감시 및 자동 실행

`Ctrl+Shift+P` -> "Tasks: Run Task"로 태스크를 실행할 수 있습니다.

## 기능

### EventHubSender
- 1000개의 JSON 메시지를 Event Hub에 배치로 전송
- 각 메시지에 사용자 정의 속성 추가
- 오류 처리 및 로깅

### EventHubReceiver
- 두 가지 수신 모드 지원:
  1. **EventProcessorClient**: Azure Storage를 사용한 체크포인팅 (권장)
  2. **EventHubConsumerClient**: 단순 수신 (체크포인팅 없음)
- 모든 파티션에서 이벤트 수신
- JSON 파싱 및 사용자 정의 속성 처리
- 자동 체크포인팅 (EventProcessorClient 사용 시)

## 참고사항

- EventProcessorClient를 사용할 때는 Azure Storage 연결 문자열이 필요합니다
- Storage 연결 문자열이 없으면 자동으로 EventHubConsumerClient를 사용합니다
- Receiver는 'q' 키를 눌러 종료할 수 있습니다 (EventProcessorClient 모드)
- EventHubConsumerClient 모드는 45초 후 자동 종료됩니다