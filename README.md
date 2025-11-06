# Azure Event Hubs 데모
이 저장소에는 Azure Event Hubs를 사용하여 실시간 데이터 스트리밍을 시연하는 데모 애플리케이션이 포함되어 있습니다. 이 데모는 이벤트 허브에 데이터를 게시하고, 소비하는 방법을 보여줍니다.

## 요구 사항
- Azure 구독
- Azure Event Hubs 네임스페이스 및 이벤트 허브
- Python 3.12 이상


## Azure Portal 에서 Event Hubs 설정
1. Azure Portal에 로그인합니다.
2. Event Hubs 네임스페이스를 생성합니다.
3. 네임스페이스 내에서 이벤트 허브를 생성합니다.

```bash
# uv 설치
curl -LsSf https://astral.sh/uv/install.sh | sh

# uv 초기화 후 가상환경 및 Python 버전 설정
uv venv --python 3.12 .venv
source .venv/bin/activate

python --version
# Python 3.12.x 출력 예상
```

# vscode 인터프리터 설정
- `Ctrl + Shift + P`를 눌러 명령 팔레트를 엽니다.
- "Python: 인터프리터 선택"을 검색하고 선택합니다.
- 목록에서 `.venv` 폴더 내의 Python 인터프리터를 선택합니다.

# 패키지 설치
```bash
uv sync
```

## 환경 변수 설정
```

export EVENT_HUB_FULLY_QUALIFIED_NAMESPACE=yournamespace.servicebus.windows.net
export EVENT_HUB_NAME=hub1
export SCHEMA_REGISTRY_ENDPOINT=yournamespace.servicebus.windows.net
export SCHEMA_GROUP=avroschema
```

# CBS Token 인증 오류 관련 안내
Event Hubs 네트워크 설정이 "Selected networks"로 되어 있을 경우, 클라이언트의 IP가 허용된 목록에 추가되어 있지 않으면 `CBS Token authentication failed` 오류가 발생할 수 있습니다.

이 오류가 발생하면 Azure Portal에서 Event Hubs 네트워크 설정에 접속하여, 클라이언트가 접근하는 IP 주소를 허용 목록에 추가해 주세요.

## 해결 방법
1. Azure Portal에서 Event Hubs 리소스의 네트워크 설정으로 이동합니다.
2. "Selected networks"가 활성화되어 있다면, "Add your client IP" 또는 "Add IP range"를 통해 접근하는 IP를 허용합니다.
3. 변경 후 클라이언트에서 다시 연결을 시도합니다.
