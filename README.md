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
4. "공유 액세스 정책"에서 "RootManageSharedAccessKey" 정책을 선택하고 연결 문자열을 복사합니다.


```bash

# uv 초기화 후 가상환경 및 Python 버전 설정
uv init
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