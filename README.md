# Direct Agent API

FastAPI와 uv를 사용한 백엔드 서버입니다.

## 기능

- RESTful API 엔드포인트
- 자동 API 문서 생성 (Swagger UI)
- Pydantic을 사용한 데이터 검증
- CRUD 작업 지원

## 설치 및 실행

### 1. 의존성 설치

```bash
uv sync
```

### 2. 환경변수 설정

프로젝트 루트에 `.env` 파일을 생성하고 필요한 환경변수를 설정합니다:

```bash
# .env 파일 예시
APP_NAME=Direct Agent API
APP_VERSION=0.1.0
DEBUG=True
HOST=0.0.0.0
PORT=8000
DATABASE_URL=sqlite:///./direct_agent.db
API_SECRET_KEY=your-secret-key-here
JWT_SECRET_KEY=your-jwt-secret-key-here
LOG_LEVEL=INFO
```

### 3. 서버 실행

```bash
uv run start
```

또는 직접 실행:

```bash
uv run python main.py
```

### 4. API 문서 확인

서버 실행 후 다음 URL에서 API 문서를 확인할 수 있습니다:

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## API 엔드포인트

- `GET /` - 루트 엔드포인트
- `GET /health` - 헬스 체크
- `GET /config` - 환경변수 설정 정보 조회 (개발용)
- `GET /items` - 모든 아이템 조회
- `GET /items/{item_id}` - 특정 아이템 조회
- `POST /items` - 새 아이템 생성
- `PUT /items/{item_id}` - 아이템 업데이트
- `DELETE /items/{item_id}` - 아이템 삭제

## 환경변수 사용법

### 설정 클래스 사용

```python
from app.core.config import settings

# 환경변수 값 사용
print(f"앱 이름: {settings.app_name}")
print(f"디버그 모드: {settings.debug}")
print(f"데이터베이스 URL: {settings.database_url}")
```

### 새로운 환경변수 추가

1. `.env` 파일에 변수 추가:

```bash
NEW_VARIABLE=value
```

2. `app/core/config.py`의 `Settings` 클래스에 필드 추가:

```python
new_variable: str = Field(default="default_value", env="NEW_VARIABLE")
```

### 환경변수 검증

애플리케이션 시작 시 필수 환경변수가 설정되었는지 자동으로 검증합니다.

## 예시 사용법

### 아이템 생성

```bash
curl -X POST "http://localhost:8000/items" \
     -H "Content-Type: application/json" \
     -d '{"name": "테스트 아이템", "description": "테스트용 아이템입니다", "price": 1000.0}'
```

### 아이템 조회

```bash
curl -X GET "http://localhost:8000/items"
```
# direct-fast
