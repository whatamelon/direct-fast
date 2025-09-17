# Direct Agent API

FastAPI와 uv를 사용한 백엔드 서버입니다.

## 기능

- RESTful API 엔드포인트
- 자동 API 문서 생성 (Swagger UI)
- Pydantic을 사용한 데이터 검증
- CRUD 작업 지원
- 카페24 쇼핑몰 API 연동
- 제품 관리 및 조회 기능

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

# 카페24 API 설정
CAFE24_MALL_ID=your_mall_id
CAFE24_CLIENT_ID=your_client_id
CAFE24_CLIENT_SECRET=your_client_secret
CAFE24_REFRESH_TOKEN=your_refresh_token

# AWS S3 설정 (메타 광고 이미지 생성용)
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_S3_BUCKET=your_s3_bucket_name
AWS_REGION_NAME=ap-northeast-2
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

### 기본 엔드포인트

- `GET /` - 루트 엔드포인트
- `GET /health` - 헬스 체크
- `GET /config` - 환경변수 설정 정보 조회 (개발용)

### 아이템 관리

- `GET /items` - 모든 아이템 조회
- `GET /items/{item_id}` - 특정 아이템 조회
- `POST /items` - 새 아이템 생성
- `PUT /items/{item_id}` - 아이템 업데이트
- `DELETE /items/{item_id}` - 아이템 삭제

### 카페24 API 연동

- `GET /api/v1/cafe24/products` - 제품 목록 조회
- `GET /api/v1/cafe24/products/{product_no}` - 특정 제품 상세 조회
- `GET /api/v1/cafe24/products/search` - 제품 검색
- `GET /api/v1/cafe24/health` - 카페24 API 연결 상태 확인

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

## 카페24 API 사용법

### 환경변수 설정

카페24 API를 사용하기 위해 다음 환경변수를 설정해야 합니다:

```bash
# .env 파일에 추가
CAFE24_MALL_ID=your_mall_id
CAFE24_CLIENT_ID=your_client_id
CAFE24_CLIENT_SECRET=your_client_secret
CAFE24_REFRESH_TOKEN=your_refresh_token
```

### Python 코드에서 사용

```python
from src.interfaces.cafe24 import create_cafe24_client, get_products, get_product

# 클라이언트 생성
client = create_cafe24_client()

# 제품 목록 조회
products = client.get_products(limit=10)
print(f"총 제품 수: {products['count']}")

# 특정 제품 조회
product = client.get_product(product_no=12345)
print(f"제품명: {product['product']['product_name']}")

# 편의 함수 사용
products = get_products(limit=5, display=True)  # 진열 중인 제품만
```

### API 엔드포인트 사용

#### 제품 목록 조회

```bash
# 기본 조회
curl "http://localhost:8000/api/v1/cafe24/products"

# 필터링 조회
curl "http://localhost:8000/api/v1/cafe24/products?display=true&limit=5"

# 이미지 포함 조회
curl "http://localhost:8000/api/v1/cafe24/products?embed=images,variants"
```

#### 제품 검색

```bash
# 제품명으로 검색
curl "http://localhost:8000/api/v1/cafe24/products/search?q=테스트&limit=10"
```

#### 특정 제품 조회

```bash
curl "http://localhost:8000/api/v1/cafe24/products/12345"
```

#### 연결 상태 확인

```bash
curl "http://localhost:8000/api/v1/cafe24/health"
```

### 사용 예시 스크립트

프로젝트에 포함된 예시 스크립트를 실행할 수 있습니다:

```bash
uv run python examples/cafe24_example.py
```

### 지원하는 기능

- 제품 목록 조회 (페이지네이션, 필터링 지원)
- 제품 상세 조회
- 제품 검색 (제품명, 제품 코드)
- 제품 생성, 수정, 삭제
- 이미지 및 옵션 정보 포함 조회
- 자동 액세스 토큰 갱신
- 타입 힌트 및 데이터 검증

## 메타 카탈로그 광고 이미지 생성

### 기능

- 카페24에서 진열중이고 판매중인 상품을 자동으로 가져옴
- 각 상품에 대해 메타 광고용 이미지를 자동 생성
- 생성된 이미지를 S3에 업로드
- 메타 카탈로그용 CSV 파일 생성 및 S3 업로드
- 매일 새벽 3시에 자동 실행되는 스케줄러

### 사용법

#### 1. 환경변수 설정

```bash
# .env 파일에 추가
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_S3_BUCKET=your_s3_bucket_name
AWS_REGION_NAME=ap-northeast-2
```

#### 2. 수동 실행

```python
from src.events.meta_catalog_ad.index import test_meta_catalog_process

# 테스트 실행 (5개 상품만 처리)
result = await test_meta_catalog_process()
print(result)
```

#### 3. 스케줄러 확인

```python
from src.tasks.jobs import get_all_jobs

# 등록된 작업 목록 확인
jobs = get_all_jobs()
for job in jobs:
    print(f"작업 ID: {job['id']}, 다음 실행: {job['next_run_time']}")
```

### 생성되는 파일

1. **메타 광고 이미지**: `meta_image/{product_code}.jpg`
2. **카탈로그 CSV**: `meta_catalog/relay_meta_dept_{YYYYMMDD_HHMMSS}.csv`

### CSV 형식

생성되는 CSV는 다음 컬럼을 포함합니다:

- `id`: 상품번호
- `title`: 상품명
- `description`: 상품 설명
- `availability`: 재고 상태 (in stock)
- `condition`: 상품 상태 (used)
- `price`: 소비자가
- `link`: 상품 링크
- `image_link`: 메타 광고 이미지 URL
- `brand`: 브랜드명
- `sale_price`: 판매가
- `color`: 색상 정보

### 필터링 옵션

제품 목록 조회 시 다음 필터를 사용할 수 있습니다:

- `limit`: 조회할 제품 수 (최대 100)
- `offset`: 조회 시작 위치
- `product_name`: 제품명으로 검색
- `product_code`: 제품 코드로 검색
- `display`: 진열 여부 (true/false)
- `selling`: 판매 여부 (true/false)
- `product_condition`: 제품 상태
- `created_start_date`, `created_end_date`: 생성일 범위
- `updated_start_date`, `updated_end_date`: 수정일 범위
- `embed`: 포함할 추가 정보 (images, variants, categories)

# direct-fast
