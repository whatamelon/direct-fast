# 스케줄러 사용법

이 문서는 Direct Agent API의 스케줄러 기능 사용법을 설명합니다.

## 개요

Direct Agent API는 APScheduler를 사용하여 백그라운드 작업을 스케줄링할 수 있습니다. 스케줄러는 다음과 같은 기능을 제공합니다:

- 간격 기반 작업 (Interval Jobs)
- Cron 표현식 기반 작업 (Cron Jobs)
- 특정 날짜/시간에 실행되는 작업 (Date Jobs)
- 작업 상태 모니터링 및 통계

## 기본 사용법

### 1. 스케줄러 시작

```python
from src.core.scheduler import start_scheduler

# 스케줄러 시작
await start_scheduler()
```

### 2. 작업 등록

#### 간격 기반 작업

```python
from src.core.scheduler import add_interval_job

# 5분마다 실행
add_interval_job(
    func=my_function,
    job_id="my_interval_job",
    minutes=5,
    name="5분마다 실행되는 작업"
)

# 1시간마다 실행
add_interval_job(
    func=my_function,
    job_id="hourly_job",
    hours=1,
    name="1시간마다 실행되는 작업"
)
```

#### Cron 기반 작업

```python
from src.core.scheduler import add_cron_job

# 매일 새벽 3시에 실행
add_cron_job(
    func=my_function,
    job_id="daily_job",
    hour=3,
    minute=0,
    name="매일 새벽 3시 작업"
)

# 매주 일요일 오전 9시에 실행
add_cron_job(
    func=my_function,
    job_id="weekly_job",
    day_of_week="sun",
    hour=9,
    minute=0,
    name="매주 일요일 작업"
)
```

#### 특정 날짜/시간 작업

```python
from src.core.scheduler import add_date_job
from datetime import datetime, timedelta

# 1시간 후에 실행
run_date = datetime.now() + timedelta(hours=1)
add_date_job(
    func=my_function,
    job_id="one_time_job",
    run_date=run_date,
    name="1시간 후 실행되는 작업"
)
```

### 3. 작업 관리

#### 작업 목록 조회

```python
from src.tasks.jobs import get_all_jobs

jobs = get_all_jobs()
for job in jobs:
    print(f"작업 ID: {job['id']}")
    print(f"작업명: {job['name']}")
    print(f"다음 실행: {job['next_run_time']}")
    print(f"트리거: {job['trigger']}")
    print("---")
```

#### 특정 작업 조회

```python
from src.tasks.jobs import get_job

job_info = get_job("my_job_id")
if job_info:
    print(f"작업 정보: {job_info}")
```

#### 작업 제거

```python
from src.tasks.jobs import unregister_job

success = unregister_job("my_job_id")
if success:
    print("작업이 제거되었습니다.")
```

#### 작업 일시정지/재개

```python
from src.core.scheduler import pause_job, resume_job

# 작업 일시정지
pause_job("my_job_id")

# 작업 재개
resume_job("my_job_id")
```

### 4. 스케줄러 상태 확인

```python
from src.core.scheduler import get_scheduler_status

status = get_scheduler_status()
print(f"실행 중: {status['is_running']}")
print(f"등록된 작업 수: {status['job_count']}")
print("다음 실행 시간:")
for job in status['next_run_times']:
    print(f"  {job['job_id']}: {job['next_run_time']}")
```

## 기본 작업들

Direct Agent API에는 다음과 같은 기본 작업들이 미리 등록되어 있습니다:

### 1. 헬스 체크 작업

- **ID**: `health_check`
- **실행 주기**: 5분마다
- **기능**: 시스템 상태 확인

### 2. 데이터 정리 작업

- **ID**: `data_cleanup`
- **실행 주기**: 매일 새벽 2시
- **기능**: 오래된 데이터 정리

### 3. 백업 작업

- **ID**: `backup`
- **실행 주기**: 매주 일요일 새벽 3시
- **기능**: 시스템 백업

### 4. API 동기화 작업

- **ID**: `api_sync`
- **실행 주기**: 30분마다
- **기능**: 외부 API 동기화

### 5. 알림 작업

- **ID**: `notification`
- **실행 주기**: 매일 오전 9시
- **기능**: 일일 알림 발송

### 6. 메타 카탈로그 광고 이미지 생성 작업

- **ID**: `meta_catalog_ad`
- **실행 주기**: 매일 새벽 3시
- **기능**: 카페24 상품의 메타 광고 이미지 생성

## 사용자 정의 작업

### 1. 작업 함수 작성

```python
async def my_custom_job():
    """사용자 정의 작업 함수"""
    print("사용자 정의 작업이 실행되었습니다.")
    # 여기에 실제 작업 로직을 작성
    return {"status": "completed", "message": "작업 완료"}
```

### 2. 작업 등록

```python
from src.tasks.jobs import register_custom_job

# 간격 기반 작업 등록
register_custom_job(
    job_name="my_custom_job",
    job_data={"param1": "value1", "param2": "value2"},
    minutes=30  # 30분마다 실행
)

# Cron 기반 작업 등록
register_custom_job(
    job_name="daily_custom_job",
    hour=14,  # 오후 2시
    minute=30  # 30분
)
```

## 작업 통계

### 1. 통계 조회

```python
from src.tasks.jobs import job_stats

# 모든 작업 통계
all_stats = job_stats.get_stats()
print(f"전체 통계: {all_stats}")

# 특정 작업 통계
job_stats = job_stats.get_stats("my_job_id")
print(f"작업 통계: {job_stats}")
```

### 2. 통계 정보

각 작업의 통계에는 다음 정보가 포함됩니다:

- `total_executions`: 총 실행 횟수
- `successful_executions`: 성공한 실행 횟수
- `failed_executions`: 실패한 실행 횟수
- `last_execution`: 마지막 실행 시간
- `average_execution_time`: 평균 실행 시간 (초)

## 에러 처리

### 1. 작업 실행 에러

작업 실행 중 에러가 발생하면 자동으로 로그에 기록됩니다:

```python
# 작업 함수에서 예외 발생 시
async def my_job():
    raise Exception("작업 실행 중 오류 발생")
    # 이 에러는 자동으로 로그에 기록되고 통계에 반영됩니다.
```

### 2. 에러 모니터링

```python
from src.tasks.jobs import job_stats

# 실패한 작업 확인
stats = job_stats.get_stats("my_job_id")
if stats.get("failed_executions", 0) > 0:
    print(f"작업 실패 횟수: {stats['failed_executions']}")
```

## 모범 사례

### 1. 작업 함수 작성 시 주의사항

- 작업 함수는 `async` 함수로 작성
- 예외 처리를 적절히 구현
- 실행 시간이 긴 작업은 배치 처리 고려
- 메모리 사용량을 고려한 구현

### 2. 스케줄링 전략

- 시스템 부하를 고려한 실행 시간 설정
- 중복 실행을 방지하기 위한 적절한 간격 설정
- 중요한 작업은 백업 스케줄 고려

### 3. 모니터링

- 정기적으로 작업 상태 확인
- 실패한 작업에 대한 알림 설정
- 성능 지표 모니터링

## 예시: 완전한 워크플로우

```python
import asyncio
from src.core.scheduler import start_scheduler
from src.tasks.jobs import register_custom_job, get_all_jobs

async def main():
    # 1. 스케줄러 시작
    await start_scheduler()

    # 2. 사용자 정의 작업 등록
    register_custom_job(
        job_name="data_processing",
        job_data={"batch_size": 100},
        minutes=15  # 15분마다 실행
    )

    # 3. 등록된 작업 확인
    jobs = get_all_jobs()
    print(f"등록된 작업 수: {len(jobs)}")

    # 4. 스케줄러 실행 유지
    try:
        while True:
            await asyncio.sleep(60)  # 1분마다 상태 확인
    except KeyboardInterrupt:
        print("스케줄러를 중지합니다.")

if __name__ == "__main__":
    asyncio.run(main())
```

## 문제 해결

### 1. 작업이 실행되지 않는 경우

- 스케줄러가 시작되었는지 확인
- 작업 ID가 중복되지 않았는지 확인
- Cron 표현식이 올바른지 확인

### 2. 작업이 중복 실행되는 경우

- `coalesce` 설정 확인
- `max_instances` 설정 조정

### 3. 메모리 사용량이 높은 경우

- 작업 함수의 메모리 사용량 확인
- 불필요한 데이터 정리
- 배치 크기 조정

이 문서를 참고하여 Direct Agent API의 스케줄러 기능을 효과적으로 활용하세요.
