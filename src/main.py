from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import uvicorn
import asyncio
from contextlib import asynccontextmanager

from .core.config import settings, validate_required_settings
from .core.scheduler import start_scheduler, stop_scheduler, get_scheduler_status, get_all_jobs
from .tasks.jobs import register_default_jobs, get_job_list, get_scheduler_info

# 애플리케이션 생명주기 관리
@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 시 실행되는 함수"""
    # 시작 시
    print("🚀 애플리케이션 시작 중...")
    
    # 스케줄러 시작
    await start_scheduler()
    
    # 기본 작업들 등록
    register_default_jobs()
    
    print("✅ 스케줄러가 시작되고 기본 작업들이 등록되었습니다.")
    
    yield
    
    # 종료 시
    print("🛑 애플리케이션 종료 중...")
    
    # 스케줄러 중지
    await stop_scheduler()
    
    print("✅ 스케줄러가 중지되었습니다.")

# FastAPI 애플리케이션 인스턴스 생성
app = FastAPI(
    title=settings.app_name,
    description="FastAPI를 사용한 백엔드 서버 (스케줄러 포함)",
    version=settings.app_version,
    debug=settings.debug,
    lifespan=lifespan
)

# Pydantic 모델 정의
class Item(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    price: float
    is_available: bool = True

class ItemCreate(BaseModel):
    name: str
    description: Optional[str] = None
    price: float
    is_available: bool = True

# 인메모리 데이터 저장소
items_db = []
next_id = 1

# 루트 엔드포인트
@app.get("/")
async def root():
    return {
        "message": f"{settings.app_name}에 오신 것을 환영합니다!", 
        "version": settings.app_version,
        "debug": settings.debug
    }

# 헬스 체크 엔드포인트
@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "서버가 정상적으로 작동 중입니다"}

# 환경변수 정보 조회 엔드포인트 (개발용)
@app.get("/config")
async def get_config():
    """현재 설정된 환경변수 정보를 조회합니다 (개발/디버그용)"""
    return {
        "app_name": settings.app_name,
        "app_version": settings.app_version,
        "debug": settings.debug,
        "host": settings.host,
        "port": settings.port,
        "log_level": settings.log_level,
        "database_configured": bool(settings.database_url),
        "api_keys_configured": {
            "cafe24": bool(settings.cafe24_api_key),
            "gemini": bool(settings.gemini_api_key),
            "bfl": bool(settings.bfl_api_key),
            "aws": bool(settings.aws_access_key_id and settings.aws_secret_access_key)
        }
    }

# 모든 아이템 조회
@app.get("/items", response_model=List[Item])
async def get_items():
    return items_db

# 특정 아이템 조회
@app.get("/items/{item_id}", response_model=Item)
async def get_item(item_id: int):
    for item in items_db:
        if item.id == item_id:
            return item
    raise HTTPException(status_code=404, detail="아이템을 찾을 수 없습니다")

# 새 아이템 생성
@app.post("/items", response_model=Item)
async def create_item(item: ItemCreate):
    global next_id
    new_item = Item(
        id=next_id,
        name=item.name,
        description=item.description,
        price=item.price,
        is_available=item.is_available
    )
    items_db.append(new_item)
    next_id += 1
    return new_item

# 아이템 업데이트
@app.put("/items/{item_id}", response_model=Item)
async def update_item(item_id: int, item: ItemCreate):
    for i, existing_item in enumerate(items_db):
        if existing_item.id == item_id:
            updated_item = Item(
                id=item_id,
                name=item.name,
                description=item.description,
                price=item.price,
                is_available=item.is_available
            )
            items_db[i] = updated_item
            return updated_item
    raise HTTPException(status_code=404, detail="아이템을 찾을 수 없습니다")

# 아이템 삭제
@app.delete("/items/{item_id}")
async def delete_item(item_id: int):
    for i, item in enumerate(items_db):
        if item.id == item_id:
            deleted_item = items_db.pop(i)
            return {"message": f"아이템 '{deleted_item.name}'이 삭제되었습니다"}
    raise HTTPException(status_code=404, detail="아이템을 찾을 수 없습니다")

# 스케줄러 관련 API 엔드포인트들

# 스케줄러 상태 조회
@app.get("/scheduler/status")
async def get_scheduler_status_endpoint():
    """스케줄러 상태 정보를 조회합니다"""
    try:
        status = get_scheduler_status()
        return {
            "success": True,
            "data": status,
            "message": "스케줄러 상태를 조회했습니다"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"스케줄러 상태 조회 실패: {str(e)}")

# 등록된 작업 목록 조회
@app.get("/scheduler/jobs")
async def get_scheduler_jobs():
    """등록된 모든 작업 목록을 조회합니다"""
    try:
        jobs = get_job_list()
        return {
            "success": True,
            "data": jobs,
            "count": len(jobs),
            "message": f"총 {len(jobs)}개의 작업이 등록되어 있습니다"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"작업 목록 조회 실패: {str(e)}")

# 특정 작업 정보 조회
@app.get("/scheduler/jobs/{job_id}")
async def get_job_info(job_id: str):
    """특정 작업의 상세 정보를 조회합니다"""
    try:
        from core.scheduler import get_job
        job_info = get_job(job_id)
        if job_info:
            return {
                "success": True,
                "data": job_info,
                "message": f"작업 '{job_id}'의 정보를 조회했습니다"
            }
        else:
            raise HTTPException(status_code=404, detail=f"작업 '{job_id}'을 찾을 수 없습니다")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"작업 정보 조회 실패: {str(e)}")

# 작업 일시정지
@app.post("/scheduler/jobs/{job_id}/pause")
async def pause_job(job_id: str):
    """특정 작업을 일시정지합니다"""
    try:
        from core.scheduler import pause_job
        success = pause_job(job_id)
        if success:
            return {
                "success": True,
                "message": f"작업 '{job_id}'이 일시정지되었습니다"
            }
        else:
            raise HTTPException(status_code=400, detail=f"작업 '{job_id}' 일시정지 실패")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"작업 일시정지 실패: {str(e)}")

# 작업 재개
@app.post("/scheduler/jobs/{job_id}/resume")
async def resume_job(job_id: str):
    """일시정지된 작업을 재개합니다"""
    try:
        from core.scheduler import resume_job
        success = resume_job(job_id)
        if success:
            return {
                "success": True,
                "message": f"작업 '{job_id}'이 재개되었습니다"
            }
        else:
            raise HTTPException(status_code=400, detail=f"작업 '{job_id}' 재개 실패")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"작업 재개 실패: {str(e)}")

# 작업 제거
@app.delete("/scheduler/jobs/{job_id}")
async def remove_job_endpoint(job_id: str):
    """특정 작업을 스케줄러에서 제거합니다"""
    try:
        from core.scheduler import remove_job
        success = remove_job(job_id)
        if success:
            return {
                "success": True,
                "message": f"작업 '{job_id}'이 제거되었습니다"
            }
        else:
            raise HTTPException(status_code=400, detail=f"작업 '{job_id}' 제거 실패")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"작업 제거 실패: {str(e)}")

# 사용자 정의 작업 등록을 위한 모델
class CustomJobRequest(BaseModel):
    job_name: str
    job_data: Optional[Dict[str, Any]] = None
    schedule_type: str = "interval"  # "interval", "cron", "date"
    schedule_config: Dict[str, Any]

# 사용자 정의 작업 등록
@app.post("/scheduler/jobs/custom")
async def register_custom_job(request: CustomJobRequest):
    """사용자 정의 작업을 스케줄러에 등록합니다"""
    try:
        from tasks.jobs import register_custom_job
        
        # 스케줄 타입에 따른 설정
        if request.schedule_type == "date":
            if "run_date" not in request.schedule_config:
                raise HTTPException(status_code=400, detail="날짜 작업의 경우 'run_date'가 필요합니다")
            schedule_kwargs = request.schedule_config
        else:
            schedule_kwargs = request.schedule_config
        
        success = register_custom_job(
            job_name=request.job_name,
            job_data=request.job_data,
            **schedule_kwargs
        )
        
        if success:
            return {
                "success": True,
                "message": f"사용자 정의 작업 '{request.job_name}'이 등록되었습니다",
                "job_id": f"custom_{request.job_name}"
            }
        else:
            raise HTTPException(status_code=400, detail=f"작업 '{request.job_name}' 등록 실패")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"사용자 정의 작업 등록 실패: {str(e)}")

# 스케줄러 통계 조회
@app.get("/scheduler/stats")
async def get_scheduler_stats():
    """스케줄러 실행 통계를 조회합니다"""
    try:
        from tasks.jobs import job_stats
        stats = job_stats.get_stats()
        return {
            "success": True,
            "data": stats,
            "message": "스케줄러 통계를 조회했습니다"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"통계 조회 실패: {str(e)}")

# 서버 실행
if __name__ == "__main__":
    # 환경변수 검증
    validate_required_settings()
    
    print(f"🚀 {settings.app_name} v{settings.app_version} 시작 중...")
    print(f"📍 서버 주소: http://{settings.host}:{settings.port}")
    print(f"🔧 디버그 모드: {settings.debug}")
    
    uvicorn.run(
        app, 
        host=settings.host, 
        port=settings.port,
        log_level=settings.log_level.lower()
    )

def main():
    uvicorn.run(
        app, 
        host=settings.host, 
        port=settings.port,
        log_level=settings.log_level.lower()
    )
