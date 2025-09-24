import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
import json

from ..core.config import settings
from ..core.scheduler import (
    add_interval_job,
    add_cron_job,
    add_date_job,
    remove_job,
    get_all_jobs,
    get_scheduler_status
)

# 로거 설정
logger = logging.getLogger(__name__)

# 작업 함수들
async def health_check_job():
    """시스템 상태 확인 작업 (매 5분마다 실행)"""
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"🏥 헬스 체크 실행됨: {current_time}")
        
        # 여기에 실제 헬스 체크 로직을 추가할 수 있습니다
        # 예: 데이터베이스 연결 확인, 외부 API 상태 확인 등
        
        return {
            "status": "healthy",
            "timestamp": current_time,
            "message": "시스템이 정상적으로 작동 중입니다"
        }
    except Exception as e:
        logger.error(f"헬스 체크 작업 오류: {e}")
        return {
            "status": "error",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": f"헬스 체크 실패: {str(e)}"
        }

async def data_cleanup_job():
    """데이터 정리 작업 (매일 새벽 2시에 실행)"""
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"🧹 데이터 정리 작업 실행됨: {current_time}")
        
        # 여기에 실제 데이터 정리 로직을 추가할 수 있습니다
        # 예: 오래된 로그 파일 삭제, 임시 파일 정리, 데이터베이스 정리 등
        
        return {
            "status": "completed",
            "timestamp": current_time,
            "message": "데이터 정리 작업이 완료되었습니다"
        }
    except Exception as e:
        logger.error(f"데이터 정리 작업 오류: {e}")
        return {
            "status": "error",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": f"데이터 정리 실패: {str(e)}"
        }

async def backup_job():
    """백업 작업 (매주 일요일 새벽 3시에 실행)"""
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"💾 백업 작업 실행됨: {current_time}")
        
        # 여기에 실제 백업 로직을 추가할 수 있습니다
        # 예: 데이터베이스 백업, 파일 백업, 클라우드 스토리지 업로드 등
        
        return {
            "status": "completed",
            "timestamp": current_time,
            "message": "백업 작업이 완료되었습니다"
        }
    except Exception as e:
        logger.error(f"백업 작업 오류: {e}")
        return {
            "status": "error",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": f"백업 실패: {str(e)}"
        }

async def api_sync_job():
    """API 동기화 작업 (매 30분마다 실행)"""
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"🔄 API 동기화 작업 실행됨: {current_time}")
        
        # 여기에 실제 API 동기화 로직을 추가할 수 있습니다
        # 예: 외부 API에서 데이터 가져오기, 내부 데이터 동기화 등
        
        return {
            "status": "completed",
            "timestamp": current_time,
            "message": "API 동기화 작업이 완료되었습니다"
        }
    except Exception as e:
        logger.error(f"API 동기화 작업 오류: {e}")
        return {
            "status": "error",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": f"API 동기화 실패: {str(e)}"
        }

async def notification_job():
    """알림 작업 (매일 오전 9시에 실행)"""
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"📢 알림 작업 실행됨: {current_time}")
        
        # 여기에 실제 알림 로직을 추가할 수 있습니다
        # 예: 이메일 발송, 슬랙 알림, SMS 발송 등
        
        return {
            "status": "completed",
            "timestamp": current_time,
            "message": "알림 작업이 완료되었습니다"
        }
    except Exception as e:
        logger.error(f"알림 작업 오류: {e}")
        return {
            "status": "error",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": f"알림 발송 실패: {str(e)}"
        }

async def custom_job(job_name: str, job_data: Dict[str, Any] = None):
    """사용자 정의 작업"""
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"🔧 사용자 정의 작업 실행됨: {job_name} - {current_time}")
        
        # 작업 데이터가 있으면 로그에 출력
        if job_data:
            logger.info(f"작업 데이터: {json.dumps(job_data, ensure_ascii=False, indent=2)}")
        
        # 여기에 실제 사용자 정의 작업 로직을 추가할 수 있습니다
        
        return {
            "status": "completed",
            "job_name": job_name,
            "timestamp": current_time,
            "message": f"사용자 정의 작업 '{job_name}'이 완료되었습니다",
            "data": job_data
        }
    except Exception as e:
        logger.error(f"사용자 정의 작업 오류: {job_name} - {e}")
        return {
            "status": "error",
            "job_name": job_name,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": f"사용자 정의 작업 '{job_name}' 실패: {str(e)}",
            "data": job_data
        }

# 작업 등록 함수들
def register_default_jobs():
    """기본 작업들을 스케줄러에 등록"""
    try:
        # 헬스 체크 작업 (매 5분마다)
        result = add_interval_job(
            func=health_check_job,
            job_id="health_check",
            minutes=5,
            name="시스템 헬스 체크"
        )
        if not result:
            logger.error("헬스 체크 작업 등록 실패")
        
        # # 데이터 정리 작업 (매일 새벽 2시)
        # add_cron_job(
        #     func=data_cleanup_job,
        #     job_id="data_cleanup",
        #     hour=2,
        #     minute=0,
        #     name="데이터 정리"
        # )
        
        # # 백업 작업 (매주 일요일 새벽 3시)
        # add_cron_job(
        #     func=backup_job,
        #     job_id="backup",
        #     day_of_week="sun",
        #     hour=3,
        #     minute=0,
        #     name="시스템 백업"
        # )
        
        # # API 동기화 작업 (매 30분마다)
        # result = add_interval_job(
        #     func=api_sync_job,
        #     job_id="api_sync",
        #     minutes=30,
        #     name="API 동기화"
        # )
        # if not result:
        #     logger.error("API 동기화 작업 등록 실패")
        
        # # 알림 작업 (매일 오전 9시)
        # add_cron_job(
        #     func=notification_job,
        #     job_id="notification",
        #     hour=9,
        #     minute=0,
        #     name="일일 알림"
        # )
        
        # 메타 카탈로그 광고 이미지 생성 작업 (매시간 정시)
        result = add_cron_job(
            func=meta_catalog_ad_job,
            job_id="meta_catalog_ad",
            minute=0,  # 매시간 0분에 실행 (정시)
            name="메타 카탈로그 광고 이미지 생성"
        )
        if not result:
            logger.error("메타 카탈로그 광고 이미지 생성 작업 등록 실패")
        
        logger.info("기본 작업들이 스케줄러에 등록되었습니다.")
        return True
        
    except Exception as e:
        logger.error(f"기본 작업 등록 실패: {e}")
        return False

def register_custom_job(job_name: str, job_data: Dict[str, Any] = None, **schedule_kwargs):
    """사용자 정의 작업을 스케줄러에 등록"""
    try:
        # 람다 함수로 job_name과 job_data를 전달
        job_func = lambda: asyncio.create_task(custom_job(job_name, job_data))
        
        # 스케줄 설정이 없으면 기본값으로 1시간마다 실행
        if not schedule_kwargs:
            schedule_kwargs = {"minutes": 60}
        
        # 작업 등록
        if "run_date" in schedule_kwargs:
            # 특정 날짜/시간에 실행
            add_date_job(
                func=job_func,
                job_id=f"custom_{job_name}",
                run_date=schedule_kwargs["run_date"],
                name=f"사용자 정의 작업: {job_name}"
            )
        elif any(key in schedule_kwargs for key in ["year", "month", "day", "hour", "minute", "second", "day_of_week"]):
            # Cron 스케줄
            add_cron_job(
                func=job_func,
                job_id=f"custom_{job_name}",
                name=f"사용자 정의 작업: {job_name}",
                **schedule_kwargs
            )
        else:
            # 간격 기반 스케줄
            add_interval_job(
                func=job_func,
                job_id=f"custom_{job_name}",
                name=f"사용자 정의 작업: {job_name}",
                **schedule_kwargs
            )
        
        logger.info(f"사용자 정의 작업이 등록되었습니다: {job_name}")
        return True
        
    except Exception as e:
        logger.error(f"사용자 정의 작업 등록 실패: {job_name} - {e}")
        return False

def unregister_job(job_id: str):
    """작업을 스케줄러에서 제거"""
    try:
        success = remove_job(job_id)
        if success:
            logger.info(f"작업이 제거되었습니다: {job_id}")
        else:
            logger.warning(f"작업 제거 실패: {job_id}")
        return success
    except Exception as e:
        logger.error(f"작업 제거 중 오류: {job_id} - {e}")
        return False

def get_job_list() -> List[Dict[str, Any]]:
    """등록된 모든 작업 목록 조회"""
    try:
        jobs = get_all_jobs()
        logger.info(f"등록된 작업 수: {len(jobs)}")
        return jobs
    except Exception as e:
        logger.error(f"작업 목록 조회 실패: {e}")
        return []

def get_scheduler_info() -> Dict[str, Any]:
    """스케줄러 상태 정보 조회"""
    try:
        status = get_scheduler_status()
        logger.info("스케줄러 상태 정보를 조회했습니다.")
        return status
    except Exception as e:
        logger.error(f"스케줄러 상태 조회 실패: {e}")
        return {"error": str(e)}

# 작업 실행 통계
class JobStats:
    """작업 실행 통계 관리"""
    
    def __init__(self):
        self.stats = {}
    
    def record_execution(self, job_id: str, success: bool, execution_time: float = None):
        """작업 실행 기록"""
        if job_id not in self.stats:
            self.stats[job_id] = {
                "total_executions": 0,
                "successful_executions": 0,
                "failed_executions": 0,
                "last_execution": None,
                "average_execution_time": 0.0
            }
        
        stats = self.stats[job_id]
        stats["total_executions"] += 1
        stats["last_execution"] = datetime.now().isoformat()
        
        if success:
            stats["successful_executions"] += 1
        else:
            stats["failed_executions"] += 1
        
        if execution_time is not None:
            # 평균 실행 시간 계산
            current_avg = stats["average_execution_time"]
            total = stats["total_executions"]
            stats["average_execution_time"] = (current_avg * (total - 1) + execution_time) / total
    
    def get_stats(self, job_id: str = None) -> Dict[str, Any]:
        """통계 정보 조회"""
        if job_id:
            return self.stats.get(job_id, {})
        return self.stats

# 전역 통계 인스턴스
job_stats = JobStats()

# 통계 기록 데코레이터
def track_execution(job_id: str):
    """작업 실행을 추적하는 데코레이터"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = datetime.now()
            success = False
            try:
                result = await func(*args, **kwargs)
                success = True
                return result
            except Exception as e:
                logger.error(f"작업 실행 오류: {job_id} - {e}")
                raise
            finally:
                execution_time = (datetime.now() - start_time).total_seconds()
                job_stats.record_execution(job_id, success, execution_time)
        return wrapper
    return decorator


# 메타 카탈로그 광고 이미지 생성 작업
async def meta_catalog_ad_job(spreadsheet_id: str = None, generate_images: bool = True):
    """메타 카탈로그 광고 이미지 생성 작업 (1시간마다 실행)"""
    try:
        import importlib.util
        import sys
        import os
        
        # 기본 스프레드시트 ID 설정 (설정이 없으면 기본값 사용)
        if not spreadsheet_id:
            # 기본 스프레드시트 URL에서 ID 추출
            default_url = "https://docs.google.com/spreadsheets/d/1fUMh5PimIjvI6_ef2VK6zQa_NC9xGvUnhkLK2qs1r5k/edit?gid=0#gid=0"
            from ..interfaces.google_sheet import get_spreadsheet_id_from_url
            spreadsheet_id = get_spreadsheet_id_from_url(default_url)
        
        # 동적으로 모듈 로드
        module_path = os.path.join(os.path.dirname(__file__), "..", "events", "meta-catalog-ad", "index.py")
        spec = importlib.util.spec_from_file_location("meta_catalog_ad", module_path)
        meta_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(meta_module)
        meta_job = meta_module.meta_catalog_ad_job
        logger.info(f"🚀 메타 카탈로그 광고 이미지 생성 작업 시작 (스프레드시트: {spreadsheet_id})")
        
        result = await meta_job(spreadsheet_id, generate_images=generate_images)
        
        logger.info(f"메타 카탈로그 광고 이미지 생성 작업 완료: {result}")
        return result
        
    except Exception as e:
        logger.error(f"메타 카탈로그 광고 이미지 생성 작업 실패: {e}")
        return {
            "status": "error",
            "message": f"작업 실패: {e}",
            "timestamp": datetime.now().isoformat()
        }
