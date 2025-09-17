import logging
from typing import Dict, Any, Callable, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler import events
from datetime import datetime, timedelta
import asyncio

from .config import settings

# 로거 설정
logger = logging.getLogger(__name__)

class SchedulerManager:
    """APScheduler를 사용한 스케줄러 관리 클래스"""
    
    def __init__(self):
        """스케줄러 초기화"""
        # JobStore 설정 (메모리 기반)
        jobstores = {
            'default': MemoryJobStore()
        }
        
        # Executor 설정 (비동기 실행)
        executors = {
            'default': AsyncIOExecutor()
        }
        
        # Job 기본 설정
        job_defaults = {
            'coalesce': False,  # 중복 작업 병합하지 않음
            'max_instances': 3,  # 최대 동시 실행 인스턴스 수
            'misfire_grace_time': 15  # 작업 실행 지연 허용 시간(초)
        }
        
        # 스케줄러 인스턴스 생성
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='Asia/Seoul'  # 한국 시간대
        )
        
        # 이벤트 리스너 등록
        self.scheduler.add_listener(self._job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self._job_error, EVENT_JOB_ERROR)
        
        self._is_running = False
    
    def _job_executed(self, event: events.JobExecutionEvent):
        """작업 실행 완료 이벤트 핸들러"""
        logger.info(f"작업 실행 완료: {event.job_id} - 실행 시간: {event.scheduled_run_time}")
    
    def _job_error(self, event: events.JobExecutionEvent):
        """작업 실행 오류 이벤트 핸들러"""
        logger.error(f"작업 실행 오류: {event.job_id} - 오류: {event.exception}")
    
    async def start(self):
        """스케줄러 시작"""
        if not self._is_running:
            self.scheduler.start()
            self._is_running = True
            logger.info("스케줄러가 시작되었습니다.")
        else:
            logger.warning("스케줄러가 이미 실행 중입니다.")
    
    async def stop(self):
        """스케줄러 중지"""
        if self._is_running:
            self.scheduler.shutdown(wait=True)
            self._is_running = False
            logger.info("스케줄러가 중지되었습니다.")
        else:
            logger.warning("스케줄러가 실행 중이 아닙니다.")
    
    def add_interval_job(
        self,
        func: Callable,
        job_id: str,
        seconds: Optional[int] = None,
        minutes: Optional[int] = None,
        hours: Optional[int] = None,
        days: Optional[int] = None,
        weeks: Optional[int] = None,
        **kwargs
    ) -> bool:
        """간격 기반 작업 추가"""
        try:
            # 최소 하나의 시간 단위가 설정되어야 함
            if not any([seconds, minutes, hours, days, weeks]):
                logger.error(f"간격 작업 추가 실패: {job_id} - 최소 하나의 시간 단위(seconds, minutes, hours, days, weeks)를 설정해야 합니다.")
                return False
            
            # None이 아닌 값만 전달
            trigger_kwargs = {}
            if seconds is not None:
                trigger_kwargs['seconds'] = seconds
            if minutes is not None:
                trigger_kwargs['minutes'] = minutes
            if hours is not None:
                trigger_kwargs['hours'] = hours
            if days is not None:
                trigger_kwargs['days'] = days
            if weeks is not None:
                trigger_kwargs['weeks'] = weeks
            
            trigger = IntervalTrigger(**trigger_kwargs)
            
            self.scheduler.add_job(
                func=func,
                trigger=trigger,
                id=job_id,
                replace_existing=True,
                **kwargs
            )
            
            logger.info(f"간격 작업 추가됨: {job_id} - 간격: {trigger}")
            return True
            
        except Exception as e:
            logger.error(f"간격 작업 추가 실패: {job_id} - 오류: {e}")
            return False
    
    def add_cron_job(
        self,
        func: Callable,
        job_id: str,
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
        week: Optional[int] = None,
        day_of_week: Optional[str] = None,
        hour: Optional[int] = None,
        minute: Optional[int] = None,
        second: Optional[int] = None,
        **kwargs
    ) -> bool:
        """Cron 표현식 기반 작업 추가"""
        try:
            trigger = CronTrigger(
                year=year,
                month=month,
                day=day,
                week=week,
                day_of_week=day_of_week,
                hour=hour,
                minute=minute,
                second=second,
                timezone='Asia/Seoul'
            )
            
            self.scheduler.add_job(
                func=func,
                trigger=trigger,
                id=job_id,
                replace_existing=True,
                **kwargs
            )
            
            logger.info(f"Cron 작업 추가됨: {job_id} - 스케줄: {trigger}")
            return True
            
        except Exception as e:
            logger.error(f"Cron 작업 추가 실패: {job_id} - 오류: {e}")
            return False
    
    def add_date_job(
        self,
        func: Callable,
        job_id: str,
        run_date: datetime,
        **kwargs
    ) -> bool:
        """특정 날짜/시간에 실행되는 작업 추가"""
        try:
            trigger = DateTrigger(run_date=run_date, timezone='Asia/Seoul')
            
            self.scheduler.add_job(
                func=func,
                trigger=trigger,
                id=job_id,
                replace_existing=True,
                **kwargs
            )
            
            logger.info(f"일회성 작업 추가됨: {job_id} - 실행 시간: {run_date}")
            return True
            
        except Exception as e:
            logger.error(f"일회성 작업 추가 실패: {job_id} - 오류: {e}")
            return False
    
    def remove_job(self, job_id: str) -> bool:
        """작업 제거"""
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"작업 제거됨: {job_id}")
            return True
        except Exception as e:
            logger.error(f"작업 제거 실패: {job_id} - 오류: {e}")
            return False
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """특정 작업 정보 조회"""
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                return {
                    'id': job.id,
                    'name': job.name,
                    'next_run_time': job.next_run_time,
                    'trigger': str(job.trigger)
                }
            return None
        except Exception as e:
            logger.error(f"작업 조회 실패: {job_id} - 오류: {e}")
            return None
    
    def get_all_jobs(self) -> list:
        """모든 작업 목록 조회"""
        try:
            jobs = []
            for job in self.scheduler.get_jobs():
                jobs.append({
                    'id': job.id,
                    'name': job.name,
                    'next_run_time': job.next_run_time,
                    'trigger': str(job.trigger)
                })
            return jobs
        except Exception as e:
            logger.error(f"작업 목록 조회 실패: {e}")
            return []
    
    def pause_job(self, job_id: str) -> bool:
        """작업 일시정지"""
        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"작업 일시정지됨: {job_id}")
            return True
        except Exception as e:
            logger.error(f"작업 일시정지 실패: {job_id} - 오류: {e}")
            return False
    
    def resume_job(self, job_id: str) -> bool:
        """작업 재개"""
        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"작업 재개됨: {job_id}")
            return True
        except Exception as e:
            logger.error(f"작업 재개 실패: {job_id} - 오류: {e}")
            return False
    
    def is_running(self) -> bool:
        """스케줄러 실행 상태 확인"""
        return self._is_running and self.scheduler.running
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """스케줄러 상태 정보 반환"""
        return {
            'is_running': self.is_running(),
            'job_count': len(self.scheduler.get_jobs()),
            'next_run_times': [
                {
                    'job_id': job.id,
                    'next_run_time': job.next_run_time
                }
                for job in self.scheduler.get_jobs()
            ]
        }

# 전역 스케줄러 인스턴스
scheduler_manager = SchedulerManager()

# 편의 함수들
async def start_scheduler():
    """스케줄러 시작"""
    await scheduler_manager.start()

async def stop_scheduler():
    """스케줄러 중지"""
    await scheduler_manager.stop()

def add_interval_job(func: Callable, job_id: str, seconds: Optional[int] = None, minutes: Optional[int] = None, hours: Optional[int] = None, days: Optional[int] = None, weeks: Optional[int] = None, **kwargs) -> bool:
    """간격 기반 작업 추가"""
    return scheduler_manager.add_interval_job(func, job_id, seconds=seconds, minutes=minutes, hours=hours, days=days, weeks=weeks, **kwargs)

def add_cron_job(func: Callable, job_id: str, **kwargs) -> bool:
    """Cron 기반 작업 추가"""
    return scheduler_manager.add_cron_job(func, job_id, **kwargs)

def add_date_job(func: Callable, job_id: str, run_date: datetime, **kwargs) -> bool:
    """일회성 작업 추가"""
    return scheduler_manager.add_date_job(func, job_id, run_date, **kwargs)

def remove_job(job_id: str) -> bool:
    """작업 제거"""
    return scheduler_manager.remove_job(job_id)

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """작업 정보 조회"""
    return scheduler_manager.get_job(job_id)

def get_all_jobs() -> list:
    """모든 작업 목록 조회"""
    return scheduler_manager.get_all_jobs()

def pause_job(job_id: str) -> bool:
    """작업 일시정지"""
    return scheduler_manager.pause_job(job_id)

def resume_job(job_id: str) -> bool:
    """작업 재개"""
    return scheduler_manager.resume_job(job_id)

def get_scheduler_status() -> Dict[str, Any]:
    """스케줄러 상태 조회"""
    return scheduler_manager.get_scheduler_status()
