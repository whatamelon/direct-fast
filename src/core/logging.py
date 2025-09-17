import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


class Logger:
    """로깅을 위한 기본 클래스"""
    
    def __init__(self, name: str = "direct_agent", log_level: str = "INFO"):
        self.name = name
        self.log_level = getattr(logging, log_level.upper())
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """로거 설정"""
        logger = logging.getLogger(self.name)
        logger.setLevel(self.log_level)
        
        # 이미 핸들러가 있다면 제거 (중복 방지)
        if logger.handlers:
            logger.handlers.clear()
        
        # 포매터 설정
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 콘솔 핸들러
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 파일 핸들러 (logs 디렉토리에 저장)
        self._setup_file_handler(logger, formatter)
        
        return logger
    
    def _setup_file_handler(self, logger: logging.Logger, formatter: logging.Formatter):
        """파일 핸들러 설정"""
        try:
            # logs 디렉토리 생성
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            
            # 날짜별 로그 파일
            log_file = log_dir / f"{self.name}_{datetime.now().strftime('%Y%m%d')}.log"
            
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(self.log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
        except Exception as e:
            # 파일 핸들러 설정 실패 시 콘솔에만 로깅
            logger.warning(f"파일 핸들러 설정 실패: {e}")
    
    def debug(self, message: str, *args, **kwargs):
        """디버그 레벨 로깅"""
        self.logger.debug(message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs):
        """정보 레벨 로깅"""
        self.logger.info(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs):
        """경고 레벨 로깅"""
        self.logger.warning(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs):
        """에러 레벨 로깅"""
        self.logger.error(message, *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs):
        """치명적 에러 레벨 로깅"""
        self.logger.critical(message, *args, **kwargs)
    
    def exception(self, message: str, *args, **kwargs):
        """예외 정보와 함께 에러 로깅"""
        self.logger.exception(message, *args, **kwargs)


# 기본 로거 인스턴스
default_logger = Logger()

# 편의 함수들
def get_logger(name: Optional[str] = None, log_level: str = "INFO") -> Logger:
    """로거 인스턴스 반환"""
    if name:
        return Logger(name, log_level)
    return default_logger

def debug(message: str, *args, **kwargs):
    """디버그 로깅"""
    default_logger.debug(message, *args, **kwargs)

def info(message: str, *args, **kwargs):
    """정보 로깅"""
    default_logger.info(message, *args, **kwargs)

def warning(message: str, *args, **kwargs):
    """경고 로깅"""
    default_logger.warning(message, *args, **kwargs)

def error(message: str, *args, **kwargs):
    """에러 로깅"""
    default_logger.error(message, *args, **kwargs)

def critical(message: str, *args, **kwargs):
    """치명적 에러 로깅"""
    default_logger.critical(message, *args, **kwargs)

def exception(message: str, *args, **kwargs):
    """예외 정보와 함께 에러 로깅"""
    default_logger.exception(message, *args, **kwargs)


# 사용 예시
if __name__ == "__main__":
    # 기본 사용법
    info("애플리케이션이 시작되었습니다.")
    debug("디버그 메시지입니다.")
    warning("경고 메시지입니다.")
    error("에러 메시지입니다.")
    
    # 커스텀 로거 사용
    custom_logger = get_logger("custom_module", "DEBUG")
    custom_logger.info("커스텀 로거에서 로깅합니다.")
    
    # 예외 로깅
    try:
        result = 1 / 0
    except ZeroDivisionError:
        exception("0으로 나누기 에러가 발생했습니다.")
