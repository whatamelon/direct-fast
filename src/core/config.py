import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

class Settings(BaseSettings):
    """애플리케이션 설정 클래스"""
    
    # 애플리케이션 기본 설정
    app_name: str = Field(default="Direct Agent API", env="APP_NAME")
    app_version: str = Field(default="0.1.0", env="APP_VERSION")
    debug: bool = Field(default=False, env="DEBUG")
    
    # 서버 설정
    host: str = Field(default="0.0.0.0", env="HOST")
    port: int = Field(default=8000, env="PORT")
    
    # 데이터베이스 설정
    database_url: str = Field(default="sqlite:///./direct_agent.db", env="DATABASE_URL")
    
    # API 키 및 시크릿
    api_secret_key: str = Field(default="", env="API_SECRET_KEY")
    jwt_secret_key: str = Field(default="", env="JWT_SECRET_KEY")
    
    # 외부 서비스 설정
    cafe24_api_key: Optional[str] = Field(default=None, env="CAFE24_API_KEY")
    gemini_api_key: Optional[str] = Field(default=None, env="GEMINI_API_KEY")
    bfl_api_key: Optional[str] = Field(default=None, env="BFL_API_KEY")
    aws_access_key_id: Optional[str] = Field(default=None, env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: Optional[str] = Field(default=None, env="AWS_SECRET_ACCESS_KEY")
    aws_bucket_name: Optional[str] = Field(default=None, env="AWS_BUCKET_NAME")
    aws_region: Optional[str] = Field(default=None, env="AWS_REGION")
    aws_endpoint_url: Optional[str] = Field(default=None, env="AWS_ENDPOINT_URL")
    
    # 로깅 설정
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

# 전역 설정 인스턴스 생성
settings = Settings()

def get_settings() -> Settings:
    """설정 인스턴스를 반환하는 함수"""
    return settings

# 환경변수 검증 함수
def validate_required_settings():
    """필수 환경변수가 설정되었는지 검증"""
    required_vars = [
        ("API_SECRET_KEY", settings.api_secret_key),
        ("JWT_SECRET_KEY", settings.jwt_secret_key),
    ]
    
    missing_vars = []
    for var_name, var_value in required_vars:
        if not var_value or var_value == "your-secret-key-here":
            missing_vars.append(var_name)
    
    if missing_vars:
        print(f"⚠️  경고: 다음 환경변수가 설정되지 않았습니다: {', '.join(missing_vars)}")
        print("   .env 파일에서 해당 변수들을 설정해주세요.")
    
    return len(missing_vars) == 0

# 애플리케이션 시작 시 설정 검증
if __name__ == "__main__":
    validate_required_settings()
