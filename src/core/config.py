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
    
    # 외부 서비스 설정
    # 카페24 API 설정 (새로운 방식)
    cafe24_auth_key: Optional[str] = Field(default=None, env="CAFE24_AUTH_KEY")
    cafe24_token_url: str = Field(default="https://rs.the-relay.kr/v1/admin/token/access", env="CAFE24_TOKEN_URL")
    cafe24_stage: str = Field(default="prod", env="CAFE24_STAGE")
    cafe24_brand_id: str = Field(default="02", env="CAFE24_BRAND_ID")
    cafe24_brand_domain: str = Field(default="dept", env="CAFE24_BRAND_DOMAIN")
    
    # 기타 API 설정
    gemini_api_key: Optional[str] = Field(default=None, env="GEMINI_API_KEY")
    google_sheet_api_key: Optional[str] = Field(default=None, env="GOOGLE_SHEET_API_KEY")
    google_credentials_path: Optional[str] = Field(default=None, env="GOOGLE_CREDENTIALS_PATH")
    google_service_account_path: Optional[str] = Field(default=None, env="GOOGLE_SERVICE_ACCOUNT_PATH")
    google_token_path: Optional[str] = Field(default=None, env="GOOGLE_TOKEN_PATH")
    meta_catalog_spreadsheet_id: Optional[str] = Field(default=None, env="META_CATALOG_SPREADSHEET_ID")
    bfl_api_key: Optional[str] = Field(default=None, env="BFL_API_KEY")
    aws_access_key_id: Optional[str] = Field(default=None, env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: Optional[str] = Field(default=None, env="AWS_SECRET_ACCESS_KEY")
    aws_s3_bucket: Optional[str] = Field(default="dept.the-relay", env="AWS_BUCKET_NAME")
    aws_region: Optional[str] = Field(default="ap-northeast-2", env="AWS_REGION_NAME")
    aws_endpoint_url: Optional[str] = Field(default=None, env="AWS_ENDPOINT_URL")
    
    # 로깅 설정
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # 추가 필드 무시

# 전역 설정 인스턴스 생성
settings = Settings()

def get_settings() -> Settings:
    """설정 인스턴스를 반환하는 함수"""
    return settings