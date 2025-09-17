"""
API v1 라우터

API v1 버전의 모든 엔드포인트를 통합하는 라우터입니다.
"""

from fastapi import APIRouter

from .endpoints import webhook, cafe24, relay

# API v1 라우터 생성
api_router = APIRouter(prefix="/api/v1")

# 각 엔드포인트 라우터를 포함
api_router.include_router(webhook.router)
api_router.include_router(cafe24.router)
api_router.include_router(relay.router)
