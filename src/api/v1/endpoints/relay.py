"""
릴레이 API 엔드포인트

외부 API를 중계하는 릴레이 서비스의 엔드포인트를 제공합니다.
Cafe24 인증 토큰 관리 및 API 중계 기능을 포함합니다.
"""

import asyncio
import base64
import time
from typing import Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel, Field
import httpx
from src.core.config import get_settings

router = APIRouter(prefix="/relay", tags=["relay"])

# 전역 변수로 토큰 관리
_cafe24_token: Optional[str] = None
_token_expires_at: Optional[int] = None
_is_refreshing_token: bool = False

# 토큰 유효 기간 (24시간)
TOKEN_VALIDITY_DURATION = 24 * 60 * 60 * 1000  # 24시간을 밀리초로

# 릴레이 API 기본 URL
RELAY_BASE_URL = "https://rs.the-relay.kr/v1/admin"

# Relay OI API 기본 URL
RELAY_OI_BASE_URL = "https://oi.the-relay.kr"

# Relay OI API 인증 정보
RELAY_API_ID = "corner"
RELAY_API_PASSWORD = "5FYRUWikQdPFezY"


class TokenResponse(BaseModel):
    """토큰 응답 모델"""
    token: str
    expires_at: int
    expires_in: int


class ErrorResponse(BaseModel):
    """에러 응답 모델"""
    error: str
    message: str
    status_code: int


class GetRelayItemRequest(BaseModel):
    """GET /direct/v10/items 요청 파라미터"""
    id: Optional[int] = Field(None, description="상품 ID (integer, int64)")
    wash_code: Optional[str] = Field(None, description="상품 세탁코드. 8자 이상")


class DirectItemDto(BaseModel):
    """DirectItemDto - 상품 정보"""
    itemId: int = Field(..., description="상품 ID")
    washCode: str = Field(..., description="세탁코드")
    itemName: str = Field(..., description="상품명")
    price: int = Field(..., description="공급가,판매가 (원)")
    brandId: str = Field(..., description="브랜드 ID")
    brandName: str = Field(..., description="브랜드명")
    brandAlias: str = Field(..., description="브랜드 별칭")
    categoryId: str = Field(..., description="카테고리 ID")
    categoryName1: str = Field(..., description="카테고리명 1")
    categoryName2: str = Field(..., description="카테고리명 2")
    categoryName3: str = Field(..., description="카테고리명 3")
    categoryName3Alias: str = Field(..., description="카테고리명 3 별칭")
    expectedPrice: Optional[int] = Field(None, description="소비자가")


class RelayItemResponse(BaseModel):
    """CommonResponseBodyDirectItemDto - API 응답 구조"""
    code: str = Field(..., description="응답 코드")
    info: str = Field(..., description="응답 메시지")
    data: DirectItemDto = Field(..., description="상품 데이터")


def get_relay_oi_auth_headers() -> Dict[str, str]:
    """Relay OI API 인증 헤더를 생성합니다."""
    credentials = base64.b64encode(f"{RELAY_API_ID}:{RELAY_API_PASSWORD}".encode()).decode()
    return {
        "Authorization": f"Basic {credentials}",
        "Content-Type": "application/json",
    }


async def relay_oi_api_request(
    path: str,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0
) -> Dict[str, Any]:
    """
    Relay OI API 공통 요청 함수
    
    Args:
        path: API 경로
        method: HTTP 메서드 (GET, POST, PUT, DELETE)
        params: 쿼리 파라미터
        body: 요청 본문
        timeout: 요청 타임아웃 (초)
    
    Returns:
        API 응답 데이터
    """
    url = f"{RELAY_OI_BASE_URL}{path}"
    headers = get_relay_oi_auth_headers()
    
    async with httpx.AsyncClient() as client:
        try:
            if method.upper() == "GET":
                response = await client.get(url, params=params, headers=headers, timeout=timeout)
            elif method.upper() == "POST":
                response = await client.post(url, json=body, headers=headers, timeout=timeout)
            elif method.upper() == "PUT":
                response = await client.put(url, json=body, headers=headers, timeout=timeout)
            elif method.upper() == "DELETE":
                response = await client.delete(url, json=body, headers=headers, timeout=timeout)
            else:
                raise HTTPException(status_code=400, detail=f"지원하지 않는 HTTP 메서드: {method}")
            
            if response.is_success:
                return response.json()
            else:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Relay OI API 오류: {response.status_code} {response.reason_phrase}"
                )
                
        except httpx.TimeoutException:
            raise HTTPException(status_code=408, detail="요청 시간이 초과되었습니다.")
        except httpx.RequestError as e:
            raise HTTPException(status_code=500, detail=f"네트워크 오류: {str(e)}")


def get_cafe24_token() -> Optional[str]:
    """저장된 Cafe24 토큰을 반환합니다."""
    global _cafe24_token
    return _cafe24_token


def save_cafe24_token(token: str, expires_at: int) -> None:
    """Cafe24 토큰을 저장합니다."""
    global _cafe24_token, _token_expires_at
    _cafe24_token = token
    _token_expires_at = expires_at


def clear_cafe24_token() -> None:
    """Cafe24 토큰을 삭제합니다."""
    global _cafe24_token, _token_expires_at
    _cafe24_token = None
    _token_expires_at = None


def is_cafe24_token_expired() -> bool:
    """Cafe24 토큰이 만료되었는지 확인합니다."""
    global _token_expires_at
    if _token_expires_at is None:
        return True
    return time.time() * 1000 >= _token_expires_at


async def get_cafe24_authorization_token() -> str:
    """Cafe24 인증 토큰을 발급받습니다."""
    settings = get_settings()
    
    # 환경변수에서 인증 키 가져오기
    auth_key = getattr(settings, 'cafe24_auth_key', None)
    if not auth_key:
        raise HTTPException(
            status_code=500, 
            detail="Cafe24 인증 키가 설정되지 않았습니다. CAFE24_AUTH_KEY 환경변수를 확인해주세요."
        )
    
    # Basic 인증 헤더 생성
    auth_header = f"Basic {auth_key}"
    
    headers = {
        "X-MGR-AUTH": auth_header,
        "X-Mgr-Product": "direct",
        "Brand-Domain": "dept",
    }
    
    url = f"{RELAY_BASE_URL}/token/access?stage=prod&brand_id=02"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, timeout=30.0)
            
            if not response.is_success:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Cafe24 Authorization Token Error: {response.status_code} {response.reason_phrase}"
                )
            
            token = response.text.strip()
            
            # 토큰과 만료시간을 저장
            expires_at = int(time.time() * 1000) + TOKEN_VALIDITY_DURATION
            save_cafe24_token(token, expires_at)
            
            return token
            
        except httpx.TimeoutException:
            raise HTTPException(status_code=408, detail="요청 시간이 초과되었습니다.")
        except httpx.RequestError as e:
            raise HTTPException(status_code=500, detail=f"네트워크 오류: {str(e)}")


async def refresh_cafe24_token_if_needed() -> str:
    """필요시에만 Cafe24 토큰을 갱신합니다."""
    global _is_refreshing_token
    
    # 이미 갱신 중이면 대기
    if _is_refreshing_token:
        while _is_refreshing_token:
            await asyncio.sleep(0.1)
        token = get_cafe24_token()
        if token:
            return token
    
    # 토큰이 없거나 만료되었으면 갱신
    if not get_cafe24_token() or is_cafe24_token_expired():
        _is_refreshing_token = True
        try:
            token = await get_cafe24_authorization_token()
            _is_refreshing_token = False
            return token
        except Exception as error:
            _is_refreshing_token = False
            clear_cafe24_token()  # 실패 시 토큰 삭제
            raise error
    
    return get_cafe24_token()


@router.get("/token/access", response_model=TokenResponse)
async def get_cafe24_token_endpoint():
    """
    Cafe24 인증 토큰을 발급받습니다.
    
    새로운 토큰을 발급받거나 기존 토큰이 유효한 경우 해당 토큰을 반환합니다.
    """
    try:
        token = await refresh_cafe24_token_if_needed()
        
        return TokenResponse(
            token=token,
            expires_at=_token_expires_at or 0,
            expires_in=TOKEN_VALIDITY_DURATION
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"토큰 발급 중 오류가 발생했습니다: {str(e)}")


@router.get("/token/status")
async def get_token_status():
    """
    현재 토큰 상태를 확인합니다.
    
    토큰의 유효성과 만료 시간을 확인할 수 있습니다.
    """
    token = get_cafe24_token()
    is_expired = is_cafe24_token_expired()
    
    return {
        "has_token": token is not None,
        "is_expired": is_expired,
        "expires_at": _token_expires_at,
        "is_refreshing": _is_refreshing_token
    }


@router.delete("/token")
async def clear_token():
    """
    저장된 토큰을 삭제합니다.
    
    토큰을 강제로 삭제하여 다음 요청 시 새 토큰을 발급받도록 합니다.
    """
    clear_cafe24_token()
    return {"message": "토큰이 삭제되었습니다."}


@router.get("/health")
async def health_check():
    """
    릴레이 서비스 상태를 확인합니다.
    
    릴레이 API 서버와의 연결 상태를 확인합니다.
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{RELAY_BASE_URL}/health", timeout=10.0)
            
            if response.is_success:
                return {
                    "status": "healthy",
                    "message": "릴레이 서비스 연결 정상",
                    "base_url": RELAY_BASE_URL
                }
            else:
                return {
                    "status": "unhealthy",
                    "message": f"릴레이 서비스 응답 오류: {response.status_code}",
                    "base_url": RELAY_BASE_URL
                }
                
    except httpx.TimeoutException:
        return {
            "status": "timeout",
            "message": "릴레이 서비스 응답 시간 초과",
            "base_url": RELAY_BASE_URL
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"릴레이 서비스 연결 실패: {str(e)}",
            "base_url": RELAY_BASE_URL
        }


@router.post("/proxy/{path:path}")
async def proxy_request(
    path: str,
    request_data: Optional[Dict[str, Any]] = None,
    x_mgr_auth: Optional[str] = Header(None, alias="X-MGR-AUTH"),
    x_mgr_product: Optional[str] = Header(None, alias="X-Mgr-Product"),
    brand_domain: Optional[str] = Header(None, alias="Brand-Domain")
):
    """
    릴레이 API로 요청을 프록시합니다.
    
    인증된 토큰을 사용하여 릴레이 API로 요청을 중계합니다.
    """
    try:
        # 토큰이 필요하면 자동으로 갱신
        token = await refresh_cafe24_token_if_needed()
        
        # 요청 헤더 구성
        headers = {
            "X-MGR-AUTH": f"Bearer {token}",
            "X-Mgr-Product": x_mgr_product or "direct",
            "Brand-Domain": brand_domain or "dept",
            "Content-Type": "application/json"
        }
        
        url = f"{RELAY_BASE_URL}/{path}"
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url, 
                json=request_data, 
                headers=headers, 
                timeout=30.0
            )
            
            if response.is_success:
                return response.json()
            else:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"릴레이 API 오류: {response.status_code} {response.reason_phrase}"
                )
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"프록시 요청 중 오류가 발생했습니다: {str(e)}")


@router.get("/proxy/{path:path}")
async def proxy_get_request(
    path: str,
    x_mgr_auth: Optional[str] = Header(None, alias="X-MGR-AUTH"),
    x_mgr_product: Optional[str] = Header(None, alias="X-Mgr-Product"),
    brand_domain: Optional[str] = Header(None, alias="Brand-Domain")
):
    """
    릴레이 API로 GET 요청을 프록시합니다.
    
    인증된 토큰을 사용하여 릴레이 API로 GET 요청을 중계합니다.
    """
    try:
        # 토큰이 필요하면 자동으로 갱신
        token = await refresh_cafe24_token_if_needed()
        
        # 요청 헤더 구성
        headers = {
            "X-MGR-AUTH": f"Bearer {token}",
            "X-Mgr-Product": x_mgr_product or "direct",
            "Brand-Domain": brand_domain or "dept"
        }
        
        url = f"{RELAY_BASE_URL}/{path}"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, timeout=30.0)
            
            if response.is_success:
                return response.json()
            else:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"릴레이 API 오류: {response.status_code} {response.reason_phrase}"
                )
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"프록시 요청 중 오류가 발생했습니다: {str(e)}")


# Relay OI API 특화 함수들
async def get_relay_item(params: Optional[GetRelayItemRequest] = None) -> RelayItemResponse:
    """
    Relay OI API에서 상품 정보를 조회합니다.
    
    Args:
        params: 상품 조회 파라미터 (id 또는 wash_code)
    
    Returns:
        상품 정보 응답
    """
    query_params = {}
    
    if params:
        if params.id is not None:
            query_params["id"] = params.id
        if params.wash_code is not None:
            query_params["wash_code"] = params.wash_code
    
    response_data = await relay_oi_api_request(
        path="/direct/v10/items/prices",
        method="GET",
        params=query_params
    )
    
    return RelayItemResponse(**response_data)


@router.get("/oi/items", response_model=RelayItemResponse)
async def get_relay_item_endpoint(
    id: Optional[int] = None,
    wash_code: Optional[str] = None
):
    """
    Relay OI API를 통해 상품 정보를 조회합니다.
    
    Args:
        id: 상품 ID (integer, int64)
        wash_code: 상품 세탁코드 (8자 이상)
    
    Returns:
        상품 정보 응답
    """
    try:
        params = GetRelayItemRequest(id=id, wash_code=wash_code)
        return await get_relay_item(params)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"상품 조회 중 오류가 발생했습니다: {str(e)}")


@router.get("/oi/health")
async def relay_oi_health_check():
    """
    Relay OI API 서비스 상태를 확인합니다.
    
    Relay OI API 서버와의 연결 상태를 확인합니다.
    """
    try:
        async with httpx.AsyncClient() as client:
            headers = get_relay_oi_auth_headers()
            response = await client.get(f"{RELAY_OI_BASE_URL}/health", headers=headers, timeout=10.0)
            
            if response.is_success:
                return {
                    "status": "healthy",
                    "message": "Relay OI API 연결 정상",
                    "base_url": RELAY_OI_BASE_URL
                }
            else:
                return {
                    "status": "unhealthy",
                    "message": f"Relay OI API 응답 오류: {response.status_code}",
                    "base_url": RELAY_OI_BASE_URL
                }
                
    except httpx.TimeoutException:
        return {
            "status": "timeout",
            "message": "Relay OI API 응답 시간 초과",
            "base_url": RELAY_OI_BASE_URL
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Relay OI API 연결 실패: {str(e)}",
            "base_url": RELAY_OI_BASE_URL
        }
