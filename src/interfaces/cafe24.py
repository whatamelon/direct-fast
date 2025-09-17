"""
카페24 API 통신 모듈

카페24 쇼핑몰 API와 통신하기 위한 공통 모듈 및 각 API별 구현체를 제공합니다.
"""

import requests
import logging
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from ..core.config import get_settings

logger = logging.getLogger(__name__)


class Cafe24APIError(Exception):
    """카페24 API 관련 예외 클래스"""
    pass


class ProductStatus(str, Enum):
    """제품 상태 열거형"""
    NORMAL = "N"  # 정상
    SOLD_OUT = "S"  # 품절
    HIDDEN = "H"  # 숨김
    DISCONTINUED = "D"  # 단종


class ProductDisplayStatus(str, Enum):
    """제품 진열 상태 열거형"""
    DISPLAY = "T"  # 진열
    HIDDEN = "F"  # 미진열


@dataclass
class Cafe24Credentials:
    """카페24 API 인증 정보"""
    mall_id: str
    client_id: str
    client_secret: str
    refresh_token: str


@dataclass
class ProductImage:
    """제품 이미지 정보"""
    id: int
    path: str
    filename: str
    alt_text: Optional[str] = None


@dataclass
class ProductVariant:
    """제품 옵션 정보"""
    variant_code: str
    variant_name: str
    price: int
    stock_quantity: int
    use_inventory: bool
    inventory_quantity: int


@dataclass
class Product:
    """제품 정보"""
    product_no: int
    product_code: str
    product_name: str
    price: int
    retail_price: int
    supply_price: int
    display: bool
    selling: bool
    product_condition: str
    summary_description: str
    description: str
    mobile_description: str
    images: List[ProductImage]
    variants: List[ProductVariant]
    created_date: datetime
    updated_date: datetime


class Cafe24APIClient:
    """카페24 API 공통 클라이언트"""
    
    def __init__(self, credentials: Cafe24Credentials):
        self.credentials = credentials
        self.access_token: Optional[str] = None
        self.base_url = f"https://{credentials.mall_id}.cafe24api.com/api/v2"
        
    def _get_access_token(self) -> str:
        """액세스 토큰을 발급받습니다."""
        url = f"{self.base_url}/oauth/token"
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.credentials.refresh_token,
            'client_id': self.credentials.client_id,
            'client_secret': self.credentials.client_secret
        }
        
        try:
            response = requests.post(url, data=data)
            response.raise_for_status()
            token_data = response.json()
            return token_data['access_token']
        except requests.exceptions.RequestException as e:
            logger.error(f"액세스 토큰 발급 실패: {e}")
            raise Cafe24APIError(f"액세스 토큰 발급 실패: {e}")
    
    def _ensure_access_token(self) -> None:
        """액세스 토큰이 유효한지 확인하고 필요시 갱신합니다."""
        if not self.access_token:
            self.access_token = self._get_access_token()
    
    def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """API 요청을 수행합니다."""
        self._ensure_access_token()
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'X-Cafe24-Api-Version': '2022-03-01'
        }
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API 요청 실패: {method} {endpoint}, {e}")
            raise Cafe24APIError(f"API 요청 실패: {e}")


class ProductAPI(Cafe24APIClient):
    """제품 관련 API 클래스"""
    
    def get_products(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        since_product_no: Optional[int] = None,
        product_name: Optional[str] = None,
        product_code: Optional[str] = None,
        display: Optional[bool] = None,
        selling: Optional[bool] = None,
        product_condition: Optional[str] = None,
        created_start_date: Optional[str] = None,
        created_end_date: Optional[str] = None,
        updated_start_date: Optional[str] = None,
        updated_end_date: Optional[str] = None,
        embed: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        제품 목록을 조회합니다.
        
        Args:
            limit: 조회할 제품 수 (기본값: 10, 최대: 100)
            offset: 조회 시작 위치 (기본값: 0)
            since_product_no: 특정 제품 번호 이후의 제품들 조회
            product_name: 제품명으로 검색
            product_code: 제품 코드로 검색
            display: 진열 여부로 필터링
            selling: 판매 여부로 필터링
            product_condition: 제품 상태로 필터링
            created_start_date: 생성일 시작 (YYYY-MM-DD)
            created_end_date: 생성일 종료 (YYYY-MM-DD)
            updated_start_date: 수정일 시작 (YYYY-MM-DD)
            updated_end_date: 수정일 종료 (YYYY-MM-DD)
            embed: 포함할 추가 정보 (images, variants, categories 등)
        
        Returns:
            제품 목록과 메타데이터가 포함된 딕셔너리
        """
        params = {}
        
        if limit is not None:
            params['limit'] = limit
        if offset is not None:
            params['offset'] = offset
        if since_product_no is not None:
            params['since_product_no'] = since_product_no
        if product_name is not None:
            params['product_name'] = product_name
        if product_code is not None:
            params['product_code'] = product_code
        if display is not None:
            params['display'] = 'T' if display else 'F'
        if selling is not None:
            params['selling'] = 'T' if selling else 'F'
        if product_condition is not None:
            params['product_condition'] = product_condition
        if created_start_date is not None:
            params['created_start_date'] = created_start_date
        if created_end_date is not None:
            params['created_end_date'] = created_end_date
        if updated_start_date is not None:
            params['updated_start_date'] = updated_start_date
        if updated_end_date is not None:
            params['updated_end_date'] = updated_end_date
        if embed is not None:
            params['embed'] = ','.join(embed)
        
        return self._make_request('GET', '/admin/products', params=params)
    
    def get_product(self, product_no: int, embed: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        특정 제품의 상세 정보를 조회합니다.
        
        Args:
            product_no: 제품 번호
            embed: 포함할 추가 정보 (images, variants, categories 등)
        
        Returns:
            제품 상세 정보
        """
        params = {}
        if embed is not None:
            params['embed'] = ','.join(embed)
        
        return self._make_request('GET', f'/admin/products/{product_no}', params=params)
    
    def create_product(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        새 제품을 생성합니다.
        
        Args:
            product_data: 제품 생성 데이터
        
        Returns:
            생성된 제품 정보
        """
        return self._make_request('POST', '/admin/products', data=product_data)
    
    def update_product(self, product_no: int, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        기존 제품을 수정합니다.
        
        Args:
            product_no: 제품 번호
            product_data: 제품 수정 데이터
        
        Returns:
            수정된 제품 정보
        """
        return self._make_request('PUT', f'/admin/products/{product_no}', data=product_data)
    
    def delete_product(self, product_no: int) -> Dict[str, Any]:
        """
        제품을 삭제합니다.
        
        Args:
            product_no: 제품 번호
        
        Returns:
            삭제 결과
        """
        return self._make_request('DELETE', f'/admin/products/{product_no}')


def create_cafe24_client(
    mall_id: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    refresh_token: Optional[str] = None
) -> ProductAPI:
    """
    카페24 API 클라이언트를 생성합니다.
    
    Args:
        mall_id: 쇼핑몰 ID (설정에서 자동 로드)
        client_id: 클라이언트 ID (설정에서 자동 로드)
        client_secret: 클라이언트 시크릿 (설정에서 자동 로드)
        refresh_token: 리프레시 토큰 (설정에서 자동 로드)
    
    Returns:
        ProductAPI 인스턴스
    """
    settings = get_settings()
    
    credentials = Cafe24Credentials(
        mall_id=mall_id or settings.cafe24_mall_id or "",
        client_id=client_id or settings.cafe24_client_id or "",
        client_secret=client_secret or settings.cafe24_client_secret or "",
        refresh_token=refresh_token or settings.cafe24_refresh_token or ""
    )
    
    # 필수 정보 검증
    if not all([credentials.mall_id, credentials.client_id, 
                credentials.client_secret, credentials.refresh_token]):
        raise ValueError("카페24 API 인증 정보가 부족합니다. 환경변수를 확인해주세요.")
    
    return ProductAPI(credentials)


# 편의 함수들
def get_products(**kwargs) -> Dict[str, Any]:
    """제품 목록을 조회하는 편의 함수"""
    client = create_cafe24_client()
    return client.get_products(**kwargs)


def get_product(product_no: int, **kwargs) -> Dict[str, Any]:
    """특정 제품을 조회하는 편의 함수"""
    client = create_cafe24_client()
    return client.get_product(product_no, **kwargs)
