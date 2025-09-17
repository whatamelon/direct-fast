"""
카페24 API 엔드포인트

카페24 쇼핑몰 API와 연동하는 FastAPI 엔드포인트를 제공합니다.
"""

from typing import Dict, Any, Optional, List
from fastapi import APIRouter, HTTPException, Query, Path
from pydantic import BaseModel, Field

from src.interfaces.cafe24 import (
    create_cafe24_client, 
    get_products, 
    get_product,
    Cafe24APIError
)

router = APIRouter(prefix="/cafe24", tags=["cafe24"])


class ProductListResponse(BaseModel):
    """제품 목록 응답 모델"""
    products: List[Dict[str, Any]]
    count: int
    limit: int
    offset: int


class ProductResponse(BaseModel):
    """제품 상세 응답 모델"""
    product: Dict[str, Any]


@router.get("/products", response_model=ProductListResponse)
async def get_products_endpoint(
    limit: Optional[int] = Query(None, ge=1, le=100, description="조회할 제품 수 (최대 100)"),
    offset: Optional[int] = Query(None, ge=0, description="조회 시작 위치"),
    since_product_no: Optional[int] = Query(None, description="특정 제품 번호 이후의 제품들 조회"),
    product_name: Optional[str] = Query(None, description="제품명으로 검색"),
    product_code: Optional[str] = Query(None, description="제품 코드로 검색"),
    display: Optional[bool] = Query(None, description="진열 여부로 필터링"),
    selling: Optional[bool] = Query(None, description="판매 여부로 필터링"),
    product_condition: Optional[str] = Query(None, description="제품 상태로 필터링"),
    created_start_date: Optional[str] = Query(None, description="생성일 시작 (YYYY-MM-DD)"),
    created_end_date: Optional[str] = Query(None, description="생성일 종료 (YYYY-MM-DD)"),
    updated_start_date: Optional[str] = Query(None, description="수정일 시작 (YYYY-MM-DD)"),
    updated_end_date: Optional[str] = Query(None, description="수정일 종료 (YYYY-MM-DD)"),
    embed: Optional[str] = Query(None, description="포함할 추가 정보 (images,variants,categories)")
):
    """
    제품 목록을 조회합니다.
    
    카페24 쇼핑몰의 제품 목록을 다양한 필터 조건으로 조회할 수 있습니다.
    """
    try:
        # embed 파라미터를 리스트로 변환
        embed_list = None
        if embed:
            embed_list = [item.strip() for item in embed.split(',')]
        
        result = get_products(
            limit=limit,
            offset=offset,
            since_product_no=since_product_no,
            product_name=product_name,
            product_code=product_code,
            display=display,
            selling=selling,
            product_condition=product_condition,
            created_start_date=created_start_date,
            created_end_date=created_end_date,
            updated_start_date=updated_start_date,
            updated_end_date=updated_end_date,
            embed=embed_list
        )
        
        return ProductListResponse(
            products=result.get('products', []),
            count=result.get('count', 0),
            limit=result.get('limit', 10),
            offset=result.get('offset', 0)
        )
        
    except Cafe24APIError as e:
        raise HTTPException(status_code=400, detail=f"카페24 API 오류: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")


@router.get("/products/{product_no}", response_model=ProductResponse)
async def get_product_endpoint(
    product_no: int = Path(..., description="제품 번호"),
    embed: Optional[str] = Query(None, description="포함할 추가 정보 (images,variants,categories)")
):
    """
    특정 제품의 상세 정보를 조회합니다.
    
    제품 번호를 통해 특정 제품의 상세 정보를 조회할 수 있습니다.
    """
    try:
        # embed 파라미터를 리스트로 변환
        embed_list = None
        if embed:
            embed_list = [item.strip() for item in embed.split(',')]
        
        result = get_product(product_no, embed=embed_list)
        
        return ProductResponse(product=result.get('product', {}))
        
    except Cafe24APIError as e:
        raise HTTPException(status_code=400, detail=f"카페24 API 오류: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")


@router.get("/products/search")
async def search_products(
    q: str = Query(..., description="검색어"),
    limit: Optional[int] = Query(10, ge=1, le=100, description="조회할 제품 수"),
    offset: Optional[int] = Query(0, ge=0, description="조회 시작 위치")
):
    """
    제품을 검색합니다.
    
    제품명이나 제품 코드로 제품을 검색할 수 있습니다.
    """
    try:
        # 제품명으로 검색 시도
        result = get_products(
            product_name=q,
            limit=limit,
            offset=offset
        )
        
        # 제품명 검색 결과가 없으면 제품 코드로 검색
        if not result.get('products'):
            result = get_products(
                product_code=q,
                limit=limit,
                offset=offset
            )
        
        return {
            "products": result.get('products', []),
            "count": result.get('count', 0),
            "limit": result.get('limit', 10),
            "offset": result.get('offset', 0),
            "search_query": q
        }
        
    except Cafe24APIError as e:
        raise HTTPException(status_code=400, detail=f"카페24 API 오류: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")


@router.get("/health")
async def health_check():
    """
    카페24 API 연결 상태를 확인합니다.
    """
    try:
        # 간단한 API 호출로 연결 상태 확인
        client = create_cafe24_client()
        result = client.get_products(limit=1)
        
        return {
            "status": "healthy",
            "message": "카페24 API 연결 정상",
            "mall_id": client.credentials.mall_id
        }
        
    except Cafe24APIError as e:
        return {
            "status": "unhealthy",
            "message": f"카페24 API 연결 실패: {str(e)}",
            "mall_id": None
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"서버 오류: {str(e)}",
            "mall_id": None
        }
