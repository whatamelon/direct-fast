#!/usr/bin/env python3
"""
카페24 API 사용 예시

이 스크립트는 카페24 API 모듈의 사용법을 보여줍니다.
"""

import os
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.interfaces.cafe24 import (
    create_cafe24_client,
    get_products,
    get_product,
    Cafe24Credentials,
    Cafe24APIError
)


def example_basic_usage():
    """기본 사용법 예시"""
    print("=== 카페24 API 기본 사용법 ===")
    
    try:
        # 환경변수에서 설정을 자동으로 로드하여 클라이언트 생성
        client = create_cafe24_client()
        
        # 제품 목록 조회 (기본 설정)
        print("\n1. 제품 목록 조회 (기본 설정):")
        products = client.get_products(limit=5)
        print(f"총 제품 수: {products.get('count', 0)}")
        print(f"조회된 제품 수: {len(products.get('products', []))}")
        
        # 제품 목록 조회 (이미지 포함)
        print("\n2. 제품 목록 조회 (이미지 포함):")
        products_with_images = client.get_products(
            limit=3, 
            embed=['images']
        )
        print(f"이미지 포함 제품 수: {len(products_with_images.get('products', []))}")
        
        # 특정 제품 상세 조회
        if products.get('products'):
            first_product = products['products'][0]
            product_no = first_product.get('product_no')
            
            print(f"\n3. 제품 상세 조회 (제품 번호: {product_no}):")
            product_detail = client.get_product(
                product_no, 
                embed=['images', 'variants']
            )
            product_info = product_detail.get('product', {})
            print(f"제품명: {product_info.get('product_name', 'N/A')}")
            print(f"가격: {product_info.get('price', 0):,}원")
            print(f"진열 여부: {'진열' if product_info.get('display') else '미진열'}")
            print(f"판매 여부: {'판매' if product_info.get('selling') else '미판매'}")
        
    except ValueError as e:
        print(f"❌ 설정 오류: {e}")
        print("환경변수를 확인해주세요:")
        print("- CAFE24_MALL_ID")
        print("- CAFE24_CLIENT_ID") 
        print("- CAFE24_CLIENT_SECRET")
        print("- CAFE24_REFRESH_TOKEN")
    except Cafe24APIError as e:
        print(f"❌ 카페24 API 오류: {e}")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")


def example_manual_credentials():
    """수동으로 인증 정보를 제공하는 예시"""
    print("\n=== 수동 인증 정보 제공 예시 ===")
    
    # 실제 사용 시에는 환경변수나 안전한 방법으로 관리하세요
    credentials = Cafe24Credentials(
        mall_id="your_mall_id",
        client_id="your_client_id", 
        client_secret="your_client_secret",
        refresh_token="your_refresh_token"
    )
    
    try:
        from src.interfaces.cafe24 import ProductAPI
        client = ProductAPI(credentials)
        
        # 제품 검색 예시
        print("\n제품명으로 검색:")
        search_results = client.get_products(
            product_name="테스트",
            limit=5
        )
        print(f"검색 결과: {search_results.get('count', 0)}개")
        
    except Exception as e:
        print(f"❌ 오류: {e}")


def example_filtering():
    """필터링 사용 예시"""
    print("\n=== 필터링 사용 예시 ===")
    
    try:
        client = create_cafe24_client()
        
        # 진열 중인 제품만 조회
        print("\n1. 진열 중인 제품만 조회:")
        displayed_products = client.get_products(
            display=True,
            limit=5
        )
        print(f"진열 중인 제품 수: {displayed_products.get('count', 0)}")
        
        # 판매 중인 제품만 조회
        print("\n2. 판매 중인 제품만 조회:")
        selling_products = client.get_products(
            selling=True,
            limit=5
        )
        print(f"판매 중인 제품 수: {selling_products.get('count', 0)}")
        
        # 특정 기간에 생성된 제품 조회
        print("\n3. 최근 생성된 제품 조회:")
        recent_products = client.get_products(
            created_start_date="2024-01-01",
            limit=5
        )
        print(f"최근 생성된 제품 수: {recent_products.get('count', 0)}")
        
    except Exception as e:
        print(f"❌ 오류: {e}")


def example_convenience_functions():
    """편의 함수 사용 예시"""
    print("\n=== 편의 함수 사용 예시 ===")
    
    try:
        # 편의 함수를 사용한 제품 목록 조회
        print("\n1. 편의 함수로 제품 목록 조회:")
        products = get_products(limit=3)
        print(f"조회된 제품 수: {len(products.get('products', []))}")
        
        # 편의 함수를 사용한 특정 제품 조회
        if products.get('products'):
            first_product = products['products'][0]
            product_no = first_product.get('product_no')
            
            print(f"\n2. 편의 함수로 제품 상세 조회 (제품 번호: {product_no}):")
            product_detail = get_product(product_no)
            product_info = product_detail.get('product', {})
            print(f"제품명: {product_info.get('product_name', 'N/A')}")
            
    except Exception as e:
        print(f"❌ 오류: {e}")


if __name__ == "__main__":
    print("카페24 API 사용 예시")
    print("=" * 50)
    
    # 기본 사용법
    example_basic_usage()
    
    # 수동 인증 정보 제공 (실제 값이 없으므로 오류 발생 예상)
    example_manual_credentials()
    
    # 필터링 사용법
    example_filtering()
    
    # 편의 함수 사용법
    example_convenience_functions()
    
    print("\n" + "=" * 50)
    print("예시 완료!")
    print("\n실제 사용을 위해서는 다음 환경변수를 설정해주세요:")
    print("- CAFE24_MALL_ID: 카페24 쇼핑몰 ID")
    print("- CAFE24_CLIENT_ID: 카페24 앱 클라이언트 ID")
    print("- CAFE24_CLIENT_SECRET: 카페24 앱 클라이언트 시크릿")
    print("- CAFE24_REFRESH_TOKEN: 카페24 리프레시 토큰")
