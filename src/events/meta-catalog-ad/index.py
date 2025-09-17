import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import json

from ...interfaces.cafe24 import create_cafe24_client, Cafe24APIError
from ...utils.meta.meta_advertise_image import meta_advertise_image, MetaAdvertiseImageOptions
from ...core.config import get_settings

# 로거 설정
logger = logging.getLogger(__name__)

# 브랜드명 리스트 (실제 데이터로 교체 필요)
META_BRAND_NAMES_LIST = [
    {"brandNameKor": "브랜드\n명"},
    {"brandNameKor": "다른브랜드"},
    {"brandNameKor": "테스트\n브랜드"}
]


class MetaCatalogAdProcessor:
    """메타 카탈로그 광고 이미지 생성 프로세서"""
    
    def __init__(self):
        self.settings = get_settings()
        self.cafe24_client = None
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.errors = []
    
    async def initialize(self):
        """초기화"""
        try:
            self.cafe24_client = create_cafe24_client()
            logger.info("카페24 클라이언트가 초기화되었습니다.")
        except Exception as e:
            logger.error(f"카페24 클라이언트 초기화 실패: {e}")
            raise
    
    async def get_active_products(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """진열중이고 판매중인 상품 목록을 가져옵니다."""
        try:
            logger.info(f"상품 목록 조회 시작 - limit: {limit}, offset: {offset}")
            
            # 카페24 API에서 진열중이고 판매중인 상품 조회
            response = self.cafe24_client.get_products(
                limit=limit,
                offset=offset,
                display=True,  # 진열중
                selling=True,  # 판매중
                embed=['images']  # 이미지 정보 포함
            )
            
            products = response.get('products', [])
            logger.info(f"조회된 상품 수: {len(products)}")
            
            return products
            
        except Cafe24APIError as e:
            logger.error(f"카페24 API 오류: {e}")
            raise
        except Exception as e:
            logger.error(f"상품 목록 조회 실패: {e}")
            raise
    
    def extract_product_data(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """상품 데이터에서 필요한 정보를 추출합니다."""
        try:
            # 기본 상품 정보
            product_no = product.get('product_no', '')
            product_code = product.get('product_code', '')
            product_name = product.get('product_name', '')
            
            # 가격 정보
            price = product.get('price', 0)
            retail_price = product.get('retail_price', 0)
            
            # 판매가 우선, 없으면 소비자가, 없으면 공급가
            sale_price = retail_price if retail_price > 0 else price
            
            # 브랜드명 (상품명에서 추출하거나 기본값 사용)
            brand = self.extract_brand_from_name(product_name)
            
            # 이미지 URL (첫 번째 이미지 사용)
            image_url = self.get_primary_image_url(product)
            
            return {
                'product_no': str(product_no),
                'product_code': str(product_code),
                'product_name': product_name,
                'brand': brand,
                'image_url': image_url,
                'price': str(price),
                'sale_price': str(sale_price)
            }
            
        except Exception as e:
            logger.error(f"상품 데이터 추출 실패: {e}")
            raise
    
    def extract_brand_from_name(self, product_name: str) -> str:
        """상품명에서 브랜드명을 추출합니다."""
        # 간단한 브랜드 추출 로직 (실제로는 더 정교한 로직 필요)
        if not product_name:
            return "기본브랜드"
        
        # 상품명의 첫 번째 단어를 브랜드로 사용
        first_word = product_name.split()[0] if product_name.split() else "기본브랜드"
        return first_word
    
    def get_primary_image_url(self, product: Dict[str, Any]) -> str:
        """상품의 주요 이미지 URL을 가져옵니다."""
        try:
            images = product.get('images', [])
            if images and len(images) > 0:
                # 첫 번째 이미지의 path 사용
                image_path = images[0].get('path', '')
                if image_path:
                    # 카페24 이미지 URL 형식으로 변환
                    return f"https://ecimg.cafe24img.com/pg2036b27689844060/relaymmemory/web/product/medium/{image_path}"
            
            # 이미지가 없으면 기본 이미지 사용
            return "https://via.placeholder.com/400x400?text=No+Image"
            
        except Exception as e:
            logger.error(f"이미지 URL 추출 실패: {e}")
            return "https://via.placeholder.com/400x400?text=No+Image"
    
    async def process_single_product(self, product_data: Dict[str, Any]) -> Optional[str]:
        """단일 상품의 메타 광고 이미지를 생성합니다."""
        try:
            logger.info(f"상품 처리 시작: {product_data['product_name']}")
            
            # 메타 광고 이미지 생성
            s3_url = await meta_advertise_image(
                item_id=int(product_data['product_code']) if product_data['product_code'].isdigit() else 0,
                image_url=product_data['image_url'],
                brand_name_kor=product_data['brand'],
                product_name=product_data['product_name'],
                sale_price=product_data['sale_price'],
                options=MetaAdvertiseImageOptions(
                    width=1080,
                    height=1080
                ),
                meta_brand_names_list=META_BRAND_NAMES_LIST,
                s3_bucket=self.settings.aws_s3_bucket
            )
            
            logger.info(f"메타 광고 이미지 생성 완료: {s3_url}")
            return s3_url
            
        except Exception as e:
            error_msg = f"상품 처리 실패 ({product_data['product_name']}): {e}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return None
    
    async def process_products_batch(self, products: List[Dict[str, Any]], batch_size: int = 10) -> List[Dict[str, Any]]:
        """상품들을 배치로 처리합니다."""
        results = []
        total_products = len(products)
        
        logger.info(f"배치 처리 시작 - 총 {total_products}개 상품, 배치 크기: {batch_size}")
        
        for i in range(0, total_products, batch_size):
            batch = products[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_products + batch_size - 1) // batch_size
            
            logger.info(f"배치 {batch_num}/{total_batches} 처리 중... ({len(batch)}개 상품)")
            
            # 배치 내 상품들을 병렬로 처리
            batch_tasks = []
            for product in batch:
                product_data = self.extract_product_data(product)
                task = self.process_single_product(product_data)
                batch_tasks.append(task)
            
            # 배치 실행
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # 결과 처리
            for j, result in enumerate(batch_results):
                product_data = self.extract_product_data(batch[j])
                if isinstance(result, Exception):
                    logger.error(f"상품 처리 예외: {product_data['product_name']} - {result}")
                    self.error_count += 1
                    results.append({
                        'product_data': product_data,
                        's3_url': None,
                        'error': str(result)
                    })
                else:
                    if result:
                        self.success_count += 1
                        results.append({
                            'product_data': product_data,
                            's3_url': result,
                            'error': None
                        })
                    else:
                        self.error_count += 1
                        results.append({
                            'product_data': product_data,
                            's3_url': None,
                            'error': '이미지 생성 실패'
                        })
                
                self.processed_count += 1
            
            # 배치 간 잠시 대기 (API 부하 방지)
            if i + batch_size < total_products:
                await asyncio.sleep(1)
        
        logger.info(f"배치 처리 완료 - 성공: {self.success_count}, 실패: {self.error_count}")
        return results
    
    async def generate_meta_catalog_csv(self, results: List[Dict[str, Any]]) -> str:
        """메타 카탈로그 CSV를 생성합니다."""
        try:
            logger.info("메타 카탈로그 CSV 생성 시작")
            
            # CSV 헤더
            headers = [
                "id", "title", "description", "availability", "condition",
                "price", "link", "image_link", "brand", "sale_price", "color"
            ]
            
            csv_rows = [headers]
            
            for result in results:
                product_data = result['product_data']
                s3_url = result['s3_url']
                
                # 상품 링크 생성
                product_link = f"https://dept.kr/product/{product_data['product_name'].replace(' ', '-').replace('/', '-')}/{product_data['product_no']}"
                
                # CSV 행 생성
                row = [
                    product_data['product_no'],  # id
                    product_data['product_name'],  # title
                    f"{product_data['product_name']}입니다.",  # description
                    "in stock",  # availability
                    "used",  # condition
                    product_data['price'],  # price
                    product_link,  # link
                    s3_url or product_data['image_url'],  # image_link (S3 URL 우선)
                    product_data['brand'],  # brand
                    product_data['sale_price'],  # sale_price
                    ""  # color
                ]
                
                csv_rows.append(row)
            
            # CSV 내용을 문자열로 변환
            csv_content = "\n".join([",".join([f'"{cell}"' for cell in row]) for row in csv_rows])
            
            logger.info(f"메타 카탈로그 CSV 생성 완료 - {len(csv_rows)-1}개 상품")
            return csv_content
            
        except Exception as e:
            logger.error(f"CSV 생성 실패: {e}")
            raise
    
    async def save_csv_to_s3(self, csv_content: str, filename: str) -> str:
        """CSV 파일을 S3에 저장합니다."""
        try:
            import boto3
            from io import StringIO
            
            # AWS 자격 증명 설정
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.settings.aws_access_key_id,
                aws_secret_access_key=self.settings.aws_secret_access_key,
                region_name=self.settings.aws_region
            )
            
            # CSV 내용을 바이트로 변환
            csv_bytes = csv_content.encode('utf-8')
            
            # S3에 업로드
            s3_key = f"meta_catalog/{filename}"
            s3_client.put_object(
                Bucket=self.settings.aws_s3_bucket,
                Key=s3_key,
                Body=csv_bytes,
                ContentType='text/csv; charset=utf-8'
            )
            
            s3_url = f"https://{self.settings.aws_s3_bucket}.s3.amazonaws.com/{s3_key}"
            logger.info(f"CSV 파일 S3 업로드 완료: {s3_url}")
            return s3_url
            
        except Exception as e:
            logger.error(f"CSV S3 업로드 실패: {e}")
            raise
    
    async def run_full_process(self, limit: int = 1000) -> Dict[str, Any]:
        """전체 프로세스를 실행합니다."""
        start_time = datetime.now()
        logger.info("메타 카탈로그 광고 이미지 생성 프로세스 시작")
        
        try:
            # 초기화
            await self.initialize()
            
            # 상품 목록 조회
            products = await self.get_active_products(limit=limit)
            
            if not products:
                logger.warning("처리할 상품이 없습니다.")
                return {
                    "status": "warning",
                    "message": "처리할 상품이 없습니다.",
                    "processed_count": 0,
                    "success_count": 0,
                    "error_count": 0
                }
            
            # 상품 처리
            results = await self.process_products_batch(products, batch_size=10)
            
            # CSV 생성
            csv_content = await self.generate_meta_catalog_csv(results)
            
            # CSV 파일명 생성 (날짜 포함)
            filename = f"relay_meta_dept_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            # S3에 저장
            csv_s3_url = await self.save_csv_to_s3(csv_content, filename)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "status": "completed",
                "message": "메타 카탈로그 광고 이미지 생성 완료",
                "processed_count": self.processed_count,
                "success_count": self.success_count,
                "error_count": self.error_count,
                "csv_url": csv_s3_url,
                "duration_seconds": duration,
                "errors": self.errors[:10]  # 최대 10개 오류만 포함
            }
            
            logger.info(f"프로세스 완료 - 처리: {self.processed_count}, 성공: {self.success_count}, 실패: {self.error_count}")
            return result
            
        except Exception as e:
            logger.error(f"프로세스 실행 실패: {e}")
            return {
                "status": "error",
                "message": f"프로세스 실행 실패: {e}",
                "processed_count": self.processed_count,
                "success_count": self.success_count,
                "error_count": self.error_count,
                "errors": self.errors
            }


# 스케줄러 작업 함수
async def meta_catalog_ad_job():
    """메타 카탈로그 광고 이미지 생성 작업 (매일 새벽 3시 실행)"""
    try:
        logger.info("🚀 메타 카탈로그 광고 이미지 생성 작업 시작")
        
        processor = MetaCatalogAdProcessor()
        result = await processor.run_full_process(limit=1000)  # 최대 1000개 상품 처리
        
        logger.info(f"메타 카탈로그 광고 이미지 생성 작업 완료: {result}")
        return result
        
    except Exception as e:
        logger.error(f"메타 카탈로그 광고 이미지 생성 작업 실패: {e}")
        return {
            "status": "error",
            "message": f"작업 실패: {e}",
            "timestamp": datetime.now().isoformat()
        }


# 테스트용 함수
async def test_meta_catalog_process():
    """테스트용 함수"""
    try:
        logger.info("테스트 시작")
        
        processor = MetaCatalogAdProcessor()
        result = await processor.run_full_process(limit=5)  # 테스트용으로 5개만
        
        logger.info(f"테스트 완료: {result}")
        return result
        
    except Exception as e:
        logger.error(f"테스트 실패: {e}")
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    # 테스트 실행
    asyncio.run(test_meta_catalog_process())
