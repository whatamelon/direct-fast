import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Set, Tuple
import boto3
from botocore.exceptions import ClientError

from ...interfaces.cafe24 import create_cafe24_client, Cafe24APIError
from ...interfaces.google_sheet import create_google_sheet_interface, GoogleSheetError, get_spreadsheet_id_from_url
from ...utils.meta.meta_advertise_image import meta_advertise_image, MetaAdvertiseImageOptions
from ...core.config import get_settings
from ...assets.files.brand_names import brandNames

# 로거 설정
logger = logging.getLogger(__name__)

class MetaCatalogAdProcessor:
    """메타 카탈로그 광고 이미지 생성 프로세서"""
    
    def __init__(self, spreadsheet_id: str, generate_images: bool = True):
        self.settings = get_settings()
        self.spreadsheet_id = spreadsheet_id
        self.cafe24_client = None
        self.google_sheet = None
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.errors = []
        self.new_products_count = 0
        self.existing_products_count = 0
        
        # 이미지 생성 토글 옵션 (True: 이미지 생성, False: 이미지 생성 건너뛰기)
        self.generate_images = generate_images
        print(f"🖼️ 이미지 생성 모드: {'ON' if self.generate_images else 'OFF'}")
    
    async def initialize(self):
        """초기화"""
        try:
            self.cafe24_client = create_cafe24_client()
            self.google_sheet = create_google_sheet_interface()
            print("✅ 카페24 클라이언트와 Google Sheets 인터페이스가 초기화되었습니다.")
        except Exception as e:
            print(f"❌ 클라이언트 초기화 실패: {e}")
            raise
    
    async def get_existing_products_from_sheet(self) -> Tuple[List[Dict[str, Any]], Set[str]]:
        """Google Sheets에서 기존 상품 데이터를 가져옵니다."""
        try:
            print("📊 [STEP 1] Google Sheets에서 기존 상품 데이터 조회 시작")
            print(f"📋 스프레드시트 ID: {self.spreadsheet_id}")
            
            # 스프레드시트의 모든 데이터 읽기 (헤더 포함)
            values = self.google_sheet.read_values(self.spreadsheet_id, "Sheet1!A:K")
            
            if not values or len(values) <= 1:
                print("📝 기존 상품 데이터가 없습니다. (새로운 스프레드시트)")
                return [], set()
            
            # 헤더와 데이터 분리
            headers = values[0]
            data_rows = values[1:]
            
            print(f"📋 스프레드시트 헤더: {headers}")
            print(f"📊 총 데이터 행 수: {len(data_rows)}개")
            
            # 상품 데이터를 딕셔너리로 변환
            existing_products = []
            existing_product_codes = set()
            
            for row_idx, row in enumerate(data_rows):
                if len(row) < len(headers):
                    # 부족한 컬럼을 빈 문자열로 채움
                    row.extend([''] * (len(headers) - len(row)))
                
                # product_no를 문자열로 변환하여 타입 일관성 보장
                product_no = str(row[0]) if len(row) > 0 and row[0] else ''
                
                product_data = {
                    'idx': row_idx + 2,
                    'product_no': product_no,
                    'title': row[1] if len(row) > 1 else '',
                    'description': row[2] if len(row) > 2 else '',
                    'availability': row[3] if len(row) > 3 else '',
                    'condition': row[4] if len(row) > 4 else '',
                    'price': row[5] if len(row) > 5 else '',
                    'link': row[6] if len(row) > 6 else '',
                    'image_link': row[7] if len(row) > 7 else '',
                    'brand': row[8] if len(row) > 8 else '',
                    'sale_price': row[9] if len(row) > 9 else '',
                    'color': row[10] if len(row) > 10 else ''
                }
                
                existing_products.append(product_data)
                if product_no:  # 빈 문자열이 아닌 경우만 추가
                    existing_product_codes.add(product_no)
            
            print(f"✅ [STEP 1 완료] 기존 상품 {len(existing_products)}개 조회 완료")
            print(f"🔢 고유 상품 코드 수: {len(existing_product_codes)}개")
            print(f"🔍 기존 상품 코드 예시: {list(existing_product_codes)[:5]}")
            return existing_products, existing_product_codes
            
        except GoogleSheetError as e:
            print(f"❌ [STEP 1 실패] Google Sheets 데이터 조회 실패: {e}")
            raise
        except Exception as e:
            print(f"❌ [STEP 1 실패] 기존 상품 데이터 조회 실패: {e}")
            raise
    
    async def get_active_products(self, limit: int = 100) -> List[Dict[str, Any]]:
        """진열중이고 판매중인 전체 상품 목록을 가져옵니다."""
        try:
            print("🛍️ [STEP 2] 카페24에서 현재 상품 목록 조회 시작")
            print(f"📦 배치 크기: {limit}개")
            
            # since_product_no를 사용하여 모든 상품 조회
            all_products = []
            since_product_no = 0
            batch_count = 0
            
            while True:
                batch_count += 1
                print(f"")
                print(f"----------------------------------------------------------------------------------")
                print(f"")
                print(f"📦 [배치 {batch_count}] 카페24 API 호출 중... (since_product_no: {since_product_no})")
                
                # 카페24 API에서 상품 조회
                response = self.cafe24_client.get_products(
                    limit=limit,
                    display=True,
                    since_product_no=since_product_no,
                    embed=['images']  # 이미지 정보 포함
                )
                
                products = response.get('products', [])
                
                if not products:
                    print("🏁 더 이상 조회할 상품이 없습니다.")
                    break
                
                all_products.extend(products)
                print(f"✅ [배치 {batch_count} 완료] {len(products)}개 상품 조회됨 (누적: {len(all_products)}개)")
                
                # 마지막 상품의 product_no를 다음 since_product_no로 설정
                since_product_no = products[-1]['product_no']
                print(f"🔄 다음 배치를 위한 since_product_no: {since_product_no}")
                
                # 조회된 상품 수가 limit보다 적으면 마지막 배치
                if len(products) < limit:
                    print("🏁 마지막 배치입니다. (limit보다 적은 상품 수)")
                    break
                
                # API 부하 방지를 위한 잠시 대기
                # await asyncio.sleep(0.1)
            
            print(f"✅ [STEP 2 완료] 카페24 상품 조회 완료 - 총 {len(all_products)}개 상품")
            return all_products
            
        except Cafe24APIError as e:
            print(f"❌ [STEP 2 실패] 카페24 API 오류: {e}")
            raise
        except Exception as e:
            print(f"❌ [STEP 2 실패] 상품 목록 조회 실패: {e}")
            raise
    
    async def get_out_of_stock_products(self, limit: int = 100) -> List[Dict[str, Any]]:
        """품절된 상품 목록을 가져옵니다."""
        try:
            print("📦 품절 상품 조회 시작")
            print(f"📦 배치 크기: {limit}개")
            
            # 품절 상품 조회 (stock_quantity가 0인 상품들)
            all_products = []
            since_product_no = 0
            batch_count = 0
            
            while True:
                batch_count += 1
                print(f"")
                print(f"----------------------------------------------------------------------------------")
                print(f"")
                print(f"📦 [품절 상품 배치 {batch_count}] 카페24 API 호출 중... (since_product_no: {since_product_no})")
                
                # 카페24 API에서 품절 상품 조회
                response = self.cafe24_client.get_products(
                    limit=limit,
                    display=True,
                    stock_quantity_max=0,
                    stock_quantity_min=0,
                    use_inventory=True,
                    since_product_no=since_product_no,
                    embed=['images']  # 이미지 정보 포함
                )
                
                products = response.get('products', [])
                
                if not products:
                    print("🏁 더 이상 조회할 품절 상품이 없습니다.")
                    break
                
                all_products.extend(products)
                print(f"✅ [품절 상품 배치 {batch_count} 완료] {len(products)}개 상품 조회됨 (누적: {len(all_products)}개)")
                
                # 마지막 상품의 product_no를 다음 since_product_no로 설정
                since_product_no = products[-1]['product_no']
                print(f"🔄 다음 배치를 위한 since_product_no: {since_product_no}")
                
                # 조회된 상품 수가 limit보다 적으면 마지막 배치
                if len(products) < limit:
                    print("🏁 마지막 배치입니다. (limit보다 적은 상품 수)")
                    break
                
                # API 부하 방지를 위한 잠시 대기
                # await asyncio.sleep(0.1)
            
            print(f"✅ 품절 상품 조회 완료 - 총 {len(all_products)}개 상품")
            return all_products
            
        except Cafe24APIError as e:
            print(f"❌ 품절 상품 조회 실패 - 카페24 API 오류: {e}")
            raise
        except Exception as e:
            print(f"❌ 품절 상품 조회 실패: {e}")
            raise
    
    def compare_products(self, existing_product_codes: Set[str], 
                        current_products: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        기존 상품과 현재 상품을 비교하여 신규 상품과 기존 상품을 구분합니다.
        
        Args:
            existing_product_codes: 기존 상품 코드 집합 (A)
            current_products: 현재 상품 리스트 (B)
            
        Returns:
            (신규 상품 리스트, 기존 상품 리스트)
        """
        try:
            print("🔍 [STEP 3] 상품 비교 시작")
            
            # 현재 상품 코드 집합 생성 (문자열로 변환하여 타입 일관성 보장)
            current_product_codes = {str(product['product_no']) for product in current_products}
            
            print(f"📊 현재 상품 코드 수: {len(current_product_codes)}개")
            print(f"📊 기존 상품 코드 수: {len(existing_product_codes)}개")
            print(f"🔍 현재 상품 코드 예시: {list(current_product_codes)[:5]}")
            print(f"🔍 기존 상품 코드 예시: {list(existing_product_codes)[:5]}")
            
            # A에 없는데 B에 있는 상품들 (신규 상품)
            new_product_codes = current_product_codes - existing_product_codes
            print(f"🔍 신규 상품 코드 수: {len(new_product_codes)}개")
            print(f"🔍 신규 상품 코드 예시: {list(new_product_codes)[:5]}")
            
            new_products = [product for product in current_products 
                           if str(product['product_no']) in new_product_codes]
            
            # A에 있고 B에도 있는 상품들 (기존 상품)
            existing_product_codes_in_current = current_product_codes & existing_product_codes
            print(f"🔍 기존 상품 코드 수 (교집합): {len(existing_product_codes_in_current)}개")
            print(f"🔍 기존 상품 코드 예시: {list(existing_product_codes_in_current)[:5]}")
            
            existing_products = [product for product in current_products 
                               if str(product['product_no']) in existing_product_codes_in_current]
            
            self.new_products_count = len(new_products)
            self.existing_products_count = len(existing_products)
            
            print(f"✅ [STEP 3 완료] 상품 비교 완료")
            print(f"🆕 신규 상품: {self.new_products_count}개")
            print(f"🔄 기존 상품: {self.existing_products_count}개")
            
            return new_products, existing_products
            
        except Exception as e:
            print(f"❌ [STEP 3 실패] 상품 비교 실패: {e}")
            raise
    
    def extract_product_data(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """상품 데이터에서 필요한 정보를 추출합니다."""
        try:
            # 기본 상품 정보
            product_no = product.get('product_no', '')
            product_code = product.get('product_code', '')
            product_name = product.get('product_name', '')
            
            # 가격 정보
            price = product.get('price', 0).replace('.00', '')
            retail_price = product.get('retail_price', 0).replace('.00', '')

            # print(f"")
            # print(f"----------------------------------------------------------------------------------")
            # print(f"")
            # print(f"product_code: {product_code}")
            # print(f"price: {price}")
            # print(f"retail_price: {retail_price}")
            
            # 판매가 우선, 없으면 소비자가, 없으면 공급가
            sale_price = price
            
            # 브랜드명 (상품명에서 추출하거나 기본값 사용)
            brand = self.extract_brand_from_name(product_name)
            # print(f"brand: {brand}")
            
            # 이미지 URL (첫 번째 이미지 사용)
            image_url = self.get_primary_image_url(product)
            
            return {
                'product_no': str(product_no),
                'product_code': str(product_code),
                'custom_product_code': str(product.get('custom_product_code', '')),
                'product_name': product_name,
                'brand': brand,
                'image_url': image_url,
                'price': str(retail_price),
                'sale_price': str(sale_price)
            }
            
        except Exception as e:
            logger.error(f"상품 데이터 추출 실패: {e}")
            raise
    
    def extract_brand_from_name(self, product_name: str) -> str:
        """상품명에서 브랜드명을 추출합니다."""
        if not product_name:
            return "기본브랜드"
        
        # 특별한 브랜드 예외처리 (매칭이 잘 안 되는 브랜드들)
        # 브랜드명 파일의 실제 패턴에 맞춘 키워드 매핑
        special_brand_keywords = {
            # 커스텀멜로우 관련 키워드들
            '커스텀멜로우': '커스텀\n멜로우',
            '커스텀 멜로우': '커스텀\n멜로우',
            'customellow': '커스텀\n멜로우',
            'customel': '커스텀\n멜로우',  # 영문 첫 부분
            
            # 먼싱웨어 관련 키워드들
            '먼싱웨어': '먼싱웨어',
            'munsingware': '먼싱웨어',
            'munsing wear': '먼싱웨어',
            'munsing': '먼싱웨어',  # 영문 첫 부분
        }
        
        # 특별한 브랜드 키워드 체크
        product_name_lower = product_name.lower()
        for keyword, brand_name in special_brand_keywords.items():
            if keyword.lower() in product_name_lower:
                # print(f"🎯 특별 브랜드 매칭 성공: {product_name} → {brand_name}")
                return brand_name
        
        # 브랜드명 리스트에서 매칭되는 브랜드 찾기
        for brand in brandNames:
            brand_name_kor_original = brand.get('brandNameKor', '').strip()
            brand_name_eng_original = brand.get('brandNameEng', '').strip()
            
            # 매칭을 위해 \n을 공백으로 변환한 버전도 준비
            brand_name_kor_for_match = brand_name_kor_original.replace('\n', ' ').strip()
            brand_name_eng_for_match = brand_name_eng_original.replace('\n', ' ').strip()
            
            # 상품명에 브랜드명이 포함되어 있는지 확인 (대소문자 구분 없이)
            if (brand_name_kor_for_match and brand_name_kor_for_match.lower() in product_name.lower()) or \
               (brand_name_eng_for_match and brand_name_eng_for_match.lower() in product_name.lower()):
                # print(f"🎯 브랜드 매칭 성공: {product_name} → {brand_name_kor_original} (원본 줄바꿈 유지)")
                return brand_name_kor_original  # 원본 줄바꿈을 유지한 브랜드명 반환
        
        # 매칭되는 브랜드가 없으면 상품명의 첫 번째 단어를 브랜드로 사용
        first_word = product_name.split()[0] if product_name.split() else "기본브랜드"
        print(f"⚠️ 브랜드 매칭 실패: {product_name} → {first_word}")
        return first_word
    
    def get_primary_image_url(self, product: Dict[str, Any]) -> str:
        """상품의 주요 이미지 URL을 가져옵니다."""
        try:
            # product가 문자열인 경우 JSON 파싱
            if isinstance(product, str):
                import json
                product = json.loads(product)
            
            # 우선순위: detail_image > list_image > small_image > tiny_image
            image_url = (
                product.get('list_image') or 
                product.get('detail_image') or 
                product.get('small_image') or 
                product.get('tiny_image')
            )
            
            if image_url:
                # print(f"🖼️ 이미지 URL 추출 성공: {image_url}")
                return image_url
            
            # 이미지가 없으면 기본 이미지 사용
            print("⚠️ 이미지 URL을 찾을 수 없음 - 기본 이미지 사용")
            return "https://via.placeholder.com/400x400?text=No+Image"
            
        except Exception as e:
            print(f"❌ 이미지 URL 추출 실패: {e}")
            return "https://via.placeholder.com/400x400?text=No+Image"
    
    def check_s3_image_exists(self, custom_product_code: str) -> bool:
        """S3에 해당 상품의 메타 광고 이미지가 이미 존재하는지 확인합니다."""
        try:
            # S3 클라이언트 생성
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.settings.aws_access_key_id,
                aws_secret_access_key=self.settings.aws_secret_access_key,
                region_name=self.settings.aws_region
            )
            
            # S3 키 생성 (meta_advertise_image.py와 동일한 패턴)
            s3_key = f"meta_image/{custom_product_code}.jpg"
            
            # S3 객체 존재 여부 확인
            s3_client.head_object(Bucket=self.settings.aws_s3_bucket, Key=s3_key)
            print(f"✅ S3에 이미지 존재: {s3_key}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"📝 S3에 이미지 없음: {s3_key}")
                return False
            else:
                print(f"⚠️ S3 체크 중 오류: {e}")
                return False
        except Exception as e:
            print(f"⚠️ S3 체크 중 예외: {e}")
            return False
    
    async def process_single_product(self, product_data: Dict[str, Any]) -> Optional[str]:
        """단일 상품의 메타 광고 이미지를 생성합니다."""
        try:
            custom_product_code = product_data.get('custom_product_code', '')
            product_name = product_data['product_name']
            
            # print(f"🖼️ 상품 처리 시작: {product_name}")
            
            # 이미지 생성이 비활성화된 경우 기본 이미지 URL 반환
            if not self.generate_images:
                # 기존 이미지가 있으면 사용, 없으면 원본 상품 이미지 사용
                if custom_product_code:
                    s3_url = f"https://s3.ap-northeast-2.amazonaws.com/{self.settings.aws_s3_bucket}/meta_image/{custom_product_code}.jpg"
                    # print(f"⏭️ 이미지 생성 건너뛰기 (기존 URL 사용): {s3_url}")
                    return s3_url
                else:
                    # custom_product_code가 없으면 원본 상품 이미지 사용
                    original_image_url = product_data.get('image_url', 'https://via.placeholder.com/400x400?text=No+Image')
                    # print(f"⏭️ 이미지 생성 건너뛰기 (원본 이미지 사용): {original_image_url}")
                    return original_image_url
            
            # S3에 이미지가 이미 존재하는지 확인
            if custom_product_code and self.check_s3_image_exists(custom_product_code):
                # 기존 이미지 URL 생성
                s3_url = f"https://s3.ap-northeast-2.amazonaws.com/{self.settings.aws_s3_bucket}/meta_image/{custom_product_code}.jpg"
                # print(f"♻️ 기존 이미지 재사용: {s3_url}")
                return s3_url
            
            # print(f"🆕 새 이미지 생성 필요: {product_name}")
            
            # 메타 광고 이미지 생성
            s3_url = await meta_advertise_image(
                item_id=int(product_data['product_code']) if product_data['product_code'].isdigit() else 0,
                image_url=product_data['image_url'],
                product_name=product_data['product_name'],
                sale_price=product_data['sale_price'],
                options=MetaAdvertiseImageOptions(
                    width=1080,
                    height=1080
                ),
                s3_bucket=self.settings.aws_s3_bucket,
                brandNameKor=product_data['brand'],
                custom_product_code=custom_product_code
            )
            
            # print(f"✅ 메타 광고 이미지 생성 완료: {s3_url}")
            return s3_url
            
        except Exception as e:
            error_msg = f"상품 처리 실패 ({product_data['product_name']}): {e}"
            print(f"❌ {error_msg}")
            self.errors.append(error_msg)
            return None
    
    async def update_existing_products_status(self, existing_products: List[Dict[str, Any]], 
                                            out_of_stock_products: List[Dict[str, Any]]) -> None:
        """
        기존 상품들의 상태를 업데이트합니다.
        품절 상품 목록에 있는 상품들을 'out of stock'으로 설정합니다.
        
        Args:
            existing_products: 스프레드시트의 기존 상품 데이터 리스트
            out_of_stock_products: 품절된 상품 리스트
        """
        try:
            print(f"📊 기존 상품 {len(existing_products)}개 상태 확인 중...")
            print(f"📦 품절 상품 {len(out_of_stock_products)}개 확인 중...")
            
            # 품절 상품을 product_no로 인덱싱 (문자열로 변환)
            out_of_stock_product_nos = {str(product['product_no']) for product in out_of_stock_products}
            print(f"🔍 품절 상품 번호 집합: {len(out_of_stock_product_nos)}개")
            print(f"🔍 품절 상품 번호 예시: {list(out_of_stock_product_nos)[:5]}")
            
            # 업데이트할 데이터 준비
            update_data = []
            updated_count = 0
            matched_count = 0
            
            for sheet_product in existing_products:
                product_no = str(sheet_product['product_no'])  # 문자열로 변환
                row_index = sheet_product['idx']
                
                # 품절 상품 목록에 있는지 확인
                if product_no in out_of_stock_product_nos:
                    matched_count += 1
                    # 품절 상품이므로 'out of stock'으로 설정
                    availability = 'out of stock'
                    
                    # 현재 availability가 'out of stock'이 아닌 경우에만 업데이트
                    if sheet_product['availability'] != availability:
                        update_data.append({
                            'range': f'Sheet1!D{row_index}',  # availability 컬럼 (D)
                            'values': [[availability]]
                        })
                        updated_count += 1
                        print(f"🔄 상품 {product_no} 품절 상태 업데이트: {sheet_product['availability']} → {availability}")
            
            print(f"🔍 매칭된 상품 수: {matched_count}개")
            print(f"🔍 실제 업데이트할 상품 수: {updated_count}개")
            
            if update_data:
                # 배치 업데이트 실행
                self.google_sheet.write_multiple_ranges(
                    self.spreadsheet_id, 
                    update_data, 
                    value_input_option='USER_ENTERED'
                )
                
                print(f"✅ 품절 상품 상태 업데이트 완료 - {len(update_data)}개 상품")
            else:
                print("✅ 업데이트할 품절 상품이 없습니다.")
            
        except GoogleSheetError as e:
            print(f"❌ 품절 상품 상태 업데이트 실패: {e}")
            raise
        except Exception as e:
            print(f"❌ 품절 상품 상태 업데이트 실패: {e}")
            raise
    
    async def update_existing_products_images(self, existing_products: List[Dict[str, Any]], 
                                            current_products: List[Dict[str, Any]]) -> None:
        """
        기존 상품들의 메타 광고 이미지를 새로 생성하고 스프레드시트의 이미지 URL을 업데이트합니다.
        
        Args:
            existing_products: 스프레드시트의 기존 상품 데이터 리스트
            current_products: 현재 카페24에서 조회한 상품 리스트
        """
        try:
            if not self.generate_images:
                print(f"⏭️ 이미지 생성이 비활성화되어 있어 기존 상품 이미지 업데이트를 건너뜁니다.")
                return
                
            print(f"🖼️ 기존 상품 {len(existing_products)}개 이미지 업데이트 시작...")
            
            # 현재 상품을 product_no로 인덱싱
            current_products_dict = {product['product_no']: product for product in current_products}
            
            # 업데이트할 데이터 준비
            update_data = []
            processed_count = 0
            
            for sheet_product in existing_products:
                product_no = sheet_product['product_no']
                
                # 현재 상품 목록에 있는지 확인
                if product_no in current_products_dict:
                    current_product = current_products_dict[product_no]
                    row_index = sheet_product['idx']
                    
                    try:
                        # 상품 데이터 추출
                        product_data = self.extract_product_data(current_product)
                        custom_product_code = product_data.get('custom_product_code', '')
                        
                        # S3에 이미지가 이미 존재하는지 확인
                        if custom_product_code and self.check_s3_image_exists(custom_product_code):
                            # 기존 이미지 URL 생성
                            s3_url = f"https://s3.ap-northeast-2.amazonaws.com/{self.settings.aws_s3_bucket}/meta_image/{custom_product_code}.jpg"
                            
                            # 이미지 URL 업데이트 데이터 추가
                            update_data.append({
                                'range': f'Sheet1!H{row_index}',  # image_link 컬럼 (H)
                                'values': [[s3_url]]
                            })
                            processed_count += 1
                            print(f"♻️ 상품 {product_no} 기존 이미지 재사용: {s3_url}")
                        else:
                            # 메타 광고 이미지 생성
                            s3_url = await self.process_single_product(product_data)
                            
                            if s3_url:
                                # 이미지 URL 업데이트 데이터 추가
                                update_data.append({
                                    'range': f'Sheet1!H{row_index}',  # image_link 컬럼 (H)
                                    'values': [[s3_url]]
                                })
                                processed_count += 1
                                print(f"🖼️ 상품 {product_no} 새 이미지 생성: {s3_url}")
                            else:
                                print(f"⚠️ 상품 {product_no} 이미지 생성 실패")
                            
                    except Exception as e:
                        print(f"❌ 상품 {product_no} 이미지 업데이트 실패: {e}")
                        continue
            
            if update_data:
                # 배치 업데이트 실행 (100개씩 나누어서 처리)
                batch_size = 100
                for i in range(0, len(update_data), batch_size):
                    batch = update_data[i:i + batch_size]
                    self.google_sheet.write_multiple_ranges(
                        self.spreadsheet_id, 
                        batch, 
                        value_input_option='USER_ENTERED'
                    )
                    print(f"📝 배치 {i//batch_size + 1} 업데이트 완료 ({len(batch)}개 상품)")
                
                print(f"✅ 기존 상품 이미지 업데이트 완료 - {len(update_data)}개 상품")
            else:
                print("✅ 업데이트할 기존 상품이 없습니다.")
            
        except GoogleSheetError as e:
            print(f"❌ 기존 상품 이미지 업데이트 실패: {e}")
            raise
        except Exception as e:
            print(f"❌ 기존 상품 이미지 업데이트 실패: {e}")
            raise
    
    
    async def process_products_batch(self, products: List[Dict[str, Any]], batch_size: int = 100) -> List[Dict[str, Any]]:
        """상품들을 배치로 처리합니다."""
        results = []
        total_products = len(products)

        print(f"")
        print(f"----------------------------------------------------------------------------------")
        print(f"")
        print(f"🖼️ 배치 처리 시작 - 총 {total_products}개 상품, 배치 크기: {batch_size}")
        
        for i in range(0, total_products, batch_size):
            batch = products[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_products + batch_size - 1) // batch_size
            
            print(f"📦 [배치 {batch_num}/{total_batches}] 처리 중... ({len(batch)}개 상품)")
            
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
                    print(f"❌ 상품 처리 예외: {product_data['product_name']} - {result}")
                    self.error_count += 1
                    results.append({
                        'product_data': product_data,
                        's3_url': None,
                        'error': str(result)
                    })
                else:
                    if result:
                        self.success_count += 1
                        # print(f"✅ 상품 처리 성공: {product_data['product_name']}")
                        results.append({
                            'product_data': product_data,
                            's3_url': result,
                            'error': None
                        })
                    else:
                        self.error_count += 1
                        print(f"❌ 상품 처리 실패: {product_data['product_name']}")
                        results.append({
                            'product_data': product_data,
                            's3_url': None,
                            'error': '이미지 생성 실패'
                        })
                
                self.processed_count += 1
            
            print(f"📊 [배치 {batch_num} 완료] 성공: {self.success_count}, 실패: {self.error_count}")
            
            # 배치 간 잠시 대기 (API 부하 방지) - 100개씩 처리하므로 2초 대기
            if i + batch_size < total_products:
                await asyncio.sleep(1)
        
        print(f"✅ 배치 처리 완료 - 총 성공: {self.success_count}, 총 실패: {self.error_count}")
        return results
    
    async def add_new_products_to_sheet(self, results: List[Dict[str, Any]]) -> None:
        """
        신규 상품들을 Google Sheets에 추가합니다.
        
        Args:
            results: 신규 상품 처리 결과 리스트
        """
        try:
            if not results:
                print("📝 추가할 신규 상품이 없습니다.")
                return
            
            print(f"📝 신규 상품 {len(results)}개를 Google Sheets에 추가 시작")
            
            # CSV 형식으로 데이터 준비
            new_rows = []
            
            for result in results:
                product_data = result['product_data']
                s3_url = result['s3_url']
                
                # 상품 링크 생성
                product_link = f"https://dept.kr/product/{product_data['product_name'].replace(' ', '-').replace('/', '-')}/{product_data['product_no']}"
                
                # 새 행 생성 (숫자 데이터는 정수로 변환하여 작은따옴표 문제 방지)
                row = [
                    str(product_data['product_no']),  # id (문자열로 변환)
                    product_data['product_name'],  # title
                    f"{product_data['product_name']}입니다.",  # description
                    "in stock",  # availability
                    "used",  # condition
                    int(float(product_data['price'])) if product_data['price'] else 0,  # price (숫자로 변환)
                    product_link,  # link
                    s3_url or product_data['image_url'],  # image_link (S3 URL 우선)
                    product_data['brand'].replace('\n', '').replace(' ', ''),  # brand (띄어쓰기 모두 제거)
                    int(float(product_data['sale_price'])) if product_data['sale_price'] else 0,  # sale_price (숫자로 변환)
                    ""  # color
                ]
                
                new_rows.append(row)
                # print(f"✅ 신규 상품: {product_data['product_no']} - {product_data['product_name']} - {s3_url}")
            
            if new_rows:
                # 스프레드시트 맨 아래에 추가
                print(f"상품 스프레드 시트에 추가 시작: {len(new_rows)}개")
                self.google_sheet.append_values(
                    self.spreadsheet_id,
                    "Sheet1!A:K",
                    new_rows,
                    value_input_option='USER_ENTERED'
                )
                
                print(f"✅ 신규 상품 {len(new_rows)}개 추가 완료")
            
        except GoogleSheetError as e:
            print(f"❌ 신규 상품 추가 실패1: {e}")
            raise
        except Exception as e:
            print(f"❌ 신규 상품 추가 실패2: {e}")
            raise
    
    
    async def run_full_process(self) -> Dict[str, Any]:
        """전체 프로세스를 실행합니다."""
        start_time = datetime.now()
        print("🚀 [프로세스 시작] 메타 카탈로그 광고 이미지 생성 및 Google Sheets 업데이트")
        print(f"⏰ 시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # 초기화
            print("🔧 클라이언트 초기화 중...")
            await self.initialize()
            print("✅ 클라이언트 초기화 완료")
            
            # 1. 기존 스프레드시트 데이터 조회 (A)
            existing_products, existing_product_codes = await self.get_existing_products_from_sheet()
            
            # 2. 현재 활성 상품 목록 조회 (B) - 전체 상품 조회
            current_products = await self.get_active_products()
            
            if not current_products:
                print("⚠️ 현재 활성 상품이 없습니다.")
                return {
                    "status": "warning",
                    "message": "현재 활성 상품이 없습니다.",
                    "processed_count": 0,
                    "success_count": 0,
                    "error_count": 0,
                    "new_products_count": 0,
                    "existing_products_count": 0
                }
            
            # 3. 품절 상품 목록 조회 (C) - 품절 상품 조회
            out_of_stock_products = await self.get_out_of_stock_products(limit=100) 
            print(f"📦 품절 상품 {len(out_of_stock_products)}개 조회 완료")
            
            # 4. 상품 비교 (A와 B 비교)
            new_products, existing_current_products = self.compare_products(
                existing_product_codes, current_products
            )
            
            # 5. 품절 상품 상태 업데이트 (품절 상품을 'out of stock'으로 설정)
            print("🔄 [STEP 5] 품절 상품 상태 업데이트 시작")
            await self.update_existing_products_status(existing_products, out_of_stock_products)
            print("✅ [STEP 5 완료] 품절 상품 상태 업데이트 완료")
            
            # 6. 기존 상품 이미지 업데이트 건너뛰기 (신규 상품만 이미지 생성)
            print("⏭️ [STEP 6] 기존 상품 이미지 업데이트 건너뛰기 (신규 상품만 처리)")
            
            # 7. 신규 상품 처리 (메타 광고 이미지 생성)
            new_product_results = []
            if new_products:
                print(f"🖼️ [STEP 7] 신규 상품 {len(new_products)}개 메타 광고 이미지 생성 시작")
                new_product_results = await self.process_products_batch(new_products, batch_size=100)
                
                # 8. 신규 상품을 Google Sheets에 추가
                print("📝 [STEP 8] 신규 상품을 Google Sheets에 추가 중...")
                await self.add_new_products_to_sheet(new_product_results)
                print("✅ [STEP 8 완료] 신규 상품 추가 완료")
            else:
                print("📝 신규 상품이 없어 추가 작업을 건너뜁니다.")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(f"🏁 [프로세스 완료] 총 소요 시간: {duration:.2f}초")
            print(f"📊 최종 결과:")
            print(f"   🆕 신규 상품: {self.new_products_count}개")
            print(f"   🔄 기존 상품: {self.existing_products_count}개")
            print(f"   ✅ 성공: {self.success_count}개")
            print(f"   ❌ 실패: {self.error_count}개")
            
            result = {
                "status": "completed",
                "message": "메타 카탈로그 Google Sheets 업데이트 완료",
                "processed_count": self.processed_count,
                "success_count": self.success_count,
                "error_count": self.error_count,
                "new_products_count": self.new_products_count,
                "existing_products_count": self.existing_products_count,
                "spreadsheet_id": self.spreadsheet_id,
                "duration_seconds": duration,
                "errors": self.errors[:10]  # 최대 10개 오류만 포함
            }
            
            return result
            
        except Exception as e:
            print(f"❌ [프로세스 실패] 실행 실패: {e}")
            return {
                "status": "error",
                "message": f"프로세스 실행 실패: {e}",
                "processed_count": self.processed_count,
                "success_count": self.success_count,
                "error_count": self.error_count,
                "new_products_count": self.new_products_count,
                "existing_products_count": self.existing_products_count,
                "errors": self.errors
            }


# 스케줄러 작업 함수
async def meta_catalog_ad_job(spreadsheet_id: str, generate_images: bool = True):
    """메타 카탈로그 광고 이미지 생성 작업 (매일 새벽 3시 실행)"""
    try:
        print("🚀 메타 카탈로그 광고 이미지 생성 작업 시작")
        
        processor = MetaCatalogAdProcessor(spreadsheet_id, generate_images=generate_images)
        result = await processor.run_full_process()  # 전체 상품 처리
        
        print(f"✅ 메타 카탈로그 광고 이미지 생성 작업 완료: {result}")
        return result
        
    except Exception as e:
        print(f"❌ 메타 카탈로그 광고 이미지 생성 작업 실패: {e}")
        return {
            "status": "error",
            "message": f"작업 실패: {e}",
            "timestamp": datetime.now().isoformat()
        }


# 테스트용 함수
async def test_meta_catalog_process(spreadsheet_id: str, generate_images: bool = True):
    """테스트용 함수"""
    try:
        print("🧪 테스트 시작")
        
        processor = MetaCatalogAdProcessor(spreadsheet_id, generate_images=generate_images)
        result = await processor.run_full_process()  # 전체 상품 처리
        
        print(f"✅ 테스트 완료: {result}")
        return result
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    # 테스트 실행 (스프레드시트 ID 필요)
    url = "https://docs.google.com/spreadsheets/d/1fUMh5PimIjvI6_ef2VK6zQa_NC9xGvUnhkLK2qs1r5k/edit?gid=0#gid=0"
    test_spreadsheet_id = get_spreadsheet_id_from_url(url)
    
    # 이미지 생성 토글 설정 (True: 이미지 생성, False: 이미지 생성 건너뛰기)
    GENERATE_IMAGES = False  # 👈 이 변수 하나만 바꾸면 됩니다!
    
    asyncio.run(test_meta_catalog_process(test_spreadsheet_id, generate_images=GENERATE_IMAGES))
