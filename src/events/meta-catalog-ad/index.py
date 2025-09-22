import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Set, Tuple

from ...interfaces.cafe24 import create_cafe24_client, Cafe24APIError
from ...interfaces.google_sheet import create_google_sheet_interface, GoogleSheetError, get_spreadsheet_id_from_url
from ...utils.meta.meta_advertise_image import meta_advertise_image, MetaAdvertiseImageOptions
from ...core.config import get_settings
from ...assets.files.brand_names import brandNames

# 로거 설정
logger = logging.getLogger(__name__)

class MetaCatalogAdProcessor:
    """메타 카탈로그 광고 이미지 생성 프로세서"""
    
    def __init__(self, spreadsheet_id: str):
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
                
                product_data = {
                    'idx': row_idx + 2,
                    'product_no': row[0] if len(row) > 0 else '',
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
                existing_product_codes.add(row[0])  # 상품 ID 수집
            
            print(f"✅ [STEP 1 완료] 기존 상품 {len(existing_products)}개 조회 완료")
            print(f"🔢 고유 상품 코드 수: {len(existing_product_codes)}개")
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
            
            # 현재 상품 코드 집합 생성
            current_product_codes = {product['product_no'] for product in current_products}
            
            print(f"📊 현재 상품 코드 수: {len(current_product_codes)}개")
            print(f"📊 기존 상품 코드 수: {len(existing_product_codes)}개")
            
            # A에 없는데 B에 있는 상품들 (신규 상품)
            new_product_codes = current_product_codes - existing_product_codes
            new_products = [product for product in current_products 
                           if product['product_no'] in new_product_codes]
            
            # A에 있고 B에도 있는 상품들 (기존 상품)
            existing_product_codes_in_current = current_product_codes & existing_product_codes
            existing_products = [product for product in current_products 
                               if product['product_no'] in existing_product_codes_in_current]
            
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

            print(f"")
            print(f"----------------------------------------------------------------------------------")
            print(f"")
            print(f"product_code: {product_code}")
            print(f"price: {price}")
            print(f"retail_price: {retail_price}")
            
            # 판매가 우선, 없으면 소비자가, 없으면 공급가
            sale_price = price
            
            # 브랜드명 (상품명에서 추출하거나 기본값 사용)
            brand = self.extract_brand_from_name(product_name)
            print(f"brand: {brand}")
            
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
        
        # 브랜드명 리스트에서 매칭되는 브랜드 찾기
        for brand in brandNames:
            brand_name_kor = brand.get('brandNameKor', '').replace('\n', ' ').strip()
            brand_name_eng = brand.get('brandNameEng', '').replace('\n', ' ').strip()
            
            # 상품명에 브랜드명이 포함되어 있는지 확인 (대소문자 구분 없이)
            if (brand_name_kor and brand_name_kor.lower() in product_name.lower()) or \
               (brand_name_eng and brand_name_eng.lower() in product_name.lower()):
                print(f"🎯 브랜드 매칭 성공: {product_name} → {brand_name_kor}")
                return brand_name_kor
        
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
                print(f"🖼️ 이미지 URL 추출 성공: {image_url}")
                return image_url
            
            # 이미지가 없으면 기본 이미지 사용
            print("⚠️ 이미지 URL을 찾을 수 없음 - 기본 이미지 사용")
            return "https://via.placeholder.com/400x400?text=No+Image"
            
        except Exception as e:
            print(f"❌ 이미지 URL 추출 실패: {e}")
            return "https://via.placeholder.com/400x400?text=No+Image"
    
    async def process_single_product(self, product_data: Dict[str, Any]) -> Optional[str]:
        """단일 상품의 메타 광고 이미지를 생성합니다."""
        try:
            print(f"🖼️ 상품 처리 시작: {product_data['product_name']}")
            
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
                custom_product_code=product_data.get('custom_product_code', '')
            )
            
            print(f"✅ 메타 광고 이미지 생성 완료: {s3_url}")
            return s3_url
            
        except Exception as e:
            error_msg = f"상품 처리 실패 ({product_data['product_name']}): {e}"
            print(f"❌ {error_msg}")
            self.errors.append(error_msg)
            return None
    
    async def update_existing_products_status(self, existing_products: List[Dict[str, Any]], 
                                            current_products: List[Dict[str, Any]]) -> None:
        """
        기존 상품들의 상태를 업데이트합니다.
        soldout 상태에 따라 availability를 'in stock' 또는 'out of stock'으로 설정합니다.
        
        Args:
            existing_products: 스프레드시트의 기존 상품 데이터 리스트
            current_products: 현재 카페24에서 조회한 상품 리스트
        """
        try:
            print(f"📊 기존 상품 {len(existing_products)}개 상태 확인 중...")
            
            # 현재 상품을 product_no로 인덱싱
            current_products_dict = {product['product_no']: product for product in current_products}
            
            # 업데이트할 데이터 준비
            update_data = []
            updated_count = 0
            
            for sheet_product in existing_products:
                product_no = sheet_product['product_no']
                
                # 현재 상품 목록에 있는지 확인
                if product_no in current_products_dict:
                    current_product = current_products_dict[product_no]
                    row_index = sheet_product['idx']
                    
                    # soldout 상태 확인 (T = 품절, F = 판매중)
                    soldout = current_product.get('soldout', 'F')
                    availability = 'out of stock' if soldout == 'T' else 'in stock'
                    
                    # 현재 availability와 다른 경우에만 업데이트
                    if sheet_product['availability'] != availability:
                        update_data.append({
                            'range': f'Sheet1!D{row_index}',  # availability 컬럼 (D)
                            'values': [[availability]]
                        })
                        updated_count += 1
                        print(f"🔄 상품 {product_no} 상태 업데이트: {sheet_product['availability']} → {availability}")
            
            if update_data:
                # 배치 업데이트 실행
                self.google_sheet.write_multiple_ranges(
                    self.spreadsheet_id, 
                    update_data, 
                    value_input_option='USER_ENTERED'
                )
                
                print(f"✅ 기존 상품 상태 업데이트 완료 - {len(update_data)}개 상품")
            else:
                print("✅ 업데이트할 기존 상품이 없습니다.")
            
        except GoogleSheetError as e:
            print(f"❌ 기존 상품 상태 업데이트 실패: {e}")
            raise
        except Exception as e:
            print(f"❌ 기존 상품 상태 업데이트 실패: {e}")
            raise
    
    
    async def process_products_batch(self, products: List[Dict[str, Any]], batch_size: int = 10) -> List[Dict[str, Any]]:
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
                        print(f"✅ 상품 처리 성공: {product_data['product_name']}")
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
            
            # 배치 간 잠시 대기 (API 부하 방지)
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
                
                # 새 행 생성
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
                
                new_rows.append(row)
            
            if new_rows:
                # 스프레드시트 맨 아래에 추가
                self.google_sheet.append_values(
                    self.spreadsheet_id,
                    "Sheet1!A:K",
                    new_rows,
                    value_input_option='USER_ENTERED'
                )
                
                print(f"✅ 신규 상품 {len(new_rows)}개 추가 완료")
            
        except GoogleSheetError as e:
            print(f"❌ 신규 상품 추가 실패: {e}")
            raise
        except Exception as e:
            print(f"❌ 신규 상품 추가 실패: {e}")
            raise
    
    
    async def run_full_process(self, limit: int = 1000) -> Dict[str, Any]:
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
            
            # 3. 상품 비교 (A와 B 비교)
            new_products, existing_current_products = self.compare_products(
                existing_product_codes, current_products
            )
            
            # 4. 기존 상품 상태 업데이트 (soldout 상태에 따라)
            print("🔄 [STEP 4] 기존 상품 상태 업데이트 시작")
            await self.update_existing_products_status(existing_products, existing_current_products)
            print("✅ [STEP 4 완료] 기존 상품 상태 업데이트 완료")
            
            # 5. 신규 상품 처리 (메타 광고 이미지 생성)
            new_product_results = []
            if new_products:
                print(f"🖼️ [STEP 5] 신규 상품 {len(new_products)}개 메타 광고 이미지 생성 시작")
                new_product_results = await self.process_products_batch(new_products, batch_size=10)
                
                # 6. 신규 상품을 Google Sheets에 추가
                print("📝 [STEP 6] 신규 상품을 Google Sheets에 추가 중...")
                await self.add_new_products_to_sheet(new_product_results)
                print("✅ [STEP 6 완료] 신규 상품 추가 완료")
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
async def meta_catalog_ad_job(spreadsheet_id: str):
    """메타 카탈로그 광고 이미지 생성 작업 (매일 새벽 3시 실행)"""
    try:
        print("🚀 메타 카탈로그 광고 이미지 생성 작업 시작")
        
        processor = MetaCatalogAdProcessor(spreadsheet_id)
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
async def test_meta_catalog_process(spreadsheet_id: str):
    """테스트용 함수"""
    try:
        print("🧪 테스트 시작")
        
        processor = MetaCatalogAdProcessor(spreadsheet_id)
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
    asyncio.run(test_meta_catalog_process(test_spreadsheet_id))
