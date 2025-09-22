import asyncio
import io
import json
import os
from typing import Optional, Dict, Any, List
from PIL import Image, ImageDraw, ImageFont
import requests
import boto3
from botocore.exceptions import ClientError


class MetaAdvertiseImageOptions:
    def __init__(self, width: int = 1080, height: int = 1080, dept_image_url: str = None):
        self.width = width
        self.height = height
        self.dept_image_url = dept_image_url or "./src/assets/images/dept_logo.png"


def calculate_text_height(
    text: str,
    max_width: int,
    line_height: int,
    font: ImageFont.ImageFont,
    auto_wrap: bool = False
) -> int:
    """
    텍스트의 높이를 계산합니다 (실제로 그리지는 않음)
    """
    lines = []
    
    if auto_wrap:
        # 상품명용: 189px 기준으로 자동 줄바꿈
        words = text.split(" ")
        current_line = words[0] if words else ""
        
        for i in range(1, len(words)):
            word = words[i]
            test_line = current_line + " " + word
            
            # 텍스트 크기 측정 (임시 draw 객체 사용)
            temp_img = Image.new('RGB', (1, 1))
            temp_draw = ImageDraw.Draw(temp_img)
            bbox = temp_draw.textbbox((0, 0), test_line, font=font)
            text_width = bbox[2] - bbox[0]
            
            if text_width > max_width:
                lines.append(current_line)
                current_line = word
            else:
                current_line = test_line
        
        lines.append(current_line)
    else:
        # 브랜드명용: 기존 \n 기준 줄바꿈
        lines = text.split("\n")
    
    # 빈 줄이 아닌 줄의 개수만 계산
    valid_lines = [line for line in lines if line.strip()]
    return len(valid_lines) * line_height


def draw_wrapped_text(
    draw: ImageDraw.Draw,
    text: str,
    center_x: int,
    y: int,
    max_width: int,
    line_height: int,
    font: ImageFont.ImageFont,
    auto_wrap: bool = False,
    fill_color: str = "#000000"
) -> int:
    """
    줄바꿈을 처리하는 텍스트 렌더링 함수 (가운데 정렬)
    """
    lines = []
    
    if auto_wrap:
        # 상품명용: 189px 기준으로 자동 줄바꿈
        words = text.split(" ")
        current_line = words[0] if words else ""
        
        for i in range(1, len(words)):
            word = words[i]
            test_line = current_line + " " + word
            
            # 텍스트 크기 측정
            bbox = draw.textbbox((0, 0), test_line, font=font)
            text_width = bbox[2] - bbox[0]
            
            if text_width > max_width:
                lines.append(current_line)
                current_line = word
            else:
                current_line = test_line
        
        lines.append(current_line)
    else:
        # 브랜드명용: 기존 \n 기준 줄바꿈
        lines = text.split("\n")
    
    # 각 줄을 가운데 정렬하여 그리기
    current_y = y
    for line in lines:
        if line.strip():  # 빈 줄이 아닌 경우만 그리기
            # 텍스트 크기 측정
            bbox = draw.textbbox((0, 0), line, font=font)
            text_width = bbox[2] - bbox[0]
            
            # 가운데 정렬을 위한 x 좌표 계산
            text_x = center_x - (text_width // 2)
            
            # 텍스트 그리기
            draw.text((text_x, current_y), line, font=font, fill=fill_color)
            print(f"텍스트 그리기: '{line}' at ({text_x}, {current_y}) with color {fill_color}")
        
        current_y += line_height
    
    return current_y - y  # 실제 사용된 높이 반환


def load_image_from_url(url: str) -> Image.Image:
    """URL에서 이미지를 로드합니다."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return Image.open(io.BytesIO(response.content))
    except Exception as e:
        raise Exception(f"이미지를 로드할 수 없습니다: {e}")


def upload_to_s3(bucket_name: str, key: str, image: Image.Image) -> str:
    """S3에 이미지를 업로드합니다."""
    try:
        from ...core.config import get_settings
        settings = get_settings()
        
        # AWS 자격 증명 설정
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region
        )
        
        # 이미지를 바이트로 변환
        img_buffer = io.BytesIO()
        image.save(img_buffer, format='JPEG', quality=95)
        img_buffer.seek(0)
        
        # S3에 업로드
        s3_client.upload_fileobj(
            img_buffer,
            bucket_name,
            key,
            ExtraArgs={'ContentType': 'image/jpeg'}
        )
        
        # S3 URL 생성 (올바른 형식)
        s3_url = f"https://s3.ap-northeast-2.amazonaws.com/{bucket_name}/{key}"
        print(f"bucket_name: {bucket_name}")
        print(f"key: {key}")
        print(f"s3_url: {s3_url}")
        return s3_url
        
    except ClientError as e:
        raise Exception(f"S3 업로드에 실패했습니다: {e}")


async def meta_advertise_image(
    item_id: int,
    image_url: str,
    product_name: str,
    sale_price: str,
    options: Optional[MetaAdvertiseImageOptions] = None,
    s3_bucket: str = None,
    brandNameKor: str = None,
    custom_product_code: str = None
) -> str:
    """
    메타 광고 이미지를 생성합니다.
    
    Args:
        item_id: 상품 ID
        image_url: 상품 이미지 URL
        brand_name_kor: 브랜드 한글명
        product_name: 상품명
        sale_price: 판매가격
        options: 이미지 옵션
        meta_brand_names_list: 브랜드명 리스트
        s3_bucket: S3 버킷명
    
    Returns:
        S3에 업로드된 이미지 URL
    """
    if options is None:
        options = MetaAdvertiseImageOptions()
    
    if s3_bucket is None:
        # 설정에서 S3 버킷명 가져오기
        from ...core.config import get_settings
        settings = get_settings()
        s3_bucket = settings.aws_s3_bucket
        
        if s3_bucket is None:
            raise ValueError("S3 버킷명이 필요합니다. AWS_S3_BUCKET 환경변수를 설정해주세요.")
    
    try:
        # 이미지 로드
        try:
            dept_img = Image.open(options.dept_image_url)
            # DEPT 이미지가 RGBA인 경우 RGB로 변환
            if dept_img.mode == 'RGBA':
                # 흰색 배경으로 변환
                background = Image.new('RGB', dept_img.size, (255, 255, 255))
                background.paste(dept_img, mask=dept_img.split()[-1])  # 알파 채널을 마스크로 사용
                dept_img = background
            elif dept_img.mode != 'RGB':
                dept_img = dept_img.convert('RGB')
            print(f"DEPT 이미지 로드 성공: {dept_img.size}, 모드: {dept_img.mode}")
        except Exception as e:
            print(f"DEPT 이미지 로드 실패: {e}")
            # 기본 DEPT 이미지 생성 (흰색 배경에 검은색 텍스트)
            dept_img = Image.new('RGB', (120, 40), (255, 255, 255))
            dept_draw = ImageDraw.Draw(dept_img)
            dept_draw.text((10, 10), "DEPT", fill=(0, 0, 0))
        
        right_img = load_image_from_url(image_url)
        
        # 캔버스 생성
        canvas = Image.new('RGB', (options.width, options.height), '#FFFFFF')
        draw = ImageDraw.Draw(canvas)
        
        # 1:1 캔버스를 좌우로 분할
        left_width = 277  # 왼쪽 영역 (277px)
        right_width = 803  # 오른쪽 영역 (803px)
        
        # DEPT 이미지를 왼쪽 상단에 배치 (top: 80px, left: 62px)
        max_dept_size = 120  # 최대 크기
        dept_aspect_ratio = dept_img.width / dept_img.height
        
        if dept_aspect_ratio > 1:
            # 가로가 더 긴 경우
            dept_width = min(max_dept_size, dept_img.width)
            dept_height = int(dept_width / dept_aspect_ratio)
        else:
            # 세로가 더 긴 경우
            dept_height = min(max_dept_size, dept_img.height)
            dept_width = int(dept_height * dept_aspect_ratio)
        
        dept_x = 62
        dept_y = 80  # top: 80px
        
        # DEPT 이미지 리사이즈 및 배치
        dept_img_resized = dept_img.resize((dept_width, dept_height), Image.Resampling.LANCZOS)
        canvas.paste(dept_img_resized, (dept_x, dept_y))
        
        # 폰트 설정 (프로젝트 내 Pretendard TTF 파일들 사용)
        brand_font = None
        product_font = None
        price_font = None
        
        try:
            # 프로젝트 내 Pretendard TTF 파일들 경로
            font_dir = "./src/assets/fonts/pretendard/"
            
            # 각 텍스트에 맞는 폰트 weight 설정
            # 브랜드명: Bold (48px)
            brand_font_path = os.path.join(font_dir, "Pretendard-Bold.ttf")
            # 상품명: Regular (32px)
            product_font_path = os.path.join(font_dir, "Pretendard-Regular.ttf")
            # 가격: Bold (35px)
            price_font_path = os.path.join(font_dir, "Pretendard-Bold.ttf")
            
            # 폰트 로드 시도
            if os.path.exists(brand_font_path):
                brand_font = ImageFont.truetype(brand_font_path, 48)
                print(f"브랜드 폰트 로드 성공: Pretendard-Bold.ttf (48px)")
            else:
                print(f"브랜드 폰트 파일을 찾을 수 없습니다: {brand_font_path}")
            
            if os.path.exists(product_font_path):
                product_font = ImageFont.truetype(product_font_path, 32)
                print(f"상품명 폰트 로드 성공: Pretendard-Regular.ttf (32px)")
            else:
                print(f"상품명 폰트 파일을 찾을 수 없습니다: {product_font_path}")
            
            if os.path.exists(price_font_path):
                price_font = ImageFont.truetype(price_font_path, 35)
                print(f"가격 폰트 로드 성공: Pretendard-Bold.ttf (35px)")
            else:
                print(f"가격 폰트 파일을 찾을 수 없습니다: {price_font_path}")
            
            # 폰트 로드 실패 시 대체 폰트 시도
            if brand_font is None or product_font is None or price_font is None:
                print("일부 폰트 로드 실패 - 대체 폰트 시도 중...")
                
                # 시스템 폰트 경로들
                system_font_paths = [
                    "/System/Library/Fonts/Pretendard-Regular.ttf",
                    "/System/Library/Fonts/Pretendard-Bold.ttf",
                    "/Library/Fonts/Pretendard-Regular.ttf",
                    "/Library/Fonts/Pretendard-Bold.ttf",
                    "/System/Library/Fonts/AppleSDGothicNeo.ttc",
                    "/System/Library/Fonts/Helvetica.ttc",
                    "/System/Library/Fonts/Arial.ttf",
                ]
                
                # 대체 폰트 로드
                bold_font_path = None
                regular_font_path = None
                
                for font_path in system_font_paths:
                    try:
                        if os.path.exists(font_path):
                            if "Bold" in font_path or "bold" in font_path:
                                if bold_font_path is None:
                                    bold_font_path = font_path
                            else:
                                if regular_font_path is None:
                                    regular_font_path = font_path
                    except:
                        continue
                
                # 대체 폰트 적용
                if brand_font is None and bold_font_path:
                    brand_font = ImageFont.truetype(bold_font_path, 48)
                    print(f"브랜드 대체 폰트 로드: {bold_font_path}")
                
                if product_font is None and regular_font_path:
                    product_font = ImageFont.truetype(regular_font_path, 32)
                    print(f"상품명 대체 폰트 로드: {regular_font_path}")
                
                if price_font is None and bold_font_path:
                    price_font = ImageFont.truetype(bold_font_path, 35)
                    print(f"가격 대체 폰트 로드: {bold_font_path}")
            
            # 모든 폰트 로드 실패 시 기본 폰트 사용
            if brand_font is None:
                print("브랜드 폰트 로드 실패 - 기본 폰트 사용")
                brand_font = ImageFont.load_default()
            
            if product_font is None:
                print("상품명 폰트 로드 실패 - 기본 폰트 사용")
                product_font = ImageFont.load_default()
            
            if price_font is None:
                print("가격 폰트 로드 실패 - 기본 폰트 사용")
                price_font = ImageFont.load_default()
                
        except Exception as e:
            print(f"폰트 로드 중 오류 발생: {e}, 기본 폰트 사용")
            brand_font = ImageFont.load_default()
            product_font = ImageFont.load_default()
            price_font = ImageFont.load_default()
        
        # 텍스트 요소들을 세로로 나란히 배치하고 가운데 정렬
        text_container_width = 189  # 텍스트 컨테이너 너비
        
        # 각 텍스트 요소의 실제 높이를 동적으로 계산 (그리지 않고 높이만 측정)
        # 브랜드 한글명 높이 계산
        brand_kor_height = calculate_text_height(
            brandNameKor or "",
            text_container_width,
            70,
            brand_font,
            False  # 브랜드명은 기존 줄바꿈 사용
        )
        
        # 상품명 높이 계산
        product_name_height = calculate_text_height(
            product_name or "",
            text_container_width,
            40,
            product_font,
            True  # 상품명은 자동 줄바꿈 사용
        )
        
        # 가격 높이 계산 (가격은 한 줄이므로 폰트 크기 + 여유 공간)
        price_height = 35 + 5  # 폰트 크기 + 5px 여유
        
        # 고정된 간격 사용
        brand_to_product_gap = 12  # 브랜드 한글명과 상품명 간격 (고정)
        product_to_price_gap = 40  # 상품명과 가격 간격 (고정)
        
        # 전체 텍스트 그룹의 총 높이 계산
        total_text_group_height = (
            brand_kor_height +
            brand_to_product_gap +
            product_name_height +
            product_to_price_gap +
            price_height
        )
        
        # 텍스트 그룹의 시작 Y 좌표 계산 (전체 높이 중앙에 오도록)
        text_group_start_y = (options.height - total_text_group_height) // 2
        
        # 브랜드 한글명 (굵은 글씨, 검은색)
        print(f"브랜드명 렌더링: {brandNameKor}")
        kor_text_height = draw_wrapped_text(
            draw,
            brandNameKor or "",
            left_width // 2,  # 가운데 정렬을 위해 leftWidth의 중앙 사용
            text_group_start_y,
            text_container_width,
            70,
            brand_font,
            False,  # 브랜드명은 기존 줄바꿈 사용
            "#000000"  # 검은색
        )
        
        # 상품명 (일반 글씨, 회색, 자동 줄바꿈 적용)
        print(f"상품명 렌더링: {product_name}")
        product_text_height = draw_wrapped_text(
            draw,
            product_name or "",
            left_width // 2,  # 가운데 정렬을 위해 leftWidth의 중앙 사용
            text_group_start_y + kor_text_height + brand_to_product_gap,
            text_container_width,
            40,
            product_font,
            True,  # 상품명은 자동 줄바꿈 사용
            "#9C9C9C"  # 회색
        )
        
        # 가격 (굵은 글씨, 검은색)
        price_text = f"{int(float(sale_price)):,} ₩"
        print(f"가격 렌더링: {price_text}")
        
        # 가격 텍스트의 바운딩 박스 계산
        bbox = draw.textbbox((0, 0), price_text, font=price_font)
        text_width = bbox[2] - bbox[0]
        
        # 가격 텍스트 그리기 (가운데 정렬)
        price_x = left_width // 2 - text_width // 2
        price_y = (
            text_group_start_y +
            kor_text_height +
            brand_to_product_gap +
            product_text_height +
            product_to_price_gap
        )
        
        draw.text((price_x, price_y), price_text, font=price_font, fill="#000000")
        
        # 오른쪽 이미지를 전체 높이에 맞춰서 배치
        right_height = options.height  # 전체 높이 사용
        right_y = 0  # 맨 위부터 시작
        
        # 오른쪽 이미지 리사이즈 및 배치
        right_img_resized = right_img.resize((right_width, right_height), Image.Resampling.LANCZOS)
        canvas.paste(right_img_resized, (left_width, right_y))
        
        # S3에 업로드 (custom_product_code 사용)
        filename = custom_product_code if custom_product_code else str(item_id)
        s3_key = f"meta_image/250922/{filename}.jpg"
        s3_url = upload_to_s3(s3_bucket, s3_key, canvas)
        
        return s3_url
        
    except Exception as e:
        raise Exception(f"이미지 생성 중 오류가 발생했습니다: {e}")


# 사용 예시
if __name__ == "__main__":
    
    # 비동기 함수 실행 예시
    async def main():
        try:
            result = await meta_advertise_image(
                item_id=12345,
                image_url="https://example.com/product.jpg",
                product_name="상품명입니다",
                sale_price="50000",
                s3_bucket="your-bucket-name",
                brandNameKor="브랜드명"
            )
            print(f"생성된 이미지 URL: {result}")
        except Exception as e:
            print(f"오류 발생: {e}")
    
    # asyncio.run(main())
