import asyncio
import io
import json
from typing import Optional, Dict, Any, List
from PIL import Image, ImageDraw, ImageFont
import requests
import boto3
from botocore.exceptions import ClientError


class MetaAdvertiseImageOptions:
    def __init__(self, width: int = 1080, height: int = 1080, dept_image_url: str = None):
        self.width = width
        self.height = height
        self.dept_image_url = dept_image_url or "/src/assets/images/dept_logo.png"


def draw_wrapped_text(
    draw: ImageDraw.Draw,
    text: str,
    x: int,
    y: int,
    max_width: int,
    line_height: int,
    font: ImageFont.ImageFont,
    auto_wrap: bool = False
) -> int:
    """
    줄바꿈을 처리하는 텍스트 렌더링 함수
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
    
    # 각 줄을 그리기
    current_y = y
    for line in lines:
        draw.text((x, y), line, font=font)
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
        
        # S3 URL 생성
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{key}"
        return s3_url
        
    except ClientError as e:
        raise Exception(f"S3 업로드에 실패했습니다: {e}")


async def meta_advertise_image(
    item_id: int,
    image_url: str,
    brand_name_kor: str,
    product_name: str,
    sale_price: str,
    options: Optional[MetaAdvertiseImageOptions] = None,
    meta_brand_names_list: List[Dict[str, str]] = None,
    s3_bucket: str = None
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
    
    if meta_brand_names_list is None:
        meta_brand_names_list = []
    
    if s3_bucket is None:
        # 설정에서 S3 버킷명 가져오기
        from ...core.config import get_settings
        settings = get_settings()
        s3_bucket = settings.aws_s3_bucket
        
        if s3_bucket is None:
            raise ValueError("S3 버킷명이 필요합니다. AWS_S3_BUCKET 환경변수를 설정해주세요.")
    
    # 브랜드 정보 찾기
    brand = None
    for b in meta_brand_names_list:
        if b.get("brandNameKor", "").replace("\n", "") == brand_name_kor:
            brand = b
            break
    
    brand_name_kor_with_newline = brand.get("brandNameKor") if brand else brand_name_kor
    
    try:
        # 이미지 로드
        dept_img = load_image_from_url(options.dept_image_url)
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
        
        # 폰트 설정 (시스템 폰트 사용)
        try:
            brand_font = ImageFont.truetype("arial.ttf", 48)
            product_font = ImageFont.truetype("arial.ttf", 32)
            price_font = ImageFont.truetype("arial.ttf", 35)
        except OSError:
            # 기본 폰트 사용
            brand_font = ImageFont.load_default()
            product_font = ImageFont.load_default()
            price_font = ImageFont.load_default()
        
        # 텍스트 요소들을 세로로 나란히 배치하고 가운데 정렬
        text_container_width = 189  # 텍스트 컨테이너 너비
        
        # 각 텍스트 요소의 실제 높이를 동적으로 계산
        # 브랜드 한글명 높이 계산
        brand_kor_height = draw_wrapped_text(
            draw,
            brand_name_kor_with_newline or "",
            left_width // 2,
            0,  # 임시 Y값, 실제 높이만 측정
            text_container_width,
            70,
            brand_font,
            False  # 브랜드명은 기존 줄바꿈 사용
        )
        
        # 상품명 높이 계산
        product_name_height = draw_wrapped_text(
            draw,
            product_name,
            left_width // 2,
            0,  # 임시 Y값, 실제 높이만 측정
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
        
        # 브랜드 한글명
        draw.fill = "#000000"
        kor_text_height = draw_wrapped_text(
            draw,
            brand_name_kor_with_newline or "",
            left_width // 2,  # 가운데 정렬을 위해 leftWidth의 중앙 사용
            text_group_start_y,
            text_container_width,
            70,
            brand_font,
            False  # 브랜드명은 기존 줄바꿈 사용
        )
        
        # 상품명 (자동 줄바꿈 적용)
        draw.fill = "#9C9C9C"
        product_text_height = draw_wrapped_text(
            draw,
            product_name,
            left_width // 2,  # 가운데 정렬을 위해 leftWidth의 중앙 사용
            text_group_start_y + kor_text_height + brand_to_product_gap,
            text_container_width,
            40,
            product_font,
            True  # 상품명은 자동 줄바꿈 사용
        )
        
        # 가격
        draw.fill = "#000000"
        price_text = f"{int(float(sale_price)):,} ₩"
        
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
        
        draw.text((price_x, price_y), price_text, font=price_font)
        
        # 오른쪽 이미지를 전체 높이에 맞춰서 배치
        right_height = options.height  # 전체 높이 사용
        right_y = 0  # 맨 위부터 시작
        
        # 오른쪽 이미지 리사이즈 및 배치
        right_img_resized = right_img.resize((right_width, right_height), Image.Resampling.LANCZOS)
        canvas.paste(right_img_resized, (left_width, right_y))
        
        # S3에 업로드
        s3_key = f"meta_image/{item_id}.jpg"
        s3_url = upload_to_s3(s3_bucket, s3_key, canvas)
        
        return s3_url
        
    except Exception as e:
        raise Exception(f"이미지 생성 중 오류가 발생했습니다: {e}")


# 사용 예시
if __name__ == "__main__":
    # 브랜드명 리스트 예시
    meta_brand_names_list = [
        {"brandNameKor": "브랜드\n명"},
        {"brandNameKor": "다른브랜드"}
    ]
    
    # 비동기 함수 실행 예시
    async def main():
        try:
            result = await meta_advertise_image(
                item_id=12345,
                image_url="https://example.com/product.jpg",
                brand_name_kor="브랜드명",
                product_name="상품명입니다",
                sale_price="50000",
                s3_bucket="your-bucket-name"
            )
            print(f"생성된 이미지 URL: {result}")
        except Exception as e:
            print(f"오류 발생: {e}")
    
    # asyncio.run(main())
