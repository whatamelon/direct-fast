"""
이미지 압축 유틸리티 모듈

Pillow를 사용하여 이미지를 압축하는 다양한 함수들을 제공합니다.
"""

import os
from pathlib import Path
from typing import Optional, Tuple, Union
from PIL import Image, ImageOps
import logging

logger = logging.getLogger(__name__)


def compress_image(
    input_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    quality: int = 85,
    max_width: Optional[int] = None,
    max_height: Optional[int] = None,
    format: str = "JPEG",
    optimize: bool = True
) -> str:
    """
    이미지를 압축하여 저장합니다.
    
    Args:
        input_path: 입력 이미지 파일 경로
        output_path: 출력 이미지 파일 경로 (None이면 원본 파일명에 _compressed 추가)
        quality: 압축 품질 (1-100, 기본값: 85)
        max_width: 최대 너비 (픽셀)
        max_height: 최대 높이 (픽셀)
        format: 출력 포맷 (JPEG, PNG, WebP 등)
        optimize: 최적화 여부
        
    Returns:
        str: 압축된 이미지 파일 경로
        
    Raises:
        FileNotFoundError: 입력 파일이 존재하지 않는 경우
        ValueError: 잘못된 매개변수 값
        Exception: 이미지 처리 중 오류 발생
    """
    input_path = Path(input_path)
    
    if not input_path.exists():
        raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {input_path}")
    
    if not (1 <= quality <= 100):
        raise ValueError("품질은 1-100 사이의 값이어야 합니다")
    
    # 출력 경로 설정
    if output_path is None:
        stem = input_path.stem
        suffix = input_path.suffix
        output_path = input_path.parent / f"{stem}_compressed{suffix}"
    else:
        output_path = Path(output_path)
    
    try:
        # 이미지 열기
        with Image.open(input_path) as img:
            # RGB 모드로 변환 (JPEG는 RGB만 지원)
            if format.upper() == "JPEG" and img.mode in ("RGBA", "LA", "P"):
                # 투명도가 있는 이미지는 흰색 배경으로 변환
                background = Image.new("RGB", img.size, (255, 255, 255))
                if img.mode == "P":
                    img = img.convert("RGBA")
                background.paste(img, mask=img.split()[-1] if img.mode == "RGBA" else None)
                img = background
            elif img.mode != "RGB":
                img = img.convert("RGB")
            
            # 크기 조정
            if max_width or max_height:
                img = resize_image(img, max_width, max_height)
            
            # 메타데이터 제거 (EXIF 데이터 등)
            img = ImageOps.exif_transpose(img)
            
            # 압축 옵션 설정
            save_kwargs = {
                "format": format.upper(),
                "quality": quality,
                "optimize": optimize
            }
            
            # 포맷별 추가 옵션
            if format.upper() == "JPEG":
                save_kwargs["progressive"] = True
            elif format.upper() == "PNG":
                save_kwargs["compress_level"] = 6
            elif format.upper() == "WEBP":
                save_kwargs["method"] = 6  # 최고 압축률
            
            # 이미지 저장
            img.save(output_path, **save_kwargs)
            
            # 파일 크기 정보 로깅
            original_size = input_path.stat().st_size
            compressed_size = output_path.stat().st_size
            compression_ratio = (1 - compressed_size / original_size) * 100
            
            logger.info(
                f"이미지 압축 완료: {input_path.name} -> {output_path.name} "
                f"(원본: {original_size:,} bytes, 압축: {compressed_size:,} bytes, "
                f"압축률: {compression_ratio:.1f}%)"
            )
            
            return str(output_path)
            
    except Exception as e:
        logger.error(f"이미지 압축 중 오류 발생: {e}")
        raise


def resize_image(
    img: Image.Image,
    max_width: Optional[int] = None,
    max_height: Optional[int] = None,
    maintain_aspect_ratio: bool = True
) -> Image.Image:
    """
    이미지 크기를 조정합니다.
    
    Args:
        img: PIL Image 객체
        max_width: 최대 너비
        max_height: 최대 높이
        maintain_aspect_ratio: 종횡비 유지 여부
        
    Returns:
        Image.Image: 크기가 조정된 이미지
    """
    if not max_width and not max_height:
        return img
    
    original_width, original_height = img.size
    
    if maintain_aspect_ratio:
        # 종횡비를 유지하면서 크기 조정
        if max_width and max_height:
            # 두 제한 모두 있는 경우, 더 작은 비율로 조정
            width_ratio = max_width / original_width
            height_ratio = max_height / original_height
            ratio = min(width_ratio, height_ratio)
        elif max_width:
            ratio = max_width / original_width
        else:  # max_height
            ratio = max_height / original_height
        
        new_width = int(original_width * ratio)
        new_height = int(original_height * ratio)
    else:
        # 종횡비 무시하고 지정된 크기로 조정
        new_width = max_width or original_width
        new_height = max_height or original_height
    
    return img.resize((new_width, new_height), Image.Resampling.LANCZOS)


def get_image_info(image_path: Union[str, Path]) -> dict:
    """
    이미지 파일의 정보를 반환합니다.
    
    Args:
        image_path: 이미지 파일 경로
        
    Returns:
        dict: 이미지 정보 (크기, 포맷, 모드, 파일 크기 등)
    """
    image_path = Path(image_path)
    
    if not image_path.exists():
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {image_path}")
    
    with Image.open(image_path) as img:
        file_size = image_path.stat().st_size
        
        return {
            "filename": image_path.name,
            "format": img.format,
            "mode": img.mode,
            "size": img.size,
            "width": img.width,
            "height": img.height,
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2)
        }


def batch_compress_images(
    input_dir: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    quality: int = 85,
    max_width: Optional[int] = None,
    max_height: Optional[int] = None,
    format: str = "JPEG",
    supported_formats: Tuple[str, ...] = (".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff")
) -> list:
    """
    디렉토리 내의 모든 이미지를 일괄 압축합니다.
    
    Args:
        input_dir: 입력 디렉토리 경로
        output_dir: 출력 디렉토리 경로 (None이면 입력 디렉토리와 동일)
        quality: 압축 품질
        max_width: 최대 너비
        max_height: 최대 높이
        format: 출력 포맷
        supported_formats: 지원하는 파일 확장자
        
    Returns:
        list: 압축된 파일 경로 목록
    """
    input_dir = Path(input_dir)
    
    if not input_dir.exists():
        raise FileNotFoundError(f"입력 디렉토리를 찾을 수 없습니다: {input_dir}")
    
    if output_dir is None:
        output_dir = input_dir
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    compressed_files = []
    
    # 지원하는 이미지 파일 찾기
    image_files = []
    for ext in supported_formats:
        image_files.extend(input_dir.glob(f"*{ext}"))
        image_files.extend(input_dir.glob(f"*{ext.upper()}"))
    
    logger.info(f"총 {len(image_files)}개의 이미지 파일을 찾았습니다.")
    
    for image_file in image_files:
        try:
            # 출력 파일명 생성
            output_file = output_dir / f"{image_file.stem}_compressed{image_file.suffix}"
            
            # 이미지 압축
            compressed_path = compress_image(
                input_path=image_file,
                output_path=output_file,
                quality=quality,
                max_width=max_width,
                max_height=max_height,
                format=format
            )
            
            compressed_files.append(compressed_path)
            
        except Exception as e:
            logger.error(f"파일 압축 실패 {image_file.name}: {e}")
            continue
    
    logger.info(f"일괄 압축 완료: {len(compressed_files)}개 파일 처리됨")
    return compressed_files


# 사용 예제
if __name__ == "__main__":
    # 단일 파일 압축 예제
    try:
        compressed_path = compress_image(
            input_path="example.jpg",
            quality=80,
            max_width=1920,
            max_height=1080
        )
        print(f"압축 완료: {compressed_path}")
        
        # 이미지 정보 출력
        info = get_image_info(compressed_path)
        print(f"이미지 정보: {info}")
        
    except Exception as e:
        print(f"오류 발생: {e}")
