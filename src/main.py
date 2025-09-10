from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
from core.config import settings, validate_required_settings

# FastAPI 애플리케이션 인스턴스 생성
app = FastAPI(
    title=settings.app_name,
    description="FastAPI를 사용한 백엔드 서버",
    version=settings.app_version,
    debug=settings.debug
)

# Pydantic 모델 정의
class Item(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    price: float
    is_available: bool = True

class ItemCreate(BaseModel):
    name: str
    description: Optional[str] = None
    price: float
    is_available: bool = True

# 인메모리 데이터 저장소
items_db = []
next_id = 1

# 루트 엔드포인트
@app.get("/")
async def root():
    return {
        "message": f"{settings.app_name}에 오신 것을 환영합니다!", 
        "version": settings.app_version,
        "debug": settings.debug
    }

# 헬스 체크 엔드포인트
@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "서버가 정상적으로 작동 중입니다"}

# 환경변수 정보 조회 엔드포인트 (개발용)
@app.get("/config")
async def get_config():
    """현재 설정된 환경변수 정보를 조회합니다 (개발/디버그용)"""
    return {
        "app_name": settings.app_name,
        "app_version": settings.app_version,
        "debug": settings.debug,
        "host": settings.host,
        "port": settings.port,
        "log_level": settings.log_level,
        "database_configured": bool(settings.database_url),
        "api_keys_configured": {
            "cafe24": bool(settings.cafe24_api_key),
            "gemini": bool(settings.gemini_api_key),
            "bfl": bool(settings.bfl_api_key),
            "aws": bool(settings.aws_access_key_id and settings.aws_secret_access_key)
        }
    }

# 모든 아이템 조회
@app.get("/items", response_model=List[Item])
async def get_items():
    return items_db

# 특정 아이템 조회
@app.get("/items/{item_id}", response_model=Item)
async def get_item(item_id: int):
    for item in items_db:
        if item.id == item_id:
            return item
    raise HTTPException(status_code=404, detail="아이템을 찾을 수 없습니다")

# 새 아이템 생성
@app.post("/items", response_model=Item)
async def create_item(item: ItemCreate):
    global next_id
    new_item = Item(
        id=next_id,
        name=item.name,
        description=item.description,
        price=item.price,
        is_available=item.is_available
    )
    items_db.append(new_item)
    next_id += 1
    return new_item

# 아이템 업데이트
@app.put("/items/{item_id}", response_model=Item)
async def update_item(item_id: int, item: ItemCreate):
    for i, existing_item in enumerate(items_db):
        if existing_item.id == item_id:
            updated_item = Item(
                id=item_id,
                name=item.name,
                description=item.description,
                price=item.price,
                is_available=item.is_available
            )
            items_db[i] = updated_item
            return updated_item
    raise HTTPException(status_code=404, detail="아이템을 찾을 수 없습니다")

# 아이템 삭제
@app.delete("/items/{item_id}")
async def delete_item(item_id: int):
    for i, item in enumerate(items_db):
        if item.id == item_id:
            deleted_item = items_db.pop(i)
            return {"message": f"아이템 '{deleted_item.name}'이 삭제되었습니다"}
    raise HTTPException(status_code=404, detail="아이템을 찾을 수 없습니다")

# 서버 실행
if __name__ == "__main__":
    # 환경변수 검증
    validate_required_settings()
    
    print(f"🚀 {settings.app_name} v{settings.app_version} 시작 중...")
    print(f"📍 서버 주소: http://{settings.host}:{settings.port}")
    print(f"🔧 디버그 모드: {settings.debug}")
    
    uvicorn.run(
        app, 
        host=settings.host, 
        port=settings.port,
        log_level=settings.log_level.lower()
    )

def main():
    uvicorn.run(
        app, 
        host=settings.host, 
        port=settings.port,
        log_level=settings.log_level.lower()
    )
