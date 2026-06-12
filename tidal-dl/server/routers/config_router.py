"""全局配置 API"""
from fastapi import APIRouter, Depends
from database import get_db_dependency
from config import get_config, update_config

router = APIRouter(prefix="/api/config", tags=["Config"])


@router.get("")
def get_all_config(db=Depends(get_db_dependency)):
    """获取全局配置"""
    return get_config(db)


@router.put("")
def update_all_config(data: dict, db=Depends(get_db_dependency)):
    """更新全局配置（部分更新）"""
    update_config(db, data)
    return {"message": "配置已更新", "config": get_config(db)}
