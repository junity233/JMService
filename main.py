import asyncio
import logging
import os
import json
import shutil
from typing import Dict, Any, Optional
from pathlib import Path

import jmcomic
from fastapi import FastAPI, HTTPException
from starlette.responses import FileResponse
from pydantic import BaseModel, Field, validator

# 配置结构化日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Settings(BaseModel):
    """应用配置模型，使用Pydantic进行验证"""
    download_path: str = Field("./download", description="下载文件存储路径")
    host: str = Field("0.0.0.0", description="服务器监听地址")
    port: int = Field(8000, description="服务器监听端口", ge=1, le=65535)
    temp_path: str = Field("./temp", description="临时文件存储路径")

    @validator('download_path', 'temp_path')
    def validate_paths(cls, v: str) -> str:
        """验证路径是否有效并创建目录"""
        path = Path(v)
        path.mkdir(parents=True, exist_ok=True)
        return v

    @classmethod
    def from_file(cls, path: str) -> "Settings":
        """从JSON文件加载配置"""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return cls(**data)
        except (FileNotFoundError, json.JSONDecodeError, TypeError) as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            logger.info("使用默认配置")
            return cls()


# 初始化应用
settings = Settings.from_file('config.json')
app = FastAPI(
    title="JM漫画服务",
    description="提供漫画下载和PDF转换服务",
    version="1.0.0"
)
jm_client = jmcomic.JmOption.default().new_jm_client()


class ComicMetadata(BaseModel):
    """漫画元数据模型"""
    title: str
    authors: list
    tags: list
    oname: Optional[str] = None
    authoroname: Optional[str] = None


async def get_comic_metadata(_id: str) -> ComicMetadata:
    """异步获取漫画元数据"""
    try:
        # 使用asyncio.to_thread代替run_in_executor
        meta_info = await asyncio.to_thread(jm_client.get_album_detail, _id)
        return ComicMetadata(
            title=meta_info.title,
            authors=meta_info.authors,
            tags=meta_info.tags,
            oname=meta_info.oname,
            authoroname=meta_info.authoroname
        )
    except Exception as e:
        logger.error(f"获取漫画元数据失败 (ID: {_id}): {str(e)}")
        raise HTTPException(status_code=404, detail=f"获取漫画信息失败: {str(e)}")


async def download_comic(_id: str, pdf_path: str) -> None:
    """异步下载漫画并转换为PDF"""
    # 安全执行子进程
    process = await asyncio.create_subprocess_exec(
        "python", "./downloader.py", _id, pdf_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await process.communicate()
    
    if process.returncode != 0:
        error_msg = stderr.decode() if stderr else "未知错误"
        logger.error(f"下载进程失败 (ID: {_id}): {error_msg}")
        raise RuntimeError(f"下载进程失败: {error_msg}")
    
    logger.info(f"成功下载漫画 (ID: {_id})")


def save_metadata(metadata: ComicMetadata, path: str) -> None:
    """保存元数据到文件"""
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(metadata.model_dump(), f, ensure_ascii=False, indent=2)
        logger.debug(f"元数据已保存到 {path}")
    except Exception as e:
        logger.error(f"保存元数据失败: {str(e)}")
        raise


def create_pdf_response(_id: str) -> FileResponse:
    """创建PDF文件响应"""
    # 生成文件存储目录路径和PDF文件路径
    dir_path = Path(settings.download_path) / _id
    file_path = dir_path / "content.pdf"
    meta_path = dir_path / "meta.json"

    if not file_path.exists():
        logger.error(f"PDF文件不存在: {file_path}")
        raise HTTPException(status_code=404, detail="PDF文件不存在")

    try:
        # 读取元数据文件并解析JSON内容，用于生成响应文件名
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta_info = json.load(f)
    except FileNotFoundError:
        logger.error(f"元数据文件不存在: {meta_path}")
        raise HTTPException(status_code=404, detail="元数据文件不存在")
    except json.JSONDecodeError:
        logger.error(f"元数据文件格式错误: {meta_path}")
        raise HTTPException(status_code=500, detail="元数据文件格式错误")

    # 创建包含PDF文件的HTTP响应，使用元数据标题作为文件名（默认使用ID）
    filename = f"{meta_info.get('title', _id)}.pdf"
    logger.info(f"返回PDF文件: {filename}")
    return FileResponse(
        path=str(file_path),
        media_type="application/pdf",
        filename=filename,
    )


@app.get("/download", summary="下载漫画", response_description="返回PDF文件")
async def download(_id: str):
    """
    下载指定ID的漫画并转换为PDF
    
    - **_id**: 漫画ID
    
    如果漫画已下载，直接返回；否则下载并转换为PDF后返回
    """
    logger.info(f"收到下载请求: ID={_id}")
    dir_path = Path(settings.download_path) / _id
    pdf_path = dir_path / "content.pdf"
    meta_path = dir_path / "meta.json"

    # 检查是否已下载
    if dir_path.exists() and meta_path.exists() and pdf_path.exists():
        logger.info(f"漫画已存在，直接返回 (ID: {_id})")
        return create_pdf_response(_id)

    # 确保目录存在
    dir_path.mkdir(parents=True, exist_ok=True)
    
    try:
        # 获取元数据
        metadata = await get_comic_metadata(_id)
        
        # 下载漫画并转换为PDF
        await download_comic(_id, str(pdf_path))
        
        # 保存元数据
        save_metadata(metadata, str(meta_path))
        
        # 返回PDF响应
        return create_pdf_response(_id)
        
    except Exception as e:
        logger.error(f"下载失败 (ID: {_id}): {str(e)}", exc_info=True)
        # 清理目录
        if dir_path.exists():
            shutil.rmtree(dir_path, ignore_errors=True)
            logger.info(f"已清理目录: {dir_path}")
        
        # 返回错误响应
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))


@app.on_event("startup")
async def startup_event():
    """应用启动时执行的操作"""
    logger.info(f"服务启动于 {settings.host}:{settings.port}")
    logger.info(f"下载路径: {settings.download_path}")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        app, 
        host=settings.host, 
        port=settings.port,
        log_level="info"
    )
