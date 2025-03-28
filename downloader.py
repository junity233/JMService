import sys
import os
import logging
import shutil
from typing import List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import jmcomic
from PIL import Image

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 禁用jmcomic库的日志
jmcomic.disable_jm_log()


@contextmanager
def cleanup_context(temp_dir: Path):
    """上下文管理器，确保在处理完成后清理临时目录"""
    try:
        yield
    finally:
        try:
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
                logger.info(f"已清理临时目录: {temp_dir}")
        except Exception as e:
            logger.warning(f"清理临时目录失败: {e}")


def load_image(file_path: str) -> Optional[Image.Image]:
    """加载单个图像文件，处理可能的异常"""
    try:
        img = Image.open(file_path)
        return img
    except (IOError, OSError) as e:
        logger.error(f"无法打开图像 {file_path}: {e}")
        return None


def process_chapter(chapter_path: Path) -> List[Image.Image]:
    """处理单个章节的所有图像"""
    logger.info(f"处理章节: {chapter_path.name}")
    
    # 获取所有webp文件
    files = [f for f in chapter_path.iterdir() if f.suffix.lower() == '.webp']
    
    if not files:
        logger.warning(f"章节 {chapter_path.name} 中没有找到.webp文件")
        return []
    
    # 尝试按数字排序文件
    try:
        files_sorted = sorted(files, key=lambda x: int(x.stem))
    except ValueError:
        logger.warning(f"无法对章节 {chapter_path.name} 中的文件进行排序，使用默认排序")
        files_sorted = sorted(files)
    
    # 并行加载图像
    with ThreadPoolExecutor(max_workers=min(10, len(files_sorted))) as executor:
        images = list(executor.map(load_image, [str(f) for f in files_sorted]))
    
    # 过滤掉None值（加载失败的图像）
    valid_images = [img for img in images if img is not None]
    
    if not valid_images:
        logger.warning(f"章节 {chapter_path.name} 中没有有效图像")
    else:
        logger.info(f"章节 {chapter_path.name} 成功加载 {len(valid_images)}/{len(files)} 张图像")
    
    return valid_images


def convert_comic_to_pdf(comic_id: str, pdf_path: str) -> None:
    """将漫画转换为PDF文件"""
    base_path = Path(f"./temp/{comic_id}")
    
    if not base_path.exists():
        raise FileNotFoundError(f"基础目录 {base_path} 不存在")
    
    # 获取所有章节目录
    chapters = [d for d in base_path.iterdir() if d.is_dir()]
    chapters.sort(key=lambda x: x.name)  # 确保章节按名称排序
    
    if not chapters:
        raise ValueError(f"在 {base_path} 中没有找到章节目录")
    
    logger.info(f"开始处理漫画 ID: {comic_id}，共 {len(chapters)} 个章节")
    
    # 处理所有章节
    total_images = []
    for chapter in chapters:
        chapter_images = process_chapter(chapter)
        total_images.extend(chapter_images)
    
    if not total_images:
        raise ValueError("在所有章节中都没有找到有效图像")
    
    # 创建PDF文件的父目录（如果不存在）
    pdf_dir = Path(pdf_path).parent
    pdf_dir.mkdir(parents=True, exist_ok=True)
    
    # 保存为PDF
    try:
        logger.info(f"正在创建PDF，共 {len(total_images)} 张图像")
        total_images[0].save(
            pdf_path,
            "PDF",
            resolution=100.0,
            save_all=True,
            append_images=total_images[1:]
        )
        logger.info(f"PDF创建成功: {pdf_path}")
    except Exception as e:
        logger.error(f"保存PDF失败: {e}")
        raise RuntimeError(f"无法保存PDF: {e}") from e
    finally:
        # 关闭所有图像
        for img in total_images:
            try:
                img.close()
            except Exception:
                pass


def download_comic(comic_id: str) -> None:
    """下载漫画"""
    try:
        logger.info(f"开始下载漫画 ID: {comic_id}")
        option = jmcomic.create_option_by_file("download_config.yml")
        detail = jmcomic.download_album(comic_id, option=option)[0]
        logger.info(f"漫画下载完成: {detail.title}")
    except Exception as e:
        logger.error(f"下载漫画失败: {e}")
        raise RuntimeError(f"下载漫画失败: {e}") from e


def main() -> None:
    """主函数"""
    if len(sys.argv) < 3:
        logger.error("参数不足: 需要提供漫画ID和PDF输出路径")
        print("用法: python downloader.py <comic_id> <pdf_path>")
        sys.exit(1)
    
    comic_id = sys.argv[1]
    pdf_path = sys.argv[2]
    temp_dir = Path(f"./temp/{comic_id}")
    
    try:
        with cleanup_context(temp_dir):
            # 下载漫画
            download_comic(comic_id)
            
            # 转换为PDF
            convert_comic_to_pdf(comic_id, pdf_path)
        
        sys.exit(0)
    except Exception as e:
        logger.error(f"处理失败: {e}")
        print(f"错误: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
