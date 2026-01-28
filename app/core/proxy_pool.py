"""代理池管理器 - 从URL动态获取代理IP"""

import asyncio
import aiohttp
import json
import time
from typing import Optional, List
from app.core.logger import logger


class ProxyPool:
    """代理池管理器"""
    
    def __init__(self):
        self._pool_url: Optional[str] = None
        self._static_proxy: Optional[str] = None
        self._current_proxy: Optional[str] = None
        self._last_fetch_time: float = 0
        self._fetch_interval: int = 300  # 5分钟刷新一次
        self._enabled: bool = False
        self._lock = asyncio.Lock()
    
    def configure(self, proxy_url: str, proxy_pool_url: str = "", proxy_pool_interval: int = 300):
        """配置代理池
        
        Args:
            proxy_url: 静态代理URL（socks5h://xxx 或 http://xxx）
            proxy_pool_url: 代理池API URL，返回单个代理地址
            proxy_pool_interval: 代理池刷新间隔（秒）
        """
        self._static_proxy = self._normalize_proxy(proxy_url) if proxy_url else None
        pool_url = proxy_pool_url.strip() if proxy_pool_url else None
        if pool_url and self._looks_like_proxy_url(pool_url):
            normalized_proxy = self._normalize_proxy(pool_url)
            if not self._static_proxy:
                self._static_proxy = normalized_proxy
                logger.warning("[ProxyPool] proxy_pool_url看起来是代理地址，已作为静态代理使用，请改用proxy_url")
            else:
                logger.warning("[ProxyPool] proxy_pool_url看起来是代理地址，已忽略（使用proxy_url）")
            pool_url = None
        self._pool_url = pool_url
        self._fetch_interval = proxy_pool_interval
        self._enabled = bool(self._pool_url)
        
        if self._enabled:
            logger.info(f"[ProxyPool] 代理池已启用: {self._pool_url}, 刷新间隔: {self._fetch_interval}s")
        elif self._static_proxy:
            logger.info(f"[ProxyPool] 使用静态代理: {self._static_proxy}")
            self._current_proxy = self._static_proxy
        else:
            logger.info("[ProxyPool] 未配置代理")
    
    async def get_static_proxy(self, session_id: str) -> Optional[str]:
        """根据session_id获取静态绑定代理

        Args:
            session_id: 会话ID (通常是SSO Token)

        Returns:
            代理URL或None
        """
        if not self._enabled or not self._pool_url:
            return None

        try:
            # 构造基础URL (移除尾部斜杠和可能存在的路径后缀)
            base_url = self._pool_url.rstrip('/')
            if "/proxy/" in base_url:
                base_url = base_url.split("/proxy/")[0]

            # 构造静态代理请求URL (使用 Query 参数避免 JWT 特殊字符问题)
            request_url = f"{base_url}/proxy/grok/static?session_id={session_id}"
            logger.info(f"[ProxyPool] 请求静态代理: {request_url[:100]}...")

            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(request_url) as response:
                    response_text = await response.text()
                    logger.info(f"[ProxyPool] 静态代理响应: status={response.status}, body={response_text}")

                    if response.status == 200:
                        proxy_url = None
                        # 尝试 JSON 格式 {"url": "..."}
                        try:
                            data = json.loads(response_text)
                            proxy_url = data.get("url")
                        except (json.JSONDecodeError, TypeError):
                            pass
                        # 回退到纯文本格式 (socks5://... 或 http://...)
                        if not proxy_url and response_text.strip():
                            text = response_text.strip()
                            if text.startswith(("socks5://", "socks5h://", "http://", "https://")):
                                proxy_url = text

                        if proxy_url:
                            proxy = self._normalize_proxy(proxy_url)
                            logger.info(f"[ProxyPool] 成功获取静态代理: {proxy} (session_id: {session_id[:10]}...)")
                            return proxy
                        else:
                            logger.warning(f"[ProxyPool] 无法解析代理地址: {response_text}")
                    else:
                        logger.warning(f"[ProxyPool] 获取静态代理失败: HTTP {response.status}, body={response_text}")

        except Exception as e:
            logger.warning(f"[ProxyPool] 获取静态代理异常: {e}")

        return None

    async def get_proxy(self) -> Optional[str]:
        """获取代理地址
        
        Returns:
            代理URL或None
        """
        # 如果未启用代理池，返回静态代理
        if not self._enabled:
            return self._static_proxy
        
        # 检查是否需要刷新
        now = time.time()
        if not self._current_proxy or (now - self._last_fetch_time) >= self._fetch_interval:
            async with self._lock:
                # 双重检查
                if not self._current_proxy or (now - self._last_fetch_time) >= self._fetch_interval:
                    await self._fetch_proxy()
        
        return self._current_proxy
    
    async def force_refresh(self) -> Optional[str]:
        """强制刷新代理（用于403错误重试）
        
        Returns:
            新的代理URL或None
        """
        if not self._enabled:
            return self._static_proxy
        
        async with self._lock:
            await self._fetch_proxy()
        
        return self._current_proxy
    
    async def _fetch_proxy(self):
        """从代理池URL获取新的代理"""
        try:
            logger.debug(f"[ProxyPool] 正在从代理池获取新代理: {self._pool_url}")
            
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self._pool_url) as response:
                    if response.status == 200:
                        proxy_text = await response.text()
                        proxy = self._normalize_proxy(proxy_text.strip())
                        
                        # 验证代理格式
                        if self._validate_proxy(proxy):
                            self._current_proxy = proxy
                            self._last_fetch_time = time.time()
                            logger.info(f"[ProxyPool] 成功获取新代理: {proxy}")
                        else:
                            logger.error(f"[ProxyPool] 代理格式无效: {proxy}")
                            # 降级到静态代理
                            if not self._current_proxy:
                                self._current_proxy = self._static_proxy
                    else:
                        logger.error(f"[ProxyPool] 获取代理失败: HTTP {response.status}")
                        # 降级到静态代理
                        if not self._current_proxy:
                            self._current_proxy = self._static_proxy
        
        except asyncio.TimeoutError:
            logger.error("[ProxyPool] 获取代理超时")
            if not self._current_proxy:
                self._current_proxy = self._static_proxy
        
        except Exception as e:
            logger.error(f"[ProxyPool] 获取代理异常: {e}")
            # 降级到静态代理
            if not self._current_proxy:
                self._current_proxy = self._static_proxy
    
    def _validate_proxy(self, proxy: str) -> bool:
        """验证代理格式
        
        Args:
            proxy: 代理URL
        
        Returns:
            是否有效
        """
        if not proxy:
            return False
        
        # 支持的协议
        valid_protocols = ['http://', 'https://', 'socks5://', 'socks5h://']
        
        return any(proxy.startswith(proto) for proto in valid_protocols)

    def _normalize_proxy(self, proxy: str) -> str:
        """标准化代理URL（sock5/socks5 → socks5h://）"""
        if not proxy:
            return proxy

        proxy = proxy.strip()
        if proxy.startswith("sock5h://"):
            proxy = proxy.replace("sock5h://", "socks5h://", 1)
        if proxy.startswith("sock5://"):
            proxy = proxy.replace("sock5://", "socks5://", 1)
        if proxy.startswith("socks5://"):
            return proxy.replace("socks5://", "socks5h://", 1)
        return proxy

    def _looks_like_proxy_url(self, url: str) -> bool:
        """判断URL是否像代理地址（避免误把代理池API当代理）"""
        return url.startswith(("sock5://", "sock5h://", "socks5://", "socks5h://"))
    
    def get_current_proxy(self) -> Optional[str]:
        """获取当前使用的代理（同步方法）
        
        Returns:
            当前代理URL或None
        """
        return self._current_proxy or self._static_proxy


# 全局代理池实例
proxy_pool = ProxyPool()
