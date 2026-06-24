import re
import time
import threading
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.file import FileItem
from app.schemas.types import EventType, NotificationType
from app.core.config import settings


class QBRawGuard(_PluginBase):
    """
    ============================================================
    原盘通知 v2.6.1 — 事件驱动秒级拦截 · 定时扫描兜底 · 延迟回扫清理
    ============================================================
    事件驱动（DownloadAdded）：新种子秒级响应，不受标题预检限制
    快速拦截（Fast）：标题预检 → 文件结构正则匹配 → 命中处理
    全量兜底（Full）：低频补漏，默认关闭，按需开启
    延迟回扫：清理拦截后异步产生的孤儿入库记录
    ============================================================
    """
    plugin_name = "原盘通知"
    plugin_desc = "智能拦截 BDVM / ISO / DVD 原盘种子，事件驱动秒级响应 + 定时扫描兜底；命中后联动清理下载文件与入库记录，杜绝 Emby 无法播放的媒体污染。"
    plugin_icon = "https://cdn-icons-png.flaticon.com/512/564/564619.png"
    plugin_version = "2.6.1"
    plugin_author = "MoviePilot Agent"
    author_url = "https://github.com/jxxghp/MoviePilot/pull/5687"
    plugin_config_prefix = "qbrawguard_"
    plugin_order = 29
    auth_level = 1

    TITLE_HINTS = (
        "bdmv", "certificate", "video_ts", "audio_ts", "hvdvd_ts",
        ".iso", ".img", ".nrg",
        "complete.bluray", "complete.blu-ray", "complete_uhd",
        "bdiso", "bd25", "bd50",
        "uhd.bluray", "uhd.blu-ray", "uhd bluray", "uhd_bluray",
        "blu-ray", "full.disc",
        "-mteam", "-hds", "-hdsky", "-chdbits", "-52pt", "-pter",
        "thor@hds", "pete@hds", "blu-ray.diy", "bluray.diy",
        "blu-ray.avc", "bluray.avc", "bluray.remux", "blu-ray.remux",
    )

    DEFAULT_PATTERNS = r"""# Blu-ray / UHD Blu-ray / 3D Blu-ray 原盘
(?i)(^|[/\\])(BDMV|CERTIFICATE|AACS)([/\\]|$)
(?i)(^|[/\\])BDMV[/\\](BACKUP|PLAYLIST|CLIPINF|STREAM|AUXDATA|BDJO|JAR|META)([/\\]|$)
(?i)(^|[/\\])BDMV[/\\](index|MovieObject)\.bdmv$
(?i)\.(bdmv|mpls|clpi)$
# m2ts/ssif 需 BDMV 路径上下文，避免误判 WEB-DL 的 m2ts 流（HBO Max 等流媒体底层用 MPEG-TS/m2ts 封装）
(?i)(^|[/\\])BDMV[/\\].*\.(m2ts|ssif)$
# DVD / HD DVD 原盘
(?i)(^|[/\\])(VIDEO_TS|AUDIO_TS|HVDVD_TS)([/\\]|$)
(?i)(^|[/\\])VIDEO_TS[/\\].*\.(ifo|bup|vob)$
(?i)(^|[/\\])HVDVD_TS[/\\].*\.(evo|ifo|bup|map|xpl)$
# VCD / SVCD 原盘
(?i)(^|[/\\])(VCD|SVCD|MPEGAV|SEGMENT|EXT)([/\\]|$)
(?i)(^|[/\\])(VCD|SVCD|MPEGAV)[/\\].*\.(dat|mpg|mpeg)$
# 光盘镜像、分卷镜像、镜像描述/索引文件
(?i)\.(iso|img|nrg|mdf|mds|ccd|cue|bin|toast|udf|dmg|isz|cdi|b5t|b6t|bwt|sub|dvdmedia)$
(?i)\.i\d{2}$
"""

    # 配置字段默认值。init_plugin 和 update_config 都从这里读取，避免重复维护字段名。
    CONFIG_DEFAULTS = {
        "enabled": False,
        "fast_scan_enabled": True,
        "full_scan_enabled": False,
        "downloaders": [],
        "interval": 2,
        "full_interval": 0,
        "action": "stop",
        "tag": "原盘拦截",
        "include_completed": True,
        "retry_failed": True,
        "notify": True,
        "notify_type": "Agent",
        "alert_image": "https://cdn-icons-png.flaticon.com/512/564/564619.png",
        "test_title": "阿凡达：火与烬 (2025)",
        "test_subtitle": "Avatar Fire and Ash 2025 2160p UHD Blu-ray DoVi HDR10 HEVC TrueHD 7.1-Thor@HDSky",
        "test_site": "馒头",
        "test_seeders": "111",
        "test_tags": "中字 4k 中配 hdr10 DoVi",
        "test_format": "光盘镜像文件",
        "test_message": (
            "站点：馒头\n质量：UHD HDR10 DoVi 2160p\n大小：92.61G\n"
            "种子：Avatar Fire and Ash 2025 2160p UHD Blu-ray DoVi HDR10 HEVC TrueHD 7.1-Thor@HDSky\n"
            "发布时间：2026-06-02 06:03:02\n做种数：111\n促销：50%\nHit&Run：否\n"
            "标签：中字 4k 中配 hdr10 DoVi\n"
            "描述：阿凡达：火与烬 / 阿凡达3 / 阿凡达3：带种者 / 阿凡达3：火与灰 / 阿凡达3：火与烬"
            " 【UHD原盘 DIY国语DTS配音 官译简繁粤/双语字幕】"
        ),
        "patterns": "",  # 空时回落到 DEFAULT_PATTERNS
    }

    # ═══════════════════════════════════════════════════════════
    # 生命周期
    # ═══════════════════════════════════════════════════════════

    def init_plugin(self, config: dict = None):
        c = config or {}
        self.downloader_helper = DownloaderHelper()

        # 统一从 CONFIG_DEFAULTS 取字段，类型保持原样，不在每个字段上单独 bool/int 转换
        for k, default in self.CONFIG_DEFAULTS.items():
            v = c.get(k, default)
            if isinstance(default, bool):
                v = bool(v)
            elif isinstance(default, int) and not isinstance(default, bool):
                try:
                    v = int(v) if v is not None else default
                except (TypeError, ValueError):
                    v = default
            setattr(self, k, v)

        # 后置归一化与边界处理
        self.interval = max(self.interval, 1)
        self.full_interval = max(self.full_interval, 0)
        if self.action not in ("stop", "delete"):
            self.action = "stop"
        if not self.alert_image:
            self.alert_image = self.CONFIG_DEFAULTS["alert_image"]
        if not self.patterns:
            self.patterns = self.DEFAULT_PATTERNS
        self.regex = self._compile(self.patterns)
        self._svc_cache = {"ts": 0.0, "items": {}}

        self.processed = self.get_data("processed") or {}
        self._survivors: set = set()
        self._fast_running = False
        self._full_running = False
        self._rescan_running = False
        self._lock = threading.Lock()
        self._cleaning: set = set()
        self._oplog: list = self.get_data("oplog") or []
        # 延迟回扫队列：{hash: {name, downloader, attempts, next_ts, first_ts, total_cleaned, idle_rounds}}
        self._rescan_queue: dict = self.get_data("rescan_queue") or {}
        # 状态检查缓存（避免 get_page 每次请求都查 DB）
        self._status_cache = {"ts": 0, "checks": None}

        self.eventmanager.register(EventType.DownloadAdded)(self.on_download_added)

        # 持久化时回写 patterns 当前值（可能是默认或用户自定义）
        self.update_config({k: getattr(self, k) for k in self.CONFIG_DEFAULTS})
        if self.enabled:
            logger.info(f"{self.plugin_name} v{self.plugin_version} 已启用")

    def get_state(self) -> bool:
        return self.enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [{"cmd": "/test_raw_notify", "event": "TestRawNotify", "desc": "测试原盘通知"}]

    def get_api(self) -> List[Dict[str, Any]]:
        return [{
            "path": "/test_notify",
            "endpoint": self._test_notify,
            "methods": ["GET"],
            "auth": "apikey",
            "summary": "测试原盘通知发送",
            "description": "发送一条测试通知到 Telegram，验证通知通道是否正常",
        }, {
            "path": "/manual_rescan",
            "endpoint": self._manual_rescan_api,
            "methods": ["GET"],
            "auth": "apikey",
            "summary": "手动回扫孤儿入库记录",
            "description": "对所有 processed 中已成功删除的种子，重新清理后续产生的转移记录与硬链接",
        }]

    def _test_notify(self) -> dict:
        """发送融合下载通知样式的模拟原盘拦截通知。"""
        try:
            title = f"{self.test_title} 原盘格式拦截"
            text = self._build_download_style_notice(
                name=self.test_title,
                matched=[self.test_format],
                downloader="QB",
                torrent={
                    "name": self.test_subtitle,
                    "tags": self.test_tags,
                    "num_seeds": self.test_seeders,
                    "site": self.test_site,
                },
                extra=self.test_message,
                fmt=self.test_format,
            )
            self.post_message(
                mtype=self._notification_type(),
                title=title,
                text=text,
                image=self._notice_image(),
            )
            logger.info(f"{self.plugin_name} 模拟拦截测试通知已发送")
            self._add_oplog("测试通知", 0, 0, 1, 0)
            return {"success": True, "message": "模拟原盘拦截通知已发送，请检查所选通知场景对应的接收端"}
        except Exception as e:
            logger.error(f"{self.plugin_name} 测试通知失败：{e}")
            return {"success": False, "message": f"发送失败：{e}"}

    def _manual_rescan_api(self) -> dict:
        """手动触发一次全量回扫，清理已拦截种子的孤儿入库记录。"""
        try:
            cleaned = self._manual_rescan()
            if cleaned > 0:
                msg = f"手动回扫完成，清理 {cleaned} 条孤儿入库记录"
                logger.info(f"{self.plugin_name} {msg}")
            else:
                msg = "手动回扫完成，未发现孤儿入库记录"
            return {"success": True, "message": msg, "cleaned": cleaned}
        except Exception as e:
            logger.error(f"{self.plugin_name} 手动回扫失败：{e}")
            return {"success": False, "message": f"手动回扫失败：{e}"}

    def stop_service(self):
        with self._lock:
            self._fast_running = False
            self._full_running = False
            self._rescan_running = False

    # ═══════════════════════════════════════════════════════════
    # 类级懒加载工具（避免每次扫描/回扫重复实例化）
    # ═══════════════════════════════════════════════════════════

    @property
    def _sc(self):
        """懒加载 StorageChain 实例，线程安全（只读操作）。"""
        if not hasattr(self, "__sc"):
            from app.chain.storage import StorageChain
            self.__sc = StorageChain()
        return self.__sc

    @property
    def _to(self):
        """懒加载 TransferHistoryOper 实例。"""
        if not hasattr(self, "__to"):
            from app.db.transferhistory_oper import TransferHistoryOper
            self.__to = TransferHistoryOper()
        return self.__to

    # ═══════════════════════════════════════════════════════════
    # 调度器
    # ═══════════════════════════════════════════════════════════

    def get_service(self) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []
        services = []
        if self.fast_scan_enabled:
            services.append({
                "id": "QBRawGuardFast", "name": "QB原盘快速拦截", "trigger": "interval",
                "func": self._run_fast_scan,
                "kwargs": {"seconds": max(self.interval, 1) * 60},
            })
        if self.full_scan_enabled:
            sec = (self.full_interval * 60) if self.full_interval > 0 else max(self.interval * 5 * 60, 150)
            services.append({
                "id": "QBRawGuardFull", "name": "QB原盘全量兜底", "trigger": "interval",
                "func": self._run_full_scan,
                "kwargs": {"seconds": sec},
            })
        # 延迟回扫：清理拦截后异步产生的孤儿入库记录
        services.append({
            "id": "QBRawGuardRescan", "name": "QB原盘孤儿回扫", "trigger": "interval",
            "func": self._run_orphan_rescan,
            "kwargs": {"seconds": 30},
        })
        return services

    def _run_fast_scan(self):
        self._run_locked("_fast_running", self._scan, "fast")

    def _run_full_scan(self):
        self._run_locked("_full_running", self._scan, "full")

    def _run_locked(self, flag: str, func, *args):
        """通用单例运行包装：避免同名任务并发执行。"""
        with self._lock:
            if getattr(self, flag, False):
                return
            setattr(self, flag, True)
        try:
            func(*args)
        finally:
            with self._lock:
                setattr(self, flag, False)

    def _do_orphan_rescan_safe(self):
        """带空队列短路的回扫入口。"""
        if not self._rescan_queue:
            return
        try:
            self._do_orphan_rescan()
        except Exception as e:
            logger.error(f"{self.plugin_name} 延迟回扫异常：{e}")

    # ═══════════════════════════════════════════════════════════
    # 核心扫描
    # ═══════════════════════════════════════════════════════════

    def _scan(self, mode: str = "fast"):
        is_fast = mode == "fast"
        label = "快速拦截" if is_fast else "全量兜底"
        start = time.time()
        total = checked = hits = 0
        hit_names: List[str] = []
        try:
            services_dict = self._services()
            if not services_dict:
                self._add_oplog(label, 0, 0, 0, 0, err="无可用下载器")
                logger.warning(f"{self.plugin_name} {label} 无可用 QB 下载器")
                return
            for downloader, service in services_dict.items():
                torrents, err = service.instance.get_torrents()
                if err:
                    logger.warning(f"{self.plugin_name} 获取 {downloader} 种子列表失败：{err}")
                    continue
                for torrent in torrents or []:
                    h = self._val(torrent, "hash", "hashString")
                    name = self._val(torrent, "name", "title") or ""
                    if not h or self._skip(torrent):
                        continue
                    with self._lock:
                        if self._processed_ok(h, present=True):
                            continue
                    total += 1
                    if is_fast and not self._suspect_name(name):
                        continue
                    if not is_fast:
                        with self._lock:
                            if str(h).lower() in self._survivors:
                                continue
                    checked += 1
                    try:
                        files = self._file_names(service, h, downloader)
                        matched = self._match(files)
                        if matched:
                            hits += 1
                            self._hit(downloader, service, torrent, matched)
                            hit_names.append(name)
                        else:
                            with self._lock:
                                self._mark_nonsuspect(h, name)
                    except Exception as e:
                        logger.error(f"{self.plugin_name} {label} 异常 [{self._short_name(name)}]: {e}")
        except Exception as e:
            logger.error(f"{self.plugin_name} {label} 严重异常：{e}")
        finally:
            elapsed = time.time() - start
            self._add_oplog(label, total, checked, hits, elapsed,
                            err="" if total > 0 else "无待检任务",
                            hit_names=hit_names if hit_names else None)

    # ═══════════════════════════════════════════════════════════
    # 事件驱动
    # ═══════════════════════════════════════════════════════════

    def on_download_added(self, event):
        if not self.enabled:
            return
        h = event.event_data.get("hash")
        if not h:
            return
        with self._lock:
            if self._processed_ok(h, present=True):
                return
        downloader = event.event_data.get("downloader")
        service = self._get_service(downloader)
        if not service:
            return
        try:
            matched = self._match(self._file_names(service, h, downloader))
            if not matched:
                return
            torrents, err = service.instance.get_torrents(ids=h)
            if err or not torrents:
                return
            self._hit(downloader, service, torrents[0], matched)
        except Exception as e:
            logger.error(f"{self.plugin_name} 事件处理异常：{e}")

    # ═══════════════════════════════════════════════════════════
    # 下载器
    # ═══════════════════════════════════════════════════════════

    def _services(self) -> Dict[str, Any]:
        now = time.time()
        cache = getattr(self, "_svc_cache", {"ts": 0.0, "items": {}})
        if cache.get("items") and now - float(cache.get("ts") or 0) < 30:
            return cache.get("items") or {}
        services = (
            self.downloader_helper.get_services(name_filters=self.downloaders)
            if self.downloaders else self.downloader_helper.get_services()
        )
        items = {n: s for n, s in (services or {}).items()
                 if s and s.instance
                 and self.downloader_helper.is_downloader("qbittorrent", service=s)
                 and not s.instance.is_inactive()}
        self._svc_cache = {"ts": now, "items": items}
        return items

    def _get_service(self, name: str):
        if not name:
            return None
        try:
            svc = self.downloader_helper.get_service(name)
            if svc and svc.instance \
               and self.downloader_helper.is_downloader("qbittorrent", service=svc) \
               and not svc.instance.is_inactive():
                return svc
        except Exception:
            pass
        return None

    def _skip(self, torrent: Any) -> bool:
        if self.include_completed:
            return False
        state = str(self._val(torrent, "state") or "").lower()
        return float(self._val(torrent, "progress") or 0) >= 1 or \
            any(x in state for x in ("upload", "seed", "stalledup"))

    def _processed_ok(self, h: str, present: bool = False) -> bool:
        item = self.processed.get(str(h).lower()) or self.processed.get(h)
        if not item:
            return False
        if present and self.action == "delete" and item.get("ok"):
            logger.info(f"{self.plugin_name} 发现已标记成功但仍存在的任务，重新执行删除：{self._short_name(item.get('name') or h)}")
            return False
        return bool(item.get("ok") or not self.retry_failed)

    def _mark_nonsuspect(self, h: str, name: str = ""):
        hl = str(h).lower()
        self._survivors.add(hl)
        if len(self._survivors) > 5000:
            self._survivors = set(list(self._survivors)[-3000:])

    @staticmethod
    def _file_names(service: Any, h: str, downloader: str) -> List[str]:
        return [str(f.get("name") or f.get("path", "")).replace("\\", "/")
                for f in (service.instance.get_files(h) or [])
                if f.get("name") or f.get("path")]

    # ═══════════════════════════════════════════════════════════
    # 匹配
    # ═══════════════════════════════════════════════════════════

    def _match(self, names: List[str]) -> List[str]:
        ret = []
        for name in names:
            if any(r.search(name) for r in self.regex):
                ret.append(name)
                if len(ret) >= 3:
                    break
        return ret

    @staticmethod
    def _compile(patterns: str) -> List[re.Pattern]:
        regs = []
        for line in (patterns or "").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                regs.append(re.compile(line))
            except re.error as e:
                logger.warning(f"原盘通知 正则无效：{line}，{e}")
        return regs

    # ═══════════════════════════════════════════════════════════
    # 命中
    # ═══════════════════════════════════════════════════════════

    def _hit(self, downloader: str, service: Any, torrent: Any, matched: List[str]):
        h = str(self._val(torrent, "hash", "hashString") or "").lower()
        name = self._val(torrent, "name", "title") or h
        with self._lock:
            if h in self._cleaning:
                return
            self._cleaning.add(h)
        if self.action == "delete":
            self._record(h, downloader, name, matched, False)
            self._full_cleanup(downloader, h, name, matched)
        else:
            ok = bool(service.instance.stop_torrents(ids=h))
            if self.tag:
                try:
                    service.instance.set_torrents_tag(ids=h, tags=[self.tag])
                except Exception:
                    pass
            self._record(h, downloader, name, matched, ok)
            with self._lock:
                self._cleaning.discard(h)
        if self.notify:
            self._notify(downloader, name, matched, torrent)

    # ═══════════════════════════════════════════════════════════
    # 四件套
    # ═══════════════════════════════════════════════════════════

    def _full_cleanup(self, downloader: str, h: str, name: str, matched: List[str]):
        try:
            cleaned_ids = set()
            for history in (self._to.list_by_hash(h) or []):
                self._clean_one(history, h, name, self._sc, self._to)
                cleaned_ids.add(history.id)
            deleted = False
            svc = self._get_service(downloader)
            if svc and svc.instance:
                try:
                    svc.instance.delete_torrents(delete_file=True, ids=h)
                except Exception:
                    self.chain.remove_torrents(hashs=[h], delete_file=True, downloader=downloader)
            else:
                self.chain.remove_torrents(hashs=[h], delete_file=True, downloader=downloader)
            time.sleep(2)
            deleted = self._torrent_gone(downloader, h)
            if not deleted:
                logger.warning(f"{self.plugin_name} 删除后任务仍存在，将保留失败状态等待下次重试：{self._short_name(name)}")
            with self._lock:
                if h in self.processed:
                    self.processed[h]["ok"] = deleted
                    self.processed[h]["cleaned"] = len(cleaned_ids)
                    self.processed[h]["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    if not deleted:
                        self.processed[h]["err"] = "删除后任务仍在下载器中"
                    else:
                        self.processed[h].pop("err", None)
                    self.save_data("processed", self.processed)
                self._cleaning.discard(h)
                # 入队延迟回扫：清理 MoviePilot 整理流程后续产生的孤儿入库记录
                if deleted:
                    self._enqueue_rescan(h, name, downloader, initial_cleaned=len(cleaned_ids))
        except Exception as e:
            logger.error(f"{self.plugin_name} 四件套异常 [{self._short_name(name)}]: {e}")
            with self._lock:
                self._cleaning.discard(h)

    def _torrent_gone(self, downloader: str, h: str) -> bool:
        """确认任务是否已从下载器消失，避免误把删除失败标记为成功。"""
        try:
            svc = self._get_service(downloader)
            if not svc or not svc.instance:
                return False
            torrents, err = svc.instance.get_torrents(ids=h)
            if err:
                logger.warning(f"{self.plugin_name} 删除结果确认失败：{err}")
                return False
            return not torrents
        except Exception as e:
            logger.warning(f"{self.plugin_name} 删除结果确认异常：{e}")
            return False

    def _clean_one(self, history, h: str, name: str, storage_chain, oper):
        hid = history.id
        if history.dest and history.dest_fileitem:
            try:
                storage_chain.delete_media_file(FileItem(**history.dest_fileitem))
            except Exception:
                pass
        if history.src and history.src_fileitem:
            try:
                storage_chain.delete_media_file(FileItem(**history.src_fileitem))
            except Exception:
                pass
        self.eventmanager.send_event(EventType.DownloadFileDeleted, {
            "hash": history.download_hash or h, "src": history.src or name,
        })
        try:
            oper.delete(hid)
        except Exception:
            pass

    # ═══════════════════════════════════════════════════════════
    # 延迟回扫：清理 QBRawGuard 拦截后异步产生的孤儿入库记录
    # ═══════════════════════════════════════════════════════════
    # 回扫节奏：30s/120s/300s/600s/1200s/1800s 共 6 次；每次扫到非 0 条则重置 idle_rounds；
    # 连续 2 轮无新增或总计超 60 分钟自动出队，避免长期占用。

    RESCAN_INTERVALS = [30, 90, 180, 300, 600, 900, 900, 900]  # 累计 ~ 65 分钟
    RESCAN_MAX_LIFETIME = 60 * 60  # 60 分钟兜底
    RESCAN_MAX_IDLE_ROUNDS = 2  # 连续 2 轮 0 新增则出队

    def _enqueue_rescan(self, h: str, name: str, downloader: str, initial_cleaned: int = 0):
        """把 hash 加入延迟回扫队列。"""
        if not h:
            return
        now = time.time()
        item = self._rescan_queue.get(h) or {}
        item.update({
            "name": name,
            "downloader": downloader,
            "attempts": item.get("attempts", 0),
            "first_ts": item.get("first_ts", now),
            "next_ts": now + self.RESCAN_INTERVALS[0],
            "total_cleaned": item.get("total_cleaned", initial_cleaned),
            "idle_rounds": 0,
        })
        self._rescan_queue[h] = item
        self.save_data("rescan_queue", self._rescan_queue)
        logger.info(f"{self.plugin_name} 入队延迟回扫：{self._short_name(name)}（首扫 {self.RESCAN_INTERVALS[0]}s 后）")

    def _run_orphan_rescan(self):
        """兼容旧调度入口（外部可能仍引用），实际调用 _run_locked。"""
        self._run_locked("_rescan_running", self._do_orphan_rescan_safe)

    def _do_orphan_rescan(self):
        now = time.time()
        # 取快照避免迭代时修改
        with self._lock:
            queue_snapshot = list(self._rescan_queue.items())
        changed = False
        for h, item in queue_snapshot:
            try:
                if now < item.get("next_ts", 0):
                    continue
                age = now - item.get("first_ts", now)
                # 执行一次回扫
                cleaned = self._rescan_once(h, item)
                attempts = item.get("attempts", 0) + 1
                idle_rounds = item.get("idle_rounds", 0)
                if cleaned > 0:
                    idle_rounds = 0
                    item["last_cleaned_ts"] = now
                    item["total_cleaned"] = item.get("total_cleaned", 0) + cleaned
                    logger.info(f"{self.plugin_name} 延迟回扫命中：{self._short_name(item.get('name', ''))} 清理 {cleaned} 条孤儿记录")
                    self._add_oplog("延迟回扫", 0, 0, 0, cleaned, sample=self._short_name(item.get("name", "")))
                else:
                    idle_rounds += 1
                item["attempts"] = attempts
                item["idle_rounds"] = idle_rounds
                # 出队条件
                give_up = age > self.RESCAN_MAX_LIFETIME or idle_rounds >= self.RESCAN_MAX_IDLE_ROUNDS or attempts >= len(self.RESCAN_INTERVALS)
                if give_up:
                    with self._lock:
                        self._rescan_queue.pop(h, None)
                    total = item.get("total_cleaned", 0)
                    if total > 0:
                        logger.info(f"{self.plugin_name} 延迟回扫完成：{self._short_name(item.get('name', ''))} 累计清理 {total} 条")
                    changed = True
                else:
                    # 安排下一次
                    interval = self.RESCAN_INTERVALS[min(attempts, len(self.RESCAN_INTERVALS) - 1)]
                    item["next_ts"] = now + interval
                    with self._lock:
                        self._rescan_queue[h] = item
                    changed = True
            except Exception as e:
                logger.warning(f"{self.plugin_name} 单项回扫异常：{e}")
        if changed:
            self.save_data("rescan_queue", self._rescan_queue)

    def _rescan_once(self, h: str, item: dict) -> int:
        """执行单次回扫：通过 download_hash 精准查找未清理的转移记录并清理。"""
        cleaned = 0
        name = item.get("name", "")
        try:
            histories = self._to.list_by_hash(h) or []
            for history in histories:
                try:
                    self._clean_one(history, h, name, self._sc, self._to)
                    cleaned += 1
                except Exception as e:
                    logger.warning(f"{self.plugin_name} 回扫清理记录失败 id={getattr(history, 'id', '?')}：{e}")
        except Exception as e:
            logger.warning(f"{self.plugin_name} 回扫查询失败：{e}")
        return cleaned

    def _manual_rescan(self) -> int:
        """手动触发一次全量回扫（也清理 processed 中已删任务遗留的转移记录）。"""
        cleaned_total = 0
        # 遍历 processed 中所有 delete 模式的种子，按 hash 查转移记录。
        # 即使 ok=false（删除时报失败），任务也可能后续被手动清掉，留下孤儿记录，仍需回扫。
        for h, info in list(self.processed.items()):
            if info.get("action") != "delete":
                continue
            try:
                histories = self._to.list_by_hash(h) or []
                if not histories:
                    continue
                # 仅当下载器中已确认无该任务时才清理，避免误删运行中下载的入库
                downloader = info.get("downloader", "")
                if downloader and not self._torrent_gone(downloader, h):
                    logger.info(f"{self.plugin_name} 手动回扫跳过：任务仍在下载器中 {self._short_name(info.get('name', ''))}")
                    continue
                for history in histories:
                    try:
                        self._clean_one(history, h, info.get("name", ""), self._sc, self._to)
                        cleaned_total += 1
                    except Exception:
                        pass
            except Exception:
                pass
        if cleaned_total > 0:
            self._add_oplog("手动回扫", 0, 0, 0, cleaned_total, sample="manual")
        return cleaned_total
    # ═══════════════════════════════════════════════════════════
    # 通知（修复：走系统通知通道）
    # ═══════════════════════════════════════════════════════════

    def _record(self, h: str, downloader: str, name: str, matched: List[str], ok: bool):
        self.processed[h] = {
            "downloader": downloader, "name": name,
            "matched": matched[:10], "action": self.action, "ok": ok,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        if len(self.processed) > 1000:
            self.processed = dict(list(self.processed.items())[-1000:])
        self.save_data("processed", self.processed)

    def _notification_type(self):
        mapping = {
            "Download": NotificationType.Download, "资源下载": NotificationType.Download,
            "Organize": NotificationType.Organize, "整理入库": NotificationType.Organize,
            "Plugin": NotificationType.Plugin, "插件": NotificationType.Plugin,
            "Agent": NotificationType.Agent, "智能体": NotificationType.Agent,
            "Other": NotificationType.Other, "其它": NotificationType.Other,
        }
        return mapping.get(self.notify_type, NotificationType.Agent)

    def _build_download_style_notice(self, name: str, matched: List[str], downloader: str = "QB",
                                     torrent: Any = None, extra: str = "", fmt: str = "") -> str:
        """构建接近 MoviePilot 下载通知的原盘拦截通知，不暴露 hash 和真实路径。"""
        format_hint = fmt or self._detect_format(matched)
        action = "删除" if self.action == "delete" else "停止下载"
        lines = []
        if extra:
            for line in str(extra).splitlines():
                line = line.strip()
                if line:
                    lines.append(line)
        info = self._torrent_notice_info(torrent, name)

        def add(label, value):
            value = str(value or "").strip()
            if value and not any(x.startswith(f"{label}：") for x in lines):
                lines.append(f"{label}：{value}")

        add("站点", info.get("site"))
        add("质量", info.get("quality"))
        add("大小", info.get("size"))
        add("种子", info.get("torrent_title"))
        add("发布时间", info.get("pubdate"))
        add("做种数", info.get("seeders"))
        add("促销", info.get("promotion"))
        add("Hit&Run", info.get("hit_and_run"))
        add("标签", info.get("tags") or self.tag)
        add("描述", info.get("description"))
        lines.append(f"判定格式：{format_hint}")
        if matched:
            lines.append("判定依据：" + "、".join(self._safe_format_hint(x) for x in matched[:3]))
        lines.append(f"处理动作：{action}")
        return "\n".join(lines)

    def _build_notice_text(self, name: str, matched: List[str], downloader: str = "QB",
                           site: str = "未知", seeders: str = "未知", tags: str = "原盘拦截",
                           subtitle: str = "", fmt: str = "", action: str = "", extra: str = "") -> str:
        """兼容旧调用，内部转为下载通知融合样式。"""
        return self._build_download_style_notice(
            name=name, matched=matched, downloader=downloader,
            torrent={"name": subtitle or name, "site": site, "num_seeds": seeders, "tags": tags},
            extra=extra, fmt=fmt,
        )

    @staticmethod
    def _detect_format(matched: List[str]) -> str:
        """根据命中文件名归纳原盘格式。"""
        text = " ".join(matched or []).lower()
        if "bdmv" in text or "certificate" in text:
            return "Blu-ray/UHD Blu-ray 原盘结构（BDMV/CERTIFICATE）"
        if "video_ts" in text or ".ifo" in text or ".vob" in text:
            return "DVD 原盘结构（VIDEO_TS）"
        if any(x in text for x in (".iso", ".img", ".nrg", ".mdf", ".mds", ".cue", ".bin")):
            return "光盘镜像文件"
        return "Emby 可能无法直接识别的原盘结构"

    @staticmethod
    def _safe_format_hint(path: str) -> str:
        """只返回格式层面的脱敏命中依据，不暴露真实路径。"""
        lower = str(path).lower()
        for key, label in (
            ("bdmv", "BDMV 蓝光目录"), ("certificate", "CERTIFICATE 蓝光证书目录"),
            ("video_ts", "VIDEO_TS DVD目录"), ("hvdvd_ts", "HVDVD_TS HD-DVD目录"),
            (".iso", "ISO 光盘镜像"), (".img", "IMG 光盘镜像"), (".nrg", "NRG 光盘镜像"),
            (".mdf", "MDF/MDS 光盘镜像"), (".cue", "CUE/BIN 镜像索引"), (".m2ts", "M2TS 原盘流文件"),
        ):
            if key in lower:
                return label
        return "原盘结构特征"

    def _notify(self, downloader: str, name: str, matched: List[str], torrent: Any = None):
        info = self._torrent_notice_info(torrent, name)
        display_name = info.get("display_title") or self._display_title(name)
        text = self._build_download_style_notice(
            name=display_name, matched=matched, downloader=downloader, torrent=torrent
        )
        title = f"{display_name} 原盘格式拦截"
        try:
            self.post_message(
                mtype=self._notification_type(),
                title=title, text=text, image=self._notice_image(),
            )
            logger.info(f"{self.plugin_name} 通知已发送（系统通知通道）")
        except Exception as e:
            logger.warning(f"{self.plugin_name} 通知发送失败：{e}")
            self._add_oplog("通知", 0, 0, 0, 0, err=f"通知失败：{e}")

    # ═══════════════════════════════════════════════════════════
    # 健康检查
    # ═══════════════════════════════════════════════════════════

    # 状态→Vuetify 主题色 class 映射（自动跟随浅色/深色模式）
    _STATUS_TEXT_CLASS = {
        "success": "text-success",
        "warning": "text-warning",
        "error": "text-error",
        "info": "text-info",
        "default": "text-medium-emphasis",
    }
    _STATUS_COLOR_PROP = {
        "success": "success",
        "warning": "warning",
        "error": "error",
        "info": "info",
        "default": "grey",
    }

    def _check_status(self) -> List[dict]:
        """运行时状态检查，返回 8 个检查项。每项含 icon/label/status/text。30s 内复用缓存。"""
        now = time.time()
        cache = getattr(self, "_status_cache", {"ts": 0, "checks": None})
        if cache.get("checks") and (now - cache["ts"]) < 30:
            return cache["checks"]
        checks = []
        on, fast, full = self.enabled, self.fast_scan_enabled, self.full_scan_enabled

        def push(icon, label, ok, text_on, text_off="已关闭", warn=None):
            if warn is not None:
                status = "warning"
                text = warn
            elif ok:
                status, text = "success", text_on
            else:
                status, text = "default", text_off
            checks.append({"icon": icon, "label": label, "status": status, "text": text})

        push("mdi-power-plug", "插件状态", on, "已启用", "已停用")
        push("mdi-lightning-bolt", "快速拦截", on and fast, f"每 {self.interval} 分钟")
        full_min = self.full_interval if self.full_interval > 0 else self.interval * 5
        push("mdi-shield-check", "全量兜底", on and full, f"每 {full_min} 分钟")

        # 下载器
        try:
            svcs = self._services()
            if svcs:
                push("mdi-download", "QB下载器", True, "、".join(svcs.keys()) + " 已连接")
            else:
                push("mdi-download-off", "QB下载器", False, "", "未配置")
                checks[-1]["status"] = "error"
                checks[-1]["text"] = "无可用 Qbittorrent"
        except Exception as e:
            checks.append({"icon": "mdi-download-off", "label": "QB下载器", "status": "error", "text": str(e)})

        # 通知通道
        try:
            from app.helper.service import ServiceConfigHelper
            switches = ServiceConfigHelper.get_notification_switches() or []
            configs = ServiceConfigHelper.get_notification_configs() or []
            nt = self._notification_type()
            expected_type = nt.value

            # 优先查通知场景开关
            switch = next((s for s in switches if s.type == expected_type and s.action and s.action != "none"), None)
            # 再查通知渠道中是否包含该场景
            enabled_configs = [c for c in configs if c.enabled and expected_type in (c.switchs or [])]
            if switch:
                action_label = {"all": "全部", "user": "仅用户", "admin": "仅管理"}.get(switch.action, switch.action)
                ch_names = "、".join([c.name for c in enabled_configs]) if enabled_configs else "通知渠道"
                push("mdi-bell-ring", "通知通道", True, f"「{expected_type}」→ {action_label}（{ch_names}）")
            elif enabled_configs:
                ch_names = "、".join([c.name for c in enabled_configs])
                push("mdi-bell-ring", "通知通道", True, f"「{expected_type}」→ 已配置（{ch_names}）")
            elif not switches and not configs:
                checks.append({"icon": "mdi-bell-off", "label": "通知通道",
                               "status": "error", "text": "未配置通知场景与通知渠道"})
            else:
                available = [s.type for s in switches if s.action and s.action != "none"]
                if not available:
                    available = [c.name for c in configs if c.enabled]
                checks.append({"icon": "mdi-bell-off", "label": "通知通道",
                               "status": "warning",
                               "text": f"「{expected_type}」未开启（可用: {', '.join(available[:5])}）"})
        except Exception as e:
            checks.append({"icon": "mdi-bell-off", "label": "通知通道", "status": "error", "text": str(e)})

        # 事件驱动
        push("mdi-flash", "事件拦截", on, "监听 DownloadAdded", "跟随插件")

        # 正则有效性
        valid = sum(1 for r in self.regex if r)
        if valid > 0:
            push("mdi-regex", "识别规则", True, f"{valid} 条规则就绪")
        else:
            checks.append({"icon": "mdi-regex", "label": "识别规则", "status": "error", "text": "无有效规则"})

        # 拦截统计
        total_hits = sum(1 for v in (self.processed or {}).values() if v.get("matched"))
        if total_hits > 0:
            checks.append({"icon": "mdi-alert-octagon", "label": "历史拦截",
                           "status": "info", "text": f"累计 {total_hits} 次命中"})
        else:
            checks.append({"icon": "mdi-alert-octagon", "label": "历史拦截",
                           "status": "default", "text": "暂未命中"})
        self._status_cache = {"ts": now, "checks": checks}
        return checks

    # ═══════════════════════════════════════════════════════════
    # 操作日志
    # ═══════════════════════════════════════════════════════════

    def _add_oplog(self, mode: str, total: int, checked: int, hits: int,
                   elapsed: float, err: str = "", sample: str = "", hit_names: list = None):
        if err:
            detail = err
        elif mode in ("延迟回扫", "手动回扫"):
            detail = f"清理 {hits} 条孤儿"
            if sample and sample != "manual":
                detail += f"（{self._short_name(sample)}）"
        else:
            if hit_names:
                # 有具体命中列表：显示种子名和操作
                names_text = "、".join([self._short_name(n) for n in hit_names[:5]])
                suffix = f"... 等{len(hit_names)}个" if len(hit_names) > 5 else ""
                detail = f"命中 {names_text}{suffix}，已{'删除' if self.action == 'delete' else '停止'}"
            else:
                detail = f"扫描 {total} 个任务" + (f"（命中 {hits}）" if hits else "，无命中")
        entry = {
            "time": datetime.now().strftime("%m-%d %H:%M"),
            "mode": mode,
            "detail": detail,
            "elapsed": f"{elapsed:.1f}s" if isinstance(elapsed, (int, float)) else str(elapsed),
        }
        self._oplog.insert(0, entry)
        if len(self._oplog) > 100:
            self._oplog = self._oplog[:100]
        self.save_data("oplog", self._oplog)
        # 同时输出到系统日志（订阅助手风格：直接使用 logger.info）
        log_line = f"原盘通知 [{mode}] {detail}"
        if isinstance(elapsed, (int, float)) and elapsed > 0:
            log_line += f"（{elapsed:.1f}s）"
        logger.info(log_line)

    # ═══════════════════════════════════════════════════════════
    # 字符串工具
    # ═══════════════════════════════════════════════════════════

    def _torrent_notice_info(self, torrent: Any, fallback_name: str = "") -> Dict[str, str]:
        info = self._history_notice_info(torrent, fallback_name)
        torrent_title = self._val(torrent, "name", "title") or fallback_name
        if not info.get("torrent_title"):
            info["torrent_title"] = self._clean_line(torrent_title)
        if not info.get("tags"):
            info["tags"] = self._clean_line(self._val(torrent, "tags", "tag"))
        seeders = self._val(torrent, "num_seeds", "seeders", "seeds")
        if not info.get("seeders") and seeders not in (None, ""):
            info["seeders"] = str(seeders)
        site = self._val(torrent, "site", "site_name", "tracker")
        if not info.get("site"):
            info["site"] = self._site_name(site)
        size = self._val(torrent, "size", "total_size")
        if not info.get("size"):
            info["size"] = self._fmt_size(size)
        added = self._val(torrent, "added_on", "addition_date", "created_at")
        if not info.get("pubdate"):
            info["pubdate"] = self._fmt_time(added)
        if not info.get("promotion"):
            info["promotion"] = self._clean_line(self._val(torrent, "volume_factor", "promotion"))
        if not info.get("hit_and_run"):
            info["hit_and_run"] = self._clean_line(self._val(torrent, "hit_and_run", "hnr"))
        desc = self._val(torrent, "description", "subtitle", "comment")
        if not info.get("description"):
            info["description"] = self._clean_line(desc)
        return info

    def _history_notice_info(self, torrent: Any, fallback_name: str = "") -> Dict[str, str]:
        """优先从 MoviePilot 下载历史提取站点、质量、促销等下载通知元数据。"""
        history = self._download_history(torrent, fallback_name)
        if not history:
            return {}
        note = self._val(history, "note") or {}
        if not isinstance(note, dict):
            note = {}
        torrent_name = self._val(history, "torrent_name") or self._val(history, "title") or fallback_name
        title = self._val(history, "title") or ""
        year = self._val(history, "year") or ""
        seasons = self._val(history, "seasons") or ""
        display_title = f"{title} ({year}){seasons}" if title and year else ""
        return {
            "display_title": self._clean_line(display_title),
            "torrent_title": self._clean_line(torrent_name),
            "site": self._clean_line(self._val(history, "torrent_site") or note.get("site") or note.get("site_name")),
            "quality": self._clean_line(note.get("quality") or note.get("quality_name") or note.get("resource_pix") or note.get("resource_type")),
            "size": self._clean_line(note.get("size") or note.get("torrent_size")),
            "pubdate": self._clean_line(note.get("pubdate") or note.get("publish_time") or self._val(history, "date")),
            "seeders": self._clean_line(note.get("seeders") or note.get("num_seeds") or note.get("seeder")),
            "promotion": self._clean_line(note.get("promotion") or note.get("volume_factor") or note.get("free_state")),
            "hit_and_run": self._clean_line(note.get("hit_and_run") or note.get("hr") or note.get("hit_run")),
            "tags": self._clean_line(note.get("tags") or note.get("labels")),
            "description": self._clean_line(self._val(history, "torrent_description") or note.get("description") or note.get("subtitle")),
        }

    def _download_history(self, torrent: Any, fallback_name: str = "") -> Optional[Any]:
        """按 hash 优先反查下载历史，失败时按最近记录中的种子名做窄匹配。"""
        try:
            from app.db.downloadhistory_oper import DownloadHistoryOper
            oper = DownloadHistoryOper()
            h = self._val(torrent, "hash", "hashString", "download_hash")
            if h:
                history = oper.get_by_hash(str(h))
                if history:
                    return history
            torrent_name = self._clean_line(self._val(torrent, "name", "title") or fallback_name).lower()
            if not torrent_name:
                return None
            for history in (oper.list_by_page(page=1, count=50) or []):
                name = self._clean_line(self._val(history, "torrent_name") or "").lower()
                if name and (name == torrent_name or name in torrent_name or torrent_name in name):
                    return history
        except Exception as e:
            logger.debug(f"{self.plugin_name} 下载历史元数据查询失败：{e}")
        return None

    @staticmethod
    def _clean_line(value: Any) -> str:
        text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
        return re.sub(r"\s+", " ", text)[:240]

    @staticmethod
    def _fmt_size(value: Any) -> str:
        try:
            size = float(value or 0)
            if size <= 0:
                return ""
            for unit in ("B", "K", "M", "G", "T"):
                if size < 1024 or unit == "T":
                    return f"{size:.2f}{unit}" if unit != "B" else f"{int(size)}B"
                size /= 1024
        except Exception:
            return str(value or "")

    @staticmethod
    def _fmt_time(value: Any) -> str:
        try:
            if value in (None, ""):
                return ""
            value = float(value)
            if value > 0:
                return datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(value or "")[:19]
        return ""

    @staticmethod
    def _site_name(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if "://" in text:
            text = text.split("://", 1)[-1].split("/", 1)[0]
        return text[:80]

    @staticmethod
    def _display_title(name: str) -> str:
        text = str(name or "").strip()
        text = re.sub(r"[._]+", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text[:80] or "下载任务"

    def _suspect_name(self, name: str) -> bool:
        """标题预检：只用于快速拦截降噪，事件驱动和全量兜底不依赖它。"""
        text = str(name or "").lower()
        if not text:
            return False
        return any(hint in text for hint in self.TITLE_HINTS)

    def _notice_image(self) -> str:
        """通知图片兜底。MoviePilot 当前 ImageHelper 不支持本地 file 路径，空值走纯文本，避免 Telegram 发送失败。"""
        image = str(getattr(self, "alert_image", "") or "").strip()
        if image.startswith("file://") or image.startswith("/"):
            return ""
        return image

    @staticmethod
    def _short_name(name: str) -> str:
        return (name[:60] + "…") if len(name) > 60 else name

    @staticmethod
    def _val(obj: Any, *keys: str) -> Any:
        for key in keys:
            if isinstance(obj, dict) and key in obj:
                return obj[key]
            if hasattr(obj, key):
                return getattr(obj, key)
            try:
                val = obj.get(key)
                if val is not None:
                    return val
            except Exception:
                pass
        return None

    # ═══════════════════════════════════════════════════════════
    # 集成仪表盘 get_page()
    #  Apple Liquid Glass 风格 · 统计 + 健康检查 + 操作
    # ═══════════════════════════════════════════════════════════

    def _glass_card_style(self, blur: int = 14, opacity: float = 0.55) -> str:
        """液态玻璃卡片 CSS（半透明 + 毛玻璃模糊 + 微妙边框）。"""
        return (
            f"background: rgba(var(--v-theme-surface), {opacity}); "
            f"backdrop-filter: blur({blur}px); "
            f"-webkit-backdrop-filter: blur({blur}px); "
            f"border: 1px solid rgba(var(--v-theme-on-surface), 0.06);"
        )

    # ═══════════════════════════════════════════════════════════
    # 首页（get_page）：统计概览 + 健康检查 + 可交互操作按钮
    # ═══════════════════════════════════════════════════════════

    def get_page(self) -> List[dict]:
        checks = self._check_status()
        icon = self.plugin_icon or ""
        return [
            {"component": "VCard", "props": {
                "variant": "flat", "elevation": "0", "rounded": "xl", "class": "mb-4 pa-4",
                "style": self._glass_card_style(blur=18, opacity=0.5),
            }, "content": [
                {"component": "VRow", "props": {"align": "center", "no-gutters": True}, "content": [
                    {"component": "VAvatar", "props": {"color": "primary", "variant": "tonal", "rounded": "lg", "size": 44},
                     "content": [
                        {"component": "VImg", "props": {"src": icon, "width": 28, "height": 28}}
                    ]},
                    {"component": "VCol", "props": {"class": "ml-3"}, "content": [
                        {"component": "div", "props": {"class": "text-h6 font-weight-bold"},
                         "text": f"原盘通知 v{self.plugin_version}"},
                        {"component": "div", "props": {"class": "text-body-2 text-medium-emphasis"},
                         "text": "BDMV / ISO / DVD 原盘拦截 · 事件驱动 + 定时扫描 + 延迟回扫"},
                    ]},
                    {"component": "VSpacer"},
                    {"component": "VChip", "props": {
                        "color": "success" if self.enabled else "grey", "variant": "flat", "size": "small",
                        "prepend-icon": "mdi-play-circle" if self.enabled else "mdi-pause-circle",
                        "text": "运行中" if self.enabled else "已停用"
                    }},
                ]},
            ]},
            self._stats_row(),
            {"component": "div", "props": {"class": "text-subtitle-2 font-weight-medium mb-2 mt-4"},
             "text": "系统健康检查"},
            self._health_row(checks),
            {"component": "VRow", "props": {"class": "mt-3"}, "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    self._action_button("发送模拟拦截测试通知", "mdi-send", "warning", "test_notify")
                ]},
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    self._action_button(f"手动回扫孤儿入库（队列 {len(self._rescan_queue)}）",
                                        "mdi-broom", "error", "manual_rescan")
                ]},
            ]},
            {"component": "VAlert", "props": {
                "type": "info", "variant": "tonal", "density": "compact", "class": "mt-3",
                "text": "点击右上角「设置」进入完整配置：拦截参数、通知通道、测试字段、识别规则。",
            }},
        ]

    # ═══════════════════════════════════════════════════════════
    # 设置页（3 Tab 分页：基本设置 / 通知与测试 / 高级规则）
    # ═══════════════════════════════════════════════════════════

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        try:
            opts = [{"title": c.name, "value": c.name} for c in self.downloader_helper.get_configs().values()
                    if "qb" in f"{getattr(c, 'type', '')}{c.name}".lower()]
        except Exception:
            opts = []
        form = [{"component": "VForm", "content": [
            *self._form_tabs(opts),
        ]}]
        defaults = {
            "enabled": False, "fast_scan_enabled": True, "full_scan_enabled": False,
            "downloaders": [], "interval": 2, "full_interval": 0,
            "action": "stop", "notify": True, "notify_type": "Agent", "tag": "原盘拦截",
            "include_completed": True, "retry_failed": True,
            "alert_image": "https://cdn-icons-png.flaticon.com/512/564/564619.png",
            "test_message": "站点：馒头\n质量：UHD HDR10 DoVi 2160p\n大小：92.61G\n种子：Avatar Fire and Ash 2025 2160p UHD Blu-ray DoVi HDR10 HEVC TrueHD 7.1-Thor@HDSky\n发布时间：2026-06-02 06:03:02\n做种数：111\n促销：50%\nHit&Run：否\n标签：中字 4k 中配 hdr10 DoVi\n描述：阿凡达：火与烬 / 阿凡达3 / 阿凡达3：带种者 / 阿凡达3：火与灰 / 阿凡达3：火与烬 【UHD原盘 DIY国语DTS配音 官译简繁粤/双语字幕】",
            "test_title": "阿凡达：火与烬 (2025)",
            "test_subtitle": "Avatar Fire and Ash 2025 2160p UHD Blu-ray DoVi HDR10 HEVC TrueHD 7.1-Thor@HDSky",
            "test_site": "馒头", "test_seeders": "111",
            "test_tags": "中字 4k 中配 hdr10 DoVi",
            "test_format": "光盘镜像文件",
            "patterns": self.DEFAULT_PATTERNS,
        }
        return form, defaults

    # ── Tab 分页容器（3 Tab）────────────────────────────────

    def _form_tabs(self, downloader_opts: list) -> List[dict]:
        titles = ["基本设置", "通知与测试", "高级规则"]
        icons = ["mdi-tune", "mdi-bell-ring", "mdi-regex"]
        windows = [
            self._tab_basic(downloader_opts),
            self._tab_notify(),
            self._tab_advanced(),
        ]
        tab_items = [{"component": "VTab", "props": {"value": i, "prepend-icon": icons[i]}, "text": t}
                     for i, t in enumerate(titles)]
        win_items = [{"component": "VWindowItem", "props": {"value": i}, "content": w}
                     for i, w in enumerate(windows)]
        return [
            {"component": "VTabs", "props": {"model": "_tab", "grow": True},
             "content": tab_items},
            {"component": "VWindow", "props": {"model": "_tab", "style": "padding-top: 16px"},
             "content": win_items},
        ]

    # ── Tab 0：基本设置 ───────────────────────────────────────

    def _tab_basic(self, downloader_opts: list) -> list:
        return [
            self._field_row(
                self._switch("enabled", "启用插件"),
                self._switch("fast_scan_enabled", "快速拦截", "标题预检→文件匹配，低开销优先命中"),
                self._switch("full_scan_enabled", "全量兜底", "低频补漏，事件驱动已覆盖所有新任务"),
            ),
            self._field_row(
                self._number("interval", "快速间隔（分钟）", min_val=1),
                self._number("full_interval", "全量间隔（分钟）", min_val=5, hint="留空=自动（快速×5）"),
                self._select("action", "命中动作", [
                    {"title": "停止下载", "value": "stop"},
                    {"title": "删除并联动清理", "value": "delete"},
                ], hint="删除会联动清理文件/入库/记录"),
                self._switch("include_completed", "检查已完成", "避免间隔期内任务漏检"),
            ),
            self._field_row(
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    {"component": "VSelect", "props": {
                        "multiple": True, "chips": True, "model": "downloaders",
                        "label": "QB 下载器", "items": downloader_opts,
                        "hint": "留空 = 全部 QB 下载器", "persistent-hint": True,
                    }}
                ]},
                self._text("tag", "命中标签"),
                self._switch("retry_failed", "失败重试"),
            ),
        ]

    # ── Tab 1：通知与测试 ─────────────────────────────────────

    def _tab_notify(self) -> list:
        return [
            # 卡片 1：通知配置
            {"component": "VCard", "props": {
                "variant": "tonal", "elevation": "0", "rounded": "lg", "class": "mb-4 pa-4",
            }, "content": [
                {"component": "div", "props": {"class": "text-subtitle-2 font-weight-medium mb-3"},
                 "text": "通知配置"},
                self._field_row(
                    self._switch("notify", "发送通知"),
                    self._select("notify_type", "通知场景", [
                        {"title": "智能体", "value": "Agent"},
                        {"title": "插件", "value": "Plugin"},
                        {"title": "资源下载", "value": "Download"},
                        {"title": "整理入库", "value": "Organize"},
                        {"title": "其它", "value": "Other"},
                    ], hint="借用 MP 对应通知频道发送"),
                    self._text("alert_image", "报警图地址", hint="留空自动恢复默认图标"),
                ),
            ]},
            # 卡片 2：模拟通知测试字段
            {"component": "VCard", "props": {
                "variant": "tonal", "elevation": "0", "rounded": "lg", "class": "mb-4 pa-4",
            }, "content": [
                {"component": "div", "props": {"class": "text-subtitle-2 font-weight-medium mb-3"},
                 "text": "模拟通知字段"},
                self._field_row(
                    self._text("test_title", "测试标题", md=6),
                    self._text("test_subtitle", "测试种子名/副标题", md=6),
                ),
                self._field_row(
                    self._text("test_site", "测试站点"),
                    self._text("test_seeders", "测试做种数"),
                    self._text("test_tags", "测试标签"),
                ),
                self._field_row(
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "test_format", "label": "判定格式",
                            "hint": "只写格式依据，不暴露路径/hash", "persistent-hint": True,
                        }}
                    ]},
                ),
                self._field_row(
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VTextarea", "props": {
                            "model": "test_message", "label": "下载通知字段（多行）",
                            "rows": 4, "hint": "按 MP 下载通知格式逐行填写", "persistent-hint": True,
                        }}
                    ]},
                ),
            ]},
        ]

    # ── Tab 2：高级规则 ───────────────────────────────────────

    def _tab_advanced(self) -> list:
        return [
            {"component": "VAlert", "props": {
                "type": "warning", "variant": "tonal", "density": "compact", "class": "mb-3",
                "text": "一般不需修改。规则过宽可能误杀正常任务，建议先用默认规则在小范围验证后再调整。",
            }},
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [
                    {"component": "VTextarea", "props": {
                        "model": "patterns", "label": "原盘识别正则（每行一条）",
                        "rows": 8, "hint": "以 # 开头的行为注释；留空自动恢复内置默认规则",
                        "persistent-hint": True,
                    }}
                ]},
            ]},
        ]

    # ── 表单字段工厂（3 列栅格，md=4）────────────────────────────

    def _field_row(self, *cols: dict) -> dict:
        """多个 VCol 包入一个 VRow。"""
        return {"component": "VRow", "content": list(cols)}

    def _switch(self, key: str, label: str, hint: str = "") -> dict:
        return self._col(4, "VSwitch", {"model": key, "label": label,
                          "hint": hint, "persistent-hint": bool(hint)})

    def _text(self, key: str, label: str, hint: str = "", md: int = 4) -> dict:
        return self._col(md, "VTextField", {"model": key, "label": label,
                          "hint": hint, "persistent-hint": bool(hint)})

    def _number(self, key: str, label: str, min_val: int = 1, hint: str = "", md: int = 4) -> dict:
        return self._col(md, "VTextField", {"model": key, "label": label, "type": "number",
                          "min": min_val, "hint": hint, "persistent-hint": bool(hint)})

    def _select(self, key: str, label: str, items: list, hint: str = "", md: int = 4) -> dict:
        return self._col(md, "VSelect", {"model": key, "label": label, "items": items,
                          "hint": hint, "persistent-hint": bool(hint)})

    @staticmethod
    def _col(cols: int, component: str, props: dict) -> dict:
        return {"component": "VCol", "props": {"cols": 12, "md": cols},
                "content": [{"component": component, "props": props}]}

    # ── 液态玻璃卡片组件工厂（Tab 2 运行状态 / 顶部标题卡复用）─────

    def _stats_row(self) -> dict:
        """构建顶部统计卡片行（液态玻璃）。"""
        hits = sum(1 for v in (self.processed or {}).values() if v.get("matched"))
        stats = [
            ("mdi-alert-octagon", "累计拦截", f"{hits} 次", "error"),
            ("mdi-shield-search", "幸存缓存", f"{len(self._survivors)} 个", "info"),
            ("mdi-timer-sand", "回扫队列", f"{len(self._rescan_queue)} 个",
             "warning" if self._rescan_queue else "success"),
            ("mdi-database-check", "处理记录", f"{len(self.processed or {})} 条", "primary"),
        ]
        return {"component": "VRow", "content": [
            {"component": "VCol", "props": {"cols": 6, "md": 3},
             "content": [self._metric_card(icon, label, value, color)]}
            for icon, label, value, color in stats
        ]}

    def _health_row(self, checks: List[dict]) -> dict:
        """构建健康检查卡片行。"""
        return {"component": "VRow", "content": [
            {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [self._status_card(item)]}
            for item in checks
        ]}

    def _action_button(self, text: str, icon: str, color: str, endpoint: str) -> dict:
        """操作按钮（MP 原生 tonal + rounded-lg + 紫色系）。"""
        return {"component": "VBtn", "props": {
            "color": color, "variant": "tonal", "rounded": "lg", "block": True,
            "prepend-icon": icon, "class": "text-none",
        }, "text": text, "events": {
            "click": {"api": f"plugin/QBRawGuard/{endpoint}?apikey={settings.API_TOKEN}", "method": "get"}
        }}

    def _metric_card(self, icon: str, label: str, value: str, color: str = "primary") -> dict:
        """统计指标卡片（MP 原生 tonal + 苹果液态玻璃 backdrop-filter）。"""
        return {"component": "VCard", "props": {
            "variant": "tonal", "elevation": "0", "rounded": "lg",
            "style": "backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);",
        }, "content": [
            {"component": "VCardText", "props": {"class": "d-flex align-center pa-3"}, "content": [
                {"component": "VAvatar", "props": {
                    "color": color, "variant": "tonal", "rounded": "lg", "size": 44, "class": "me-3"
                }, "content": [
                    {"component": "VIcon", "props": {"size": 22}, "content": icon}
                ]},
                {"component": "div", "content": [
                    {"component": "div", "props": {"class": "text-caption text-medium-emphasis"}, "text": label},
                    {"component": "div", "props": {"class": "text-h6 font-weight-bold"}, "text": value},
                ]}
            ]}
        ]}

    def _status_card(self, item: dict) -> dict:
        """健康检查卡片（MP 原生 tonal + 苹果液态玻璃）。"""
        status = item.get("status", "default")
        color_prop = self._STATUS_COLOR_PROP.get(status, "grey")
        text_class = self._STATUS_TEXT_CLASS.get(status, "text-medium-emphasis")
        return {"component": "VCard", "props": {
            "variant": "tonal", "elevation": "0", "rounded": "lg",
            "style": "backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);",
        }, "content": [
            {"component": "VCardText", "props": {"class": "pa-3"}, "content": [
                {"component": "div", "props": {"class": "d-flex align-center mb-1"}, "content": [
                    {"component": "VIcon", "props": {"size": 18, "class": "me-1", "color": color_prop},
                     "content": item.get("icon") or "mdi-information"},
                    {"component": "span", "props": {"class": "text-caption text-medium-emphasis"},
                     "text": item.get("label", "状态")},
                ]},
                {"component": "div", "props": {"class": f"text-body-2 font-weight-medium {text_class}"},
                 "text": item.get("text", "")},
            ]}
        ]}

    def _glass_card_style(self, blur: int = 14, opacity: float = 0.55) -> str:
        """液态玻璃卡片 CSS（半透明 + 毛玻璃模糊 + 微妙边框）。"""
        return (
            f"background: rgba(var(--v-theme-surface), {opacity}); "
            f"backdrop-filter: blur({blur}px); "
            f"-webkit-backdrop-filter: blur({blur}px); "
            f"border: 1px solid rgba(var(--v-theme-on-surface), 0.06);"
        )
