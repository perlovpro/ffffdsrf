import os
import sys
import platform
import html
from dataclasses import dataclass, field

# venv check - перезапуск если нужно
if PLATFORM_SYSTEM == "Windows":
    venv_py = os.path.join(os.path.dirname(__file__), ".venv", "Scripts", "python.exe")
else:
    venv_py = os.path.join(os.path.dirname(__file__), ".venv", "bin", "python")

try:
    if not getattr(sys, "frozen", False):
        if os.path.exists(venv_py) and sys.executable != venv_py:
            os.execv(venv_py, [venv_py] + sys.argv)
except:
    pass

import asyncio
import random
import re
import statistics
import time
import json
import hashlib
import secrets
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterator, List, MutableMapping, Optional, Tuple
import requests
import logging
import ctypes

from telethon import TelegramClient, events, Button
from telethon.utils import get_peer_id
from telethon.errors import FloodWaitError
from rich.console import Console
from rich.panel import Panel
from rich.style import Style

PLATFORM_SYSTEM = platform.system()
logger = logging.getLogger(__name__)
# utf8 fix для винды
try:
    if PLATFORM_SYSTEM == "Windows":
        try:
            import ctypes
            ctypes.windll.kernel32.SetConsoleOutputCP(65001)
            ctypes.windll.kernel32.SetConsoleCP(65001)
        except:
            pass
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except:
        pass
except:
    pass
# загрузка конфига

CONFIG_TEMPLATE = """# config.txt
API_ID=123456
API_HASH=your_api_hash
PHONE=+79990000000
BOT=@your_bot_username
SESSION=final_session
PRODUCT_LINK=c_xxx
VERBOSE=false
PREEMPTIVE_QTY=true
START_INTERVAL=1.5
QTY_PRE_DELAY=1.0
RETRIES_MAX=3
RETRIES_BASE=0.15
RETRIES_JITTER=0.05
"""


class ConfigError(RuntimeError):
    """Ошибка парсинга config.txt"""


def _ensure_parent_dir(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        logger.debug("Не удалось создать каталог для %s: %s", path, exc)


def _bool_from_str(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on", "да", "д"}


def _coerce_int(value: str, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"Поле {field_name} должно быть целым числом") from exc


def _coerce_float(value: str, field_name: str) -> float:
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"Поле {field_name} должно быть числом") from exc


class RetriesConfig(MutableMapping[str, float]):
    __slots__ = ("max", "base", "jitter")

    def __init__(self, max: int = 3, base: float = 0.15, jitter: float = 0.05) -> None:
        self.max = int(max)
        self.base = float(base)
        self.jitter = float(jitter)

    @staticmethod
    def _normalize_key(key: str) -> str:
        norm = (key or "").strip().lower()
        if norm not in {"max", "base", "jitter"}:
            raise KeyError(key)
        return norm

    def __getitem__(self, key: str) -> float:
        attr = self._normalize_key(key)
        return getattr(self, attr)

    def __setitem__(self, key: str, value: float) -> None:
        attr = self._normalize_key(key)
        if attr == "max":
            self.max = int(value)
        else:
            setattr(self, attr, float(value))

    def __delitem__(self, key: str) -> None:
        raise TypeError("Удаление параметров RETRIES запрещено")

    def __iter__(self) -> Iterator[str]:
        yield from ("max", "base", "jitter")

    def __len__(self) -> int:
        return 3

    def to_dict(self) -> Dict[str, float]:
        return {"max": self.max, "base": self.base, "jitter": self.jitter}


@dataclass
class AppConfig:
    api_id: int
    api_hash: str
    phone: str
    bot: str
    session: str
    product_link: Optional[str] = None
    verbose: bool = False
    preemptive_qty: bool = True
    start_interval: float = 1.5
    qty_pre_delay: float = 1.0
    retries: RetriesConfig = field(default_factory=RetriesConfig)


class ConfigAdapter(MutableMapping[str, Any]):
    __slots__ = ("_config",)

    _SUPPORTED_KEYS = {
        "api_id": "api_id",
        "api_hash": "api_hash",
        "phone": "phone",
        "bot": "bot",
        "session": "session",
        "product_link": "product_link",
        "verbose": "verbose",
        "preemptive_qty": "preemptive_qty",
        "start_interval": "start_interval",
        "qty_pre_delay": "qty_pre_delay",
        "retries": "retries",
    }

    def __init__(self, config: AppConfig) -> None:
        self._config = config

    @classmethod
    def _attr(cls, key: str) -> str:
        lookup = cls._SUPPORTED_KEYS.get((key or "").strip().lower())
        if not lookup:
            raise KeyError(key)
        return lookup

    def __getitem__(self, key: str) -> Any:
        attr = self._attr(key)
        value = getattr(self._config, attr)
        return value

    def __setitem__(self, key: str, value: Any) -> None:
        attr = self._attr(key)
        if attr == "retries":
            if isinstance(value, RetriesConfig):
                self._config.retries = value
            elif isinstance(value, MutableMapping):
                retries = self._config.retries
                retries.max = int(value.get("max", retries.max))
                retries.base = float(value.get("base", retries.base))
                retries.jitter = float(value.get("jitter", retries.jitter))
            else:
                raise TypeError("Неверный тип для RETRIES")
            return

        if attr in {"api_id"}:
            setattr(self._config, attr, _coerce_int(value, key))
        elif attr in {"start_interval", "qty_pre_delay"}:
            setattr(self._config, attr, _coerce_float(value, key))
        elif attr in {"preemptive_qty", "verbose"}:
            if isinstance(value, str):
                setattr(self._config, attr, _bool_from_str(value))
            else:
                setattr(self._config, attr, bool(value))
        else:
            setattr(self._config, attr, value)

    def __delitem__(self, key: str) -> None:
        raise TypeError("Удаление параметров конфигурации запрещено")

    def __iter__(self) -> Iterator[str]:
        for key in self._SUPPORTED_KEYS:
            yield key.upper()

    def __len__(self) -> int:
        return len(self._SUPPORTED_KEYS)

    def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
        try:
            return self[key]
        except KeyError:
            return default

    def as_dict(self) -> Dict[str, Any]:
        data = {name.upper(): getattr(self._config, attr) for name, attr in self._SUPPORTED_KEYS.items()}
        retries = self._config.retries
        data["RETRIES"] = retries.to_dict() if isinstance(retries, RetriesConfig) else RetriesConfig().to_dict()
        return data


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        root.addHandler(handler)
    root.setLevel(level)


def load_config(config_file: str = "config.txt") -> Tuple[ConfigAdapter, Dict[str, Dict[str, str]]]:
    config_path = Path(config_file)

    if not config_path.exists():
        _ensure_parent_dir(config_path)
        config_path.write_text(CONFIG_TEMPLATE, encoding="utf-8")
        raise FileNotFoundError(f"нет файла {config_file} - создан шаблон, заполни и запусти снова")

    raw: Dict[str, str] = {}
    retries_raw: Dict[str, str] = {}
    products: Dict[str, Dict[str, str]] = {}

    with config_path.open("r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            if "=" not in text:
                logger.debug("Пропущена строка %s без '=': %s", line_num, text)
                continue
            key, value = text.split("=", 1)
            key = key.strip()
            value = value.strip()

            if key.startswith("PRODUCT_"):
                product_id = key.replace("PRODUCT_", "", 1)
                if "|" not in value:
                    logger.warning("Строка товара %s без разделителя '|': %s", product_id, text)
                    continue
                name, link = value.split("|", 1)
                products[product_id] = {"name": name.strip(), "link": link.strip()}
                continue

            if key.startswith("RETRIES_"):
                retries_raw[key.replace("RETRIES_", "", 1).lower()] = value
                continue

            raw[key.upper()] = value

    required = ["API_ID", "API_HASH", "PHONE", "BOT", "SESSION"]
    missing = [key for key in required if key not in raw]
    if missing:
        raise ConfigError(f"Отсутствуют обязательные параметры: {', '.join(missing)}")

    retries = RetriesConfig(
        max=_coerce_int(retries_raw.get("max", 3), "RETRIES_MAX"),
        base=_coerce_float(retries_raw.get("base", 0.15), "RETRIES_BASE"),
        jitter=_coerce_float(retries_raw.get("jitter", 0.05), "RETRIES_JITTER"),
    )

    config = AppConfig(
        api_id=_coerce_int(raw["API_ID"], "API_ID"),
        api_hash=str(raw["API_HASH"]),
        phone=str(raw["PHONE"]),
        bot=str(raw["BOT"]),
        session=str(raw["SESSION"]),
        product_link=raw.get("PRODUCT_LINK"),
        verbose=_bool_from_str(raw.get("VERBOSE", "false")),
        preemptive_qty=_bool_from_str(raw.get("PREEMPTIVE_QTY", "true")),
        start_interval=_coerce_float(raw.get("START_INTERVAL", 1.5), "START_INTERVAL"),
        qty_pre_delay=_coerce_float(raw.get("QTY_PRE_DELAY", 1.0), "QTY_PRE_DELAY"),
        retries=retries,
    )

    logger.debug("Конфигурация загружена: %s", config)
    logger.debug("Найдено товаров: %s", len(products))

    return ConfigAdapter(config), products

# лицензии настройки
LICENSE_SERVER_URL = "https://autobuy.cloudpub.ru"
CLIENT_VERSION = "2.5.1"
VERIFICATION_INTERVAL = 3600
HWID_TOLERANCE = 1
SECURE_STORAGE = True
FALLBACK_HWID = True
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2
DEBUG_LICENSE = False
OFFLINE_MODE = False
MESSAGES = {
    "activation_required": "🔐 нужна активация лицензии",
    "license_expired": "⚠️ лицензия истекла",
    "hwid_mismatch": "⚠️ изменилось железо",
    "network_error": "❌ ошибка сети",
    "license_valid": "✅ лицензия ок",
    "activation_success": "✅ лицензия активирована",
}

# картинки для бота
UI_MENU_BANNER_URL = "https://i.pinimg.com/736x/27/95/76/279576309ba8dc23aac7ea3722136950.jpg"
UI_SUCCESS_IMAGE_URL = "https://i.pinimg.com/736x/27/95/76/279576309ba8dc23aac7ea3722136950.jpg"

# пути для exe
def _resource_path(*parts: str) -> str:
    base_dir = None
    try:
        if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
            base_dir = Path(sys._MEIPASS)
    except:
        base_dir = None
    if base_dir is None:
        base_dir = Path(__file__).parent
    return str(base_dir.joinpath(*parts))

def _run_dir() -> Path:
    # папка рядом с exe
    try:
        if getattr(sys, "frozen", False):
            # exe
            if hasattr(sys, '_MEIPASS'):
                # pyinstaller
                return Path(sys.executable).parent
            else:
                # nuitka onefile
                
                # env переменная
                if 'NUITKA_ONEFILE_PARENT' in os.environ:
                    return Path(os.environ['NUITKA_ONEFILE_PARENT'])
                
                # из argv[0]
                try:
                    exe_path = Path(sys.argv[0])
                    if exe_path.is_absolute() and exe_path.suffix.lower() == '.exe':
                        return exe_path.parent
                except Exception:
                    pass
                
                # sys.executable
                try:
                    exe_path = Path(sys.executable)
                    if exe_path.suffix.lower() == '.exe':
                        return exe_path.parent
                except Exception:
                    pass
                
                # текущая папка
                return Path.cwd()
        else:
            # dev режим
            return Path(__file__).parent
    except Exception:
        pass
    return Path(".")

# импорт hwid
HWIDCollector = None

# пути для hwid
hwid_paths = [
    _resource_path("licensing", "app"),
    _resource_path("licensing"),
    _resource_path("")
]

# внешний путь
autobuyer_root = os.getenv("AUTOBUYER_PATH") or os.getenv("AUTOBUYER_DIR")
if autobuyer_root:
    hwid_paths.extend([
        os.path.join(autobuyer_root, "licensing", "app"),
        autobuyer_root,
    ])

# частный случай
try:
    rel_external = str(Path(__file__).parent.parent.joinpath("autobuye", "autobuyer", "licensing", "app"))
    hwid_paths.append(rel_external)
except Exception:
    pass

for hwid_path in hwid_paths:
    try:
        if hwid_path and hwid_path not in sys.path:
            sys.path.append(hwid_path)
        from hwid_collector import HWIDCollector  # type: ignore[assignment]
        break
    except ImportError:
        continue

# консоль
console = Console()

# старая hwid система
HWIDCollector = None  # отключаем
console.print("[green]✓ Активирована старая простая система HWID[/]")
# простая система hwid

red_style = Style(color="red", bold=True)
green_style = Style(color="green", bold=True)
yellow_style = Style(color="yellow", bold=True)


class LicenseClient:
    # лицензии клиент
    
    def __init__(self, server_url: str = None):
        self.server_url = (server_url or LICENSE_SERVER_URL).rstrip('/')
        self.client_version = CLIENT_VERSION
        self.hwid_collector = None  # отключаем
        self.offline_mode = OFFLINE_MODE
        self.debug = DEBUG_LICENSE
        
        # папки
        self.config_dir = self.get_config_dir()
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        self.license_file = self.config_dir / "license.json"
        self.hwid_file = self.config_dir / "hwid.json"
        self.last_key_file = self.config_dir / "last_key.json"
        # последний ключ
        self.last_entered_key: Optional[str] = None
        
        # http настройки
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': f'Porn2oAutoBuyer/{self.client_version} ({PLATFORM_SYSTEM})',
            'Content-Type': 'application/json'
        })
        
        self.timeout = REQUEST_TIMEOUT
        self.max_retries = MAX_RETRIES
        self.retry_delay = RETRY_DELAY
        self.last_verification = None
        self.verification_interval = VERIFICATION_INTERVAL
        self._watch_set: set[int] = set()
    
    def get_config_dir(self) -> Path:
        # папка для конфига
        if PLATFORM_SYSTEM == "Windows":
            try:
                if getattr(sys, "frozen", False):
                    return Path(sys.executable).parent
            except:
                pass
            appdata = os.getenv("APPDATA", os.path.expanduser("~"))
            return Path(appdata) / "Porn2oAutoBuyer"
        elif PLATFORM_SYSTEM == "Darwin":
            return Path.home() / "Library" / "Application Support" / "Porn2oAutoBuyer"
        else:
            config_home = os.getenv("XDG_CONFIG_HOME", Path.home() / ".config")
            return Path(config_home) / "porn2oautobuyer"
    
    def save_data(self, filepath: Path, data: Dict):
        # сохраняем данные
        try:
            # виндовс dpapi
            if PLATFORM_SYSTEM == "Windows":
                try:
                    import win32crypt
                    json_data = json.dumps(data, indent=2)
                    encrypted_data = win32crypt.CryptProtectData(
                        json_data.encode('utf-8'),
                        "Porn2oAutoBuyer",
                        None, None, None, 0
                    )
                    
                    with open(filepath.with_suffix('.enc'), 'wb') as f:
                        f.write(encrypted_data)
                    
                    if filepath.exists():
                        filepath.unlink()
                    
                    return
                except ImportError:
                    pass
            
            # обычный json
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            
            if PLATFORM_SYSTEM != "Windows":
                filepath.chmod(0o600)
                
        except Exception as e:
            console.print(f"[red]Ошибка сохранения данных: {e}[/]")
    
    # rust dll
    def _load_core_dll(self) -> Optional[ctypes.CDLL]:
        candidates = [
            str(_run_dir().joinpath('hwidcore.dll')),
            str(Path.cwd().joinpath('hwidcore.dll')),
        ]
        for p in candidates:
            try:
                if os.path.exists(p):
                    dll = ctypes.CDLL(p)
                    # прототипы
                    dll.anti_debug.restype = ctypes.c_int
                    dll.sha256_str.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
                    dll.sha256_str.restype = ctypes.c_int
                    return dll
            except Exception:
                continue
        return None
    
    def _rust_sha256(self, s: str) -> Optional[str]:
        try:
            if not hasattr(self, '_core_dll'):
                self._core_dll = self._load_core_dll()
            core = getattr(self, '_core_dll', None)
            if not core:
                return None
            data = s.encode('utf-8')
            buf = (ctypes.c_ubyte * 32)()
            rc = core.sha256_str(ctypes.c_char_p(data), ctypes.c_size_t(len(data)), ctypes.byref(buf))
            if rc != 0:
                return None
            import binascii
            return binascii.hexlify(bytes(buf)).decode('ascii')
        except Exception:
            return None

    def enforce_anti_debug(self) -> None:
        try:
            if not hasattr(self, '_core_dll'):
                self._core_dll = self._load_core_dll()
            core = getattr(self, '_core_dll', None)
            if core and hasattr(core, 'anti_debug'):
                rc = core.anti_debug()
                if int(rc) != 0:
                    console.print("[red]Обнаружена отладка/инструментация. Завершение.[/]")
                    sys.exit(1)
        except Exception:
            # не блокируем если ошибка
            pass
    
    def load_data(self, filepath: Path) -> Optional[Dict]:
        # загружаем данные
        try:
            # зашифрованный файл
            encrypted_file = filepath.with_suffix('.enc')
            if encrypted_file.exists() and PLATFORM_SYSTEM == "Windows":
                try:
                    import win32crypt
                    with open(encrypted_file, 'rb') as f:
                        encrypted_data = f.read()
                    
                    decrypted_data, _ = win32crypt.CryptUnprotectData(
                        encrypted_data, None, None, None, 0
                    )
                    
                    return json.loads(decrypted_data.decode('utf-8'))
                except ImportError:
                    pass
                except Exception:
                    pass
            
            # обычный json
            if filepath.exists():
                with open(filepath, 'r') as f:
                    return json.load(f)
            
            return None
        except Exception:
            return None
    
    def make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        # http запрос
        url = f"{self.server_url}{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.request(method, url, timeout=self.timeout, **kwargs)
                return response
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise
                delay = self.retry_delay ** attempt
                if self.debug:
                    console.print(f"[yellow]Retry {attempt + 1}/{self.max_retries} after {delay}s: {e}[/]")
                time.sleep(delay)
        
        raise Exception("Max retries exceeded")
    
    def collect_hwid(self) -> Tuple[str, Dict]:
        # собираем hwid
        import re as _re
        system = PLATFORM_SYSTEM
        try:
            import psutil as _psutil  # type: ignore
        except Exception:
            _psutil = None
        def _norm(s: str) -> str:
            s = (s or "").strip().upper()
            return _re.sub(r"[\s\-]", "", s)
        data: Dict[str, Any] = {
            "os": system,
            "machine": platform.machine(),
            "release": platform.release(),
        }
        try:
            if system == "Windows":
                # винда wmi
                try:
                    import wmi as _wmi  # type: ignore
                except Exception:
                    _wmi = None
                if _wmi:
                    c = _wmi.WMI()
                    try:
                        for board in c.Win32_BaseBoard():
                            data["mb_serial"] = _norm(getattr(board, "SerialNumber", "") or "")
                            break
                    except Exception:
                        pass
                    try:
                        for bios in c.Win32_BIOS():
                            data["bios_serial"] = _norm(getattr(bios, "SerialNumber", "") or "")
                            ver = " ".join(getattr(bios, "BIOSVersion", []) or []) or getattr(bios, "SMBIOSBIOSVersion", "") or ""
                            data["bios_version"] = _norm(ver)
                            break
                    except Exception:
                        pass
                    try:
                        serials: List[str] = []
                        for disk in c.Win32_DiskDrive():
                            media = str(getattr(disk, "MediaType", "") or "")
                            if "Virtual" in media:
                                continue
                            serial = getattr(disk, "SerialNumber", "") or ""
                            if serial:
                                serials.append(_norm(serial))
                        if serials:
                            data["disk_serials"] = sorted(set(serials))
                    except Exception:
                        pass
                    try:
                        for cpu in c.Win32_Processor():
                            data["cpu_id"] = _norm(getattr(cpu, "ProcessorId", "") or "")
                            data["cpu_name"] = _norm(getattr(cpu, "Name", "") or "")
                            try:
                                data["cpu_cores"] = int(getattr(cpu, "NumberOfCores", 0) or 0)
                            except Exception:
                                pass
                            break
                    except Exception:
                        pass
                # guid
                try:
                    import winreg  # type: ignore
                    with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\\Microsoft\\Cryptography") as k:
                        mg, _ = winreg.QueryValueEx(k, "MachineGuid")
                        data["machine_guid"] = _norm(mg)
                except Exception:
                    pass
            elif system == "Darwin":
                # макось
                try:
                    import ctypes, ctypes.util  # type: ignore
                    IOKit = ctypes.cdll.LoadLibrary(ctypes.util.find_library("IOKit"))
                    CoreFoundation = ctypes.cdll.LoadLibrary(ctypes.util.find_library("CoreFoundation"))
                    kIOMasterPortDefault = ctypes.c_void_p.in_dll(IOKit, "kIOMasterPortDefault")
                    IOServiceMatching = IOKit.IOServiceMatching
                    IOServiceMatching.restype = ctypes.c_void_p
                    IOServiceMatching.argtypes = [ctypes.c_char_p]
                    IOServiceGetMatchingService = IOKit.IOServiceGetMatchingService
                    IOServiceGetMatchingService.restype = ctypes.c_void_p
                    IOServiceGetMatchingService.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
                    IORegistryEntryCreateCFProperty = IOKit.IORegistryEntryCreateCFProperty
                    IORegistryEntryCreateCFProperty.restype = ctypes.c_void_p
                    IORegistryEntryCreateCFProperty.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32]
                    CFStringCreateWithCString = CoreFoundation.CFStringCreateWithCString
                    CFStringCreateWithCString.restype = ctypes.c_void_p
                    CFStringCreateWithCString.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32]
                    kCFAllocatorDefault = ctypes.c_void_p.in_dll(CoreFoundation, "kCFAllocatorDefault")
                    kCFStringEncodingUTF8 = 0x08000100
                    service = IOServiceGetMatchingService(kIOMasterPortDefault, IOServiceMatching(b"IOPlatformExpertDevice"))
                    key = CFStringCreateWithCString(kCFAllocatorDefault, b"IOPlatformUUID", kCFStringEncodingUTF8)
                    val = IORegistryEntryCreateCFProperty(service, key, kCFAllocatorDefault, 0)
                    CFStringGetCStringPtr = CoreFoundation.CFStringGetCStringPtr
                    CFStringGetCStringPtr.restype = ctypes.c_char_p
                    CFStringGetCStringPtr.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
                    uuid_c = CFStringGetCStringPtr(val, kCFStringEncodingUTF8)
                    if uuid_c:
                        data["io_platform_uuid"] = _norm(uuid_c.decode())
                except Exception:
                    pass
                try:
                    data["cpu_brand"] = _norm(platform.processor())
                except Exception:
                    pass
            else:
                # линукс
                try:
                    def _read(path: str) -> str:
                        try:
                            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                                return f.read().strip()
                        except Exception:
                            return ""
                    data["board_serial"] = _norm(_read("/sys/class/dmi/id/board_serial"))
                    data["chassis_serial"] = _norm(_read("/sys/class/dmi/id/chassis_serial"))
                    data["product_uuid"] = _norm(_read("/sys/class/dmi/id/product_uuid"))
                except Exception:
                    pass
                try:
                    with open("/proc/cpuinfo", "r", encoding="utf-8", errors="ignore") as f:
                        txt = f.read()
                    m = _re.search(r"model name\s*:\s*(.+)", txt)
                    if m:
                        data["cpu_brand"] = _norm(m.group(1))
                except Exception:
                    pass
            # память
            try:
                if _psutil:
                    data["total_ram"] = int(_psutil.virtual_memory().total)
            except Exception:
                pass
            # мак адреса тут собираем
            try:
                mac_addrs: List[str] = []
                if _psutil:
                    patterns = [
                        r"^lo$", r"loopback", r"veth", r"vbox", r"vmware", r"hyper-v",
                        r"^docker", r"^br-", r"^tun", r"^tap", r"^wg", r"^tailscale", r"^ham",
                        r"^zt", r"^npf", r"^utun", r"^llw", r"^awdl", r"^vmnet", r"^en\d+f\d+",
                        r"^bridge", r"^ZeroTier", r"^vEthernet"
                    ]
                    addrs = _psutil.net_if_addrs()
                    for name, infos in addrs.items():
                        lname = (name or "").lower()
                        if any(_re.search(p, lname) for p in patterns):
                            continue
                        for info in infos:
                            family = getattr(info, "family", None)
                            if family == _psutil.AF_LINK or str(family) == "AddressFamily.AF_LINK":
                                mac = getattr(info, "address", "") or ""
                                if mac and mac != "00:00:00:00:00:00":
                                    mac_addrs.append(_norm(mac))
                if mac_addrs:
                    data["macs"] = sorted(set(mac_addrs))
            except Exception:
                pass
        except Exception:
            # игнор ошибок
            pass
        # нормализуем данные
        order = [
            "mb_serial", "bios_serial", "bios_version", "board_serial", "chassis_serial",
            "product_uuid", "io_platform_uuid", "machine_guid", "cpu_id", "cpu_name",
            "cpu_brand", "cpu_cores", "total_ram", "disk_serials", "macs",
        ]
        norm: Dict[str, Any] = {}
        for key in order:
            val = data.get(key)
            if val is None:
                continue
            if isinstance(val, list):
                items = sorted({str(x).strip().upper().replace(" ", "").replace("-", "") for x in val})
                if items:
                    norm[key] = items
            elif isinstance(val, int):
                norm[key] = val
            else:
                s = str(val).strip().upper().replace(" ", "").replace("-", "")
                if s:
                    norm[key] = s
        # строим итоговую строку hwid
        parts: List[str] = []
        for key in order:
            if key not in norm:
                continue
            v = norm[key]
            if isinstance(v, list):
                parts.append(f"{key}={','.join(v)}")
            else:
                parts.append(f"{key}={v}")
        hwid_string = "|".join(parts)
        # метаданные
        meta: Dict[str, Any] = {
            "os": data.get("os"),
            "fields": sorted(list(norm.keys())),
            "mac_count": len(norm.get("macs", [])) if isinstance(norm.get("macs"), list) else 0,
            "method": "canonical",
                "fallback": False
            }
        return hwid_string, meta
    
    def activate_license(self, license_key: str) -> Dict:
        # активация
        console.print("[yellow]🔐 Активация лицензии...[/]")
        
        # сохраняем ключ
        try:
            self.last_entered_key = license_key
            self.save_data(self.last_key_file, {
                "key": str(license_key or ""),
                "saved_at": datetime.now(timezone.utc).isoformat()
            })
        except Exception:
            pass
        
        hwid, hwid_metadata = self.collect_hwid()
        
        if self.debug:
            console.print(f"[dim]HWID fields collected: {len(hwid_metadata)}[/]")
            console.print(f"[dim]Server: {self.server_url}[/]")
        
        request_data = {
            "key": license_key,
            "hwid": hwid,
            "hwid_meta": hwid_metadata,
            "client_version": self.client_version
        }
        
        try:
            response = self.make_request("POST", "/api/license/activate", json=request_data)
            result = response.json()
            
            if response.status_code == 200 and result.get("status") == "OK":
                license_data = {
                    "key": license_key,
                    "token": result.get("token"),
                    "expires_at": result.get("expires_at"),
                    "activated_at": datetime.now(timezone.utc).isoformat(),
                    "hwid": hwid
                }
                
                self.save_data(self.license_file, license_data)
                
                hwid_data = {
                    "hwid": hwid,
                    "metadata": hwid_metadata,
                    "collected_at": datetime.now(timezone.utc).isoformat()
                }
                self.save_data(self.hwid_file, hwid_data)
                
                console.print(f"[green]{MESSAGES.get('activation_success', '✅ Лицензия активирована успешно!')}[/]")
                return result
            elif response.status_code == 200 and result.get("status") == "HWID_WEAK":
                # игнор hwid_weak
                console.print("[green]✅ Лицензия успешно активирована[/]")
                return {"status": "OK", "message": "License activated"}
            else:
                # не раскрываем детали
                generic = "Ошибка активации. Проверьте ключ и повторите."
                if self.debug:
                    error_msg = result.get("message", generic)
                    console.print(f"[red]❌ Ошибка активации: {error_msg}[/]")
                else:
                    console.print(f"[red]❌ {generic}[/]")
                # без деталей
                safe = dict(result)
                if not self.debug:
                    safe["message"] = generic
                return safe
                
        except Exception as e:
            error_text = f"❌ Сетевая ошибка при активации: {e}" if self.debug else MESSAGES.get('network_error', '❌ Ошибка подключения к серверу лицензий')
            console.print(f"[red]{error_text}[/]")
            # без подробностей
            return {"status": "NETWORK_ERROR", "message": (str(e) if self.debug else MESSAGES.get('network_error', 'Ошибка сети')), "success": False}
    
    def verify_license(self) -> Dict:
        # проверка лицензии
        license_data = self.load_data(self.license_file)
        if not license_data:
            return {"status": "NO_LICENSE", "message": "No license found", "success": False}
        
        current_hwid, current_metadata = self.collect_hwid()
        
        request_data = {
            "key": license_data["key"],
            "hwid": current_hwid,
            "token": license_data.get("token")
        }
        
        try:
            response = self.make_request("POST", "/api/license/verify", json=request_data)
            result = response.json()
            
            if response.status_code == 200 and result.get("status") == "OK":
                # обновляем hwid
                hwid_data = {
                    "hwid": current_hwid,
                    "metadata": current_metadata,
                    "collected_at": datetime.now(timezone.utc).isoformat()
                }
                self.save_data(self.hwid_file, hwid_data)
                self.last_verification = datetime.now(timezone.utc)
                return result
            elif response.status_code == 200 and result.get("status") == "HWID_WEAK":
                # игнор hwid_weak
                return {"status": "OK", "message": "License valid"}
            else:
                # без лишнего
                if not self.debug:
                    result = dict(result)
                    if "message" in result:
                        result["message"] = "Ошибка проверки лицензии"
                return result
                
        except Exception as e:
            return {"status": "NETWORK_ERROR", "message": (str(e) if self.debug else "Ошибка сети"), "success": False}
    
    def request_rebind(self, reason: str) -> Dict:
        """Запрос на перебиндовку HWID"""
        license_data = self.load_data(self.license_file)
        if not license_data:
            return {"status": "NO_LICENSE", "message": "No license found", "success": False}
        
        saved_hwid_data = self.load_data(self.hwid_file)
        old_hwid = saved_hwid_data.get("hwid") if saved_hwid_data else license_data.get("hwid")
        old_metadata = saved_hwid_data.get("metadata", {}) if saved_hwid_data else {}
        
        new_hwid, new_metadata = self.collect_hwid()
        
        request_data = {
            "key": license_data["key"],
            "old_hwid": old_hwid,
            "new_hwid": new_hwid,
            "reason": reason,
            "old_hwid_meta": old_metadata,
            "new_hwid_meta": new_metadata,
            "client_version": self.client_version
        }
        
        try:
            response = self.make_request("POST", "/api/license/rebind/request", json=request_data)
            result = response.json()
            return result
        except Exception as e:
            return {"success": False, "error_code": "NETWORK_ERROR", "message": (str(e) if self.debug else "Ошибка сети")}
    
    def check_license_validity(self) -> bool:
        # проверка валидности
        # есть ли лицензия
        license_data = self.load_data(self.license_file)
        if not license_data:
            return False
        
        # истекла ли
        if license_data.get("expires_at"):
            try:
                expires_at = datetime.fromisoformat(license_data["expires_at"].replace('Z', '+00:00'))
                if expires_at < datetime.now(timezone.utc):
                    console.print("[yellow]⚠️ Лицензия истекла[/]")
                    return False
            except Exception:
                pass
        
        # нужна ли верификация
        if (self.last_verification is None or 
            (datetime.now(timezone.utc) - self.last_verification).total_seconds() > self.verification_interval):
            
            result = self.verify_license()
            
            if result.get("status") == "OK":
                return True
            elif result.get("status") == "HWID_MISMATCH":
                console.print("[yellow]⚠️ Обнаружено изменение оборудования[/]")
                console.print("[yellow]Ваше оборудование изменилось с момента последней активации.[/]")
                
                choice = console.input("[cyan]Запросить перебиндовку лицензии? (y/n): [/]").strip().lower()
                if choice in ['y', 'yes', 'да', 'д']:
                    reason = console.input("[cyan]Причина изменения оборудования: [/]").strip()
                    if not reason:
                        reason = "Hardware change detected"
                    
                    rebind_result = self.request_rebind(reason)
                    if rebind_result.get("success"):
                        console.print("[green]✅ Заявка на перебиндовку отправлена[/]")
                        console.print("[yellow]Ожидайте одобрения администратором[/]")
                    else:
                        console.print(f"[red]❌ Ошибка отправки заявки: {rebind_result.get('message')}[/]")
                
                return False
            else:
                # без деталей
                console.print("[red]❌ Ошибка верификации лицензии[/]")
                return False
        
        return True
    
    def run_licensing_check(self) -> bool:
        # основная проверка
        # offline режим
        if self.offline_mode:
            if self.debug:
                console.print("[yellow]⚠️ OFFLINE MODE: Пропускаем проверку лицензии[/]")
            return True
        
        try:
            license_data = self.load_data(self.license_file)
            
            if not license_data:
                # первый запуск
                console.print("\n" + "="*60)
                console.print(f"[bold yellow]{MESSAGES.get('activation_required', '🔐 ПЕРВЫЙ ЗАПУСК - АКТИВАЦИЯ ЛИЦЕНЗИИ')}[/]")
                console.print("="*60)

                while True:
                    license_key = console.input("[cyan]Введите лицензионный ключ (Enter — отмена): [/]").strip()
                    if not license_key:
                        console.print("[red]Активация отменена пользователем.[/]")
                        return False

                    # сохраняем ключ
                    try:
                        self.last_entered_key = license_key
                        self.save_data(self.last_key_file, {
                            "key": str(license_key or ""),
                            "saved_at": datetime.now(timezone.utc).isoformat()
                        })
                    except Exception:
                        pass

                    result = self.activate_license(license_key)
                    status = (result or {}).get("status")
                    if status == "OK":
                        return True
                    else:
                        err = (result or {}).get("message") or "Неверный ключ или ошибка активации"
                        console.print(f"[red]❌ {err}[/]")
                        # повтор
                        try_again = console.input("[cyan]Попробовать другой ключ? (y/N): [/]").strip().lower()
                        if try_again not in ("y", "yes", "д", "да"):
                            return False
            else:
                # последующие запуски
                is_valid = self.check_license_validity()
                if is_valid:
                    return True

                console.print("\n[bold yellow]⚠️ Лицензия недействительна или истекла.[/]")
                while True:
                    license_key = console.input("[cyan]Введите новый лицензионный ключ (Enter — отмена): [/]").strip()
                    if not license_key:
                        return False
                    result = self.activate_license(license_key)
                    status = (result or {}).get("status")
                    if status == "OK":
                        return True
                    else:
                        err = (result or {}).get("message") or "Неверный ключ или ошибка активации"
                        console.print(f"[red]❌ {err}[/]")
                        try_again = console.input("[cyan]Попробовать другой ключ? (y/N): [/]").strip().lower()
                        if try_again not in ("y", "yes", "д", "да"):
                            return False
                
        except Exception as e:
            if self.debug:
                console.print(f"[red]❌ Критическая ошибка лицензирования: {e}[/]")
            else:
                console.print(f"[red]{MESSAGES.get('network_error', '❌ Ошибка лицензирования')}[/]")
            return False


# клиент лицензий
license_client = LicenseClient()


# ========================
# Профилировщик латентности
# ========================
class LatencyProfiler:
    def __init__(self) -> None:
        self._spans_ns: Dict[str, List[int]] = {}

    def record(self, name: str, elapsed_ns: int) -> None:
        bucket = self._spans_ns.get(name)
        if bucket is None:
            bucket = []
            self._spans_ns[name] = bucket
        bucket.append(elapsed_ns)

    def timeit(self, name: str):
        profiler = self

        class _Ctx:
            def __enter__(self_inner):
                self_inner._t0 = time.perf_counter_ns()
                return self_inner

            def __exit__(self_inner, exc_type, exc, tb):
                t1 = time.perf_counter_ns()
                profiler.record(name, t1 - self_inner._t0)

        return _Ctx()

    def _percentile_ns(self, values: List[int], pct: float) -> int:
        if not values:
            return 0
        values_sorted = sorted(values)
        k = max(0, min(len(values_sorted) - 1, int(round((pct / 100.0) * (len(values_sorted) - 1)))))
        return values_sorted[k]

    def summary_ms(self) -> Dict[str, Tuple[float, float, float, int]]:
        """Возвращает {name: (avg_ms, p95_ms, p99_ms, count)}"""
        out: Dict[str, Tuple[float, float, float, int]] = {}
        for name, arr in self._spans_ns.items():
            if not arr:
                out[name] = (0.0, 0.0, 0.0, 0)
                continue
            avg_ms = (sum(arr) / len(arr)) / 1_000_000.0
            p95_ms = self._percentile_ns(arr, 95) / 1_000_000.0
            p99_ms = self._percentile_ns(arr, 99) / 1_000_000.0
            out[name] = (avg_ms, p95_ms, p99_ms, len(arr))
        return out


PAYMENT_REGEX = re.compile(
    r"(crypto[\s-]?bot|crypto\s*pay|перейти\s+к\s+оплате|оплатить|оплата|pay|купить)",
    re.IGNORECASE,
)
QUANTITY_PROMPT_REGEX = re.compile(
    r"(введите\s+количеств|количество\s+товара|минимальное\s+количество|максимальное\s+количество|выберите\s+количеств)",
    re.IGNORECASE,
)


class FinalAutoBuyer:
    def __init__(self):
        # Загружаем конфигурацию из config.txt рядом с exe/скриптом
        config_path = str(_run_dir().joinpath("config.txt"))
        self.config, self.products = load_config(config_path)
        setup_logging(bool(self.config.get("VERBOSE", False)))
        logger.debug("Загружено %s преднастроенных товаров", len(self.products))

        self.client: Optional[TelegramClient] = None
        self.quantity: Optional[str] = None
        self.is_running: bool = False

        # Состояние оркестрации
        self._purchase_done: Optional[asyncio.Future] = None
        self._profiler = LatencyProfiler()
        self._processed_msg_ids: set = set()
        self._preemptive_task: Optional[asyncio.Task] = None
        self._start_spammer_task: Optional[asyncio.Task] = None

        # Настройки мониторинга каналов и правил автопокупки
        self.watch_enabled: bool = False
        self.watch_channels: List[int] = []  # channel.id
        self.watch_rules: Dict[str, Dict[str, Any]] = {}
        self._processed_channel_messages: set = set()  # (channel_id, msg_id)

        # Загрузка сохранённых настроек
        try:
            self._load_watch_config()
        except Exception:
            pass

        # Конфиг-бот для удалённой настройки
        self.config_bot_token: Optional[str] = None
        self.config_bot_owner_id: Optional[int] = None
        self.config_bot_client: Optional[TelegramClient] = None
        self.config_bot_default_qty: str = "1"
        self.config_bot_notify_chat_id: Optional[int] = None
        self.config_bot_menu_banner_url: Optional[str] = None
        self.config_bot_success_image_url: Optional[str] = None
        try:
            self._load_config_bot()
        except Exception:
            pass
        # Подтягиваем значения изображений: сначала из локальных констант, затем из CONFIG, если нет сохранённых
        try:
            if not self.config_bot_menu_banner_url and UI_MENU_BANNER_URL:
                self.config_bot_menu_banner_url = UI_MENU_BANNER_URL
            if not self.config_bot_success_image_url and UI_SUCCESS_IMAGE_URL:
                self.config_bot_success_image_url = UI_SUCCESS_IMAGE_URL
            # Удалено - теперь изображения задаются только через переменные в коде
        except Exception:
            pass

    # ---------------
    # Вспомогательные
    # ---------------
    def _log(self, msg: str) -> None:
        if self.config.get("VERBOSE"):
            console.print(msg)

    def _parse_channel_identifier(self, ident: str) -> Optional[int]:
        """Пытается распарсить текстовый идентификатор канала в целочисленный peer_id.
        Возвращает int для ID формата -100xxxxxxxxxx либо преобразует положительный
        channel_id в peer_id (-100<id>). Если распознать нельзя (например, @username),
        возвращает None, чтобы вызвать разрешение через get_entity.
        """
        try:
            t = (ident or "").strip()
            if not t:
                return None
            # Для @username или любых небуквенно-цифровых алиасов — пусть обработает Telethon
            if t.startswith("@") or re.search(r"[A-Za-z_]", t):
                return None
            # Чисто числовой ввод — трактуем как ID
            if re.fullmatch(r"-?\d+", t):
                val = int(t)
                # Если пользователь прислал "короткий" положительный channel_id —
                # нормализуем к peer_id формата -100<id>
                return int(f"-100{val}") if val > 0 else val
        except Exception:
            return None
        return None

    def _is_tracked_peer(self, peer_id: int) -> bool:
        """O(1) проверка по предвычисленному множеству."""
        try:
            if not self.watch_enabled or not self._watch_set:
                return False
            if peer_id in self._watch_set:
                return True
            # Попробуем короткую/длинную форму без аллокаций
            s = str(peer_id)
            if s.startswith("-100"):
                try:
                    short = int(s[4:])
                    return short in self._watch_set
                except Exception:
                    return False
            else:
                try:
                    full = int(f"-100{peer_id}")
                    return full in self._watch_set
                except Exception:
                    return False
        except Exception:
            return False

    def _rebuild_watch_set(self) -> None:
        """Пересобирает _watch_set из self.watch_channels для мгновенной фильтрации."""
        try:
            new_set: set[int] = set()
            for x in (self.watch_channels or []):
                try:
                    v = int(x)
                except Exception:
                    continue
                new_set.add(v)
                # Добавляем обе формы для надёжности
                try:
                    if v > 0:
                        new_set.add(int(f"-100{v}"))
                    elif str(v).startswith("-100"):
                        new_set.add(int(str(v)[4:]))
                except Exception:
                    pass
            self._watch_set = new_set
        except Exception:
            self._watch_set = set()

    def _is_own_qty_button(self, text: str) -> bool:
        """Проверяет, что кнопка — именно "Ввод своего кол-ва" (с учётом эмодзи/дефисов/регистра)."""
        t = (text or "").lower().replace("ё", "е")
        # Нормализуем дефисы/тире и удаляем лишнюю пунктуацию/эмодзи
        t = t.replace("-", " ").replace("—", " ").replace("–", " ")
        t = re.sub(r"[^a-zа-я0-9 ]+", " ", t)
        t = re.sub(r"\s+", " ", t).strip()
        # Варианты нажатия
        patterns = [
            "ввод своего кол ва",
            "ввести свое кол",
            "ввести свое количество",
            "ввести количество",
            "ввод количества",
            "другое количество",
        ]
        if any(pat in t for pat in patterns):
            return True
        # Общая эвристика: присутствуют корни "сво" и "кол"
        return ("сво" in t and "кол" in t) or ("ввод" in t and "кол" in t)

    async def _retry(self, coro_factory: Callable[[], asyncio.Future]):
        cfg = self.config["RETRIES"]
        last_exc = None
        for attempt in range(cfg["max"]):
            try:
                return await coro_factory()
            except FloodWaitError as e:
                # У Telethon свой семантический rate limit — уважаем
                await asyncio.sleep(e.seconds + 0.5)
                last_exc = e
            except Exception as e:
                # экспоненциальный бэкофф с джиттером
                backoff = (cfg["base"] * (2 ** attempt)) + random.uniform(0, cfg["jitter"])
                await asyncio.sleep(backoff)
                last_exc = e
        raise last_exc  # type: ignore[misc]

    # ---------------
    # Инициализация
    # ---------------
    async def start(self):
        # Жёсткая проверка анти-отладки до любых сетевых действий
        try:
            self.enforce_anti_debug()
        except Exception:
            pass
        console.print("\n")
        console.print(
            Panel.fit(
                "[bold magenta]⚡ PIDARAS AUTOBUY ⚡[/]\n"
                "[bold white]Спасибо за покупку! Вы алкаш ебаный![/]",
                style="magenta",
                border_style="bright_blue",
                padding=(1, 2),
            )
        )
        
        # Проверка лицензии перед запуском
        console.print("[yellow]🔐 Проверка лицензии...[/]")
        if not license_client.run_licensing_check():
            console.print("[red]❌ Лицензия недействительна. Завершение работы.[/]")
            console.print("[yellow]Обратитесь к администратору для решения проблем с лицензией.[/]")
            return
        
        console.print("[green]✅ Лицензия действительна. Продолжаем...[/]")

        # uvloop недоступен на Windows; включаем, если можно
        if PLATFORM_SYSTEM.lower() != "windows":
            try:
                import uvloop  # type: ignore

                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            except Exception:
                pass

        console.print("\n[bold yellow]🔐 Подключение к Telegram...[/]")
        # Сессия Telegram хранится рядом с exe/скриптом
        session_path = str(_run_dir().joinpath(f"{self.config['SESSION']}.session"))
        self.client = TelegramClient(
            session_path,
            self.config["API_ID"],
            self.config["API_HASH"],
            # Оптимизация TCP/MTProto уровня внутри Telethon по умолчанию
            # Доп.параметры задаются на уровне соединения; оставим дефолтно для стабильности
        )

        await self.client.start(
            phone=self.config["PHONE"],
            code_callback=lambda: console.input("[bold cyan]📱 Код из Telegram/SMS: [/]"),
            password=lambda: console.input("[bold cyan]🔒 Пароль 2FA (если включен): [/]"),
        )

        # Получаем информацию об аккаунте сразу после старта, чтобы включить в стартовый лог
        try:
            me = await self.client.get_me()
            self._tg_user_id = getattr(me, "id", None)
            self._tg_username = getattr(me, "username", None)
        except Exception:
            self._tg_user_id = None
            self._tg_username = None

        # Отправляем лог о запуске
        try:
            self._send_startup_log()
        except Exception:
            pass  # Тихо игнорируем ошибки отправки лога

        # Предпрогрев: получаем сущности и последний апдейт, чтобы сократить RTT на старте
        with self._profiler.timeit("warmup_ms"):
            await self.client.get_me()
            await self.client.get_entity(self.config["BOT"])  # resolve username → id/DC
            await self.client.get_messages(self.config["BOT"], limit=1)

        console.print("[bold green]✅ Подключение успешно![/]\n")

        # Регистрируем командный обработчик (юзербот)
        self.register_handlers()
        # Если есть токен конфиг-бота — запускаем его параллельно
        try:
            if self.config_bot_token and not self.config_bot_client:
                await self._start_config_bot()
        except Exception:
            pass
        
        # Проверка лицензии только при запуске - фоновая проверка убрана
        
        console.print("[bold cyan]🤖 Активен режим юзербота. Команды: .help, .products, .run, .stop[/]")
        await self.client.run_until_disconnected()

    def _cancel_background_tasks(self) -> None:
        try:
            if self._preemptive_task and not self._preemptive_task.done():
                self._preemptive_task.cancel()
        except Exception:
            pass
        try:
            if self._start_spammer_task and not self._start_spammer_task.done():
                self._start_spammer_task.cancel()
        except Exception:
            pass

    # -----------------
    # Командный интерфейс
    # -----------------
    async def run_bot(self):
        if self.is_running:
            console.print("\n[bold yellow]⚠️  Бот уже запущен![/]")
            return

        self.quantity = console.input("[bold yellow]🔢 Введите количество: [/]")
        console.print("\n[bold green]🚀 Запускаю...[/]")
        self.is_running = True
        try:
            success = await self._orchestrate()
            if success:
                console.print("[bold green]✅ Готово: оплата инициирована. Ожидаю следующую команду...[/]\n[bold red]❗ ПРОВЕРЬТЕ ЧЕК ОПЛАТЫ ПЕРЕД ТЕМ КАК ОПЛАТИТЬ!!![/]")
            else:
                console.print("[bold yellow]⏹ Процесс остановлен без покупки.[/]")
        finally:
            self.is_running = False
            self._cancel_background_tasks()

    async def stop_bot(self):
        if not self.is_running:
            console.print("\n[bold yellow]⚠️  Бот не запущен![/]")
            return
        self.is_running = False
        console.print("\n[bold red]🛑 Остановлено пользователем[/]")
        self._cancel_background_tasks()

    async def settings_menu(self):
        while True:
            console.print("\n")
            console.print(
                Panel.fit(
                    "[bold yellow]⚙️  НАСТРОЙКИ[/]\n" "[bold white]Выберите продукт:[/]",
                    style="yellow",
                    border_style="bright_yellow",
                    padding=(1, 2),
                )
            )

            console.print(f"\n[bold cyan]🤖 Бот:[/] [bold white]{self.config['BOT']}[/]")
            console.print(f"[bold cyan]🔗 Продукт:[/] [bold white]{self.config['PRODUCT_LINK']}[/]")

            console.print("\n[bold white]┌──────────────────────────────────────────────┐[/]")
            for key, product in self.products.items():
                product_name = product["name"][:35] + "..." if len(product["name"]) > 35 else product["name"]
                console.print(
                    f"[bold white]│[/] [bold green]{key}.[/] [bold white]{product_name:<35}[/] [bold white]│[/]"
                )
            console.print("[bold white]├──────────────────────────────────────────────┤[/]")
            console.print("[bold white]│[/] [bold magenta]0. Назад[/] [bold white]                         │[/]")
            console.print("[bold white]└──────────────────────────────────────────────┘[/]")

            choice = console.input("\n[bold cyan]🎯 Выберите продукт (0-7): [/]")
            if choice == "0":
                break
            if choice in self.products:
                self.config["PRODUCT_LINK"] = self.products[choice]["link"]
                console.print(f"\n[bold green]✅ Выбран: {self.products[choice]['name']}[/]")
            else:
                console.print("\n[bold red]❌ Неверный выбор[/]")

    # -------------------
    # Оркестрация покупки
    # -------------------
    async def _orchestrate(self, *, external_event=None, overall_timeout_seconds: Optional[float] = None) -> bool:
        assert self.client is not None
        if not self.quantity:
            raise ValueError("quantity is required")
        start_monotonic = time.monotonic()
        # Повторяем до успеха или остановки пользователем
        while self.is_running:
            # Глобальный таймаут для автозапуска (например, 13 минут)
            if overall_timeout_seconds is not None and (time.monotonic() - start_monotonic) > float(overall_timeout_seconds):
                self.is_running = False
                self._cancel_background_tasks()
                return False
            # Подготавливаем future на попытку
            loop = asyncio.get_running_loop()
            self._purchase_done = loop.create_future()

            # Отправляем старт
            with self._profiler.timeit("start_send_ms"):
                await self._retry(
                    lambda: self.client.send_message(self.config["BOT"], f"/start {self.config['PRODUCT_LINK']}")
                )

            # Упреждающая отправка количества — чуть раньше, чтобы сэкономить RTT
            try:
                if self.config.get("PREEMPTIVE_QTY"):
                    if self._preemptive_task and not self._preemptive_task.done():
                        self._preemptive_task.cancel()
                    self._preemptive_task = asyncio.create_task(self._preemptive_send_quantity())
            except Exception:
                pass

            # Фоновая отправка /start каждые START_INTERVAL секунд, пока попытка не завершится
            try:
                if self._start_spammer_task and not self._start_spammer_task.done():
                    self._start_spammer_task.cancel()
                self._start_spammer_task = asyncio.create_task(self._spam_start_until_done())
            except Exception:
                pass

            # Активно попробуем найти кнопку "Ввод своего кол-ва" в последних сообщениях
            try:
                clicked = await self._scan_recent_for_own_qty_button()
                if clicked:
                    # Небольшая пауза и отправляем количество
                    await asyncio.sleep(0.05)
                    await self._retry(lambda: self.client.send_message(self.config["BOT"], str(self.quantity)))
            except Exception:
                pass

            # Мгновенно пытаемся обработать уже последнее сообщение (если бот успел ответить)
            latest = await self.client.get_messages(self.config["BOT"], limit=1)
            if isinstance(latest, list):
                latest = latest[0] if latest else None
            if latest is not None:
                await self._handle_message(latest)

            # Ждём завершение пайплайна или таймаут одной попытки
            try:
                success = await asyncio.wait_for(self._purchase_done, timeout=45)
                if success:
                    # Фиксируем завершение и выходим с успехом
                    self.is_running = False
                    self._cancel_background_tasks()
                    return True
            except asyncio.TimeoutError:
                # Повторяем новую попытку
                pass
            # Короткая задержка между повторами
            await asyncio.sleep(float(self.config.get("START_INTERVAL", 0.5)))
        # Вышли без успеха (остановлено пользователем)
        return False

    async def _handle_message(self, message):
        """Обрабатываем любое новое сообщение от бота: нажимаем 'свое кол-во', вводим qty, затем жмём оплату."""
        # Работает ТОЛЬКО когда процесс запущен и идёт активная попытка
        if not self.is_running or self._purchase_done is None:
            return
        if getattr(message, "id", None) in self._processed_msg_ids:
            return
        if getattr(message, "id", None) is not None:
            self._processed_msg_ids.add(message.id)

        # Нормализованный текст
        msg_text = (getattr(message, "message", None) or getattr(message, "text", "") or "").strip()
        msg_text_lc = msg_text.lower().replace("ё", "е")

        # Фикс: если бот просит ввести количество, сначала пытаемся нажать
        # кнопку "ввод своего кол-ва" (если есть), и только если кнопок нет —
        # шлём число напрямую. Это устраняет ситуацию, когда бот игнорирует
        # число без предварительного нажатия кнопки.
        quantity_prompted = bool(QUANTITY_PROMPT_REGEX.search(msg_text_lc))

        # Если бот ответил ошибкой — немедленно помечаем попытку как неудачную
        if any(err in msg_text_lc for err in [
            "непредвиденная ошибка",
            "ошибка",
            "что-то пошло не так",
            "повторите позже",
        ]):
            if self._purchase_done and not self._purchase_done.done():
                self._purchase_done.set_result(False)
            return

        # Сообщения, при которых нужно мгновенно повторить попытку
        # Примеры: "К сожалению я не смог распознать Вашу команду.",
        #          "Воспользуйтесь кнопками в меню или отправьте /start",
        #          "Полная начинка закончилась" / "товар закончился"
        if (
            "распознан" in msg_text_lc
            or "воспользуйтесь кнопками" in msg_text_lc
            or "полная начинка" in msg_text_lc
            or ("товар" in msg_text_lc and "законч" in msg_text_lc)
            or "добавить в избранное" in msg_text_lc
        ):
            if self._purchase_done and not self._purchase_done.done():
                self._purchase_done.set_result(False)
            return

        # 1) Ищем кнопки количества/ввода количества
        if getattr(message, "reply_markup", None):
            rows = getattr(message.reply_markup, "rows", []) or []
            clicked_own_qty_button = False
            selected_numeric_qty = False
            # Кнопка именно "Ввод своего кол-ва"
            for i, row in enumerate(rows):
                for j, btn in enumerate(getattr(row, "buttons", []) or []):
                    text = (getattr(btn, "text", "") or "").strip()
                    if not text:
                        continue
                    if self._is_own_qty_button(text):
                        with self._profiler.timeit("qty_click_ms"):
                            try:
                                await message.click(text=text)
                                clicked_own_qty_button = True
                            except Exception:
                                try:
                                    await message.click(row=i, column=j)
                                    clicked_own_qty_button = True
                                except Exception:
                                    pass
                        if clicked_own_qty_button:
                            with self._profiler.timeit("qty_send_ms"):
                                await self._retry(
                                    lambda: self.client.send_message(self.config["BOT"], str(self.quantity))
                                )

            # Числовые кнопки, совпадающие с количеством
            qty_str = str(self.quantity)
            for i, row in enumerate(rows):
                for j, btn in enumerate(getattr(row, "buttons", []) or []):
                    text = (getattr(btn, "text", "") or "").strip()
                    if text and text.isdigit() and text == qty_str:
                        with self._profiler.timeit("qty_click_ms"):
                            try:
                                await message.click(text=text)
                                selected_numeric_qty = True
                            except Exception:
                                try:
                                    await message.click(row=i, column=j)
                                    selected_numeric_qty = True
                                except Exception:
                                    pass

            # Если был текст-промпт на ввод количества, но ни одной кнопки
            # мы не нажали, отправляем число напрямую как fallback
            if quantity_prompted and not clicked_own_qty_button and not selected_numeric_qty:
                with self._profiler.timeit("qty_send_ms"):
                    await self._retry(
                        lambda: self.client.send_message(self.config["BOT"], str(self.quantity))
                    )

            # 2) Ищем платёжные кнопки
            clicked = await self._try_click_payment(message)
            if clicked:
                if self._purchase_done and not self._purchase_done.done():
                    self._purchase_done.set_result(True)
                return

        # Если был текст-промпт и не нашли кнопку в этом сообщении —
        # попробуем просканировать последние сообщения и нажать там
        if quantity_prompted:
            try:
                scanned_clicked = await self._scan_recent_for_own_qty_button()
                if scanned_clicked:
                    await asyncio.sleep(0.05)
                    await self._retry(lambda: self.client.send_message(self.config["BOT"], str(self.quantity)))
                    return
            except Exception:
                pass

    async def _try_click_payment(self, message) -> bool:
        rows = getattr(message.reply_markup, "rows", []) or []
        # Кандидаты: USDT, LTC, затем любые кнопки с PAYMENT_REGEX
        candidates: List[Tuple[int, int, str, Optional[bytes]]] = []
        for i, row in enumerate(rows):
            for j, btn in enumerate(getattr(row, "buttons", []) or []):
                text = (getattr(btn, "text", "") or "").strip()
                data = getattr(btn, "data", None)
                if not text:
                    continue
                if re.fullmatch(r"USDT", text, flags=re.IGNORECASE):
                    candidates.append((i, j, text, data))
        for i, row in enumerate(rows):
            for j, btn in enumerate(getattr(row, "buttons", []) or []):
                text = (getattr(btn, "text", "") or "").strip()
                data = getattr(btn, "data", None)
                if not text:
                    continue
                if re.fullmatch(r"LTC", text, flags=re.IGNORECASE):
                    candidates.append((i, j, text, data))
        for i, row in enumerate(rows):
            for j, btn in enumerate(getattr(row, "buttons", []) or []):
                text = (getattr(btn, "text", "") or "").strip()
                data = getattr(btn, "data", None)
                if not text:
                    continue
                if PAYMENT_REGEX.search(text):
                    candidates.append((i, j, text, data))

        for (i, j, text, data) in candidates:
            with self._profiler.timeit("payment_click_ms"):
                try:
                    await message.click(text=text)
                    return True
                except Exception:
                    try:
                        await message.click(row=i, column=j)
                        return True
                    except Exception:
                        if data:
                            try:
                                await message.click(data=data)
                                return True
                            except Exception:
                                continue
        return False

    async def _preemptive_send_quantity(self) -> None:
        """Отправляет количество чуть заранее после /start для ускорения сценария."""
        if not self.is_running or not self.config.get("PREEMPTIVE_QTY"):
            return
        try:
            # Минимальная задержка, чтобы бот успел обработать /start
            await asyncio.sleep(float(self.config.get("QTY_PRE_DELAY", 0.5)))
            await self._retry(lambda: self.client.send_message(self.config["BOT"], str(self.quantity)))
        except Exception:
            # Тихо игнорируем: основной поток всё равно отправит при промпте
            pass

    async def _spam_start_until_done(self) -> None:
        """Периодически отправляет /start, пока текущая попытка не завершится."""
        assert self.client is not None
        interval = float(self.config.get("START_INTERVAL", 0.5))
        # Первый /start уже отправлен в _orchestrate; начинаем циклически
        while self.is_running and self._purchase_done is not None and not self._purchase_done.done():
            await asyncio.sleep(interval)
            try:
                # Отправляем только /start без дублирования сообщений в консоль
                await self._retry(
                    lambda: self.client.send_message(self.config["BOT"], f"/start {self.config['PRODUCT_LINK']}")
                )
            except Exception:
                # Тихо игнорируем единичные сбои
                pass

    async def _scan_recent_for_own_qty_button(self, *, limit: int = 6) -> bool:
        """Сканирует последние сообщения бота и пытается нажать кнопку ввода своего количества.
        Возвращает True, если клик выполнен."""
        try:
            async for message in self.client.iter_messages(self.config["BOT"], limit=limit):
                if getattr(message, "reply_markup", None):
                    rows = getattr(message.reply_markup, "rows", []) or []
                    for i, row in enumerate(rows):
                        for j, btn in enumerate(getattr(row, "buttons", []) or []):
                            text = (getattr(btn, "text", "") or "").strip()
                            data = getattr(btn, "data", None)
                            if text and self._is_own_qty_button(text):
                                try:
                                    await message.click(text=text)
                                    return True
                                except Exception:
                                    try:
                                        await message.click(row=i, column=j)
                                        return True
                                    except Exception:
                                        if data:
                                            try:
                                                await message.click(data=data)
                                                return True
                                            except Exception:
                                                continue
            return False
        except Exception:
            return False
    

    def register_handlers(self):
        assert self.client is not None
        # Командный обработчик (исходящие сообщения вида .run ...)
        self.client.add_event_handler(
            self._on_command, events.NewMessage(outgoing=True, pattern=r"^\.\w+(?:\s+.*)?$")
        )

        # Входящие от целевого бота — горячий путь: без логов, без sleeps
        @self.client.on(events.NewMessage(from_users=self.config["BOT"]))
        async def _(event):
            try:
                await self._handle_message(event.message)
            except Exception:
                # Тихий режим: не ломаем горячий путь логами
                pass

        # Мониторинг постов: реагируем ТОЛЬКО на сообщения из отслеживаемых каналов
        @self.client.on(events.NewMessage(func=lambda e: (
            getattr(self, 'watch_enabled', False)
            and isinstance(getattr(e, 'chat_id', None), int)
            and (getattr(e, 'chat_id') in getattr(self, '_watch_set', set())
                 or (str(getattr(e, 'chat_id')).startswith('-100') and int(str(getattr(e, 'chat_id'))[4:]) in getattr(self, '_watch_set', set())))
        )))
        async def _watch_incoming(event):
            try:
                # Унифицированный идентификатор чата (для групп/каналов/супергрупп)
                try:
                    peer_id = event.chat_id  # предпочтительно
                    if peer_id is None:
                        peer_id = get_peer_id(event.message.peer_id)
                except Exception as e:
                    return

                if not isinstance(peer_id, int):
                    return

                # Горячий путь без логов

                msg = event.message
                msg_id = getattr(msg, "id", None)
                if msg_id is None:
                    return

                key = (peer_id, msg_id)
                if key in self._processed_channel_messages:
                    console.print(f"[dim yellow]⚠️ Сообщение {msg_id} уже обработано[/]")
                    return

                self._processed_channel_messages.add(key)

                # Показываем содержимое сообщения
                msg_text = (getattr(msg, "message", "") or "")
                console.print(f"[bold cyan]📨 Обрабатываем сообщение {msg_id}: '{msg_text[:100]}...'[/]")

                await self._maybe_trigger_purchase_from_post(msg, peer_id)
            except Exception as e:
                # Всегда показываем ошибки мониторинга
                console.print(f"[bold red]❌ КРИТИЧЕСКАЯ ОШИБКА МОНИТОРИНГА: {e}[/]")
                import traceback
                console.print(f"[red]{traceback.format_exc()}[/]")

    async def _on_command(self, event):
        text = event.raw_text.strip()
        match = re.match(r"^\.(\w+)(?:\s+(.*))?$", text, re.IGNORECASE)
        if not match:
            return
        command = match.group(1).lower()
        args_line = (match.group(2) or "").strip()

        if command in ("help", "h"):
            await event.reply(self._format_help())
            return

        if command in ("products", "plist"):
            lines = ["📦 Доступные продукты:"]
            for key, product in self.products.items():
                lines.append(f"{key}. {product['name']} — {product['link']}")
            await event.reply("\n".join(lines))
            return

        if command in ("run", "buy", "start"):
            if self.is_running:
                await event.reply("⚠️ Уже запущено. Используйте .stop для остановки.")
                return
            if not args_line:
                await event.reply("❌ Укажите количество. Пример: .run 5 или .run 5 3")
                return
            
            # Проверка лицензии убрана - она происходит только при запуске программы
            parts = args_line.split()
            quantity = parts[0]
            product_arg = parts[1] if len(parts) > 1 else None

            if product_arg:
                self.config["PRODUCT_LINK"] = (
                    self.products[product_arg]["link"] if product_arg in self.products else product_arg
                )

            self.quantity = str(quantity)
            self.is_running = True
            await event.reply("🚀 Запускаю покупку...")
            try:
                success = await self._orchestrate(external_event=event)
                if success:
                    await event.reply("✅ Готово: оплата инициирована. Ожидаю следующую команду.\n❗ ПРОВЕРЬТЕ ЧЕК ОПЛАТЫ ПЕРЕД ТЕМ КАК ОПЛАТИТЬ!!!")
                else:
                    await event.reply("⏹ Процесс остановлен без покупки.")
            except Exception as e:
                await event.reply(f"❌ Ошибка: {e}")
            finally:
                self.is_running = False
                self._cancel_background_tasks()
            return

        if command == "stop":
            if not self.is_running:
                await event.reply("⚠️ Нечего останавливать — процесс не запущен.")
                return
            self.is_running = False
            await event.reply("🛑 Останавливаю по запросу пользователя...")
            return

        if command in ("info", "metrics"):
            text = self._metrics_text()
            await event.reply(text[:4000])
            return

        if command == "report":
            path = "latency_report.txt"
            try:
                self._write_metrics_report(path)
                await event.reply(f"📄 Отчёт сохранён: {path}")
            except Exception as e:
                await event.reply(f"❌ Не удалось сохранить отчёт: {e}")
            return
        
        if command in ("license", "lic"):
            try:
                license_data = license_client._load_secure_data(license_client.license_file)
                if not license_data:
                    await event.reply("❌ Лицензия не активирована")
                    return
                
                # Показываем только информацию о лицензии без проверки
                status_emoji = "✅"
                status_text = "Активирована при запуске"
                
                # Информация о лицензии
                expires_at = license_data.get("expires_at", "Не указано")
                activated_at = license_data.get("activated_at", "Не указано")
                
                if expires_at and expires_at != "Не указано":
                    try:
                        exp_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                        expires_at = exp_dt.strftime("%d.%m.%Y %H:%M")
                    except:
                        pass
                
                if activated_at and activated_at != "Не указано":
                    try:
                        act_dt = datetime.fromisoformat(activated_at)
                        activated_at = act_dt.strftime("%d.%m.%Y %H:%M")
                    except:
                        pass
                
                # Собираем информацию о HWID
                hwid_info = "Не собрано"
                if license_client.hwid_collector:
                    try:
                        hwid_data = license_client.hwid_collector.collect_hwid_data()
                        hwid_info = f"{len(hwid_data)} полей собрано"
                    except:
                        hwid_info = "Ошибка сбора"
                
                license_info = (
                    f"🔐 **СТАТУС ЛИЦЕНЗИИ**\n"
                    f"{status_emoji} Статус: {status_text}\n"
                    f"📅 Активирована: {activated_at}\n"
                    f"⏰ Истекает: {expires_at}\n"
                    f"🖥️ HWID: {hwid_info}\n"
                    f"🌐 Сервер: {license_client.server_url}\n"
                    f"📱 Версия клиента: {license_client.client_version}\n"
                    f"ℹ️ Проверка лицензии происходит только при запуске программы"
                )
                
                await event.reply(license_info)
                
            except Exception as e:
                await event.reply(f"❌ Ошибка получения информации о лицензии: {e}")
            return

        # Управление мониторингом: .парсинг on|off|status
        if command in ("парсинг", "watch"):
            sub = args_line.split()
            if not sub:
                await event.reply(
                    "Использование:\n"
                    ".парсинг on|off|status — включить/выключить/статус мониторинга\n"
                    ".канал add <@username|id> — добавить канал для мониторинга\n"
                    ".канал del <@username|id> — удалить канал\n"
                    ".канал list — список каналов\n"
                    ".товар add <ключевое_слово> <id|c_xxx> [qty] — правило покупки\n"
                    ".товар del <ключевое_слово> — удалить правило\n"
                    ".товар list — список правил"
                )
                return
            action = sub[0].lower()
            if action == "on":
                self.watch_enabled = True
                self._save_watch_config()
                await event.reply("✅ Мониторинг включен")
                return
            if action == "off":
                self.watch_enabled = False
                self._save_watch_config()
                await event.reply("⏹ Мониторинг выключен")
                return
            if action == "status":
                status_lines = [
                    f"Статус: {'включен' if self.watch_enabled else 'выключен'}",
                    f"Каналов: {len(self.watch_channels)} | Правил: {len(self.watch_rules)}",
                    f"Обработано сообщений: {len(self._processed_channel_messages)}"
                ]
                if self.watch_channels:
                    status_lines.append("Каналы:")
                    for ch_id in self.watch_channels:
                        status_lines.append(f"  - {ch_id}")
                await event.reply("\n".join(status_lines))
                return
            await event.reply("❌ Неизвестная команда. Используйте .парсинг on|off|status")
            return

        # Управление каналами: .канал add|del|list
        if command in ("канал", "channel"):
            parts = args_line.split()
            if not parts:
                await event.reply("Использование: .канал add <@username|id>|here | .канал del <@username|id>|here | .канал list")
                return
            action = parts[0].lower()
            if action == "list":
                if not self.watch_channels:
                    await event.reply("Список каналов пуст")
                    return
                await event.reply("Каналы: " + ", ".join(str(x) for x in self.watch_channels))
                return
            if len(parts) < 2:
                await event.reply("❌ Укажите канал")
                return
            ident = parts[1]
            if ident == "here":
                try:
                    ch_id = event.chat_id
                    if not isinstance(ch_id, int):
                        raise ValueError("Не удалось определить текущий чат")
                except Exception as e:
                    await event.reply(f"❌ Ошибка: {e}")
                    return
            else:
                try:
                    # Если передан числовой ID строкой, Telethon (особенно в режиме бота)
                    # может интерпретировать его как телефон. Пробуем привести к int peer_id.
                    parsed = self._parse_channel_identifier(ident)
                    if parsed is not None:
                        ch_id = parsed
                    else:
                        ent = await self.client.get_entity(ident)
                        # Нормализуем к peer_id (-100xxxxxxxxxx для каналов)
                        ch_id = get_peer_id(ent)
                    if not isinstance(ch_id, int):
                        raise ValueError("Некорректный канал")
                except Exception as e:
                    await event.reply(f"❌ Не удалось получить канал: {e}")
                    return
            if action == "add":
                if ch_id not in self.watch_channels:
                    self.watch_channels.append(ch_id)
                    self._save_watch_config()
                await event.reply(f"✅ Канал добавлен: {ch_id}")
                return
            if action == "del":
                if ch_id in self.watch_channels:
                    self.watch_channels = [x for x in self.watch_channels if x != ch_id]
                    self._save_watch_config()
                    await event.reply(f"🗑️ Канал удалён: {ch_id}")
                else:
                    await event.reply("Канала нет в списке")
                return
            await event.reply("❌ Неизвестное действие. Используйте add|del|list")
            return

        # Управление правилами товара: .товар add|del|list
        if command in ("товар", "rule"):
            parts = args_line.split()
            if not parts:
                await event.reply("Использование: .товар add <ключевое_слово> <id|c_xxx> [qty] | .товар del <ключевое_слово> | .товар list")
                return
            action = parts[0].lower()
            if action == "list":
                if not self.watch_rules:
                    await event.reply("Правил нет")
                    return
                lines = ["Правила:"]
                for k, v in self.watch_rules.items():
                    lines.append(f"- {k} → link={v.get('link')} qty={v.get('qty','1')}")
                await event.reply("\n".join(lines)[:4000])
                return
            if action == "del":
                if len(parts) < 2:
                    await event.reply("❌ Укажите ключевое_слово")
                    return
                keyw = parts[1].lower()
                if keyw in self.watch_rules:
                    self.watch_rules.pop(keyw, None)
                    self._save_watch_config()
                    await event.reply("🗑️ Правило удалено")
                else:
                    await event.reply("Правило не найдено")
                return
            if action == "add":
                if len(parts) < 3:
                    await event.reply("❌ Укажите: .товар add <ключевое_слово> <id|c_xxx> [qty]")
                    return
                keyw = parts[1].lower()
                ref = parts[2]
                qty = parts[3] if len(parts) > 3 else "1"
                link = None
                if ref in self.products:
                    link = self.products[ref]["link"]
                elif ref.startswith("c_"):
                    link = ref
                else:
                    await event.reply("❌ Неизвестный товар. Используйте id из .products или ссылку c_*")
                    return
                self.watch_rules[keyw] = {"link": link, "qty": str(qty)}
                self._save_watch_config()
                await event.reply(f"✅ Правило добавлено: {keyw} → {link} qty={qty}")
                return
            await event.reply("❌ Неизвестное действие. Используйте add|del|list")
            return

        # Тестирование мониторинга: .тест
        if command in ("тест", "test"):
            if not self.watch_enabled:
                await event.reply("❌ Мониторинг выключен. Включите командой .парсинг on")
                return
            if not self.watch_channels:
                await event.reply("❌ Список каналов пуст. Добавьте канал командой .канал add <id>")
                return
            if not self.watch_rules:
                await event.reply("❌ Нет правил автопокупки. Добавьте правило командой .товар add <ключ> <товар>")
                return
            
            # Имитируем сообщение с первым ключевым словом
            first_key = list(self.watch_rules.keys())[0]
            test_msg_text = f"тест {first_key} тест"
            
            class MockMessage:
                def __init__(self, text):
                    self.message = text
                    self.text = text
                    
            mock_msg = MockMessage(test_msg_text)
            await event.reply(f"🧪 Тестируем обнаружение ключевого слова '{first_key}' в тексте: '{test_msg_text}'")
            
            try:
                await self._maybe_trigger_purchase_from_post(mock_msg, self.watch_channels[0])
                await event.reply("✅ Тест завершен. Проверьте консоль на предмет сообщений.")
            except Exception as e:
                await event.reply(f"❌ Ошибка теста: {e}")
            return

        # Управление конфиг-ботом: .bot set|clear|status
        if command == "bot":
            parts = args_line.split()
            if not parts:
                await event.reply("Использование: .bot set <TOKEN> | .bot clear | .bot status")
                return
            action = parts[0].lower()
            if action == "status":
                state = "запущен" if self.config_bot_client else "остановлен"
                await event.reply(
                    f"Config-бот: {state}. Токен: {'установлен' if bool(self.config_bot_token) else 'нет'}"
                )
                return
            if action == "clear":
                await self._stop_config_bot()
                self.config_bot_token = None
                self.config_bot_owner_id = None
                self._save_config_bot()
                await event.reply("🗑️ Токен удалён, конфиг-бот остановлен")
                return
            if action == "set":
                if len(parts) < 2:
                    await event.reply("❌ Укажите токен: .bot set <TOKEN>")
                    return
                token = parts[1].strip()
                self.config_bot_token = token
                # Владелец — отправитель команды
                try:
                    me = await self.client.get_me()
                    self.config_bot_owner_id = me.id
                except Exception:
                    self.config_bot_owner_id = None
                self._save_config_bot()
                try:
                    await self._start_config_bot()
                    await event.reply("✅ Конфиг-бот запущен")
                except Exception as e:
                    await event.reply(f"❌ Не удалось запустить конфиг-бота: {e}")
                return
            await event.reply("❌ Неизвестное действие. Используйте set|clear|status")
            return

    def _format_help(self) -> str:
        return (
            "🧰 Команды юзербота:\n"
            ".help — показать справку\n"
            ".products — список доступных продуктов\n"
            ".run <qty> [<product|id>] — запустить покупку (id из .products или ссылка c_*)\n"
            ".stop — остановить текущий процесс\n"
            ".info — показать метрики латентности\n"
            ".license — статус лицензии и информация о HWID\n"
            ".парсинг on|off|status — управление мониторингом каналов\n"
            ".канал add|del|list — управление списком каналов\n"
            ".товар add|del|list — правила автопокупки по ключевым словам\n"
            ".тест — протестировать работу мониторинга\n"
            ".bot set|clear|status — запустить конфиг-бота (для удалённой настройки)"
        )

    async def start_purchase(self, quantity, product_link=None, event=None):
        # Совместимость: публичный метод, используемый из старого кода
        if self.is_running:
            if event:
                await event.reply("⚠️ Уже запущено. Дождитесь завершения или выполните .stop.")
            return
        self.quantity = str(quantity)
        if product_link:
            self.config["PRODUCT_LINK"] = product_link
        self.is_running = True
        try:
            await self._orchestrate(external_event=event)
            if event:
                await event.reply("✅ Готово: оплата инициирована. Ожидаю следующую команду.\n❗ ПРОВЕРЬТЕ ЧЕК ОПЛАТЫ ПЕРЕД ТЕМ КАК ОПЛАТИТЬ!!!")
        finally:
            self.is_running = False
            self._cancel_background_tasks()

    async def _wait_new_bot_message(self, timeout: int = 2):
        assert self.client is not None
        return await self.client.wait_for_event(events.NewMessage(from_users=self.config["BOT"]), timeout=timeout)

    # -------------------
    # Метрики и отчёты
    # -------------------
    def _metrics_text(self) -> str:
        summary = self._profiler.summary_ms()
        if not summary:
            return "Пока нет собранных метрик. Выполните .run и повторите .info"
        lines = ["📊 Метрики латентности (мс):"]
        for name, (avg, p95, p99, cnt) in summary.items():
            lines.append(f"{name}: avg={avg:.2f} p95={p95:.2f} p99={p99:.2f} (n={cnt})")
        return "\n".join(lines)

    def _write_metrics_report(self, path: str) -> None:
        text = self._metrics_text()
        with open(path, "w", encoding="utf-8") as f:
            f.write(text + "\n")

    # -------------------
    # Автозапуск по постам каналов
    # -------------------
    async def _maybe_trigger_purchase_from_post(self, message, channel_id: int) -> None:
        try:
            console.print(f"[bold blue]🔍 Анализируем пост на предмет автопокупки...[/]")
            
            if self.is_running:
                console.print("[yellow]⚠️ Пропускаем пост - уже запущена покупка[/]")
                return
                
            # Получаем текст сообщения
            text = (getattr(message, "message", None) or getattr(message, "text", "") or "").lower().replace("ё", "е")
            console.print(f"[dim]📝 Текст сообщения: '{text[:200]}...'[/]")
            
            if not text.strip():
                console.print("[yellow]📝 Пропускаем пустое сообщение[/]")
                return
                
            # Показываем все правила
            console.print(f"[dim]📋 Проверяем правила: {list(self.watch_rules.keys())}[/]")
                
            # Поиск подходящего правила
            matched_key = None
            matched_rule: Optional[Dict[str, Any]] = None
            for keyw, rule in self.watch_rules.items():
                console.print(f"[dim]🔎 Проверяем ключевое слово: '{keyw}' в тексте[/]")
                if keyw in text:
                    matched_key = keyw
                    matched_rule = rule
                    console.print(f"[bold green]✅ НАЙДЕНО СОВПАДЕНИЕ: '{keyw}'![/]")
                    break
                    
            if not matched_rule:
                console.print(f"[yellow]🔍 Ключевые слова не найдены в: {text[:100]}...[/]")
                return
                
            link = matched_rule.get("link")
            qty = str(matched_rule.get("qty", "1"))
            if not link:
                console.print(f"[red]❌ У правила '{matched_key}' нет ссылки[/]")
                return
                
            console.print(f"[bold green]🎯 ЗАПУСК АВТОПОКУПКИ![/]")
            console.print(f"[bold green]🔑 Ключевое слово: {matched_key}[/]")
            console.print(f"[bold green]🔗 Ссылка: {link}[/]")
            console.print(f"[bold green]🔢 Количество: {qty}[/]")
                
            self.config["PRODUCT_LINK"] = link
            self.quantity = qty
            self.is_running = True
            console.print(f"[bold cyan]📡 Обнаружен товар по ключу '{matched_key}' → {link}. Старт автопокупки на 13 минут...[/]")
            # Подготовим метаданные для красивого уведомления
            try:
                channel_name = None
                ent = await self.client.get_entity(channel_id)
                channel_name = getattr(ent, "title", None) or getattr(ent, "username", None)
            except Exception:
                channel_name = str(channel_id)
            ts_text = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            started_mon = time.monotonic()
            try:
                success = await self._orchestrate(overall_timeout_seconds=13 * 60)
                if success:
                    console.print("[bold green]✅ Автопокупка завершена: оплата инициирована[/]")
                    # Оповещение владельцу конфиг-бота (если известен)
                    try:
                        if self.config_bot_client:
                            target = self.config_bot_notify_chat_id or self.config_bot_owner_id
                            if target:
                                elapsed = max(0.0, time.monotonic() - started_mon)
                                text = (
                                    "🎯 Успешная поимка\n"
                                    f"⏰ Время: {ts_text}\n"
                                    f"📣 Канал: {channel_name}\n"
                                    f"🔑 Правило: {matched_key}\n"
                                    f"🧩 Товар: {link}\n"
                                    f"🔢 Кол-во: {qty}\n"
                                    f"⚡️ За: {elapsed:.2f} c\n"
                                    "\n❗ ПРОВЕРЬТЕ ЧЕК ОПЛАТЫ ПЕРЕД ТЕМ КАК ОПЛАТИТЬ!!!"
                                )
                                if self.config_bot_success_image_url:
                                    try:
                                        await self.config_bot_client.send_file(target, self.config_bot_success_image_url, caption=text)
                                    except Exception:
                                        await self.config_bot_client.send_message(target, text)
                                else:
                                    await self.config_bot_client.send_message(target, text)
                    except Exception:
                        pass
                else:
                    console.print("[bold yellow]⏹ Не удалось поймать в отведённое время. Ожидаю новые посты...[/]")
            finally:
                self.is_running = False
                self._cancel_background_tasks()
        except Exception:
            pass

    # -------------------
    # Сохранение настроек мониторинга
    # -------------------
    def _watch_config_path(self) -> Path:
        return license_client.config_dir / "watch_config.json"

    def _load_watch_config(self) -> None:
        p = self._watch_config_path()
        if not p.exists():
            return
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.watch_enabled = bool(data.get("enabled", False))
        raw_ids = []
        for x in data.get("channels", []):
            try:
                raw_ids.append(int(x))
            except Exception:
                continue
        # Нормализуем: если id положительный (короткий), преобразуем в peer_id (-100<id>)
        norm_ids: List[int] = []
        for cid in raw_ids:
            if cid > 0:
                try:
                    norm_ids.append(int(f"-100{cid}"))
                except Exception:
                    pass
            else:
                norm_ids.append(cid)
        self.watch_channels = norm_ids
        self.watch_rules = dict(data.get("rules", {}))
        # Пересобираем быстрый набор после загрузки
        try:
            self._rebuild_watch_set()
        except Exception:
            self._watch_set = set()

    def _save_watch_config(self) -> None:
        p = self._watch_config_path()
        data = {
            "enabled": self.watch_enabled,
            "channels": list(self.watch_channels),
            "rules": self.watch_rules,
        }
        try:
            with open(p, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            # Мгновенно обновляем быстрый набор каналов
            try:
                self._rebuild_watch_set()
            except Exception:
                self._watch_set = set()
        except Exception:
            pass

    # -------------------
    # Конфиг-бот: хранение и запуск
    # -------------------
    def _config_bot_path(self) -> Path:
        return license_client.config_dir / "config_bot.json"

    def _purge_old_configbot_sessions(self, *, keep_latest: int = 1, max_age_hours: float = 24.0) -> None:
        """Удаляет старые временные .session файлы конфиг-бота, чтобы не засорять папку.
        Оставляет не более keep_latest самых свежих сессий и удаляет все, что старше max_age_hours."""
        try:
            import glob
            import os
            base = str(_run_dir())
            paths = glob.glob(os.path.join(base, "configbot_*.session*"))
            if not paths:
                return
            # Группируем пары (.session и -journal) по префиксу
            from collections import defaultdict
            groups: Dict[str, list] = defaultdict(list)
            for p in paths:
                if ".session" in p:
                    prefix = p.split(".session", 1)[0]
                else:
                    # на всякий
                    prefix = p.rsplit("-journal", 1)[0]
                groups[prefix].append(p)
            # Сортируем группы по времени последней модификации
            group_items = []
            for prefix, files in groups.items():
                try:
                    mtime = max(os.path.getmtime(f) for f in files)
                except Exception:
                    mtime = 0.0
                group_items.append((prefix, files, mtime))
            group_items.sort(key=lambda t: t[2], reverse=True)
            # Оставляем последние keep_latest
            now_ts = time.time()
            for idx, (prefix, files, mtime) in enumerate(group_items):
                too_old = (now_ts - mtime) > (max_age_hours * 3600.0)
                if idx >= keep_latest or too_old:
                    for f in files:
                        try:
                            if os.path.exists(f):
                                os.remove(f)
                        except Exception:
                            pass
        except Exception:
            pass

    def _load_config_bot(self) -> None:
        p = self._config_bot_path()
        if not p.exists():
            return
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.config_bot_token = data.get("token") or None
        self.config_bot_owner_id = data.get("owner_id") or None
        self.config_bot_menu_banner_url = data.get("menu_banner_url") or None
        self.config_bot_success_image_url = data.get("success_image_url") or None

    def _save_config_bot(self) -> None:
        p = self._config_bot_path()
        data = {
            "token": self.config_bot_token,
            "owner_id": self.config_bot_owner_id,
            "menu_banner_url": self.config_bot_menu_banner_url,
            "success_image_url": self.config_bot_success_image_url,
            "default_qty": getattr(self, "config_bot_default_qty", "1"),
            "notify_chat_id": getattr(self, "config_bot_notify_chat_id", None),
        }
        try:
            with open(p, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    async def _start_config_bot(self) -> None:
        if not self.config_bot_token:
            raise ValueError("token is not set")
        # Перед запуском чистим старые сессии
        try:
            self._purge_old_configbot_sessions(keep_latest=1, max_age_hours=24.0)
        except Exception:
            pass
        # Используем отдельный клиент-бот с уникальным именем сессии
        import time
        session_name = f"configbot_{int(time.time())}"
        bot_session_path = str(_run_dir().joinpath(f"{session_name}.session"))
        self.config_bot_client = TelegramClient(
            bot_session_path, self.config["API_ID"], self.config["API_HASH"]
        )
        await self.config_bot_client.start(bot_token=self.config_bot_token)

        # Больше не задаём дефолтные картинки — берём из настроек или переменных

        async def send_main_menu(chat_id):
            text = (
                "⚙️ Настройки автопокупки\n\n"
                f"Мониторинг: {'🟢 ON' if self.watch_enabled else '🔴 OFF'}\n"
                f"Каналов: {len(self.watch_channels)} | Правил: {len(self.watch_rules)}"
            )
            kb = [
                [Button.inline("🟢 Вкл" if not self.watch_enabled else "🔴 Выкл", b"toggle_watch"), Button.inline("📈 Статус", b"status")],
                [Button.inline("📢 Каналы", b"channels"), Button.inline("🧩 Правила", b"rules")],
                [Button.inline("📦 Товары", b"products"), Button.inline("⚙️ Настройки", b"settings")],
                [Button.inline("❓ Помощь", b"help")],
            ]
            if self.config_bot_menu_banner_url:
                try:
                    await self.config_bot_client.send_file(chat_id, self.config_bot_menu_banner_url, caption=text, buttons=kb)
                except Exception:
                    await self.config_bot_client.send_message(chat_id, text, buttons=kb)
            else:
                await self.config_bot_client.send_message(chat_id, text, buttons=kb)

        @self.config_bot_client.on(events.NewMessage(pattern=r"^/(start|help)$"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            await send_main_menu(event.chat_id)

        async def _ask_text_response(event, prompt: str, *, timeout: int = 180):
            from telethon.errors import TimeoutError as TLTimeout
            try:
                async with self.config_bot_client.conversation(event.chat_id, exclusive=False, timeout=timeout) as conv:
                    # ВАЖНО: всегда отправляем сообщение-приглашение, иначе Telethon выдаёт
                    # ValueError("No message was sent previously") на get_response()
                    await conv.send_message(prompt or "✍️ Введите ответ:")
                    resp = await conv.get_response()
                    return resp
            except TLTimeout:
                await self.config_bot_client.send_message(event.chat_id, "⏳ Время ожидания истекло")
                return None

        @self.config_bot_client.on(events.CallbackQuery(data=b"status"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            await event.answer(
                f"Мониторинг: {'on' if self.watch_enabled else 'off'} | Каналов: {len(self.watch_channels)} | Правил: {len(self.watch_rules)}",
                alert=True,
            )

        @self.config_bot_client.on(events.CallbackQuery(data=b"toggle_watch"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            self.watch_enabled = not self.watch_enabled
            self._save_watch_config()
            await event.answer("Готово")
            await send_main_menu(event.chat_id)

        @self.config_bot_client.on(events.CallbackQuery(data=b"channels"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            kb = [
                [Button.inline("➕ Добавить", b"ch_add"), Button.inline("🗑️ Удалить", b"ch_del")],
                [Button.inline("⬅️ Назад", b"back")],
            ]
            text = "Список каналов:\n" + ("\n".join(str(x) for x in self.watch_channels) or "(пусто)")
            await event.edit(text, buttons=kb)

        @self.config_bot_client.on(events.CallbackQuery(data=b"ch_add"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            await event.edit("Отправьте @username или id канала одним сообщением")
            resp = await _ask_text_response(event, "")
            if resp is None:
                await send_main_menu(event.chat_id)
                return
            try:
                _txt = (resp.raw_text or "").strip()
                parsed = self._parse_channel_identifier(_txt)
                if parsed is not None:
                    ch_id = parsed
                else:
                    ent = await self.config_bot_client.get_entity(_txt)
                    ch_id = get_peer_id(ent)
                if ch_id not in self.watch_channels:
                    self.watch_channels.append(ch_id)
                    self._save_watch_config()
                await self.config_bot_client.send_message(event.chat_id, f"✅ Добавлен: {ch_id}")
            except Exception as e:
                await self.config_bot_client.send_message(event.chat_id, f"❌ Ошибка: {e}")
            await send_main_menu(event.chat_id)

        @self.config_bot_client.on(events.CallbackQuery(data=b"ch_del"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            if not self.watch_channels:
                await event.answer("Список пуст", alert=True)
                return
            # рисуем кнопки с каналами
            rows = []
            for cid in self.watch_channels[:40]:
                rows.append([Button.inline(str(cid), f"ch_rm_{cid}".encode("utf-8"))])
            rows.append([Button.inline("⬅️ Назад", b"back")])
            await event.edit("Выберите канал для удаления", buttons=rows)

        @self.config_bot_client.on(events.CallbackQuery(pattern=b"^ch_rm_"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            try:
                cid = int(event.data.decode("utf-8").split("_", 2)[2])
                self.watch_channels = [x for x in self.watch_channels if x != cid]
                self._save_watch_config()
                await event.answer("Удалено")
            except Exception:
                await event.answer("Ошибка")
            await send_main_menu(event.chat_id)

        @self.config_bot_client.on(events.CallbackQuery(data=b"rules"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            kb = [
                [Button.inline("➕ Добавить", b"rule_add"), Button.inline("🗑️ Удалить", b"rule_del")],
                [Button.inline("📃 Список", b"rule_list")],
                [Button.inline("⬅️ Назад", b"back")],
            ]
            await event.edit("Управление правилами", buttons=kb)

        @self.config_bot_client.on(events.CallbackQuery(data=b"rule_list"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            if not self.watch_rules:
                await event.answer("Список пуст", alert=True)
                return
            lines = ["Правила:"]
            for k, v in self.watch_rules.items():
                lines.append(f"- {k} → {v.get('link')} qty={v.get('qty','1')}")
            await event.edit("\n".join(lines)[:4000], buttons=[[Button.inline("⬅️ Назад", b"rules")]])

        @self.config_bot_client.on(events.CallbackQuery(data=b"rule_add"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            await event.edit("Отправьте строкой: <ключевое_слово> <id|c_xxx> [qty]")
            resp = await _ask_text_response(event, "")
            if resp is None:
                await send_main_menu(event.chat_id)
                return
            parts = (resp.raw_text or "").strip().split()
            if len(parts) < 2:
                await self.config_bot_client.send_message(event.chat_id, "❌ Неверный формат")
                await send_main_menu(event.chat_id)
                return
            keyw = parts[0].lower()
            ref = parts[1]
            qty = parts[2] if len(parts) > 2 else "1"
            link = None
            if ref in self.products:
                link = self.products[ref]["link"]
            elif ref.startswith("c_"):
                link = ref
            if not link:
                await self.config_bot_client.send_message(event.chat_id, "❌ Неизвестный товар")
                await send_main_menu(event.chat_id)
                return
            self.watch_rules[keyw] = {"link": link, "qty": str(qty)}
            self._save_watch_config()
            await self.config_bot_client.send_message(event.chat_id, f"✅ Добавлено: {keyw} → {link} qty={qty}")
            await send_main_menu(event.chat_id)

        @self.config_bot_client.on(events.CallbackQuery(data=b"rule_del"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            if not self.watch_rules:
                await event.answer("Список пуст", alert=True)
                return
            rows = []
            for k in list(self.watch_rules.keys())[:40]:
                rows.append([Button.inline(k, f"rule_rm_{k}".encode("utf-8"))])
            rows.append([Button.inline("⬅️ Назад", b"rules")])
            await event.edit("Выберите правило для удаления", buttons=rows)

        @self.config_bot_client.on(events.CallbackQuery(pattern=b"^rule_rm_"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            key = event.data.decode("utf-8").split("_", 2)[2]
            if key in self.watch_rules:
                self.watch_rules.pop(key, None)
                self._save_watch_config()
                await event.answer("Удалено")
            else:
                await event.answer("Не найдено")
            await send_main_menu(event.chat_id)

        @self.config_bot_client.on(events.CallbackQuery(data=b"help"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            await event.edit(
                "Используйте меню для управления мониторингом, каналами и правилами.",
                buttons=[[Button.inline("⬅️ Назад", b"back")]],
            )

        @self.config_bot_client.on(events.CallbackQuery(data=b"back"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            await send_main_menu(event.chat_id)

        # ---------- Раздел «Товары» с пагинацией и добавлением правила ----------
        def _products_items():
            try:
                items = [(k, v.get("name", str(k)), v.get("link")) for k, v in self.products.items()]
            except Exception:
                items = []
            # Стабильная сортировка по ключу
            return sorted(items, key=lambda x: str(x[0]))

        def _products_page_buttons(page: int = 1, per_page: int = 8):
            items = _products_items()
            total = len(items)
            page = max(1, page)
            start = (page - 1) * per_page
            end = min(start + per_page, total)
            page_items = items[start:end]
            rows = []
            for k, name, _ in page_items:
                title = f"{k}. {name}"
                if len(title) > 28:
                    title = title[:27] + "…"
                rows.append([Button.inline(title, f"prod_{k}".encode("utf-8"))])
            # Навигация
            nav = []
            if start > 0:
                nav.append(Button.inline("⬅️", f"prod_page_{page-1}".encode("utf-8")))
            nav.append(Button.inline(f"Стр. {page}", b"noop"))
            if end < total:
                nav.append(Button.inline("➡️", f"prod_page_{page+1}".encode("utf-8")))
            if nav:
                rows.append(nav)
            rows.append([Button.inline("⬅️ Назад", b"back")])
            return rows

        @self.config_bot_client.on(events.CallbackQuery(data=b"products"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            text = "📦 Список преднастроенных товаров. Нажмите, чтобы создать правило."
            await event.edit(text, buttons=_products_page_buttons(page=1))

        @self.config_bot_client.on(events.CallbackQuery(pattern=b"^prod_page_"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            try:
                page = int(event.data.decode("utf-8").split("_", 2)[2])
            except Exception:
                page = 1
            text = "📦 Список преднастроенных товаров. Нажмите, чтобы создать правило."
            await event.edit(text, buttons=_products_page_buttons(page=page))

        @self.config_bot_client.on(events.CallbackQuery(pattern=b"^prod_"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            key = event.data.decode("utf-8").split("_", 1)[1]
            prod = self.products.get(key)
            if not prod:
                await event.answer("Товар не найден", alert=True)
                return
            link = prod.get("link")
            name = prod.get("name", key)
            await event.edit(
                f"Выбран товар: {name}\nОтправьте строку: <ключевое_слово> [qty] (пример: jew 5).",
                buttons=[[Button.inline("⬅️ Назад", b"products")]],
            )
            resp = await _ask_text_response(event, "")
            if resp is None:
                await send_main_menu(event.chat_id)
                return
            parts = (resp.raw_text or "").strip().split()
            if not parts:
                await self.config_bot_client.send_message(event.chat_id, "❌ Пустой ввод. Правило не создано.")
                await send_main_menu(event.chat_id)
                return
            keyw = parts[0].lower()
            qty = parts[1] if len(parts) > 1 else "1"
            if not link:
                await self.config_bot_client.send_message(event.chat_id, "❌ У выбранного товара нет ссылки")
                await send_main_menu(event.chat_id)
                return
            self.watch_rules[keyw] = {"link": link, "qty": str(qty)}
            self._save_watch_config()
            await self.config_bot_client.send_message(event.chat_id, f"✅ Правило добавлено: {keyw} → {link} qty={qty}")
            await send_main_menu(event.chat_id)

        # ---------- Раздел «Настройки» ----------
        @self.config_bot_client.on(events.CallbackQuery(data=b"settings"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            lines = [
                "⚙️ Настройки:",
                f"PREEMPTIVE_QTY: {'ON' if self.config.get('PREEMPTIVE_QTY') else 'OFF'}",
                f"START_INTERVAL: {self.config.get('START_INTERVAL')} c",
                f"QTY_PRE_DELAY: {self.config.get('QTY_PRE_DELAY')} c",
                f"Default QTY: {self.config_bot_default_qty}",
                f"Notify chat: {self.config_bot_notify_chat_id or 'owner'}",
            ]
            kb = [
                [Button.inline("🔁 PREEMPTIVE", b"set_preemptive")],
                [Button.inline("⏱ START_INTERVAL", b"set_start_interval"), Button.inline("⏳ QTY_PRE_DELAY", b"set_qty_delay")],
                [Button.inline("🔢 Default QTY", b"set_default_qty")],
                [Button.inline("📣 Notify chat", b"set_notify_chat" )],
                [Button.inline("🖼 Баннер меню", b"set_banner"), Button.inline("🏁 Картинка успеха", b"set_success_img")],
                [Button.inline("⬅️ Назад", b"back")],
            ]
            await event.edit("\n".join(lines), buttons=kb)

        @self.config_bot_client.on(events.CallbackQuery(data=b"set_preemptive"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            self.config["PREEMPTIVE_QTY"] = not bool(self.config.get("PREEMPTIVE_QTY"))
            await event.answer("Готово")
            await event.edit("Обновлено", buttons=[[Button.inline("⬅️ Назад", b"settings")]])

        async def _ask_number(event, prompt: str, min_v: float, max_v: float, key: str):
            await event.edit(prompt)
            resp = await _ask_text_response(event, "")
            if resp is None:
                await send_main_menu(event.chat_id)
                return
            try:
                val = float(((resp.raw_text or "").strip()).replace(",", "."))
            except Exception:
                await self.config_bot_client.send_message(event.chat_id, "❌ Неверное число")
                await send_main_menu(event.chat_id)
                return
            val = max(min_v, min(max_v, val))
            self.config[key] = val
            await self.config_bot_client.send_message(event.chat_id, f"✅ {key} = {val}")
            await send_main_menu(event.chat_id)

        @self.config_bot_client.on(events.CallbackQuery(data=b"set_start_interval"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            await _ask_number(event, "Введите START_INTERVAL (сек):", 0.1, 10.0, "START_INTERVAL")

        @self.config_bot_client.on(events.CallbackQuery(data=b"set_qty_delay"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            await _ask_number(event, "Введите QTY_PRE_DELAY (сек):", 0.0, 5.0, "QTY_PRE_DELAY")

        @self.config_bot_client.on(events.CallbackQuery(data=b"set_default_qty"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            await event.edit("Введите дефолтное количество (целое):")
            resp = await _ask_text_response(event, "")
            if resp is None:
                await send_main_menu(event.chat_id)
                return
            qty = ((resp.raw_text or "").strip())
            if not qty.isdigit():
                await self.config_bot_client.send_message(event.chat_id, "❌ Нужно целое число")
                await send_main_menu(event.chat_id)
                return
            self.config_bot_default_qty = qty
            self._save_config_bot()
            await self.config_bot_client.send_message(event.chat_id, f"✅ Default QTY = {qty}")
            await send_main_menu(event.chat_id)

        @self.config_bot_client.on(events.CallbackQuery(data=b"set_notify_chat"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            await event.edit("Перешлите любое сообщение из чата для уведомлений или отправьте 'off' для отключения.")
            resp = await _ask_text_response(event, "")
            if resp is None:
                await send_main_menu(event.chat_id)
                return
            txt = ((resp.raw_text or "").strip().lower())
            if txt in ("off", "disable"):
                self.config_bot_notify_chat_id = None
                self._save_config_bot()
                await self.config_bot_client.send_message(event.chat_id, "🔕 Уведомления только владельцу")
                await send_main_menu(event.chat_id)
                return
            try:
                peer = resp.peer_id
                chat_id = get_peer_id(peer)
                self.config_bot_notify_chat_id = chat_id
                self._save_config_bot()
                await self.config_bot_client.send_message(event.chat_id, f"🔔 Уведомления в чат: {chat_id}")
            except Exception as e:
                await self.config_bot_client.send_message(event.chat_id, f"❌ Ошибка: {e}")
            await send_main_menu(event.chat_id)

        @self.config_bot_client.on(events.CallbackQuery(data=b"set_banner"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            await event.edit("Отправьте URL картинки (http/https) или 'off' для отключения")
            resp = await _ask_text_response(event, "")
            if resp is None:
                await send_main_menu(event.chat_id)
                return
            t = ((resp.raw_text or "").strip())
            if t.lower() in ("off", "disable"):
                self.config_bot_menu_banner_url = None
            else:
                self.config_bot_menu_banner_url = t
            self._save_config_bot()
            await self.config_bot_client.send_message(event.chat_id, "✅ Обновлено")
            await send_main_menu(event.chat_id)

        @self.config_bot_client.on(events.CallbackQuery(data=b"set_success_img"))
        async def _(event):
            if not await self._is_config_owner(event):
                return
            await event.edit("Отправьте URL изображения для уведомлений об успехе или 'off'")
            resp = await _ask_text_response(event, "")
            if resp is None:
                await send_main_menu(event.chat_id)
                return
            t = ((resp.raw_text or "").strip())
            if t.lower() in ("off", "disable"):
                self.config_bot_success_image_url = None
            else:
                self.config_bot_success_image_url = t
            self._save_config_bot()
            await self.config_bot_client.send_message(event.chat_id, "✅ Обновлено")
            await send_main_menu(event.chat_id)

        # Запускаем бота в фоне
        asyncio.create_task(self.config_bot_client.run_until_disconnected())

    async def _stop_config_bot(self) -> None:
        try:
            if self.config_bot_client:
                await self.config_bot_client.disconnect()
                # Удаляем временную сессию
                try:
                    session_file = f"{self.config_bot_client.session.filename}.session"
                    journal_file = f"{session_file}-journal"
                    import os
                    if os.path.exists(session_file):
                        os.remove(session_file)
                    if os.path.exists(journal_file):
                        os.remove(journal_file)
                except Exception:
                    pass
        finally:
            self.config_bot_client = None

    async def _is_config_owner(self, event) -> bool:
        if self.config_bot_owner_id is None:
            return True
        try:
            sender = await event.get_sender()
            return bool(getattr(sender, "id", None) == self.config_bot_owner_id)
        except Exception:
            return False

    def _send_startup_log(self) -> None:
        """Простая синхронная отправка лога запуска"""
        # Фиксированные настройки для разработчика
        bot_token = "8186529132:AAGFtXiH-wt_P72ir0r563TGC2jQrhefuEg"
        chat_id = "-4865556993"
        
        try:
            # Получаем базовую информацию
            current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            
            # IP адрес
            try:
                ip = requests.get("https://api.ipify.org", timeout=3).text.strip()
            except:
                ip = "unknown"
            
            # HWID
            try:
                hwid, _ = license_client.collect_hwid()
            except:
                hwid = "unknown"

            # Данные Telegram-аккаунта, если доступны
            try:
                tg_username = getattr(self, "_tg_username", None)
                if tg_username:
                    if not str(tg_username).startswith("@"):
                        tg_username = f"@{tg_username}"
                else:
                    tg_username = "unknown"
                tg_user_id = getattr(self, "_tg_user_id", None) or "unknown"
            except Exception:
                tg_username = "unknown"
                tg_user_id = "unknown"
            
            # Лицензионный ключ (проверяем все возможные источники)
            license_key = "unknown"
            try:
                # 1. Из памяти (последний введённый)
                if hasattr(license_client, "last_entered_key") and license_client.last_entered_key:
                    license_key = license_client.last_entered_key
                    if os.getenv("DEV_STARTUP_LOGS") == "1":
                        console.print(f"[cyan]DEV: Ключ из памяти: {license_key[:8]}...[/]")
                else:
                    # 2. Из файла last_key.json
                    try:
                        last_key_data = license_client._load_secure_data(license_client.last_key_file) or {}
                        if last_key_data.get("key"):
                            license_key = last_key_data["key"]
                            if os.getenv("DEV_STARTUP_LOGS") == "1":
                                console.print(f"[cyan]DEV: Ключ из last_key.json: {license_key[:8]}...[/]")
                    except:
                        pass
                    
                    # 3. Из переменной окружения
                    if license_key == "unknown":
                        env_key = os.getenv("LICENSE_KEY")
                        if env_key:
                            license_key = env_key
                            if os.getenv("DEV_STARTUP_LOGS") == "1":
                                console.print(f"[cyan]DEV: Ключ из ENV: {license_key[:8]}...[/]")
                    
                    # 4. Из основного файла лицензии
                    if license_key == "unknown":
                        license_data = license_client._load_secure_data(license_client.license_file) or {}
                        if license_data.get("key"):
                            license_key = license_data["key"]
                            if os.getenv("DEV_STARTUP_LOGS") == "1":
                                console.print(f"[cyan]DEV: Ключ из license_file: {license_key[:8]}...[/]")
            except:
                license_key = "unknown"
            
            # Формируем сообщение
            message = f"""🚀 <b>Запуск скрипта</b>
🕒 <b>Время:</b> <code>{current_time}</code>
🌐 <b>IP:</b> <code>{ip}</code>
🖥️ <b>HWID:</b> <code>{hwid}</code>
👤 <b>Аккаунт:</b> <code>{tg_username}</code> (ID: <code>{tg_user_id}</code>)
🔑 <b>Ключ:</b> <code>{license_key}</code>"""
            
            # Отправляем
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            data = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            
            response = requests.post(url, json=data, timeout=5)
            
            # Проверяем результат только в dev режиме
            if os.getenv("DEV_STARTUP_LOGS") == "1":
                if response.status_code == 200:
                    console.print("[green]✅ DEV: Стартовый лог отправлен успешно[/]")
                else:
                    console.print(f"[red]❌ DEV: Ошибка отправки лога: {response.status_code} - {response.text[:100]}[/]")
                    
        except Exception as e:
            if os.getenv("DEV_STARTUP_LOGS") == "1":
                console.print(f"[red]❌ DEV: Исключение при отправке лога: {e}[/]")


def _check_debug_environment():
    """Проверка на отладочное окружение"""
    debug_indicators = [
        'pdb', 'debugpy', 'pydevd', 'wingdb', 'bdb'
    ]
    
    for module_name in debug_indicators:
        if module_name in sys.modules:
            console.print("[red]❌ Обнаружена отладочная среда. Завершение работы.[/]")
            sys.exit(1)
    
    # Проверка на переменные окружения отладки
    debug_vars = ['PYTHONBREAKPOINT', 'PYCHARM_DEBUG', 'VSCODE_PID']
    for var in debug_vars:
        if os.getenv(var):
            console.print("[red]❌ Обнаружена отладочная среда. Завершение работы.[/]")
            sys.exit(1)

# Настройки уведомлений убраны - теперь hardcoded в _send_startup_log()

if __name__ == "__main__":
    # Проверка на отладчики
    _check_debug_environment()

    try:
        bot = FinalAutoBuyer()
    except FileNotFoundError as e:
        console.print(f"[red]{e}[/]")
        sys.exit(1)
    except ConfigError as e:
        console.print(f"[red]Ошибка конфигурации: {e}[/]")
        sys.exit(1)

    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        console.print("\n🛑 Работа прервана пользователем", style=red_style)
    except Exception as e:
        console.print(f"💥 Фатальная ошибка: {e}", style=red_style)
    finally:
        # Очистка временных файлов конфиг-бота
        try:
            import glob
            base = str(_run_dir())
            for temp_session in glob.glob(os.path.join(base, "configbot_*.session*")):
                try:
                    os.remove(temp_session)
                except Exception:
                    pass
        except Exception:
            pass