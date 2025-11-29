from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cardinal import Cardinal
    from telebot.types import Message, CallbackQuery

import os
import json
import logging
import time
import threading
import uuid as _uuid
import requests
import re
import base64
import hmac
import hashlib
import subprocess
import glob
import platform
from datetime import datetime
from urllib.request import urlopen, Request
from concurrent.futures import ThreadPoolExecutor, as_completed
from telebot.types import InlineKeyboardMarkup as K, InlineKeyboardButton as B
from FunPayAPI import types
from FunPayAPI.common.enums import SubCategoryTypes, Currency, MessageTypes
from FunPayAPI.updater.events import NewMessageEvent, NewOrderEvent

logger = logging.getLogger("FPC.AutoSteam")
LOGGER_PREFIX = "[AutoSteam]"

AS_LICENSE_API_DEFAULT = "https://imildar.sbs/api/validate"
AS_LICENSE_KEY_DEFAULT = "227492ee42424e6aafcd32b70b392289"
AS_HMAC_SECRETF= "ec6696b3ca79ba0e36496963cd0b45d92a2389bf060f540ded11e8f1364c21e41"
AS_FILE_NAME_DEFAULT = "autosteam1.py"
try:
    import os as _os_env
    _os_env.environ.setdefault("HMAC_SECRET", AS_HMAC_SECRETF)
except Exception:
    pass

NAME = "AUTOSTEAM"  
VERSION = "1.0"
DESCRIPTION = "Система автовыдачи товаров на FunPay с интеграцией DesslyHub"
CREDITS = "@imildar"
UUID = "ad4eb3b8-d905-4bdb-a263-14793705c4ea"
SETTINGS_PAGE = False   

LICENSE_OK = False
_cardinal_instance: "Cardinal | None" = None

def _on_delete_plugin(cardinal: "Cardinal") -> None:
    pass

BIND_TO_DELETE = _on_delete_plugin

PLUGIN_STORAGE_DIR = os.path.join("storage", "autosteam")

CB_OPEN_MAIN = "AS_MAIN"
CB_TOGGLE_ACTIVE = "AS_TOGGLE_ACTIVE"
CB_OPEN_GAMES = "AS_GAMES"
CB_OPEN_SETTINGS = "AS_SETTINGS"
CB_OPEN_TEMPLATES = "AS_TEMPLATES"
CB_OPEN_BALANCE = "AS_BALANCE"
CB_AUTO_LIST_ALL = "AS_AUTO_LIST_ALL"
CB_MANUAL_SYNC = "AS_MANUAL_SYNC"
CB_BACK = "AS_BACK"
CB_CANCEL = "AS_CANCEL"
CB_TEST_PURCHASE = "AS_TEST_PURCHASE"
CB_OPEN_MOBILE = "AS_MOBILE"
CB_TEST_MOBILE = "AS_TEST_MOBILE"
CB_OPEN_BLACKLIST = "AS_BLACKLIST"
CB_OPEN_STATISTICS = "AS_STATISTICS"
CB_OPEN_ORDERS_HISTORY = "AS_ORDERS_HISTORY"
CB_LICENSE_RECHECK = "AS_LICENSE_RECHECK"

STATE_ADD_GAME = "AS_ADD_GAME"
STATE_EDIT_TEMPLATE_NAME = "AS_EDIT_TEMPLATE_NAME"
STATE_EDIT_TEMPLATE_DESC = "AS_EDIT_TEMPLATE_DESC"
STATE_EDIT_TEMPLATE_WELCOME_STEAM = "AS_EDIT_TEMPLATE_WELCOME_STEAM"
STATE_EDIT_TEMPLATE_WELCOME_MOBILE = "AS_EDIT_TEMPLATE_WELCOME_MOBILE"
STATE_EDIT_TEMPLATE_SUCCESS_STEAM = "AS_EDIT_TEMPLATE_SUCCESS_STEAM"
STATE_EDIT_TEMPLATE_SUCCESS_MOBILE = "AS_EDIT_TEMPLATE_SUCCESS_MOBILE"
STATE_SET_MARKUP = "AS_SET_MARKUP"
STATE_SET_BALANCE_THRESHOLD = "AS_SET_BALANCE_THRESHOLD"
STATE_SEARCH_GAMES = "AS_SEARCH_GAMES"

class Storage:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.settings_path = os.path.join(base_dir, "settings.json")
        self.games_path = os.path.join(base_dir, "games.json")
        self.templates_path = os.path.join(base_dir, "templates.json")
        self.orders_path = os.path.join(base_dir, "orders.json")
        self.black_list_path = os.path.join(base_dir, "black_list.json")
        self.lots_config_path = os.path.join(base_dir, "lots_config.json")
        self._ensure_dirs()
        self._init_files()
    
    def _ensure_dirs(self) -> None:
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir, exist_ok=True)
    
    def _init_files(self) -> None:
        self._init_file(self.settings_path, {
            "active": False,
            "desslyhub_api_key": "",
            "markup_percent": 10.0,
            "auto_sync_prices": True,
            "admin_id": "",
            "balance_threshold": 30.0,
            "balance_threshold_enabled": True,
            "warning_sent": False,
            "warning_time": None,
            "deactivated_lots": [],
            "auto_markup_enabled": True,
            "blacklist_enabled": True
        })
        self._init_file(self.games_path, [])
        self._init_file(self.templates_path, {
            "name_template": "{game_name} - Аренда аккаунта",
            "description_template": "🎮 Аренда аккаунта Steam для лота {game_name}\n\n✅ Гарантия качества\n✅ Мгновенная выдача\n✅ Поддержка 24/7",
            "welcome_steam_template": "🎮 Спасибо за покупку!\n\n📦 Игра: {game_name}\n🌍 Регион: {region}\n\n🔗 Отправьте ссылку на добавление в друзья:\nhttps://s.team/p/...\n\n⏱ Ожидание ссылки...",
            "welcome_mobile_template": "🎮 Спасибо за покупку!\n\n📦 Игра: {game_name}\n💎 Позиция: {position_name}\n\n📝 Отправьте {field_name}:\n\n⏱ Ожидание данных...",
            "success_steam_template": "✅ Подарок успешно отправлен!\n\n🎮 Игра: {game_name}\n🌍 Регион: {region_name}\n🆔 Transaction ID: {transaction_id}\n📊 Статус: {status}\n\n{order_link}🙏 Спасибо за покупку! Приятной игры!\n\n{admin_call_message}",
            "success_mobile_template": "✅ Пополнение успешно отправлено!\n\n🎮 Игра: {game_name}\n💎 Позиция: {position_name}\n{field_labels}{server_text}🆔 Transaction ID: {transaction_id}\n📊 Статус: {status}\n\n{order_link}🙏 Спасибо за покупку! Приятной игры!{admin_call_message}"
        })
        self._init_file(self.orders_path, [])
        self._init_file(self.black_list_path, [])
        self._init_file(self.lots_config_path, [])
    
    def _init_file(self, path: str, default_value: any) -> None:
        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8") as f:
                json.dump(default_value, f, ensure_ascii=False, indent=2)
    
    def _load(self, path: str) -> any:
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    return {}
                return json.loads(content)
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Ошибка загрузки {path}: {e}")
            return {}
    
    def _save(self, path: str, data: any) -> None:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Ошибка сохранения {path}: {e}")
    
    def load_settings(self) -> dict:
        return dict(self._load(self.settings_path))
    
    def save_settings(self, settings: dict) -> None:
        self._save(self.settings_path, settings)
    
    def load_games(self) -> list:
        result = self._load(self.games_path)
        return result if isinstance(result, list) else []
    
    def save_games(self, games: list) -> None:
        self._save(self.games_path, games)
    
    def load_templates(self) -> dict:
        return dict(self._load(self.templates_path))
    
    def save_templates(self, templates: dict) -> None:
        self._save(self.templates_path, templates)
    
    def load_orders(self) -> list:
        result = self._load(self.orders_path)
        return result if isinstance(result, list) else []
    
    def save_orders(self, orders: list) -> None:
        self._save(self.orders_path, orders)
    
    def load_black_list(self) -> list:
        result = self._load(self.black_list_path)
        if isinstance(result, list):
            if result and isinstance(result[0], dict):
                return [item.get("value", "") for item in result if item.get("value")]
            return result
        return []
    
    def save_black_list(self, black_list: list) -> None:
        self._save(self.black_list_path, black_list)
    
    def load_lots_config(self) -> list:
        result = self._load(self.lots_config_path)
        return result if isinstance(result, list) else []
    
    def save_lots_config(self, lots_config: list) -> None:
        self._save(self.lots_config_path, lots_config)


_storage: Storage | None = None
_sync_thread: threading.Thread | None = None
_balance_thread: threading.Thread | None = None
_cardinal_instance: "Cardinal" | None = None
_desslyhub_games_cache: dict | None = None
_desslyhub_cache_timestamp: float = 0
_desslyhub_cache_ttl: int = 3600
_desslyhub_cache_lock = threading.Lock()
_game_app_id_cache: dict[str, int] = {}
_game_app_id_cache_lock = threading.Lock()
_exchange_rates_cache: dict | None = None
_exchange_rates_cache_timestamp: float = 0
_exchange_rates_cache_ttl: int = 300
_exchange_rates_cache_lock = threading.Lock()
_mobile_games_cache: list | None = None
_mobile_games_cache_timestamp: float = 0
_mobile_games_cache_ttl: int = 3600
_mobile_games_cache_lock = threading.Lock()
_test_purchases: dict[str, dict] = {}
_previous_balance: float | None = None
_deactivated_lots_ids: list[int] = []

def _get_storage() -> Storage:
    global _storage
    if _storage is None:
        _storage = Storage(PLUGIN_STORAGE_DIR)
    return _storage


class DesslyHubAPI:
    """Класс-обертка для работы с DesslyHub API"""
    
    def __init__(self, api_key: str, base_url: str = "https://desslyhub.com/api/v1"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "apikey": self.api_key,
            "Content-Type": "application/json"
        }
        self.manual_rates: dict[str, float] = {}
    
    @staticmethod
    def clean_currency(currency: str) -> str:
        """Очищает валюту от лишних символов"""
        if not currency:
            return ""
        return ''.join(filter(str.isalpha, str(currency))).upper()
    
    def _get(self, path: str, **kwargs) -> dict:
        """GET запрос к API"""
        url = f"{self.base_url}{path}"
        resp = requests.get(url, headers=self.headers, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.json()
    
    def _post(self, path: str, json_payload: dict = None, **kwargs) -> dict:
        """POST запрос к API"""
        url = f"{self.base_url}{path}"
        resp = requests.post(url, headers=self.headers, json=json_payload or {}, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.json()
    
    def get_balance(self) -> float:
        """Получает баланс аккаунта"""
        try:
            data = self._get("/merchants/balance")
            if isinstance(data, dict):
                if "balance" in data:
                    return float(data["balance"])
                if "data" in data and isinstance(data["data"], dict) and "balance" in data["data"]:
                    return float(data["data"]["balance"])
            logger.warning(f"{LOGGER_PREFIX} Неожиданный формат ответа баланса: {data}")
            return 0.0
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Ошибка при получении баланса: {e}")
            return 0.0
    
    def _get_external_exchange_rates(self) -> dict:
        """Получает актуальные курсы валют из внешнего API (Frankfurter)"""
        try:
            url = "https://api.frankfurter.dev/v1/latest?base=USD"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            rates = data.get("rates", {})
            mapped = {}
            
            if "RUB" in rates:
                mapped["RUB"] = float(rates["RUB"])
            if "UAH" in rates:
                mapped["UAH"] = float(rates["UAH"])
            if "KZT" in rates:
                mapped["KZT"] = float(rates["KZT"])
            
            mapped["USD"] = 1.0
            
            logger.info(f"{LOGGER_PREFIX} Получены актуальные курсы из внешнего API: {mapped}")
            return mapped
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Не удалось получить курсы из внешнего API: {e}")
            return {}
    
    def get_exchange_rates(self) -> dict:
        """Получает курсы валют с кэшированием"""
        global _exchange_rates_cache, _exchange_rates_cache_timestamp, _exchange_rates_cache_lock
        
        import time
        current_time = time.time()
        
        with _exchange_rates_cache_lock:
            if (_exchange_rates_cache is not None and 
                current_time - _exchange_rates_cache_timestamp < _exchange_rates_cache_ttl):
                logger.debug(f"{LOGGER_PREFIX} Использован кэш курсов валют")
                return _exchange_rates_cache.copy()
        
        mapped = {}
        
        try:
            data = self._get("/exchange_rates/steam")
            rates = data.get("exchange_rates", {})
            steam_codes = {
                "1": "USD",
                "5": "RUB",
                "18": "UAH",
                "37": "KZT",
            }
            for code, currency in steam_codes.items():
                rate_value = rates.get(code)
                if rate_value is not None:
                    mapped[currency] = float(rate_value)
        except Exception as e:
            logger.warning(f"{LOGGER_PREFIX} Не удалось получить курсы с DesslyHub API: {e}")
        
        if not mapped or not mapped.get("RUB") or mapped.get("RUB", 0) < 10:
            external_rates = self._get_external_exchange_rates()
            if external_rates:
                for cur, rate in external_rates.items():
                    if cur not in mapped or mapped.get(cur, 0) < 10:
                        mapped[cur] = rate
                mapped["USD"] = 1.0
                logger.info(f"{LOGGER_PREFIX} Используем реальные курсы из внешнего API: {mapped}")
            else:
                if not mapped.get("RUB") or mapped.get("RUB", 0) < 10:
                    mapped["RUB"] = 100.0
                if not mapped.get("UAH") or mapped.get("UAH", 0) < 10:
                    mapped["UAH"] = 42.0
                if not mapped.get("KZT") or mapped.get("KZT", 0) < 10:
                    mapped["KZT"] = 500.0
                mapped["USD"] = 1.0
                logger.warning(f"{LOGGER_PREFIX} Используем fallback курсы: {mapped}")
        else:
            if "USD" not in mapped:
                mapped["USD"] = 1.0
            logger.info(f"{LOGGER_PREFIX} Используем курсы из DesslyHub API: {mapped}")
        
        for cur, rate in self.manual_rates.items():
            mapped[cur] = rate
        
        with _exchange_rates_cache_lock:
            _exchange_rates_cache = mapped.copy()
            _exchange_rates_cache_timestamp = current_time
        
        logger.info(f"{LOGGER_PREFIX} Финальные курсы валют: {mapped}")
        return mapped
    
    def set_manual_rate(self, currency: str, rate: float):
        """Устанавливает ручной курс валюты"""
        currency = self.clean_currency(currency)
        if rate <= 0:
            logger.warning(f"{LOGGER_PREFIX} Попытка установить некорректный курс для {currency}: {rate}")
            return
        self.manual_rates[currency] = rate
        logger.info(f"{LOGGER_PREFIX} Установлен ручной курс: {currency} = {rate}")
    
    def convert_to_usd(self, amount: float, currency: str) -> float | None:
        """Конвертирует сумму в USD"""
        clean_cur = self.clean_currency(currency)
        rates = self.get_exchange_rates()
        rate = rates.get(clean_cur)
        if rate is None:
            if clean_cur == "USD":
                rate = 1.0
            else:
                logger.warning(f"{LOGGER_PREFIX} Курс для {clean_cur} не найден, используем 1.0")
                rate = 1.0
        
        usd_amount = float(amount) / float(rate)
        logger.info(f"{LOGGER_PREFIX} Конвертация: {amount} {clean_cur} = {usd_amount:.2f} USD (курс {rate})")
        return usd_amount
    
    def convert_from_usd(self, usd_amount: float, target_currency: str) -> float | None:
        """Конвертирует сумму из USD в указанную валюту"""
        clean_cur = self.clean_currency(target_currency)
        
        if clean_cur == "USD":
            return float(usd_amount)
        
        rates = self.get_exchange_rates()
        rate = rates.get(clean_cur)
        
        if rate is None:
            logger.warning(f"{LOGGER_PREFIX} Курс для {clean_cur} не найден в кэше, запрашиваем из внешнего API")
            try:
                url = f"https://api.frankfurter.dev/v1/latest?base=USD&symbols={clean_cur}"
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                rate = float(data.get("rates", {}).get(clean_cur, 0))
                
                if rate and rate > 0:
                    logger.info(f"{LOGGER_PREFIX} Получен актуальный курс {clean_cur} из внешнего API: {rate}")
                else:
                    raise ValueError(f"Курс {clean_cur} не найден во внешнем API")
            except Exception as e:
                logger.error(f"{LOGGER_PREFIX} Не удалось получить курс {clean_cur} из внешнего API: {e}")
                raise ValueError(f"Не удалось получить актуальный курс для {clean_cur}. Проверьте подключение к интернету или настройте ручной курс.")
        
        converted_amount = float(usd_amount) * float(rate)
        logger.debug(f"{LOGGER_PREFIX} Конвертация: {usd_amount:.2f} USD = {converted_amount:.2f} {clean_cur} (курс {rate})")
        return converted_amount
    
    def get_transaction_status(self, transaction_id: str) -> dict:
        """Получает статус транзакции"""
        return self._get(f"/merchants/transaction/{transaction_id}/status")
    
    def get_transaction(self, transaction_id: str) -> dict:
        """Получает полную информацию о транзакции"""
        return self._get(f"/merchants/transaction/{transaction_id}")
    
    def wait_for_status(self, transaction_id: str, timeout: int = 120, interval: float = 3.0) -> dict | None:
        """Ожидает завершения транзакции"""
        elapsed = 0.0
        last = None
        while elapsed < timeout:
            try:
                last = self.get_transaction_status(transaction_id)
            except Exception as e:
                logger.error(f"{LOGGER_PREFIX} Ошибка при проверке статуса транзакции {transaction_id}: {e}")
                break
            st = None
            if isinstance(last, dict):
                st = last.get("status") or last.get("state") or (last.get("data") and last["data"].get("status"))
            if st:
                st = str(st).lower()
                if st in ("completed", "success", "done", "fulfilled"):
                    return last
                if st in ("error", "failed", "rejected", "cancelled"):
                    return last
            time.sleep(interval)
            elapsed += interval
        return last


def _kb_main(active: bool) -> K:
    kb = K()
    toggle_text = "🔴 Остановить" if active else "🟢 Запустить"
    kb.add(B(toggle_text, callback_data=CB_TOGGLE_ACTIVE))
    kb.row(
        B("📑 Лоты", callback_data=CB_OPEN_GAMES),
        B("⚙️ Настройки", callback_data=CB_OPEN_SETTINGS)
    )
    kb.row(
        B("📝 Шаблоны", callback_data=CB_OPEN_TEMPLATES),
        B("📊 Статистика", callback_data=CB_OPEN_STATISTICS)
    )
    kb.row(
        B("📜 История заказов", callback_data=CB_OPEN_ORDERS_HISTORY),
        B("🚫 Черный список", callback_data=CB_OPEN_BLACKLIST)
    )
    kb.row(B("🔑 Перепроверить лицензию", callback_data=CB_LICENSE_RECHECK))
    return kb


def _kb_games_menu() -> K:
    kb = K()
    kb.row(
        B("➕ Добавить лот", callback_data="AS_ADD_GAME"),
        B("📜 Список лотов", callback_data="AS_GAMES_LIST:0")
    )
    kb.add(B("🔍 Поиск лота", callback_data="AS_SEARCH_GAMES"))
    kb.add(B("🔙 Назад", callback_data=CB_BACK))
    return kb


def _kb_settings_menu() -> K:
    kb = K()
    storage = _get_storage()
    settings = storage.load_settings()
    
    markup = settings.get("markup_percent", 10.0)
    
    kb.add(B(f"📊 Наценка: {markup}%", callback_data="AS_EDIT_MARKUP"))
    balance_threshold = settings.get("balance_threshold", 30.0)
    balance_threshold_enabled = settings.get("balance_threshold_enabled", True)
    kb.add(B(f"💰 Порог цены: {balance_threshold} USD", callback_data="AS_EDIT_BALANCE_THRESHOLD"))
    kb.add(B(f"🔔 Мониторинг порога: {'✅' if balance_threshold_enabled else '❌'}", callback_data="AS_TOGGLE_BALANCE_THRESHOLD"))
    auto_markup = settings.get("auto_markup_enabled", True)
    kb.add(B(f"📊 Автонаценка: {'✅' if auto_markup else '❌'}", callback_data="AS_TOGGLE_AUTO_MARKUP"))
    kb.add(B("🔑 API ключ DesslyHub", callback_data="AS_EDIT_API_KEY"))
    kb.add(B("👤 Установить Admin ID", callback_data="AS_SET_ADMIN_ID"))
    kb.add(B("🔙 Назад", callback_data=CB_BACK))
    return kb


def _kb_templates_menu() -> K:
    kb = K()
    kb.add(B("📝 Приветствие Steam", callback_data="AS_EDIT_TEMPLATE_WELCOME_STEAM"))
    kb.add(B("📝 Приветствие Mobile", callback_data="AS_EDIT_TEMPLATE_WELCOME_MOBILE"))
    kb.add(B("✅ Успех Steam", callback_data="AS_EDIT_TEMPLATE_SUCCESS_STEAM"))
    kb.add(B("✅ Успех Mobile", callback_data="AS_EDIT_TEMPLATE_SUCCESS_MOBILE"))
    kb.add(B("🔙 Назад", callback_data=CB_BACK))
    return kb


def _kb_mobile_menu() -> K:
    kb = K()
    kb.add(B("🔙 Назад", callback_data=CB_BACK))
    return kb


def _kb_back() -> K:
    return K().add(B("🔙 Назад", callback_data=CB_BACK))


def _kb_cancel() -> K:
    return K().add(B("❌ Отмена", callback_data=CB_CANCEL))


def _determine_lot(cardinal: "Cardinal", game_name: str) -> dict | None:
    try:
        if not cardinal or not hasattr(cardinal, 'profile'):
            logger.warning(f"{LOGGER_PREFIX} Cardinal или profile недоступен")
            return None
        
        lots = cardinal.profile.get_sorted_lots(2)
        
        for subcategory, category_lots in lots.items():
            for lot_id, lot in category_lots.items():
                if lot.description and game_name.lower() in lot.description.lower():
                    logger.info(f"{LOGGER_PREFIX} Найден лот для игры '{game_name}': ID={lot.id}, Описание={lot.description}")
                    return {
                        "lot_id": lot.id,
                        "description": lot.description,
                        "price": lot.price,
                        "currency": lot.currency,
                        "subcategory": subcategory,
                        "active": lot.id in cardinal.curr_profile.get_sorted_lots(1) if hasattr(cardinal, 'curr_profile') else False
                    }
        
        logger.info(f"{LOGGER_PREFIX} Лот для игры '{game_name}' не найден")
        return None
        
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка при определении лота для '{game_name}': {e}")
        return None


def _get_desslyhub_games(api_key: str, use_cache: bool = True) -> dict | None:
    global _desslyhub_games_cache, _desslyhub_cache_timestamp, _desslyhub_cache_lock
    
    if use_cache:
        with _desslyhub_cache_lock:
            if _desslyhub_games_cache and (time.time() - _desslyhub_cache_timestamp) < _desslyhub_cache_ttl:
                return _desslyhub_games_cache
    
    try:
        urls_to_try = [
            "https://desslyhub.com/api/v1/service/steamgift/games",
            "https://desslyhub.com/api/v1/steam/games",
            "https://api.desslyhub.com/v2/catalog/steam-gift/games"
        ]
        
        headers = {
            "apikey": api_key,
            "Content-Type": "application/json"  
        }
        
        response = None
        last_error = None
        
        for url in urls_to_try:
            try:
                logger.debug(f"{LOGGER_PREFIX} [TEST] Запрос списка игр: URL={url}")
                response = requests.get(url, headers=headers, timeout=30)
                if response.status_code == 200:
                    break
                else:
                    logger.debug(f"{LOGGER_PREFIX} [TEST] URL {url} вернул статус {response.status_code}")
                    last_error = f"Status {response.status_code}"
            except Exception as e:
                logger.debug(f"{LOGGER_PREFIX} [TEST] Ошибка при запросе {url}: {e}")
                last_error = str(e)
                continue
        
        if not response or response.status_code != 200:
            logger.error(f"{LOGGER_PREFIX} Не удалось получить список игр ни с одного URL. Последняя ошибка: {last_error}")
            return None
        
        if response.status_code == 200:
            data = response.json()
            logger.debug(f"{LOGGER_PREFIX} [TEST] Получен ответ от API: type={type(data).__name__}, length={len(data) if isinstance(data, (list, dict)) else 'N/A'}")
            
            if isinstance(data, list) and len(data) > 0:
                logger.debug(f"{LOGGER_PREFIX} [TEST] Первая игра из списка: {json.dumps(data[0] if isinstance(data[0], dict) else str(data[0])[:200], ensure_ascii=False)}")
            elif isinstance(data, dict):
                logger.debug(f"{LOGGER_PREFIX} [TEST] Ключи в ответе: {list(data.keys())[:10]}")
            
            result = None
            if isinstance(data, dict):
                result = data
            elif isinstance(data, list):
                result = {"games": data}
            
            if result:
                games_list = result.get("games", []) or result.get("data", []) or result.get("items", [])
                logger.debug(f"{LOGGER_PREFIX} [TEST] Всего игр в ответе: {len(games_list)}")
                with _desslyhub_cache_lock:
                    _desslyhub_games_cache = result
                    _desslyhub_cache_timestamp = time.time()
                return result
        else:
            error_text = ""
            try:
                error_data = response.json()
                error_text = f": {error_data}"
            except:
                error_text = f": {response.text[:100]}"
            logger.warning(f"{LOGGER_PREFIX} DesslyHub API вернул статус {response.status_code}{error_text}")
            return None
            
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка при получении списка игр с DesslyHub: {e}")
        return None


def _get_desslyhub_price_by_app_id(app_id: int, api_key: str) -> float | None:
    try:
        urls_to_try = [
            f"https://desslyhub.com/api/v1/service/steamgift/games/{app_id}",
            f"https://desslyhub.com/api/v1/steam/games/{app_id}",
            f"https://api.desslyhub.com/v2/catalog/steam-gift/games/{app_id}"
        ]
        
        headers = {
            "apikey": api_key,
            "Content-Type": "application/json"
        }
        
        response = None
        last_error = None
        
        for url in urls_to_try:
            try:
                logger.info(f"{LOGGER_PREFIX} [TEST] Запрос информации об игре: URL={url}, app_id={app_id}")
                response = requests.get(url, headers=headers, timeout=30)
                if response.status_code == 200:
                    break
                else:
                    logger.warning(f"{LOGGER_PREFIX} [TEST] URL {url} вернул статус {response.status_code}")
                    last_error = f"Status {response.status_code}"
            except Exception as e:
                logger.warning(f"{LOGGER_PREFIX} [TEST] Ошибка при запросе {url}: {e}")
                last_error = str(e)
                continue
        
        if not response or response.status_code != 200:
            logger.error(f"{LOGGER_PREFIX} [TEST] Не удалось получить информацию об игре ни с одного URL. Последняя ошибка: {last_error}")
            return None
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict):
                if "price" in data:
                    return float(data["price"])
                elif "cost" in data:
                    return float(data["cost"])
                elif "amount" in data:
                    return float(data["amount"])
        else:
            error_text = ""
            try:
                error_data = response.json()
                error_text = f": {error_data}"
            except:
                error_text = f": {response.text[:100]}"
            logger.warning(f"{LOGGER_PREFIX} DesslyHub API вернул статус {response.status_code} для app_id {app_id}{error_text}")
            return None
            
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка при получении цены с DesslyHub для app_id {app_id}: {e}")
        return None


def _get_desslyhub_price(game_name: str, api_key: str) -> float | None:
    try:
        games_data = _get_desslyhub_games(api_key)
        if not games_data:
            return None
        
        games_list = games_data.get("games", []) or games_data.get("data", []) or games_data.get("items", [])
        if not games_list:
            return None
        
        game_name_lower = game_name.lower().strip()
        
        for game in games_list:
            if isinstance(game, dict):
                game_title = (game.get("title", "") or game.get("name", "") or 
                             game.get("game_name", "") or game.get("gameName", "")).strip()
                if not game_title:
                    continue
                
                if game_name_lower in game_title.lower() or game_title.lower() in game_name_lower:
                    app_id = game.get("app_id") or game.get("appId") or game.get("appID") or game.get("id")
                    if app_id:
                        try:
                            price = _get_desslyhub_price_by_app_id(int(app_id), api_key)
                            if price is not None:
                                logger.info(f"{LOGGER_PREFIX} Найдена цена для '{game_name}' (app_id={app_id}): {price}")
                                return price
                        except (ValueError, TypeError):
                            pass
                    
                    if "price" in game:
                        price_val = game["price"]
                        if price_val is not None:
                            logger.info(f"{LOGGER_PREFIX} Найдена цена для '{game_name}' в списке: {price_val}")
                            return float(price_val)
                    elif "cost" in game:
                        cost_val = game["cost"]
                        if cost_val is not None:
                            logger.info(f"{LOGGER_PREFIX} Найдена цена для '{game_name}' в списке: {cost_val}")
                            return float(cost_val)
                    elif "amount" in game:
                        amount_val = game["amount"]
                        if amount_val is not None:
                            logger.info(f"{LOGGER_PREFIX} Найдена цена для '{game_name}' в списке: {amount_val}")
                            return float(amount_val)
        
        logger.info(f"{LOGGER_PREFIX} Лот '{game_name}' не найден в каталоге DesslyHub")
        return None
            
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка при получении цены с DesslyHub для '{game_name}': {e}")
        return None


def _get_desslyhub_balance(api_key: str) -> dict | None:
    try:
        url = "https://desslyhub.com/api/v1/merchants/balance"
        headers = {
            "apikey": api_key,
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict):
                balance = data.get("balance") or data.get("amount") or data.get("value")
                currency = data.get("currency") or data.get("curr") or "USD"
                if balance is not None:
                    return {
                        "balance": float(balance),
                        "currency": str(currency)
                    }
        else:
            error_text = ""
            try:
                error_data = response.json()
                error_text = f": {error_data}"
            except:
                error_text = f": {response.text[:100]}"
            logger.warning(f"{LOGGER_PREFIX} DesslyHub API вернул статус {response.status_code} для баланса{error_text}")
            return None
            
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка при получении баланса с DesslyHub: {e}")
        return None


def _calculate_price_with_markup(base_price: float, markup_percent: float) -> float:
    return base_price * (1 + markup_percent / 100.0)


def _normalize_game_name(name: str) -> str:
    """Нормализует название игры для сравнения, удаляя специальные символы"""
    import re
    import unicodedata
    if not name:
        return ""
    normalized = name.strip().lower()

    normalized = unicodedata.normalize('NFKC', normalized)

    normalized = re.sub(r'^\d+\.\s*', '', normalized)
    

    normalized = re.sub(r'[®™©℠]', '', normalized)
    

    normalized = re.sub(r'[''""''""]', "'", normalized)
    

    normalized = re.sub(r'[–—―]', '-', normalized)
    

    normalized = re.sub(r'[^\w\s\u0400-\u04ff\-\':]', ' ', normalized)
    

    normalized = re.sub(r'\s+', ' ', normalized)
    
    return normalized.strip()

def _get_game_app_id_by_name(game_name: str, api_key: str) -> int | None:
    global _game_app_id_cache, _game_app_id_cache_lock
    
    game_name_normalized_key = _normalize_game_name(game_name)
    
    with _game_app_id_cache_lock:
        if game_name_normalized_key in _game_app_id_cache:
            cached_app_id = _game_app_id_cache[game_name_normalized_key]
            logger.debug(f"{LOGGER_PREFIX} [TEST] Использован кэш app_id={cached_app_id} для игры '{game_name}'")
            return cached_app_id
    
    try:
        logger.debug(f"{LOGGER_PREFIX} [TEST] Поиск app_id для игры '{game_name}'")
        games_data = _get_desslyhub_games(api_key)
        if not games_data:
            logger.error(f"{LOGGER_PREFIX} Не удалось получить список игр с DesslyHub")
            return None
        
        games_list = games_data.get("games", []) or games_data.get("data", []) or games_data.get("items", [])
        if not games_list:
            logger.error(f"{LOGGER_PREFIX} Список игр пуст")
            return None
        
        game_name_normalized = _normalize_game_name(game_name)
        game_name_lower = game_name.lower().strip()
        logger.debug(f"{LOGGER_PREFIX} [TEST] Поиск игры '{game_name}' (нормализовано: '{game_name_normalized}') в списке из {len(games_list)} игр")
        
        game_name_words = set(game_name_normalized.split())
        if not game_name_words:
            logger.warning(f"{LOGGER_PREFIX} Не удалось нормализовать название игры '{game_name}'")
            return None
        
        exact_matches = []
        high_similarity = []
        partial_matches = []
        
        for idx, game in enumerate(games_list):
            if isinstance(game, dict):
                game_title = (game.get("name", "") or game.get("title", "") or 
                             game.get("game_name", "") or game.get("gameName", "") or
                             game.get("title_ru", "") or game.get("title_en", "")).strip()
                
                if not game_title:
                    continue
                
                game_title_normalized = _normalize_game_name(game_title)
                game_title_lower = game_title.lower()
                
                app_id = game.get("appid") or game.get("app_id") or game.get("appId") or game.get("appID") or game.get("id")
                if not app_id:
                    continue
                
                try:
                    app_id_int = int(app_id)
                except (ValueError, TypeError):
                    continue
                
                if game_name_normalized == game_title_normalized or game_name_lower == game_title_lower:
                    logger.debug(f"{LOGGER_PREFIX} [TEST] Найдено точное совпадение: appid={app_id_int} для '{game_name}' (найдено как '{game_title}')")
                    with _game_app_id_cache_lock:
                        _game_app_id_cache[game_name_normalized_key] = app_id_int
                    return app_id_int
                
                game_title_words = set(game_title_normalized.split())
                common_words = game_name_words & game_title_words
                
                if not common_words:
                    continue
                
                if len(common_words) == len(game_name_words) and len(game_name_words) >= 2:
                    logger.debug(f"{LOGGER_PREFIX} [TEST] Найдено полное совпадение слов: appid={app_id_int} для '{game_name}' (найдено как '{game_title}')")
                    with _game_app_id_cache_lock:
                        _game_app_id_cache[game_name_normalized_key] = app_id_int
                    return app_id_int
                
                if game_name_normalized in game_title_normalized:
                    similarity = len(game_name_normalized) / len(game_title_normalized)
                    if similarity >= 0.7:
                        high_similarity.append((game_title, app_id_int, similarity))
                    elif similarity >= 0.5:
                        partial_matches.append((game_title, app_id_int, similarity))
                    continue
                
                if game_title_normalized in game_name_normalized:
                    similarity = len(game_title_normalized) / len(game_name_normalized)
                    if similarity >= 0.7:
                        high_similarity.append((game_title, app_id_int, similarity))
                    elif similarity >= 0.5:
                        partial_matches.append((game_title, app_id_int, similarity))
                    continue
                
                if len(common_words) >= 2:
                    similarity = len(common_words) / max(len(game_name_words), len(game_title_words))
                    if similarity >= 0.6:
                        high_similarity.append((game_title, app_id_int, similarity))
                    elif similarity >= 0.4:
                        partial_matches.append((game_title, app_id_int, similarity))
        
        if high_similarity:
            high_similarity.sort(key=lambda x: x[2], reverse=True)
            best_match = high_similarity[0]
            logger.debug(f"{LOGGER_PREFIX} [TEST] Найдено совпадение с высокой схожестью: app_id={best_match[1]} для '{game_name}' (найдено как '{best_match[0]}', схожесть={best_match[2]:.2f})")
            with _game_app_id_cache_lock:
                _game_app_id_cache[game_name_normalized_key] = best_match[1]
            return best_match[1]
        
        if partial_matches:
            partial_matches.sort(key=lambda x: x[2], reverse=True)
            best_match = partial_matches[0]
            if best_match[2] >= 0.5:
                logger.debug(f"{LOGGER_PREFIX} [TEST] Найдено частичное совпадение: app_id={best_match[1]} для '{game_name}' (найдено как '{best_match[0]}', схожесть={best_match[2]:.2f})")
                with _game_app_id_cache_lock:
                    _game_app_id_cache[game_name_normalized_key] = best_match[1]
                return best_match[1]
        
        logger.warning(f"{LOGGER_PREFIX} Игра '{game_name}' не найдена в каталоге DesslyHub. Проверено {len(games_list)} игр")
        if len(games_list) > 0 and len(games_list) <= 10:
            logger.debug(f"{LOGGER_PREFIX} [TEST] Примеры названий игр из каталога: {[g.get('name', g.get('title', 'N/A')) for g in games_list[:5] if isinstance(g, dict)]}")
        
        known_app_ids = {
            "UBERMOSH COLLECTION": 355180,
            "UBERMOSH": 355180,
            "UBERMOSH BLACK": 355180,
            "UBERMOSH:BLACK": 355180
        }
        
        if game_name.upper() in known_app_ids:
            app_id = known_app_ids[game_name.upper()]
            logger.debug(f"{LOGGER_PREFIX} [TEST] Используется известный appid={app_id} для игры '{game_name}'")
            return app_id
        
        return None
            
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка при поиске app_id для '{game_name}': {e}", exc_info=True)
        return None


def _get_package_id_by_app_id(api_key: str, app_id: int, region: str = "KZ", game_name: str = None, lot_name: str = None) -> dict | None:
    max_retries = 3
    retry_delay = 1.0
    
    for attempt in range(max_retries):
        try:
            logger.debug(f"{LOGGER_PREFIX} [TEST] Получение package_id для app_id={app_id}, region={region}, game_name={game_name}, lot_name={lot_name} (попытка {attempt + 1}/{max_retries})")
            
            url = f"https://desslyhub.com/api/v1/service/steamgift/games/{app_id}"
            headers = {
                "apikey": api_key,
                "Content-Type": "application/json"
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"{LOGGER_PREFIX} [TEST] Rate limit достигнут, ожидание {wait_time:.1f} сек перед повтором...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"{LOGGER_PREFIX} [TEST] Rate limit достигнут после {max_retries} попыток")
                    return None
            
            if response.status_code >= 500:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"{LOGGER_PREFIX} [TEST] Ошибка сервера {response.status_code}, ожидание {wait_time:.1f} сек перед повтором...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"{LOGGER_PREFIX} [TEST] Ошибка сервера {response.status_code} после {max_retries} попыток")
            
            if response.status_code == 200:
                data = response.json()
                logger.debug(f"{LOGGER_PREFIX} [TEST] Получен ответ от Get Game By App ID: {json.dumps(data, ensure_ascii=False)[:500]}")
                
                game_list = data.get("game", []) or data.get("games", []) or []
                if not game_list:
                    logger.warning(f"{LOGGER_PREFIX} Список изданий игры пуст для app_id={app_id}")
                    return None
            
                if not isinstance(game_list, list):
                    game_list = [game_list]
                
                edition_keywords = []
                if lot_name:
                    lot_name_lower = lot_name.lower()
                    lot_name_normalized = _normalize_lot_name(lot_name)
                    edition_patterns = [
                        # Полные названия изданий (приоритет)
                        (r'\bvault\s+edition\b', 'vault edition'),
                        (r'\bultimate\s+edition\b', 'ultimate edition'),
                        (r'\bdeluxe\s+edition\b', 'deluxe edition'),
                        (r'\bpremium\s+edition\b', 'premium edition'),
                        (r'\bgold\s+edition\b', 'gold edition'),
                        (r'\bstandard\s+edition\b', 'standard edition'),
                        (r'\brevolution\s+edition\b', 'revolution edition'),
                        (r'\bdefinitive\s+edition\b', 'definitive edition'),
                        (r'\bphantom\s+edition\b', 'phantom edition'),
                        (r'\bpalace\s+edition\b', 'palace edition'),
                        (r'\btournament\s+edition\b', 'tournament edition'),
                        (r'\ball-star\s+edition\b', 'all-star edition'),
                        (r'\bcomplete\s+edition\b', 'complete edition'),
                        (r'\bdigital\s+deluxe\b', 'digital deluxe'),
                        (r'\badvanced\s+edition\b', 'advanced edition'),
                        (r'\blegendary\s+edition\b', 'legendary edition'),
                        (r'\bcollector[\'\u2019]?s?\s+edition\b', 'collector edition'),
                        (r'\bgame\s+of\s+the\s+year\b', 'game of the year'),
                        (r'\bgoty\s+edition\b', 'goty edition'),
                        (r'\bgoty\b', 'goty'),
                        (r'\bchampion\s+edition\b', 'champion edition'),
                        (r'\banniversary\s+edition\b', 'anniversary edition'),
                        (r'\bspecial\s+edition\b', 'special edition'),
                        (r'\benhanced\s+edition\b', 'enhanced edition'),
                        (r'\bextended\s+edition\b', 'extended edition'),
                        (r'\bfounder[\'\u2019]?s?\s+edition\b', 'founder edition'),
                        (r'\blaunch\s+edition\b', 'launch edition'),
                        (r'\blimited\s+edition\b', 'limited edition'),
                        (r'\bplatinum\s+edition\b', 'platinum edition'),
                        (r'\bsilver\s+edition\b', 'silver edition'),
                        (r'\bbronze\s+edition\b', 'bronze edition'),
                        (r'\bsuper\s+deluxe\b', 'super deluxe'),
                        (r'\bseason\s+pass\s+edition\b', 'season pass edition'),
                        (r'\bbundle\b', 'bundle'),
                        (r'\bcollection\b', 'collection'),
                        # Короткие ключевые слова (меньший приоритет)
                        (r'\bvault\b', 'vault'),
                        (r'\bultimate\b', 'ultimate'),
                        (r'\bdeluxe\b', 'deluxe'),
                        (r'\bpremium\b', 'premium'),
                        (r'\bgold\b', 'gold'),
                        (r'\blegendary\b', 'legendary'),
                        (r'\bcollector\b', 'collector'),
                        (r'\bchampion\b', 'champion'),
                        (r'\bplatinum\b', 'platinum'),
                        (r'\benhanced\b', 'enhanced'),
                        (r'\bdefinitive\b', 'definitive'),
                        (r'\bcomplete\b', 'complete'),
                        (r'\bspecial\b', 'special'),
                    ]
                    for pattern, keyword in edition_patterns:
                        match = re.search(pattern, lot_name_lower)
                        if match:
                            edition_keywords.append(keyword)
                
                if game_name:
                    game_name_lower = game_name.lower()
                    game_name_normalized = _normalize_game_name(game_name)
                
                exact_matches = []
                partial_matches = []
                keyword_matches = []
                other_editions = []
                
                for edition in game_list:
                    if not isinstance(edition, dict):
                        continue
                    
                    package_id = edition.get("package_id")
                    if not package_id:
                        continue
                    
                    edition_name = edition.get("edition", "").strip()
                    regions_info = edition.get("regions_info", [])
                    if not isinstance(regions_info, list):
                        regions_info = []
                    
                    region_price = None
                    region_found = False
                    for region_info in regions_info:
                        if isinstance(region_info, dict) and region_info.get("region") == region:
                            region_found = True
                            price_value = region_info.get("price")
                            currency_value = region_info.get("currency") or region_info.get("curr")
                            logger.debug(f"{LOGGER_PREFIX} [TEST] Регион {region}: price={price_value}, currency={currency_value}, полные данные: {json.dumps(region_info, ensure_ascii=False)[:200]}")
                            if price_value is not None:
                                try:
                                    region_price = float(price_value)
                                except (ValueError, TypeError):
                                    logger.warning(f"{LOGGER_PREFIX} [TEST] Неверный формат цены для региона {region}: {price_value}")
                                    region_price = None
                            break
                    
                    if not region_found and regions_info:
                        first_region_info = regions_info[0] if regions_info else None
                        if first_region_info and isinstance(first_region_info, dict):
                            price_value = first_region_info.get("price")
                            if price_value is not None:
                                try:
                                    region_price = float(price_value)
                                    logger.debug(f"{LOGGER_PREFIX} [TEST] Регион {region} не найден, используется цена из первого доступного региона: {region_price}")
                                except (ValueError, TypeError):
                                    region_price = None
                    
                    if region_price is None:
                        if regions_info:
                            for alt_region_info in regions_info:
                                if isinstance(alt_region_info, dict):
                                    alt_price = alt_region_info.get("price")
                                    if alt_price is not None:
                                        try:
                                            region_price = float(alt_price)
                                            logger.debug(f"{LOGGER_PREFIX} [TEST] Цена для региона {region} не найдена, используется альтернативная цена: {region_price}")
                                            break
                                        except (ValueError, TypeError):
                                            continue
                        if region_price is None:
                            logger.warning(f"{LOGGER_PREFIX} [TEST] Цена для региона {region} не найдена для издания '{edition_name}'")
                            continue
                    
                    edition_info = {
                        "package_id": str(package_id),
                        "price": region_price,
                        "edition": edition_name
                    }
                    
                    edition_lower = edition_name.lower().strip()
                    edition_normalized = _normalize_game_name(edition_name)
                    matched = False
                    match_score = 0
                    
                    # Маппинг синонимов для поиска изданий
                    keyword_synonyms = {
                        'goty': ['goty', 'game of the year'],
                        'game of the year': ['goty', 'game of the year'],
                        'collector edition': ['collector', 'collector edition', "collector's edition"],
                        'collector': ['collector', 'collector edition', "collector's edition"],
                        'founder edition': ['founder', 'founder edition', "founder's edition"],
                        'founder': ['founder', 'founder edition', "founder's edition"],
                    }
                    
                    if edition_keywords:
                        for keyword in edition_keywords:
                            # Получаем список синонимов для ключевого слова
                            synonyms = keyword_synonyms.get(keyword, [keyword])
                            for synonym in synonyms:
                                if synonym in edition_lower:
                                    keyword_matches.append(edition_info)
                                    matched = True
                                    match_score = 100
                                    break
                            if matched:
                                break
                    
                    if not matched and game_name:
                        game_name_lower = game_name.lower().strip()
                        game_name_normalized = _normalize_game_name(game_name)
                        
                        if game_name_normalized == edition_normalized or game_name_lower == edition_lower:
                            exact_matches.append(edition_info)
                            matched = True
                            match_score = 90
                        elif game_name_normalized in edition_normalized:
                            similarity = len(game_name_normalized) / len(edition_normalized)
                            if similarity >= 0.7:
                                partial_matches.append(edition_info)
                                matched = True
                                match_score = int(similarity * 80)
                        elif edition_normalized in game_name_normalized:
                            similarity = len(edition_normalized) / len(game_name_normalized)
                            if similarity >= 0.7:
                                partial_matches.append(edition_info)
                                matched = True
                                match_score = int(similarity * 80)
                        else:
                            game_words = set(game_name_normalized.split())
                            edition_words = set(edition_normalized.split())
                            common_words = game_words & edition_words
                            
                            if common_words and len(common_words) >= 2:
                                similarity = len(common_words) / max(len(game_words), len(edition_words))
                                if similarity >= 0.6:
                                    partial_matches.append(edition_info)
                                    matched = True
                                    match_score = int(similarity * 70)
                    
                    if not matched:
                        other_editions.append(edition_info)
            
                if keyword_matches:
                    for match in keyword_matches:
                        edition_words = set(_normalize_game_name(match["edition"]).split())
                        if game_name:
                            game_words = set(_normalize_game_name(game_name).split())
                            common_words = game_words & edition_words
                            match["word_overlap"] = len(common_words)
                        else:
                            match["word_overlap"] = 0
                    keyword_matches.sort(key=lambda x: (-x.get("word_overlap", 0), x["price"], len(x["edition"])))
                    if keyword_matches:
                        selected = keyword_matches[0]
                        logger.info(f"{LOGGER_PREFIX} [PRICE] Найдено совпадение по ключевому слову издания: edition='{selected['edition']}', package_id={selected['package_id']}, region={region}, price={selected['price']} USD")
                        return {
                            "package_id": selected["package_id"],
                            "price": selected["price"],
                            "edition": selected["edition"]
                        }
            
                if exact_matches:
                    for match in exact_matches:
                        edition_words = set(_normalize_game_name(match["edition"]).split())
                        if game_name:
                            game_words = set(_normalize_game_name(game_name).split())
                            common_words = game_words & edition_words
                            match["word_overlap"] = len(common_words)
                        else:
                            match["word_overlap"] = 0
                    exact_matches.sort(key=lambda x: (-x.get("word_overlap", 0), x["price"], len(x["edition"])))
                    if exact_matches:
                        selected = exact_matches[0]
                        logger.info(f"{LOGGER_PREFIX} [PRICE] Найдено точное совпадение издания: edition='{selected['edition']}', package_id={selected['package_id']}, region={region}, price={selected['price']} USD")
                        return {
                            "package_id": selected["package_id"],
                            "price": selected["price"],
                            "edition": selected["edition"]
                        }
            
                if partial_matches:
                    for match in partial_matches:
                        edition_words = set(_normalize_game_name(match["edition"]).split())
                        if game_name:
                            game_words = set(_normalize_game_name(game_name).split())
                            common_words = game_words & edition_words
                            match["word_overlap"] = len(common_words)
                        else:
                            match["word_overlap"] = 0
                    partial_matches.sort(key=lambda x: (-x.get("word_overlap", 0), x["price"], len(x["edition"])))
                    if partial_matches:
                        selected = partial_matches[0]
                        logger.info(f"{LOGGER_PREFIX} [PRICE] Найдено частичное совпадение издания: edition='{selected['edition']}', package_id={selected['package_id']}, region={region}, price={selected['price']} USD")
                        return {
                            "package_id": selected["package_id"],
                            "price": selected["price"],
                            "edition": selected["edition"]
                        }
            
                if other_editions:
                    standard_editions = [e for e in other_editions if "standard" in e["edition"].lower() and not any(x in e["edition"].lower() for x in ["deluxe", "ultimate", "premium", "gold", "vault"])]
                    if standard_editions:
                        standard_editions.sort(key=lambda x: (x["price"], len(x["edition"])))
                        selected = standard_editions[0]
                        logger.warning(f"{LOGGER_PREFIX} [PRICE] Используется стандартное издание: edition='{selected['edition']}', package_id={selected['package_id']}, region={region}, price={selected['price']} USD")
                    else:
                        other_editions.sort(key=lambda x: (x["price"], len(x["edition"])))
                        selected = other_editions[0]
                        logger.warning(f"{LOGGER_PREFIX} [PRICE] Совпадение не найдено, используется самое дешевое издание: edition='{selected['edition']}', package_id={selected['package_id']}, region={region}, price={selected['price']} USD")
                    return {
                        "package_id": selected["package_id"],
                        "price": selected["price"],
                        "edition": selected["edition"]
                    }
                
                logger.warning(f"{LOGGER_PREFIX} package_id не найден в ответе для app_id={app_id}")
                return None
            else:
                error_text = ""
                try:
                    error_data = response.json()
                    error_text = f": {error_data}"
                except:
                    error_text = f": {response.text[:100]}"
                logger.error(f"{LOGGER_PREFIX} DesslyHub API вернул ошибку при получении package_id: status_code={response.status_code}{error_text}")
                if attempt < max_retries - 1 and response.status_code >= 500:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"{LOGGER_PREFIX} [TEST] Повторная попытка через {wait_time:.1f} сек...")
                    time.sleep(wait_time)
                    continue
                return None
        except requests.exceptions.Timeout as e:
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)
                logger.warning(f"{LOGGER_PREFIX} Таймаут при получении package_id (попытка {attempt + 1}/{max_retries}), ожидание {wait_time:.1f} сек...")
                time.sleep(wait_time)
                continue
            logger.error(f"{LOGGER_PREFIX} Таймаут при получении package_id после {max_retries} попыток: {e}")
            return None
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)
                logger.warning(f"{LOGGER_PREFIX} Ошибка сети при получении package_id (попытка {attempt + 1}/{max_retries}), ожидание {wait_time:.1f} сек...")
                time.sleep(wait_time)
                continue
            logger.error(f"{LOGGER_PREFIX} Ошибка сети при получении package_id после {max_retries} попыток: {e}")
            return None
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Ошибка при получении package_id для app_id={app_id}: {e}", exc_info=True)
            return None
    
    return None


def _send_steam_gift(api_key: str, app_id: int, friend_link: str, region: str = "KZ", game_name: str = None, lot_name: str = None) -> dict | None:
    try:
        logger.debug(f"{LOGGER_PREFIX} [TEST] Отправка подарка через DesslyHub API: app_id={app_id}, friend_link={friend_link}, region={region}, game_name={game_name}, lot_name={lot_name}")
        
        package_info = _get_package_id_by_app_id(api_key, app_id, region, game_name=game_name, lot_name=lot_name)
        if not package_info:
            logger.error(f"{LOGGER_PREFIX} Не удалось получить package_id для app_id={app_id}")
            return None
        
        package_id = package_info.get("package_id")
        game_price = package_info.get("price")
        
        if not package_id:
            logger.error(f"{LOGGER_PREFIX} package_id не найден в ответе")
            return None
        
        url = "https://desslyhub.com/api/v1/service/steamgift/sendgames"
        headers = {
            "apikey": api_key,
            "Content-Type": "application/json"
        }
        
        friend_link_cleaned = _clean_steam_link(friend_link)
        payload = {
            "invite_url": friend_link_cleaned,
            "package_id": str(package_id),
            "region": region
        }
        
        logger.debug(f"{LOGGER_PREFIX} [TEST] Запрос к DesslyHub: URL={url}")
        logger.debug(f"{LOGGER_PREFIX} [TEST] Payload (типы): invite_url={type(payload['invite_url']).__name__}, package_id={type(payload['package_id']).__name__}, region={type(payload['region']).__name__}")
        logger.debug(f"{LOGGER_PREFIX} [TEST] Payload (значения): {json.dumps(payload, ensure_ascii=False)}")
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        logger.debug(f"{LOGGER_PREFIX} [TEST] Ответ DesslyHub: status_code={response.status_code}")
        logger.debug(f"{LOGGER_PREFIX} [TEST] Response text: {response.text[:1000]}")
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"{LOGGER_PREFIX} [TEST] Подарок успешно отправлен: transaction_id={data.get('transaction_id')}, status={data.get('status')}")
            return data
        else:
            error_text = ""
            error_code = None
            error_message = ""
            try:
                error_data = response.json()
                error_code = error_data.get("error_code", "unknown")
                error_message = error_data.get("message", error_data.get("error", ""))
                
                error_descriptions = {
                    -1: "Internal server error (внутренняя ошибка сервера)",
                    -2: "Insufficient funds in the balance (недостаточно средств на балансе)",
                    -3: "Incorrect amount (неправильная сумма)",
                    -4: "Incorrect request body (неправильное тело запроса)",
                    -5: "Access denied (доступ запрещен)",
                    -51: "Invalid link for adding friends (неверная ссылка для добавления в друзья)",
                    -52: "Incorrect app ID (неправильный app ID)",
                    -53: "Information about the game not found (информация об игре не найдена)",
                    -54: "The user doesn't have the main game (у пользователя нет основной игры)",
                    -55: "The user already has the game (у пользователя уже есть игра)",
                    -56: "Unable to add as a friend (не удалось добавить в друзья)",
                    -57: "Incorrect customer region specified (неправильно указан регион клиента)",
                    -58: "The recipient's region is not available for gift (регион получателя недоступен для подарка)",
                    -59: "The user did not add/remove the bot from friends list (пользователь не добавил/удалил бота из списка друзей)"
                }
                
                error_description = error_descriptions.get(error_code, f"Неизвестная ошибка (код: {error_code})")
                error_text = f": error_code={error_code} ({error_description}), message={error_message}, full_response={json.dumps(error_data, ensure_ascii=False)}"
                logger.error(f"{LOGGER_PREFIX} [TEST] DesslyHub API вернул ошибку: status_code={response.status_code}, error_code={error_code} - {error_description}")
                if error_message:
                    logger.error(f"{LOGGER_PREFIX} [TEST] Сообщение об ошибке: {error_message}")
                logger.error(f"{LOGGER_PREFIX} [TEST] Полный ответ API: {json.dumps(error_data, ensure_ascii=False, indent=2)}")
            except Exception as e:
                error_text = f": {response.text[:500]}"
                logger.error(f"{LOGGER_PREFIX} [TEST] DesslyHub API вернул ошибку: status_code={response.status_code}, response={error_text}, parse_error={e}")
                return {"error_code": None, "error": str(e), "price": game_price}
            
            return {"error_code": error_code, "message": error_message, "price": game_price}
            
    except requests.exceptions.Timeout as e:
        logger.error(f"{LOGGER_PREFIX} [TEST] Таймаут при отправке подарка через DesslyHub: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"{LOGGER_PREFIX} [TEST] Ошибка сети при отправке подарка через DesslyHub: {e}")
        return None
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [TEST] Неожиданная ошибка при отправке подарка через DesslyHub: {e}", exc_info=True)
        return None


def _get_mobile_games(api_key: str) -> list | None:
    global _mobile_games_cache, _mobile_games_cache_timestamp, _mobile_games_cache_lock
    
    try:
        current_time = time.time()
        
        with _mobile_games_cache_lock:
            if (_mobile_games_cache is not None and 
                current_time - _mobile_games_cache_timestamp < _mobile_games_cache_ttl):
                logger.info(f"{LOGGER_PREFIX} [MOBILE] Используем кэш списка мобильных игр")
                return _mobile_games_cache.copy() if isinstance(_mobile_games_cache, list) else _mobile_games_cache
        
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Получение списка мобильных игр с DesslyHub")
        
        url = "https://desslyhub.com/api/v1/service/mobile/games"
        headers = {
            "apikey": api_key,
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            games_list = data.get("games", []) or []
            
            with _mobile_games_cache_lock:
                _mobile_games_cache = games_list
                _mobile_games_cache_timestamp = current_time
            
            logger.info(f"{LOGGER_PREFIX} [MOBILE] Получено {len(games_list)} мобильных игр")
            return games_list
        else:
            error_text = ""
            try:
                error_data = response.json()
                error_text = f": {error_data}"
            except:
                error_text = f": {response.text[:100]}"
            logger.error(f"{LOGGER_PREFIX} [MOBILE] DesslyHub API вернул ошибку при получении списка игр: status_code={response.status_code}{error_text}")
            return None
            
    except requests.exceptions.Timeout as e:
        logger.error(f"{LOGGER_PREFIX} [MOBILE] Таймаут при получении списка мобильных игр: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"{LOGGER_PREFIX} [MOBILE] Ошибка сети при получении списка мобильных игр: {e}")
        return None
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [MOBILE] Неожиданная ошибка при получении списка мобильных игр: {e}", exc_info=True)
        return None


def _get_mobile_game_by_id(api_key: str, game_id: int) -> dict | None:
    try:
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Получение информации об игре: game_id={game_id}")
        
        url = f"https://desslyhub.com/api/v1/service/mobile/games/{game_id}"
        headers = {
            "apikey": api_key,
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"{LOGGER_PREFIX} [MOBILE] Получена информация об игре: id={data.get('id')}, name={data.get('name')}")
            return data
        else:
            error_text = ""
            try:
                error_data = response.json()
                error_text = f": {error_data}"
            except:
                error_text = f": {response.text[:100]}"
            logger.error(f"{LOGGER_PREFIX} [MOBILE] DesslyHub API вернул ошибку при получении информации об игре: status_code={response.status_code}{error_text}")
            return None
            
    except requests.exceptions.Timeout as e:
        logger.error(f"{LOGGER_PREFIX} [MOBILE] Таймаут при получении информации об игре: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"{LOGGER_PREFIX} [MOBILE] Ошибка сети при получении информации об игре: {e}")
        return None
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [MOBILE] Неожиданная ошибка при получении информации об игре: {e}", exc_info=True)
        return None


def _get_mobile_game_id_by_name(game_name: str, api_key: str) -> int | None:
    try:
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Поиск game_id для игры '{game_name}'")
        
        games_list = _get_mobile_games(api_key)
        if not games_list:
            logger.warning(f"{LOGGER_PREFIX} [MOBILE] Список мобильных игр пуст")
            return None
        
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Поиск игры '{game_name}' в списке из {len(games_list)} игр")
        
        game_name_normalized = _normalize_game_name(game_name)
        game_name_lower = game_name.lower().strip()
        game_name_clean = re.sub(r'\s+', ' ', game_name_lower)
        
        def extract_base_and_metadata(name):
            name_normalized = _normalize_game_name(name)
            name_lower = name.lower().strip()
            name_clean = re.sub(r'\s+', ' ', name_lower)
            
            version_pattern = re.search(r'\b(v\d+|version\s*\d+)\b', name_lower)
            version = version_pattern.group(1) if version_pattern else None
            
            region_pattern = re.search(r'\((ru|global|sg|us|eu|asia|china|kr|jp|tw|hk|id|ph|vn|th|my|sg|in|br|mx|ar|cl|co|pe|za|ae|sa|eg|tr|pl|de|fr|es|it|uk|ca|au|nz)\)', name_lower)
            region = region_pattern.group(1) if region_pattern else None
            
            base_name = name_clean
            if version:
                base_name = base_name.replace(version, "").strip()
            if region:
                base_name = re.sub(r'\([^)]*\)', '', base_name).strip()
            
            base_name = re.sub(r'\s+', ' ', base_name)
            base_name = base_name.strip('()').strip()
            
            return base_name, version, region
        
        query_base, query_version, query_region = extract_base_and_metadata(game_name)
        
        exact_matches = []
        perfect_matches = []
        version_matches = []
        base_matches = []
        partial_matches = []
        
        for game in games_list:
            if not isinstance(game, dict):
                continue
            
            game_name_from_api = game.get("name", "")
            if not game_name_from_api:
                continue
            
            game_name_api_lower = game_name_from_api.lower().strip()
            game_name_api_clean = re.sub(r'\s+', ' ', game_name_api_lower)
            
            game_name_api_normalized = _normalize_game_name(game_name_from_api)
            
            if game_name_normalized == game_name_api_normalized or game_name_clean == game_name_api_clean:
                exact_matches.append((game.get("id"), game_name_from_api, 100))
                continue
            
            api_base, api_version, api_region = extract_base_and_metadata(game_name_from_api)
            
            query_words = set(game_name_normalized.split())
            api_words = set(game_name_api_normalized.split())
            common_words = query_words & api_words
            
            if len(common_words) == len(query_words) and len(query_words) >= 2:
                exact_matches.append((game.get("id"), game_name_from_api, 95))
                continue
            
            if query_base == api_base:
                score = 0
                if query_version and api_version and query_version == api_version:
                    score += 50
                if query_region and api_region and query_region == api_region:
                    score += 30
                if query_version is None and api_version is None:
                    score += 20
                if query_region is None and api_region is None:
                    score += 10
                
                if query_version and api_version and query_version == api_version:
                    if query_region and api_region and query_region == api_region:
                        perfect_matches.append((game.get("id"), game_name_from_api, score))
                    else:
                        version_matches.append((game.get("id"), game_name_from_api, score))
                elif query_base == api_base:
                    base_matches.append((game.get("id"), game_name_from_api, score))
            else:
                query_words = set(query_base.split())
                api_words = set(api_base.split())
                
                if query_words and api_words:
                    common_words = query_words & api_words
                    if len(common_words) >= min(2, len(query_words), len(api_words)):
                        score = len(common_words) * 10
                        partial_matches.append((game.get("id"), game_name_from_api, score))
        
        all_matches = []
        if exact_matches:
            all_matches.extend(exact_matches)
        if perfect_matches:
            all_matches.extend(perfect_matches)
        if version_matches:
            all_matches.extend(version_matches)
        if base_matches:
            all_matches.extend(base_matches)
        if partial_matches:
            all_matches.extend(partial_matches)
        
        if all_matches:
            all_matches.sort(key=lambda x: x[2], reverse=True)
            game_id, found_name, score = all_matches[0]
            
            match_type = "точное совпадение"
            if exact_matches and all_matches[0] in exact_matches:
                match_type = "точное совпадение"
            elif perfect_matches and all_matches[0] in perfect_matches:
                match_type = "идеальное совпадение (база+версия+регион)"
            elif version_matches and all_matches[0] in version_matches:
                match_type = "совпадение с версией"
            elif base_matches and all_matches[0] in base_matches:
                match_type = "совпадение базового названия"
            else:
                match_type = "частичное совпадение"
            
            logger.info(f"{LOGGER_PREFIX} [MOBILE] Найдено {match_type}: game_id={game_id} для '{game_name}' (найдено как '{found_name}', score={score})")
            return game_id
        
        logger.warning(f"{LOGGER_PREFIX} [MOBILE] Игра '{game_name}' не найдена в списке мобильных игр")
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Доступные игры: {[g.get('name') for g in games_list[:15]]}")
        return None
        
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [MOBILE] Ошибка при поиске game_id для игры '{game_name}': {e}", exc_info=True)
        return None


def _send_mobile_refill(api_key: str, position_id: int, fields: dict, reference: str = None) -> dict | None:
    try:
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Отправка пополнения мобильной игры: position_id={position_id}, fields={fields}, reference={reference}")
        
        url = "https://desslyhub.com/api/v1/service/mobile/games/refill"
        headers = {
            "apikey": api_key,
            "Content-Type": "application/json"
        }
        
        payload = {
            "position": position_id,
            "fields": fields
        }
        
        if reference:
            payload["reference"] = reference
        
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Запрос к DesslyHub: URL={url}")
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Payload: {json.dumps(payload, ensure_ascii=False)}")
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Ответ DesslyHub: status_code={response.status_code}")
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Response text: {response.text[:1000]}")
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"{LOGGER_PREFIX} [MOBILE] Пополнение успешно отправлено: transaction_id={data.get('transaction_id')}, status={data.get('status')}")
            return data
        else:
            error_text = ""
            error_code = None
            error_message = ""
            try:
                error_data = response.json()
                error_code = error_data.get("error_code", "unknown")
                error_message = error_data.get("message", "")
                
                error_descriptions = {
                    -200: "Mobile game not found (мобильная игра не найдена)",
                    -201: "Mobile game position not found (позиция мобильной игры не найдена)",
                    -2: "Insufficient funds in the balance (недостаточно средств на балансе)"
                }
                
                error_description = error_descriptions.get(error_code, f"Неизвестная ошибка (код: {error_code})")
                error_text = f": error_code={error_code} ({error_description}), message={error_message}, full_response={json.dumps(error_data, ensure_ascii=False)}"
                logger.error(f"{LOGGER_PREFIX} [MOBILE] DesslyHub API вернул ошибку: status_code={response.status_code}, error_code={error_code} - {error_description}")
                if error_message:
                    logger.error(f"{LOGGER_PREFIX} [MOBILE] Сообщение об ошибке: {error_message}")
                logger.error(f"{LOGGER_PREFIX} [MOBILE] Полный ответ API: {json.dumps(error_data, ensure_ascii=False, indent=2)}")
            except Exception as e:
                error_text = f": {response.text[:500]}"
                logger.error(f"{LOGGER_PREFIX} [MOBILE] DesslyHub API вернул ошибку: status_code={response.status_code}, response={error_text}, parse_error={e}")
                return {"error_code": None, "error": str(e)}
            
            return {"error_code": error_code, "message": error_message}
            
    except requests.exceptions.Timeout as e:
        logger.error(f"{LOGGER_PREFIX} [MOBILE] Таймаут при отправке пополнения: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"{LOGGER_PREFIX} [MOBILE] Ошибка сети при отправке пополнения: {e}")
        return None
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [MOBILE] Неожиданная ошибка при отправке пополнения: {e}", exc_info=True)
        return None


def _clean_steam_link(link: str) -> str:
    try:
        cleaned = link.strip()
        cleaned = re.sub(r'[\u200B-\u200D\uFEFF\u2060]', '', cleaned)
        cleaned = re.sub(r'[^\x20-\x7E\u0400-\u04FF]', '', cleaned)
        cleaned = cleaned.strip()
        logger.info(f"{LOGGER_PREFIX} [TEST] Очистка ссылки: исходная длина={len(link)}, очищенная длина={len(cleaned)}")
        logger.debug(f"{LOGGER_PREFIX} [TEST] Исходная ссылка: {repr(link)}")
        logger.debug(f"{LOGGER_PREFIX} [TEST] Очищенная ссылка: {repr(cleaned)}")
        return cleaned
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [TEST] Ошибка при очистке ссылки Steam: {e}", exc_info=True)
        return link.strip()


def _validate_steam_friend_link(link: str) -> bool:
    try:
        link = _clean_steam_link(link)
        if not link:
            logger.warning(f"{LOGGER_PREFIX} [TEST] Пустая ссылка на профиль Steam")
            return False
        
        steam_patterns = [
            r'https?://s\.team/[^/\s]+',
            r'https?://steamcommunity\.com/(?:profiles|id)/[^/\s]+',
            r'https?://steamcommunity\.com/friends/add/[^/\s]+'
        ]
        
        for pattern in steam_patterns:
            if re.match(pattern, link, re.IGNORECASE):
                logger.info(f"{LOGGER_PREFIX} [TEST] Валидная ссылка Steam: {link}")
                return True
        
        logger.warning(f"{LOGGER_PREFIX} [TEST] Невалидная ссылка Steam: {link}")
        return False
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [TEST] Ошибка при валидации ссылки Steam: {e}", exc_info=True)
        return False


def _create_or_update_lot(cardinal: "Cardinal", game_name: str, lot_id: int | None, subcategory_id: int | None, price: float, name_template: str, desc_template: str) -> bool:
    try:
        if not cardinal or not hasattr(cardinal, 'account') or not cardinal.account.is_initiated:
            logger.error(f"{LOGGER_PREFIX} Account не инициализирован")
            return False
        
        account = cardinal.account
        
        if lot_id:
            try:
                lot_fields = account.get_lot_fields(lot_id)
            except Exception as e:
                logger.warning(f"{LOGGER_PREFIX} Не удалось получить поля лота {lot_id}, создаем новый: {e}")
                lot_fields = None
        else:
            lot_fields = None
        
        if lot_fields is None:
            if subcategory_id is None:
                logger.error(f"{LOGGER_PREFIX} Не указан subcategory_id для создания лота")
                return False
            
            try:
                subcategory = account.get_subcategory(SubCategoryTypes.COMMON, subcategory_id)
                if not subcategory:
                    logger.error(f"{LOGGER_PREFIX} Подкатегория {subcategory_id} не найдена")
                    return False
                
                my_lots = account.get_my_subcategory_lots(subcategory_id)
                if my_lots:
                    existing_lot = my_lots[0]
                    lot_id = existing_lot.id
                    lot_fields = account.get_lot_fields(lot_id)
                else:
                    logger.warning(f"{LOGGER_PREFIX} Нет существующих лотов в подкатегории {subcategory_id}, требуется создать вручную")
                    return False
            except Exception as e:
                logger.error(f"{LOGGER_PREFIX} Ошибка при создании/получении лота: {e}")
                return False
        
        lot_fields.title_ru = name_template.format(game_name=game_name)
        lot_fields.description_ru = desc_template.format(game_name=game_name)
        lot_fields.price = price
        lot_fields.active = True
        
        account.save_lot(lot_fields)
        logger.info(f"{LOGGER_PREFIX} Лот {lot_id} обновлен для игры '{game_name}'")
        return True
        
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка при создании/обновлении лота для '{game_name}': {e}")
        return False


def _auto_list_all_games(cardinal: "Cardinal") -> dict:
    storage = _get_storage()
    games = storage.load_games()
    settings = storage.load_settings()
    templates = storage.load_templates()
    
    if not games:
        return {"success": 0, "failed": 0, "errors": ["Нет лотов для выставления"]}
    
    name_template = templates.get("name_template", "{game_name} - Аренда аккаунта")
    desc_template = templates.get("description_template", "🎮 Аренда аккаунта Steam для игры {game_name}")
    markup_percent = settings.get("markup_percent", 10.0)
    api_key = settings.get("desslyhub_api_key", "")
    
    success_count = 0
    failed_count = 0
    errors = []
    
    for game in games:
        game_name = game.get("name", "")
        if not game_name:
            failed_count += 1
            errors.append(f"Лот без названия: {game}")
            continue
        
        base_price = None
        if api_key:
            base_price = _get_desslyhub_price(game_name, api_key)
        
        if base_price is None:
            base_price = game.get("base_price", 100.0)
        
        final_price = _calculate_price_with_markup(base_price, markup_percent)
        
        lot_id = game.get("lot_id")
        subcategory_id = None
        
        if not lot_id:
            lot_info = _determine_lot(cardinal, game_name)
            if lot_info:
                lot_id = lot_info.get("lot_id")
                subcategory_id = lot_info.get("subcategory").id if lot_info.get("subcategory") else None
            else:
                if hasattr(cardinal, 'profile') and cardinal.profile:
                    subcategories = cardinal.profile.get_sorted_subcategories()
                    common_subs = subcategories.get(SubCategoryTypes.COMMON, {})
                    if common_subs:
                        first_sub = list(common_subs.values())[0]
                        subcategory_id = first_sub.id
        
        if _create_or_update_lot(cardinal, game_name, lot_id, subcategory_id, final_price, name_template, desc_template):
            success_count += 1
            if lot_id:
                game["lot_id"] = lot_id
                game["last_price"] = final_price
                game["last_sync"] = int(time.time())
        else:
            failed_count += 1
            errors.append(f"Не удалось обновить лот для '{game_name}'")
    
    storage.save_games(games)
    
    return {
        "success": success_count,
        "failed": failed_count,
        "errors": errors
    }


def _normalize_lot_name(name: str) -> str:
    import re
    import unicodedata
    if not name:
        return ""
    normalized = name.lower().strip()
    normalized = unicodedata.normalize('NFKC', normalized)
    normalized = re.sub(r'[^\w\s\u0400-\u04ff]', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized.strip()


REGION_WORDS = [
    "турция", "россия", "беларусь", "украина", "казахстан", "аргентина",
    "turkey", "russia", "belarus", "ukraine", "kazakhstan", "argentina"
]

LOT_METADATA_WORDS = [
    "автовыдача", "выдача", "подарком", "подарок", "steam", "стим",
    "игра", "pc", "game", "gift"
]

EDITION_WORDS = [
    "deluxe", "standard", "gold", "premium", "ultimate", "definitive",
    "collector", "complete", "bundle", "pack", "edition", "digital",
    "season", "collection", "vault", "royal", "anniversary", "legacy",
    "classic", "remastered", "remaster", "hd", "plus", "super", "mega"
]


def _extract_base_game_name(name: str) -> str:
    import re
    if not name:
        return ""
    text = name
    text = text.split(",")[0]
    text = re.sub(r'\[[^\]]*]', ' ', text)
    text = re.sub(r'[🎁🔵🟥⭐◄►▪️🔴]', ' ', text)
    for region in REGION_WORDS:
        text = re.sub(rf'\b{re.escape(region)}\b', ' ', text, flags=re.IGNORECASE)
    for meta in LOT_METADATA_WORDS:
        text = re.sub(rf'\b{re.escape(meta)}\b', ' ', text, flags=re.IGNORECASE)
    for edition in EDITION_WORDS:
        text = re.sub(rf'\b{re.escape(edition)}\b', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+', ' ', text)
    return text.strip().lower()


def _format_base_game_name(base_name: str) -> str:
    if not base_name:
        return ""
    words = []
    for word in base_name.split():
        if word.isdigit():
            words.append(word)
        elif len(word) <= 3:
            words.append(word.upper())
        else:
            words.append(word.capitalize())
    return " ".join(words)


_bad_game_names_logged = set()


def _log_bad_game_name(lot_name: str, raw_game_name: str | None):
    base_lot = _extract_base_game_name(lot_name)
    base_game = _extract_base_game_name(raw_game_name or "")
    if (not raw_game_name) or (base_game == base_lot) or (raw_game_name.strip().startswith("[АВТОВЫДАЧА]")):
        key = (lot_name, raw_game_name or "")
        if key not in _bad_game_names_logged:
            logger.warning(f"{LOGGER_PREFIX} ⚠️ В конфиге лота '{lot_name}' поле game_name='{raw_game_name}' требует очистки. Используйте настоящее название игры (пример: '{_format_base_game_name(base_lot)}').")
            _bad_game_names_logged.add(key)


def _derive_game_name(lot_name: str, raw_game_name: str | None) -> str:
    if raw_game_name:
        raw_clean = raw_game_name.strip()
        if raw_clean and not raw_clean.startswith("[") and "🎁" not in raw_clean and "🔵" not in raw_clean:
            if len(raw_clean) >= 2 and not raw_clean.lower().startswith("автовыдача"):
                return raw_clean
    
    candidates = []
    if raw_game_name:
        candidates.append(raw_game_name)
    candidates.append(lot_name)
    
    for candidate in candidates:
        cleaned = _extract_base_game_name(candidate)
        if cleaned and len(cleaned) > 1:
            formatted = _format_base_game_name(cleaned)
            if formatted:
                if candidate is lot_name and raw_game_name:
                    _log_bad_game_name(lot_name, raw_game_name)
                return formatted
    if raw_game_name:
        _log_bad_game_name(lot_name, raw_game_name)
    return raw_game_name or lot_name

def _calculate_similarity(name1: str, name2: str) -> float:
    norm1 = _normalize_lot_name(name1)
    norm2 = _normalize_lot_name(name2)
    
    if norm1 == norm2:
        return 1.0
    
    if norm1 in norm2 or norm2 in norm1:
        return 0.95
    
    words1 = set(norm1.split())
    words2 = set(norm2.split())
    
    if not words1 or not words2:
        return 0.0
    
    common_words = words1 & words2
    if not common_words:
        return 0.0
    
    if len(common_words) == len(words1) and len(words1) >= 2:
        return 0.9
    
    jaccard = len(common_words) / len(words1 | words2)
    word_coverage = len(common_words) / max(len(words1), len(words2))
    
    return (jaccard * 0.5 + word_coverage * 0.5)

def _find_lot_by_name_in_profile(cardinal: "Cardinal", lot_name: str) -> types.LotShortcut | None:
    try:
        if not hasattr(cardinal, 'profile') or not cardinal.profile:
            return None
        
        try:
            if hasattr(cardinal, 'account') and cardinal.account and cardinal.account.is_initiated:
                updated_profile = cardinal.account.get_user(cardinal.account.id)
                if updated_profile:
                    lots_dict = updated_profile.get_sorted_lots(1)
                    lots = list(lots_dict.values()) if lots_dict else []
                    if lots:
                        logger.debug(f"{LOGGER_PREFIX} Обновлен профиль, найдено {len(lots)} лотов")
        except Exception as e:
            logger.debug(f"{LOGGER_PREFIX} Не удалось обновить профиль: {e}")
        
        if 'lots' not in locals() or not lots:
            lots_dict = cardinal.profile.get_sorted_lots(1)
            lots = list(lots_dict.values()) if lots_dict else []
        
        if not lots:
            lots = cardinal.profile.get_lots()
        
        if hasattr(cardinal, 'curr_profile') and cardinal.curr_profile:
            try:
                curr_lots_dict = cardinal.curr_profile.get_sorted_lots(1)
                curr_lots = list(curr_lots_dict.values()) if curr_lots_dict else []
                if curr_lots:
                    lots.extend(curr_lots)
                    unique_lots = {}
                    for lot in lots:
                        unique_lots[lot.id] = lot
                    lots = list(unique_lots.values())
            except Exception:
                pass
        lot_name_normalized = _normalize_lot_name(lot_name)
        lot_name_lower = lot_name.lower().strip()
        lot_base_name = _extract_base_game_name(lot_name)
        
        regions = ["турция", "россия", "беларусь", "украина", "казахстан", "аргентина", "любой"]
        editions = ["deluxe", "standard", "gold", "premium", "revolution", "definitive", "ultimate"]
        
        lot_region = None
        lot_edition = None
        for region in regions:
            if region in lot_name_lower:
                lot_region = region
                break
        for edition in editions:
            if edition in lot_name_lower:
                lot_edition = edition
                break
        
        logger.debug(f"{LOGGER_PREFIX} Поиск лота '{lot_name}': извлеченный регион='{lot_region}', издание='{lot_edition}'")
        
        exact_match = None
        all_matches = []
        
        lot_name_words = set(lot_name_normalized.split())
        
        for lot in lots:
            lot_desc = (lot.description or "").strip()
            if not lot_desc:
                continue
            
            lot_desc_normalized = _normalize_lot_name(lot_desc)
            lot_desc_lower = lot_desc.lower()
            lot_desc_base = _extract_base_game_name(lot_desc)
            
            base_overlap = True
            if lot_base_name:
                if lot_desc_base:
                    if lot_base_name in lot_desc_base or lot_desc_base in lot_base_name:
                        base_overlap = True
                    else:
                        base_overlap = bool(set(lot_base_name.split()) & set(lot_desc_base.split()))
                else:
                    base_overlap = False
            if not base_overlap:
                continue
            
            if lot_name_normalized == lot_desc_normalized or lot_name_lower == lot_desc_lower:
                exact_match = lot
                logger.debug(f"{LOGGER_PREFIX} Найдено точное совпадение для '{lot_name}': ID={exact_match.id}, описание='{exact_match.description}'")
                break
            
            lot_desc_words = set(lot_desc_normalized.split())
            common_words = lot_name_words & lot_desc_words
            
            if not common_words:
                continue
            
            if len(common_words) == len(lot_name_words) and len(lot_name_words) >= 2:
                similarity = 0.9
            else:
                similarity = _calculate_similarity(lot_name, lot_desc)
            
            if similarity < 0.5:
                continue
            
            desc_region = None
            desc_edition = None
            for region in regions:
                if region in lot_desc_lower:
                    desc_region = region
                    break
            for edition in editions:
                if edition in lot_desc_lower:
                    desc_edition = edition
                    break
            
            region_match = False
            if lot_region and lot_region != "любой":
                if desc_region and desc_region == lot_region:
                    region_match = True
                elif lot_region in lot_desc_lower:
                    region_match = True
            else:
                region_match = True
            
            if not region_match:
                continue
            
            edition_match = True
            if lot_edition:
                if desc_edition and desc_edition == lot_edition:
                    edition_match = True
                elif lot_edition in lot_desc_lower:
                    edition_match = True
                else:
                    edition_match = False
            
            if lot_name_normalized in lot_desc_normalized or lot_desc_normalized in lot_name_normalized:
                similarity = 0.9
            
            if region_match and edition_match:
                all_matches.append((lot, similarity, region_match, edition_match))
        
        if exact_match:
            return exact_match
        
        if not all_matches:
            logger.debug(f"{LOGGER_PREFIX} Лот не найден для '{lot_name}'")
            return None
        
        all_matches.sort(key=lambda x: (x[2], x[3], x[1]), reverse=True)
        best_match = all_matches[0][0]
        best_similarity = all_matches[0][1]
        
        if best_similarity < 0.6:
            logger.warning(f"{LOGGER_PREFIX} ⚠️ Схожесть слишком низкая для '{lot_name}': ID={best_match.id}, схожесть={best_similarity:.2f}, описание='{best_match.description}'")
            return None
        
        logger.debug(f"{LOGGER_PREFIX} Найдено совпадение для '{lot_name}': ID={best_match.id}, схожесть={best_similarity:.2f}, описание='{best_match.description}'")
        return best_match
        
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка поиска лота '{lot_name}': {e}")
        return None

def _process_single_lot(lot_config, cardinal, api_key, markup_percent, api):
    lot_name = lot_config.get("lot_name", "").strip()
    if not lot_name:
        return None
    
    lot_type = lot_config.get("type", "").strip()
    raw_game_name = lot_config.get("game_name", "").strip()
    game_name = _derive_game_name(lot_name, raw_game_name)
    
    if not game_name:
        return {"error": "no_game_name", "message": f"Не указано название игры для лота '{lot_name}'"}
    
    if game_name == lot_name or (len(game_name) > 20 and any(x in game_name for x in ['[АВТОВЫДАЧА]', '🎁', '🔵STEAM', 'ПОДАРКОМ'])):
        cleaned_base = _extract_base_game_name(game_name)
        if cleaned_base and len(cleaned_base) > 1:
            formatted_name = _format_base_game_name(cleaned_base)
            if formatted_name:
                logger.debug(f"{LOGGER_PREFIX} [{lot_name}] Очищено название игры: '{game_name}' -> '{formatted_name}'")
                game_name = formatted_name
    
    try:
        funpay_lot = _find_lot_by_name_in_profile(cardinal, lot_name)
    except Exception as e:
        logger.warning(f"{LOGGER_PREFIX} Ошибка поиска лота '{lot_name}' (игра: {game_name}): {e}")
        return {"error": "lot_not_found", "message": f"Ошибка поиска лота '{lot_name}' (игра: {game_name}): {e}"}
    
    if not funpay_lot:
        return {"error": "lot_not_found", "message": f"Лот '{lot_name}' (игра: {game_name}) не найден на FunPay"}
    
    if lot_type.lower() == "steam gift":
        region = lot_config.get("region", "KZ")
    
    lot_id_str = str(funpay_lot.id)
    lot_id_type = type(funpay_lot.id).__name__
    lot_price_from_profile = getattr(funpay_lot, 'price', None)
    logger.info(f"{LOGGER_PREFIX} [{lot_name}] Найден лот: ID={lot_id_str} (тип: {lot_id_type}), описание='{funpay_lot.description}', цена из профиля={lot_price_from_profile}")
    
    base_price_usd = None
    
    if lot_type.lower() == "steam gift":
        logger.info(f"{LOGGER_PREFIX} [{lot_name}] Используется регион: {region} для запроса цены")
        app_id = _get_game_app_id_by_name(game_name, api_key)
        if app_id:
            package_info = _get_package_id_by_app_id(api_key, app_id, region, game_name, lot_name)
            if package_info:
                price_value = package_info.get("price")
                edition_name = package_info.get("edition", "N/A")
                price_currency = package_info.get("currency") or package_info.get("curr")
                logger.info(f"{LOGGER_PREFIX} [{lot_name}] API вернул: price={price_value}, currency={price_currency}, region={region}")
                
                if price_value is not None:
                    try:
                        raw_price = float(price_value)
                        
                        region_currency_map = {
                            "RU": "RUB",
                            "KZ": "KZT", 
                            "UA": "UAH",
                            "BY": "BYN",
                            "TR": "TRY",
                            "AR": "ARS"
                        }
                        expected_currency = region_currency_map.get(region, "USD")
                        
                        if price_currency and price_currency.upper() != "USD":
                            logger.info(f"{LOGGER_PREFIX} [{lot_name}] Цена в валюте {price_currency}, конвертируем в USD")
                            base_price_usd = api.convert_to_usd(raw_price, price_currency)
                            if base_price_usd is None:
                                logger.error(f"{LOGGER_PREFIX} [{lot_name}] Не удалось конвертировать {raw_price} {price_currency} в USD")
                                return {"error": "conversion_error", "message": f"Не удалось конвертировать цену для '{lot_name}'"}
                        elif raw_price < 0.1 and region in ["RU", "KZ", "UA"]:
                            logger.warning(f"{LOGGER_PREFIX} [{lot_name}] Подозрительно низкая цена {raw_price} для региона {region}, возможно цена уже в {expected_currency}")
                            base_price_usd = api.convert_to_usd(raw_price, expected_currency)
                            if base_price_usd is None or base_price_usd < 0.01:
                                base_price_usd = raw_price
                                logger.warning(f"{LOGGER_PREFIX} [{lot_name}] Используем цену как USD: {base_price_usd}")
                        else:
                            base_price_usd = raw_price
                        
                        logger.info(f"{LOGGER_PREFIX} [{lot_name}] Базовая цена: {base_price_usd:.2f} USD (издание: {edition_name}, регион: {region})")
                    except (ValueError, TypeError) as price_error:
                        logger.error(f"{LOGGER_PREFIX} [{lot_name}] Неверный формат цены из API: {price_value}, ошибка: {price_error}")
                        return {"error": "price_not_found", "message": f"Неверный формат цены для '{lot_name}' (игра: {game_name})"}
                else:
                    logger.error(f"{LOGGER_PREFIX} [{lot_name}] Цена не найдена в ответе API")
                    return {"error": "price_not_found", "message": f"Цена не найдена для '{lot_name}' (игра: {game_name})"}
            else:
                alternative_regions = ["KZ", "RU", "UA", "BY", "TR", "AR"]
                for alt_region in alternative_regions:
                    if alt_region == region:
                        continue
                    logger.debug(f"{LOGGER_PREFIX} [{lot_name}] Попытка получить цену для альтернативного региона: {alt_region}")
                    package_info = _get_package_id_by_app_id(api_key, app_id, alt_region, game_name, lot_name)
                    if package_info and package_info.get("price") is not None:
                        price_value = package_info.get("price")
                        price_currency = package_info.get("currency") or package_info.get("curr")
                        try:
                            raw_price = float(price_value)
                            
                            region_currency_map = {
                                "RU": "RUB", "KZ": "KZT", "UA": "UAH",
                                "BY": "BYN", "TR": "TRY", "AR": "ARS"
                            }
                            expected_currency = region_currency_map.get(alt_region, "USD")
                            
                            if price_currency and price_currency.upper() != "USD":
                                base_price_usd = api.convert_to_usd(raw_price, price_currency)
                            elif raw_price < 0.1:
                                base_price_usd = api.convert_to_usd(raw_price, expected_currency) or raw_price
                            else:
                                base_price_usd = raw_price
                            
                            if base_price_usd and base_price_usd > 0:
                                logger.warning(f"{LOGGER_PREFIX} [{lot_name}] Получена цена для альтернативного региона {alt_region}: {base_price_usd:.2f} USD")
                                break
                        except (ValueError, TypeError):
                            continue
                if base_price_usd is None:
                    logger.error(f"{LOGGER_PREFIX} [{lot_name}] Цена не найдена ни для одного региона")
                    return {"error": "price_not_found", "message": f"Цена не найдена для '{lot_name}' (игра: {game_name}) ни для одного региона"}
        else:
            logger.error(f"{LOGGER_PREFIX} [{lot_name}] Не удалось найти app_id для игры '{game_name}'")
            return {"error": "price_not_found", "message": f"Не удалось найти app_id для игры '{game_name}' для лота '{lot_name}'"}
    elif lot_type.lower() == "mobile refill":
        amount = lot_config.get("amount", "").strip()
        if not amount:
            return {"error": "price_not_found", "message": f"Не указана сумма для мобильной пополнения '{lot_name}' (игра: {game_name})"}
        game_id = _get_mobile_game_id_by_name(game_name, api_key)
        if not game_id:
            logger.error(f"{LOGGER_PREFIX} [{lot_name}] (игра: {game_name}) Не удалось найти game_id для мобильной игры")
            return {"error": "price_not_found", "message": f"Не удалось найти игру '{game_name}' в списке мобильных игр для лота '{lot_name}'"}
        game_info = _get_mobile_game_by_id(api_key, game_id)
        if not game_info:
            logger.error(f"{LOGGER_PREFIX} [{lot_name}] (игра: {game_name}) Не удалось получить информацию об игре game_id={game_id}")
            return {"error": "price_not_found", "message": f"Не удалось получить информацию об игре '{game_name}' (game_id={game_id}) для лота '{lot_name}'"}
        positions = game_info.get("positions", [])
        if not positions:
            logger.error(f"{LOGGER_PREFIX} [{lot_name}] (игра: {game_name}) Список позиций пуст для game_id={game_id}")
            return {"error": "price_not_found", "message": f"Список позиций пуст для игры '{game_name}' (game_id={game_id}) для лота '{lot_name}'"}
        logger.debug(f"{LOGGER_PREFIX} [{lot_name}] Поиск позиции с суммой '{amount}' в {len(positions)} позициях")
        for pos in positions:
            pos_name = pos.get("name", "").lower()
            if amount.lower() in pos_name or pos_name in amount.lower():
                price_value = pos.get("price")
                if price_value is not None:
                    try:
                        base_price_usd = float(price_value)
                        logger.info(f"{LOGGER_PREFIX} [{lot_name}] Найдена цена для позиции '{pos.get('name')}': {base_price_usd:.2f} USD")
                        break
                    except (ValueError, TypeError) as price_error:
                        logger.warning(f"{LOGGER_PREFIX} [{lot_name}] (игра: {game_name}) Неверный формат цены для позиции '{pos_name}': {price_value}, ошибка: {price_error}")
                        continue
        if base_price_usd is None:
            logger.warning(f"{LOGGER_PREFIX} [{lot_name}] (игра: {game_name}) Не найдена позиция с суммой '{amount}' в списке позиций")
            available_amounts = [pos.get("name", "") for pos in positions[:5]]
            logger.debug(f"{LOGGER_PREFIX} [{lot_name}] Доступные позиции (первые 5): {available_amounts}")
    
    if base_price_usd is None or base_price_usd <= 0:
        return {"error": "price_not_found", "message": f"Не удалось получить цену для '{lot_name}' (игра: {game_name})"}
    
    try:
        rates = api.get_exchange_rates()
        if not rates:
            logger.error(f"{LOGGER_PREFIX} [{lot_name}] (игра: {game_name}) Не удалось получить курсы валют")
            return {"error": "conversion_error", "message": f"Не удалось получить курсы валют для '{lot_name}' (игра: {game_name})"}
        
        rub_rate = rates.get("RUB", 0)
        if rub_rate <= 0:
            logger.error(f"{LOGGER_PREFIX} [{lot_name}] (игра: {game_name}) Неверный курс USD/RUB: {rub_rate}")
            return {"error": "conversion_error", "message": f"Неверный курс USD/RUB для '{lot_name}' (игра: {game_name})"}
        
        logger.debug(f"{LOGGER_PREFIX} [{lot_name}] Базовая цена: {base_price_usd:.2f} USD, наценка: {markup_percent}%, курс USD/RUB: {rub_rate}")
        final_price_usd = _calculate_price_with_markup(base_price_usd, markup_percent)
        logger.debug(f"{LOGGER_PREFIX} [{lot_name}] Цена с наценкой: {final_price_usd:.2f} USD")
        final_price_rub = api.convert_from_usd(final_price_usd, "RUB")
        if final_price_rub is None or final_price_rub <= 0:
            logger.error(f"{LOGGER_PREFIX} [{lot_name}] (игра: {game_name}) Не удалось конвертировать цену: {final_price_usd} USD -> RUB")
            return {"error": "conversion_error", "message": f"Не удалось конвертировать цену для '{lot_name}' (игра: {game_name}) из USD в RUB"}
    except Exception as conversion_error:
        logger.error(f"{LOGGER_PREFIX} [{lot_name}] (игра: {game_name}) Ошибка конвертации валюты: {conversion_error}")
        return {"error": "conversion_error", "message": f"Ошибка конвертации валюты для '{lot_name}' (игра: {game_name}): {conversion_error}"}
    
    logger.debug(f"{LOGGER_PREFIX} [{lot_name}] Цена в RUB: {final_price_rub:.2f} RUB")
    
    try:
        from FunPayAPI.common import exceptions as fp_exceptions
        
        lot_id = funpay_lot.id
        if isinstance(lot_id, str):
            if lot_id.isnumeric():
                lot_id = int(lot_id)
            elif "-" in lot_id:
                parts = lot_id.split("-")
                if len(parts) > 0 and parts[0].isnumeric():
                    lot_id = int(parts[0])
                    logger.debug(f"{LOGGER_PREFIX} [{lot_name}] Преобразовал ID из '{funpay_lot.id}' в {lot_id}")
                else:
                    logger.error(f"{LOGGER_PREFIX} ❌ Неверный формат ID лота '{lot_name}': {funpay_lot.id}")
                    return {"error": "update_error", "message": f"Неверный формат ID лота '{lot_name}': {funpay_lot.id}"}
            else:
                logger.error(f"{LOGGER_PREFIX} ❌ Неверный формат ID лота '{lot_name}': {funpay_lot.id}")
                return {"error": "update_error", "message": f"Неверный формат ID лота '{lot_name}': {funpay_lot.id}"}
        
        lot_fields = None
        current_price = None
        
        try:
            lot_fields = cardinal.account.get_lot_fields(lot_id)
            if lot_fields and hasattr(lot_fields, 'price') and lot_fields.price is not None:
                try:
                    current_price = float(lot_fields.price)
                except (ValueError, TypeError) as price_error:
                    logger.warning(f"{LOGGER_PREFIX} ⚠️ Не удалось преобразовать цену лота '{lot_name}' в число: {lot_fields.price}, ошибка: {price_error}")
                    current_price = None
        except fp_exceptions.LotParsingError as e:
            logger.warning(f"{LOGGER_PREFIX} ⚠️ LotParsingError для лота '{lot_name}' (ID: {lot_id}, исходный: {funpay_lot.id}): FunPay вернул HTML вместо данных")
            if hasattr(funpay_lot, 'price') and funpay_lot.price is not None:
                try:
                    current_price = float(funpay_lot.price)
                    logger.info(f"{LOGGER_PREFIX} Используем цену из профиля для '{lot_name}': {current_price:.2f} RUB")
                    time.sleep(0.5)
                    try:
                        lot_fields = cardinal.account.get_lot_fields(lot_id)
                    except Exception as retry_e:
                        logger.error(f"{LOGGER_PREFIX} ❌ Повторная попытка получить lot_fields для '{lot_name}' не удалась: {retry_e}")
                        return {"error": "update_error", "message": f"Не удалось получить данные лота '{lot_name}' для обновления: LotParsingError"}
                except (ValueError, TypeError) as price_error:
                    logger.error(f"{LOGGER_PREFIX} ❌ Не удалось преобразовать цену из профиля для '{lot_name}': {funpay_lot.price}, ошибка: {price_error}")
                    return {"error": "update_error", "message": f"Неверный формат цены для лота '{lot_name}'"}
            else:
                logger.error(f"{LOGGER_PREFIX} ❌ Не удалось получить цену для лота '{lot_name}': LotParsingError и цена из профиля недоступна")
                return {"error": "update_error", "message": f"Не удалось получить цену для лота '{lot_name}': LotParsingError"}
        
        if lot_fields is None:
            logger.error(f"{LOGGER_PREFIX} ❌ lot_fields не определен для лота '{lot_name}'")
            return {"error": "update_error", "message": f"Не удалось получить данные лота '{lot_name}' для обновления"}
        
        if current_price is None:
            logger.warning(f"{LOGGER_PREFIX} ⚠️ Текущая цена для лота '{lot_name}' недоступна, обновляем без проверки")
            current_price = 0.0
        
        final_price_rub_rounded = round(final_price_rub, 2)
        
        logger.debug(f"{LOGGER_PREFIX} [{lot_name}] Текущая цена на FunPay: {current_price:.2f} RUB, новая цена: {final_price_rub_rounded:.2f} RUB, разница: {abs(current_price - final_price_rub_rounded):.2f} RUB")
        
        if abs(current_price - final_price_rub_rounded) >= 0.01:
            try:
                lot_fields.price = final_price_rub_rounded
                if hasattr(lot_fields, 'active'):
                    lot_fields.active = True
                cardinal.account.save_lot(lot_fields)
                logger.info(f"{LOGGER_PREFIX} ✅ '{lot_name}': {current_price:.0f}₽ → {final_price_rub_rounded:.0f}₽")
                return {"success": True, "lot_name": lot_name}
            except Exception as save_error:
                logger.error(f"{LOGGER_PREFIX} ❌ Ошибка сохранения цены для лота '{lot_name}' (игра: {game_name}): {save_error}")
                return {"error": "update_error", "message": f"Ошибка сохранения цены для лота '{lot_name}' (игра: {game_name}): {save_error}"}
        else:
            logger.debug(f"{LOGGER_PREFIX} ⏭️ Цена для '{lot_name}' не изменилась: {current_price:.2f} RUB ≈ {final_price_rub_rounded:.2f} RUB")
            return {"success": True, "lot_name": lot_name, "skipped": True}
    except Exception as e:
        error_msg = str(e)
        error_type = type(e).__name__
        logger.warning(f"{LOGGER_PREFIX} ❌ Ошибка обновления цены для '{lot_name}': {error_type}: {error_msg}")
        return {"error": "update_error", "message": f"Ошибка обновления цены для '{lot_name}': {error_type}: {error_msg}"}

def _sync_prices_from_desslyhub(cardinal: "Cardinal") -> dict:
    storage = _get_storage()
    settings = storage.load_settings()
    lots_config = storage.load_lots_config()
    
    api_key = settings.get("desslyhub_api_key", "")
    if not api_key:
        return {"success": 0, "failed": 0, "errors": ["API ключ не установлен"]}
    
    if not settings.get("auto_markup_enabled", True):
        return {"success": 0, "failed": 0, "errors": ["Автонаценка отключена"]}
    
    markup_percent = settings.get("markup_percent", 10.0)
    success_count = 0
    failed_count = 0
    errors = []
    updated_lots = []
    
    error_stats = {
        "no_game_name": 0,
        "lot_not_found": 0,
        "price_not_found": 0,
        "conversion_error": 0,
        "update_error": 0
    }
    
    error_examples = {
        "lot_not_found": [],
        "price_not_found": [],
        "conversion_error": []
    }
    
    api = DesslyHubAPI(api_key)
    

    logger.info(f"{LOGGER_PREFIX} ⚡ ОПТИМИЗАЦИЯ: Предзагрузка данных...")
    

    _get_desslyhub_games(api_key, use_cache=True)
    

    try:
        cached_rates = api.get_exchange_rates()
        if cached_rates:
            logger.info(f"{LOGGER_PREFIX} ⚡ Курсы валют загружены: USD/RUB = {cached_rates.get('RUB', 'N/A')}")
    except Exception as e:
        cached_rates = None
        logger.warning(f"{LOGGER_PREFIX} Не удалось предзагрузить курсы валют: {e}")
    
    funpay_lots_cache = {}
    try:
        logger.info(f"{LOGGER_PREFIX} ⚡ Загрузка списка лотов FunPay...")
        profile_lots = cardinal.account.get_user().lots
        for lot in profile_lots:
            if hasattr(lot, 'description') and lot.description:
                funpay_lots_cache[lot.description.strip().lower()] = lot
        logger.info(f"{LOGGER_PREFIX} ⚡ Загружено {len(funpay_lots_cache)} лотов FunPay")
    except Exception as e:
        logger.warning(f"{LOGGER_PREFIX} Не удалось предзагрузить лоты FunPay: {e}")
    
    price_cache = {}
    
    logger.info(f"{LOGGER_PREFIX} ⚡ Начинаем синхронизацию {len(lots_config)} лотов (25 потоков)")
    
    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = {}
        for idx, lot_config in enumerate(lots_config):
            future = executor.submit(_process_single_lot, lot_config, cardinal, api_key, markup_percent, api)
            futures[future] = lot_config
        
        for future in as_completed(futures):
            result = future.result()
            if result is None:
                continue
            
            if result.get("success"):
                if not result.get("skipped"):
                    success_count += 1
                    updated_lots.append(result["lot_name"])
            else:
                failed_count += 1
                error_type = result.get("error", "unknown")
                error_stats[error_type] = error_stats.get(error_type, 0) + 1
                error_message = result.get("message", "Неизвестная ошибка")
                errors.append(error_message)
                if error_type in error_examples and len(error_examples[error_type]) < 5:
                    error_examples[error_type].append(error_message)
    
    if failed_count > 0:
        logger.warning(
            f"{LOGGER_PREFIX} Статистика ошибок синхронизации: "
            f"без названия игры={error_stats['no_game_name']}, "
            f"лот не найден={error_stats['lot_not_found']}, "
            f"цена не найдена={error_stats['price_not_found']}, "
            f"ошибка конвертации={error_stats['conversion_error']}, "
            f"ошибка обновления={error_stats['update_error']}"
        )
        if error_examples.get("lot_not_found"):
            logger.warning(f"{LOGGER_PREFIX} Примеры ошибок 'лот не найден' (первые 5):")
            for example in error_examples["lot_not_found"]:
                logger.warning(f"{LOGGER_PREFIX}   - {example}")
        if error_examples.get("price_not_found"):
            logger.warning(f"{LOGGER_PREFIX} Примеры ошибок 'цена не найдена' (первые 5):")
            for example in error_examples["price_not_found"]:
                logger.warning(f"{LOGGER_PREFIX}   - {example}")
        if error_examples.get("conversion_error"):
            logger.warning(f"{LOGGER_PREFIX} Примеры ошибок 'конвертация' (первые 5):")
            for example in error_examples["conversion_error"]:
                logger.warning(f"{LOGGER_PREFIX}   - {example}")
    
    return {
        "success": success_count,
        "failed": failed_count,
        "errors": errors,
        "updated_lots": updated_lots,
        "error_stats": error_stats
    }


def _get_mac_address() -> str:
    try:
        system = platform.system().lower()
        if system == "windows":
            out = subprocess.check_output(["getmac"], text=True, stderr=subprocess.DEVNULL)
            for line in out.splitlines():
                line = line.strip()
                if "-" in line and ":" not in line and line.count("-") == 5:
                    return line.split()[0].replace("-", ":").lower()
        else:
            for path in glob.glob("/sys/class/net/*/address"):
                if "/lo/" in path or path.endswith("/lo/address"):
                    continue
                try:
                    with open(path, "r", encoding="utf-8") as fh:
                        mac = fh.read().strip().lower()
                    if mac and mac != "00:00:00:00:00:00":
                        return mac
                except Exception:
                    continue
            out = subprocess.check_output(["cat", "/sys/class/net/eth0/address"], text=True, stderr=subprocess.DEVNULL)
            return out.strip().lower()
    except Exception:
        try:
            import uuid as _uuidlib
            return ":".join([f"{(_uuidlib.getnode() >> ele) & 0xff:02x}" for ele in range(40, -8, -8)])
        except Exception:
            return "unknown"

def _license_check(username: str | None = None) -> bool:
    try:
        api_url = os.getenv("AS_LICENSE_API", AS_LICENSE_API_DEFAULT)
        license_key = os.getenv("AS_LICENSE_KEY", AS_LICENSE_KEY_DEFAULT)
        if not (api_url and license_key):
            try:
                logger.error(f"{LOGGER_PREFIX} Лицензия: не указан ключ или URL сервиса")
            except Exception:
                pass
            return False

        mac = _get_mac_address()
        timestamp = int(time.time())
        payload = {
            "key": license_key,
            "mac": mac,
            "file_name": AS_FILE_NAME_DEFAULT,
            "username": username or None,
            "host": platform.node(),
            "platform": platform.platform(),
            "ts": timestamp,
        }

        secret = os.getenv("AS_HMAC_SECRET") or os.getenv("HMAC_SECRET", AS_HMAC_SECRETF)
        try:
            serialized = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            signature = hmac.new(secret.encode("utf-8"), serialized, hashlib.sha256).hexdigest()
        except Exception:
            base = f"{license_key}|{mac}|{timestamp}".encode("utf-8")
            signature = hmac.new(secret.encode("utf-8"), base, hashlib.sha256).hexdigest()

        headers = {
            "Content-Type": "application/json",
            "X-AS-Signature": signature,
        }

        req = Request(api_url, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"), headers=headers, method="POST")
        with urlopen(req, timeout=15) as resp:
            if resp.status != 200:
                return False
            try:
                body = resp.read().decode("utf-8", errors="ignore")
                data = json.loads(body)
            except Exception:
                return False

        ok = bool(data.get("ok") or data.get("valid") or data.get("status") == "ok")
        return ok
    except Exception as ex:
        try:
            logger.error(f"{LOGGER_PREFIX} Лицензия: ошибка проверки — {ex}")
        except Exception:
            pass
        return False

def _license_check_worker():
    global LICENSE_OK, _cardinal_instance
    last_license_check = 0
    license_check_interval = 300
    
    while True:
        try:
            now = time.time()
            if now - last_license_check > license_check_interval:
                telegram_username = None
                if _cardinal_instance and _cardinal_instance.telegram and _cardinal_instance.telegram.authorized_users:
                    try:
                        first_user_id = list(_cardinal_instance.telegram.authorized_users.keys())[0] if _cardinal_instance.telegram.authorized_users else None
                        if first_user_id and _cardinal_instance.telegram.bot:
                            try:
                                user_info = _cardinal_instance.telegram.bot.get_chat(first_user_id)
                                if hasattr(user_info, 'username') and user_info.username:
                                    telegram_username = user_info.username
                            except Exception:
                                try:
                                    chat_member = _cardinal_instance.telegram.bot.get_chat_member(first_user_id, first_user_id)
                                    if hasattr(chat_member, 'user') and hasattr(chat_member.user, 'username') and chat_member.user.username:
                                        telegram_username = chat_member.user.username
                                except Exception:
                                    pass
                    except Exception:
                        pass
                LICENSE_OK = _license_check(telegram_username)
                last_license_check = now
                if not LICENSE_OK:
                    logger.warning(f"{LOGGER_PREFIX} Лицензия не прошла периодическую проверку")
                    os.environ["AS_LICENSE_FAILED"] = "1"
                else:
                    os.environ.pop("AS_LICENSE_FAILED", None)
                    logger.info(f"{LOGGER_PREFIX} Лицензия прошла периодическую проверку")
            time.sleep(60)
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Ошибка в потоке проверки лицензии: {e}")
            time.sleep(60)

def _format_template(template: str, **kwargs) -> str:
    try:
        return template.format(**kwargs)
    except KeyError as e:
        logger.warning(f"{LOGGER_PREFIX} Отсутствует переменная в шаблоне: {e}")
        return template
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка форматирования шаблона: {e}")
        return template

def _get_mobile_game_fields_config(game_name: str, fields_info: dict, servers_info: dict) -> dict:
    game_name_lower = game_name.lower().strip()
    config = {
        "fields_to_request": [],
        "auto_server": None
    }
    
    if "arena breakout" in game_name_lower:
        for field_key in fields_info.keys():
            field_key_lower = field_key.lower()
            if "player" in field_key_lower and "id" in field_key_lower and "server" not in field_key_lower:
                config["fields_to_request"] = [field_key]
                break
    elif "mobile legends" in game_name_lower:
        server_field = None
        user_field = None
        for field_key in fields_info.keys():
            field_key_lower = field_key.lower()
            if "server" in field_key_lower and "id" in field_key_lower:
                server_field = field_key
            elif "user" in field_key_lower and "id" in field_key_lower:
                user_field = field_key
        if server_field and user_field:
            config["fields_to_request"] = [server_field, user_field]
    elif "honor of kings" in game_name_lower:
        for field_key in fields_info.keys():
            field_key_lower = field_key.lower()
            if "player" in field_key_lower and "id" in field_key_lower:
                config["fields_to_request"] = [field_key]
                break
    elif "delta force" in game_name_lower:
        for field_key in fields_info.keys():
            field_key_lower = field_key.lower()
            if "player" in field_key_lower and "id" in field_key_lower:
                config["fields_to_request"] = [field_key]
                break
    elif "8 ball pool" in game_name_lower:
        for field_key in fields_info.keys():
            field_key_lower = field_key.lower()
            if "unique" in field_key_lower and "id" in field_key_lower:
                config["fields_to_request"] = [field_key]
                break
    elif "pubg mobile" in game_name_lower and "global" in game_name_lower:
        for field_key in fields_info.keys():
            field_key_lower = field_key.lower()
            if "character" in field_key_lower and "id" in field_key_lower:
                config["fields_to_request"] = [field_key]
                break
    elif "pubg mobile" in game_name_lower and "ru" in game_name_lower:
        for field_key in fields_info.keys():
            field_key_lower = field_key.lower()
            if "player" in field_key_lower and "id" in field_key_lower:
                config["fields_to_request"] = [field_key]
                break
    elif "marvel rivals" in game_name_lower:
        for field_key in fields_info.keys():
            field_key_lower = field_key.lower()
            if "user" in field_key_lower and "id" in field_key_lower:
                config["fields_to_request"] = [field_key]
                break
    elif "genshin impact" in game_name_lower and "v2" in game_name_lower:
        server_field = None
        user_field = None
        for field_key in fields_info.keys():
            field_key_lower = field_key.lower()
            if "server" in field_key_lower:
                server_field = field_key
            elif "user" in field_key_lower and "id" in field_key_lower:
                user_field = field_key
        if server_field and user_field:
            if servers_info:
                for server_name in servers_info.keys():
                    if "europe" in server_name.lower():
                        config["auto_server"] = server_name
                        break
                if not config["auto_server"]:
                    config["auto_server"] = list(servers_info.keys())[0] if servers_info else None
            config["fields_to_request"] = [user_field]
    elif "honkai" in game_name_lower and "star rail" in game_name_lower and "v2" in game_name_lower:
        server_field = None
        user_field = None
        for field_key in fields_info.keys():
            field_key_lower = field_key.lower()
            if "server" in field_key_lower:
                server_field = field_key
            elif "user" in field_key_lower and "id" in field_key_lower:
                user_field = field_key
        if server_field and user_field:
            if servers_info:
                config["auto_server"] = list(servers_info.keys())[0] if servers_info else None
            config["fields_to_request"] = [user_field]
    else:
        field_name = None
        for field_key in fields_info.keys():
            field_key_lower = field_key.lower()
            if "player" in field_key_lower or "id" in field_key_lower or "character" in field_key_lower:
                if "server" not in field_key_lower and "region" not in field_key_lower:
                    field_name = field_key
                    break
        if not field_name:
            field_name = list(fields_info.keys())[0] if fields_info else "Player ID"
        config["fields_to_request"] = [field_name]
    
    return config

def _parse_and_save_lots_ids(cardinal: "Cardinal") -> list[int]:
    lots_ids = []
    try:
        storage = _get_storage()
        lots_config = storage.load_lots_config()
        
        if not lots_config:
            logger.warning(f"{LOGGER_PREFIX} Конфиг лотов пуст, нечего сохранять")
            return lots_ids
        
        if not hasattr(cardinal, 'profile') or not cardinal.profile:
            logger.warning(f"{LOGGER_PREFIX} Profile недоступен")
            return lots_ids
        
        all_lots = cardinal.profile.get_lots()
        config_lot_names = [lot_config.get("lot_name", "").lower().strip() for lot_config in lots_config if lot_config.get("lot_name")]
        
        for lot in all_lots:
            try:
                lot_description = (lot.description or "").lower().strip()
                if not lot_description:
                    continue
                
                is_in_config = False
                for config_lot_name in config_lot_names:
                    if not config_lot_name:
                        continue
                    if (config_lot_name == lot_description or 
                        config_lot_name in lot_description or 
                        lot_description in config_lot_name):
                        is_in_config = True
                        break
                
                if not is_in_config:
                    continue
                
                is_active = False
                if hasattr(lot, 'active'):
                    is_active = lot.active
                else:
                    lot_fields = cardinal.account.get_lot_fields(lot.id)
                    is_active = lot_fields.active
                
                if is_active:
                    lots_ids.append(lot.id)
                    logger.debug(f"{LOGGER_PREFIX} Найден активный лот из конфига: ID={lot.id}, описание={lot.description}")
            except Exception as e:
                logger.warning(f"{LOGGER_PREFIX} Ошибка проверки лота {lot.id}: {e}")
                continue
        
        logger.info(f"{LOGGER_PREFIX} Сохранено {len(lots_ids)} активных лотов из конфига для восстановления")
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка парсинга лотов: {e}")
    return lots_ids

def _deactivate_lots_by_ids(cardinal: "Cardinal", lots_ids: list[int]) -> int:
    deactivated = 0
    for lot_id in lots_ids:
        try:
            lot_fields = cardinal.account.get_lot_fields(lot_id)
            if lot_fields.active:
                lot_fields.active = False
                cardinal.account.save_lot(lot_fields)
                deactivated += 1
                logger.info(f"{LOGGER_PREFIX} Лот ID {lot_id} деактивирован")
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Ошибка деактивации лота {lot_id}: {e}")
    return deactivated

def _activate_lots_by_ids(cardinal: "Cardinal", lots_ids: list[int]) -> int:
    activated = 0
    for lot_id in lots_ids:
        try:
            lot_fields = cardinal.account.get_lot_fields(lot_id)
            if not lot_fields.active:
                lot_fields.active = True
                cardinal.account.save_lot(lot_fields)
                activated += 1
                logger.info(f"{LOGGER_PREFIX} Лот ID {lot_id} активирован")
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Ошибка активации лота {lot_id}: {e}")
    return activated

def _balance_monitor_worker():
    global _previous_balance, _cardinal_instance, _deactivated_lots_ids
    while True:
        try:
            time.sleep(60)
            if not _cardinal_instance:
                continue
            
            storage = _get_storage()
            settings = storage.load_settings()
            api_key = settings.get("desslyhub_api_key", "")
            admin_id = settings.get("admin_id", "")
            
            if not api_key:
                continue
            
            try:
                api = DesslyHubAPI(api_key)
                balance = api.get_balance()
                
                if balance is not None:
                    if _previous_balance is not None and balance > _previous_balance:
                        if admin_id and hasattr(_cardinal_instance, 'telegram') and hasattr(_cardinal_instance.telegram, 'bot'):
                            try:
                                message = (
                                    f"🔔 <b>Уведомление о пополнении баланса</b>\n\n"
                                    f"💰 <b>Новый баланс:</b> <code>{balance:.2f} USD</code>\n"
                                    f"📅 <b>Дата:</b> <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>"
                                )
                                _cardinal_instance.telegram.bot.send_message(int(admin_id), message, parse_mode="HTML")
                            except Exception as e:
                                logger.error(f"{LOGGER_PREFIX} Ошибка отправки уведомления о пополнении: {e}")
                    
                    balance_threshold_enabled = settings.get("balance_threshold_enabled", True)
                    if not balance_threshold_enabled:
                        continue
                    
                    balance_threshold = settings.get("balance_threshold", 30.0)
                    warning_sent = settings.get("warning_sent", False)
                    warning_time = settings.get("warning_time")
                    deactivated_lots = settings.get("deactivated_lots", [])
                    
                    if balance < balance_threshold:
                        if not warning_sent:
                            if admin_id and hasattr(_cardinal_instance, 'telegram') and hasattr(_cardinal_instance.telegram, 'bot'):
                                try:
                                    message = (
                                        f"⚠️ <b>Предупреждение о низком балансе</b>\n\n"
                                        f"💰 <b>Текущий баланс:</b> <code>{balance:.2f} USD</code>\n"
                                        f"📊 <b>Порог:</b> <code>{balance_threshold} USD</code>\n\n"
                                        f"⏰ <b>Через 15 минут все лоты будут автоматически выключены, если баланс не будет пополнен!</b>\n"
                                        f"📅 <b>Дата:</b> <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>"
                                    )
                                    _cardinal_instance.telegram.bot.send_message(int(admin_id), message, parse_mode="HTML")
                                    settings["warning_sent"] = True
                                    settings["warning_time"] = time.time()
                                    storage.save_settings(settings)
                                    logger.info(f"{LOGGER_PREFIX} Отправлено предупреждение о низком балансе")
                                except Exception as e:
                                    logger.error(f"{LOGGER_PREFIX} Ошибка отправки предупреждения: {e}")
                        elif warning_time:
                            elapsed = time.time() - warning_time
                            if elapsed >= 900:
                                if not deactivated_lots:
                                    lots_ids = _parse_and_save_lots_ids(_cardinal_instance)
                                    deactivated_count = _deactivate_lots_by_ids(_cardinal_instance, lots_ids)
                                    settings["deactivated_lots"] = lots_ids
                                    storage.save_settings(settings)
                                    _deactivated_lots_ids = lots_ids
                                    
                                    if admin_id and hasattr(_cardinal_instance, 'telegram') and hasattr(_cardinal_instance.telegram, 'bot'):
                                        try:
                                            message = (
                                                f"🔴 <b>Все лоты выключены</b>\n\n"
                                                f"💰 <b>Баланс:</b> <code>{balance:.2f} USD</code>\n"
                                                f"📊 <b>Порог:</b> <code>{balance_threshold} USD</code>\n"
                                                f"🔴 <b>Выключено лотов:</b> {deactivated_count}\n"
                                                f"📅 <b>Дата:</b> <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>"
                                            )
                                            _cardinal_instance.telegram.bot.send_message(int(admin_id), message, parse_mode="HTML")
                                        except Exception as e:
                                            logger.error(f"{LOGGER_PREFIX} Ошибка отправки уведомления о выключении: {e}")
                    elif balance >= balance_threshold:
                        if deactivated_lots:
                            activated_count = _activate_lots_by_ids(_cardinal_instance, deactivated_lots)
                            settings["deactivated_lots"] = []
                            settings["warning_sent"] = False
                            settings["warning_time"] = None
                            storage.save_settings(settings)
                            _deactivated_lots_ids = []
                            
                            if admin_id and hasattr(_cardinal_instance, 'telegram') and hasattr(_cardinal_instance.telegram, 'bot'):
                                try:
                                    message = (
                                        f"✅ <b>Все лоты включены</b>\n\n"
                                        f"💰 <b>Баланс:</b> <code>{balance:.2f} USD</code>\n"
                                        f"📊 <b>Порог:</b> <code>{balance_threshold} USD</code>\n"
                                        f"✅ <b>Включено лотов:</b> {activated_count}\n"
                                        f"📅 <b>Дата:</b> <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>"
                                    )
                                    _cardinal_instance.telegram.bot.send_message(int(admin_id), message, parse_mode="HTML")
                                except Exception as e:
                                    logger.error(f"{LOGGER_PREFIX} Ошибка отправки уведомления о включении: {e}")
                        elif warning_sent:
                            settings["warning_sent"] = False
                            settings["warning_time"] = None
                            storage.save_settings(settings)
                    
                    _previous_balance = balance
            except Exception as e:
                logger.error(f"{LOGGER_PREFIX} Ошибка в worker мониторинга баланса: {e}")
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Критическая ошибка в worker мониторинга баланса: {e}")
            time.sleep(60)


def _price_sync_worker():
    global _cardinal_instance
    while True:
        try:
            time.sleep(60)
            
            if not _cardinal_instance:
                continue
            
            storage = _get_storage()
            settings = storage.load_settings()
            
            if not settings.get("active", False):
                continue
            
            if not settings.get("auto_markup_enabled", True):
                continue
            
            logger.info(f"{LOGGER_PREFIX} Запуск автоматической синхронизации цен")
            result = _sync_prices_from_desslyhub(_cardinal_instance)
            
            if result["success"] > 0 or result["failed"] > 0:
                admin_id = settings.get("admin_id", "")
                if admin_id and hasattr(_cardinal_instance, 'telegram') and hasattr(_cardinal_instance.telegram, 'bot'):
                    try:
                        message = (
                            f"📊 <b>Синхронизация цен завершена</b>\n\n"
                            f"✅ <b>Обновлено:</b> {result['success']}\n"
                            f"❌ <b>Ошибок:</b> {result['failed']}\n"
                        )
                        if result.get("updated_lots"):
                            message += f"\n📋 <b>Обновленные лоты:</b>\n"
                            for lot_name in result["updated_lots"][:10]:
                                display_name = lot_name if len(lot_name) <= 80 else lot_name[:77] + "..."
                                message += f"• {display_name}\n"
                            if len(result["updated_lots"]) > 10:
                                message += f"\n... и еще {len(result['updated_lots']) - 10}\n"
                        if result.get("errors") and len(result["errors"]) <= 5:
                            message += f"\n⚠️ <b>Ошибки:</b>\n"
                            for error in result["errors"][:5]:
                                message += f"• {error}\n"
                        message += f"\n📅 <b>Дата:</b> <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>"
                        _cardinal_instance.telegram.bot.send_message(int(admin_id), message, parse_mode="HTML")
                    except Exception as e:
                        logger.error(f"{LOGGER_PREFIX} Ошибка отправки уведомления о синхронизации: {e}")
            
            logger.info(f"{LOGGER_PREFIX} Синхронизация завершена: успешно {result['success']}, ошибок {result['failed']}")
            if result.get("error_stats") and result['failed'] > 0:
                stats = result["error_stats"]
                logger.info(
                    f"{LOGGER_PREFIX} Детализация ошибок: "
                    f"без названия игры={stats.get('no_game_name', 0)}, "
                    f"лот не найден={stats.get('lot_not_found', 0)}, "
                    f"цена не найдена={stats.get('price_not_found', 0)}, "
                    f"ошибка конвертации={stats.get('conversion_error', 0)}, "
                    f"ошибка обновления={stats.get('update_error', 0)}"
                )
            
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Ошибка в worker синхронизации: {e}")
            time.sleep(60)


def init_autosteam_cp(cardinal: "Cardinal", *args):
    global _cardinal_instance, _sync_thread, _balance_thread, LICENSE_OK
    
    logger.info(f"{LOGGER_PREFIX} Инициализация плагина AUTOSTEAM с проверкой лицензии")
    _cardinal_instance = cardinal
    
    telegram_username = None
    if cardinal and cardinal.telegram and cardinal.telegram.authorized_users:
        try:
            first_user_id = list(cardinal.telegram.authorized_users.keys())[0] if cardinal.telegram.authorized_users else None
            if first_user_id and cardinal.telegram.bot:
                try:
                    user_info = cardinal.telegram.bot.get_chat(first_user_id)
                    if hasattr(user_info, 'username') and user_info.username:
                        telegram_username = user_info.username
                except Exception:
                    try:
                        chat_member = cardinal.telegram.bot.get_chat_member(first_user_id, first_user_id)
                        if hasattr(chat_member, 'user') and hasattr(chat_member.user, 'username') and chat_member.user.username:
                            telegram_username = chat_member.user.username
                    except Exception:
                        pass
        except Exception:
            pass

    LICENSE_OK = _license_check(telegram_username)
    os.environ.pop("AS_LICENSE_FAILED", None)

    try:
        if LICENSE_OK:
            logger.info(f"{LOGGER_PREFIX} ✅ Лицензия проверена успешно - плагин активирован")
        else:
            logger.error(f"{LOGGER_PREFIX} ❌ Лицензия не активна - функциональность ограничена")
            logger.warning(f"{LOGGER_PREFIX} Доступен только интерфейс управления, функциональные действия заблокированы")
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка при инициализации проверки лицензии: {e}")
        LICENSE_OK = False
    
    tg = cardinal.telegram
    bot = tg.bot
    storage = _get_storage()
    
    try:
        if hasattr(cardinal, 'blacklist') and cardinal.blacklist:
            plugin_blacklist = storage.load_black_list()
            imported_count = 0
            plugin_blacklist_lower = [u.lower() for u in plugin_blacklist]
            
            for username in cardinal.blacklist:
                if username and username.lower() not in plugin_blacklist_lower:
                    plugin_blacklist.append(username)
                    imported_count += 1 
            
            if imported_count > 0:
                storage.save_black_list(plugin_blacklist)
                logger.info(f"{LOGGER_PREFIX} Автоматически импортировано {imported_count} пользователей из черного списка Cardinal")
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка автоматического импорта черного списка: {e}")
    
    if _sync_thread is None or not _sync_thread.is_alive():
        _sync_thread = threading.Thread(target=_price_sync_worker, daemon=True)
        _sync_thread.start()
    
    if _balance_thread is None or not _balance_thread.is_alive():
        _balance_thread = threading.Thread(target=_balance_monitor_worker, daemon=True)
        _balance_thread.start()
    
    _license_check_thread = threading.Thread(target=_license_check_worker, daemon=True)
    _license_check_thread.start()
    
    def _safe_edit(c: "CallbackQuery", text: str, kb: K, parse_mode: str = None) -> None:
        try:
            if parse_mode:
                bot.edit_message_text(text, c.message.chat.id, c.message.id, reply_markup=kb, parse_mode=parse_mode)
            else:
                bot.edit_message_text(text, c.message.chat.id, c.message.id, reply_markup=kb)
        except Exception:
            try:
                bot.edit_message_reply_markup(c.message.chat.id, c.message.id, reply_markup=kb)
            except Exception:
                try:
                    if parse_mode:
                        bot.send_message(c.message.chat.id, text, reply_markup=kb, parse_mode=parse_mode)
                    else:
                        bot.send_message(c.message.chat.id, text, reply_markup=kb)
                except Exception:
                    pass
    
    def open_main(obj: "CallbackQuery | Message"):
        settings = storage.load_settings()
        active = settings.get("active", False)
        lots_config = storage.load_lots_config()

        total_lots_count = len(lots_config)
        orders = storage.load_orders()
        api_key = settings.get("desslyhub_api_key", "")
        
        balance_text = "Не установлен"
        if api_key:
            balance_data = _get_desslyhub_balance(api_key)
            if balance_data:
                balance = balance_data.get("balance", 0.0)
                currency = balance_data.get("currency", "USD")
                balance_text = f"{balance:.2f} {currency}"
        
        success_orders = [o for o in orders if o.get("status") == "success"]
        
        text = (
            f"\n"
            f"   🎮 <b>AUTOSTEAM</b>    \n"
            f"\n\n"
            f"👤 <b>Разработчик:</b> {CREDITS}\n"
            f"📦 <b>Версия:</b> {VERSION}\n"
            f"📊 <b>Статус:</b> {'🟢 Активен' if active else '🔴 Остановлен'}\n\n"
            f"📈 <b>Статистика:</b>\n"
            f"   • Лотов в конфиге: <b>{total_lots_count}</b>\n"
            f"   • Всего заказов: <b>{len(success_orders)}</b>\n"
            f"   • Наценка: <b>{settings.get('markup_percent', 10.0)}%</b>\n"
            f"   • Автонаценка: {'✅' if settings.get('auto_markup_enabled', True) else '❌'}\n"
            f"   • Баланс: <b>{balance_text}</b>\n\n"
            f"Выберите действие:"
        )
        kb = _kb_main(active)
        
        if hasattr(obj, "data"):
            c = obj
            _safe_edit(c, text, kb, parse_mode="HTML")
            bot.answer_callback_query(c.id)
        else:
            m = obj
            bot.send_message(m.chat.id, text, reply_markup=kb, parse_mode="HTML")
    
    def toggle_active(c: CallbackQuery):
        global LICENSE_OK
        bot.answer_callback_query(c.id)
        if not LICENSE_OK:
            logger.warning(f"{LOGGER_PREFIX} [TG] Попытка изменить статус без валидной лицензии, user_id={c.from_user.id}")
            bot.send_message(c.message.chat.id, "❌ Лицензия неактивна. Обратитесь к разработчику.")
            open_main(c)
            return
        settings = storage.load_settings()
        settings["active"] = not bool(settings.get("active"))
        storage.save_settings(settings)
        logger.info(f"{LOGGER_PREFIX} Статус изменен на: {settings['active']}")
        open_main(c)
    
    def license_recheck(c: CallbackQuery):
        global LICENSE_OK
        bot.answer_callback_query(c.id)
        logger.info(f"{LOGGER_PREFIX} Запуск ручной перепроверки лицензии")

        telegram_username = None
        if cardinal and cardinal.telegram and c.from_user:
            telegram_username = c.from_user.username

        old_status = LICENSE_OK
        LICENSE_OK = _license_check(telegram_username)
        os.environ.pop("AS_LICENSE_FAILED", None)

        try:
            if LICENSE_OK:
                if old_status != LICENSE_OK:
                    logger.info(f"{LOGGER_PREFIX} ✅ Перепроверка лицензии - статус изменен с {old_status} на {LICENSE_OK}")
                else:
                    logger.info(f"{LOGGER_PREFIX} ✅ Перепроверка лицензии - статус подтвержден: {LICENSE_OK}")
            else:
                if old_status != LICENSE_OK:
                    logger.error(f"{LOGGER_PREFIX} ❌ Перепроверка лицензии - статус изменен с {old_status} на {LICENSE_OK}")
                else:
                    logger.error(f"{LOGGER_PREFIX} ❌ Перепроверка лицензии - статус подтвержден: {LICENSE_OK}")
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Ошибка при перепроверке лицензии: {e}")

        open_main(c)
    
    def open_games(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        lots_config = storage.load_lots_config()
        steam_lots = sum(1 for l in lots_config if l.get("type", "").lower() == "steam gift")
        mobile_lots = sum(1 for l in lots_config if l.get("type", "").lower() == "mobile refill")
        
        text = (
            f"🎮 <b>Управление лотами</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"   • Всего лотов: <b>{len(lots_config)}</b>\n"
            f"   • Steam Gift: <b>{steam_lots}</b>\n"
            f"   • Mobile Refill: <b>{mobile_lots}</b>\n\n"
            f"Выберите действие:"
        )
        _safe_edit(c, text, _kb_games_menu(), parse_mode="HTML")
    
    def open_settings(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        settings = storage.load_settings()
        api_key_status = "✅ Установлен" if settings.get('desslyhub_api_key') else "❌ Не установлен"
        
        admin_id = settings.get('admin_id', '')
        admin_status = f"✅ Установлен (ID: {admin_id})" if admin_id else "❌ Не установлен"
        
        text = (
            f"⚙️ <b>Настройки плагина</b>\n\n"
            f"📊 <b>Текущие параметры:</b>\n"
            f"   • Наценка: <b>{settings.get('markup_percent', 10.0)}%</b>\n"
            f"   • Автонаценка: {'✅ Включена' if settings.get('auto_markup_enabled', True) else '❌ Выключена'}\n"
            f"   • Порог цены: <b>{settings.get('balance_threshold', 30.0)} USD</b>\n"
            f"   • Мониторинг порога: {'✅ Включен' if settings.get('balance_threshold_enabled', True) else '❌ Выключен'}\n"
            f"   • API ключ: {api_key_status}\n"
            f"   • Admin ID: {admin_status}\n\n"
            f"Выберите параметр для изменения:"
        )
        _safe_edit(c, text, _kb_settings_menu(), parse_mode="HTML")
    
    def open_templates(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        templates = storage.load_templates()
        welcome_steam = templates.get('welcome_steam_template', '')
        welcome_mobile = templates.get('welcome_mobile_template', '')
        success_steam = templates.get('success_steam_template', '')
        success_mobile = templates.get('success_mobile_template', '')
        
        text = (
            f"📝 <b>Шаблоны сообщений</b>\n\n"
            f"📝 <b>Приветствие Steam:</b>\n"
            f"<code>{welcome_steam[:100]}{'...' if len(welcome_steam) > 100 else ''}</code>\n\n"
            f"📝 <b>Приветствие Mobile:</b>\n"
            f"<code>{welcome_mobile[:100]}{'...' if len(welcome_mobile) > 100 else ''}</code>\n\n"
            f"✅ <b>Успех Steam:</b>\n"
            f"<code>{success_steam[:100]}{'...' if len(success_steam) > 100 else ''}</code>\n\n"
            f"✅ <b>Успех Mobile:</b>\n"
            f"<code>{success_mobile[:100]}{'...' if len(success_mobile) > 100 else ''}</code>\n\n"
            f"💡 <i>Используйте переменные для подстановки значений</i>\n\n"
            f"Выберите шаблон для редактирования:"
        )
        _safe_edit(c, text, _kb_templates_menu(), parse_mode="HTML")
    
    def open_balance(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        settings = storage.load_settings()
        api_key = settings.get("desslyhub_api_key", "")
        
        if not api_key:
            text = (
                f"💵 <b>Баланс DesslyHub</b>\n\n"
                f"❌ <b>API ключ не установлен</b>\n\n"
                f"Для получения баланса необходимо установить API ключ в настройках."
            )
        else:
            balance_data = _get_desslyhub_balance(api_key)
            if balance_data:
                balance = balance_data.get("balance", 0.0)
                currency = balance_data.get("currency", "USD")
                text = (
                    f"💵 <b>Баланс DesslyHub</b>\n\n"
                    f"💰 <b>Текущий баланс:</b> <b>{balance:.2f} {currency}</b>\n\n"
                    f"💡 <i>Баланс обновляется при каждом открытии вкладки</i>"
                )
            else:
                text = (
                    f"💵 <b>Баланс DesslyHub</b>\n\n"
                    f"❌ <b>Не удалось получить баланс</b>\n\n"
                    f"Проверьте правильность API ключа и попробуйте позже."
                )
        
        kb = K()
        kb.add(B("🔄 Обновить", callback_data=CB_OPEN_BALANCE))
        kb.add(B("🔙 Назад", callback_data=CB_BACK))
        _safe_edit(c, text, kb, parse_mode="HTML")
    
    def manual_sync(c: CallbackQuery):
        bot.answer_callback_query(c.id, "Синхронизация начата...")
        result = _sync_prices_from_desslyhub(cardinal)
        
        text = (
            f"📊 <b>Результаты синхронизации</b>\n\n"
            f"✅ <b>Успешно обновлено:</b> {result['success']}\n"
            f"❌ <b>Ошибок:</b> {result['failed']}\n"
        )
        
        if result.get("updated_lots"):
            text += f"\n📋 <b>Обновленные лоты:</b>\n"
            for lot_name in result["updated_lots"][:10]:
                display_name = lot_name if len(lot_name) <= 80 else lot_name[:77] + "..."
                text += f"   • {display_name}\n"
            if len(result["updated_lots"]) > 10:
                text += f"\n   ... и еще {len(result['updated_lots']) - 10}\n"
        
        if result.get("errors"):
            text += f"\n⚠️ <b>Ошибки:</b>\n"
            for error in result["errors"][:5]:
                text += f"   • {error}\n"
        
        kb = K()
        kb.add(B("🔙 Назад", callback_data=CB_BACK))
        _safe_edit(c, text, kb, parse_mode="HTML")
    
    def auto_list_all(c: CallbackQuery):
        bot.answer_callback_query(c.id, "Автовыставление начато...")
        result = _auto_list_all_games(cardinal)
        
        text = (
            f"🚀 <b>Результаты автовыставления</b>\n\n"
            f"✅ <b>Успешно выставлено:</b> {result['success']}\n"
            f"❌ <b>Ошибок:</b> {result['failed']}\n"
        )
        
        if result.get("errors"):
            text += f"\n⚠️ <b>Ошибки:</b>\n"
            for error in result["errors"][:5]:
                text += f"   • {error}\n"
        
        kb = K()
        kb.add(B("🔙 Назад", callback_data=CB_BACK))
        _safe_edit(c, text, kb, parse_mode="HTML")
    
    def handle_back(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        open_main(c)
    
    def add_game(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        result = bot.send_message(
            c.message.chat.id,
            "📎 <b>Отправьте JSON файл с конфигурацией лотов</b>\n\n"
            "📝 <b>Формат файла:</b>\n"
            "<code>[\n"
            '  {\n'
            '    "lot_name": "Название лота",\n'
            '    "type": "Steam Gift" или "Mobile Refill",\n'
            '    "game_name": "Название игры",\n'
            '    "region": "KZ" (для Steam Gift),\n'
            '    "amount": "60 us" (для Mobile Refill)\n'
            '  }\n'
            "]</code>\n\n"
            "💡 Файл будет сохранен в <code>lots_config.json</code>",
            reply_markup=_kb_cancel(),
            parse_mode="HTML"
        )
        tg.set_state(c.message.chat.id, result.id, c.from_user.id, STATE_ADD_GAME, {})
        logger.info(f"{LOGGER_PREFIX} Установлено состояние STATE_ADD_GAME для chat_id={c.message.chat.id}, user_id={c.from_user.id}, message_id={result.id}")
        
        check_state = tg.get_state(c.message.chat.id, c.from_user.id)
        logger.info(f"{LOGGER_PREFIX} [DEBUG] Проверка состояния после установки: {check_state}")
    
    def state_add_game(m: Message):
        logger.info(f"{LOGGER_PREFIX} [DEBUG] state_add_game вызван: chat_id={m.chat.id}, user_id={m.from_user.id}, content_type={getattr(m, 'content_type', 'unknown')}, has_document={hasattr(m, 'document') and m.document is not None}")
        
        state = tg.get_state(m.chat.id, m.from_user.id)
        logger.info(f"{LOGGER_PREFIX} [DEBUG] Состояние пользователя: {state}")
        
        if not state or state["state"] != STATE_ADD_GAME:
            logger.warning(f"{LOGGER_PREFIX} Сообщение не в состоянии STATE_ADD_GAME: state={state}, ожидалось STATE_ADD_GAME")
            return
        
        logger.info(f"{LOGGER_PREFIX} Получено сообщение в состоянии STATE_ADD_GAME: chat_id={m.chat.id}, has_document={hasattr(m, 'document') and m.document is not None}")
        
        if not hasattr(m, 'document') or not m.document:
            logger.warning(f"{LOGGER_PREFIX} Сообщение не содержит документ: content_type={getattr(m, 'content_type', 'unknown')}")
            bot.send_message(m.chat.id, "❌ Пожалуйста, отправьте JSON файл.", reply_markup=_kb_cancel())
            return
        
        if not m.document.file_name or not m.document.file_name.lower().endswith('.json'):
            bot.send_message(m.chat.id, "❌ Файл должен иметь расширение .json", reply_markup=_kb_cancel())
            return
        
        try:
            logger.info(f"{LOGGER_PREFIX} Обработка JSON файла: {m.document.file_name}, размер: {m.document.file_size} байт")
            file_info = bot.get_file(m.document.file_id)
            file_bytes = bot.download_file(file_info.file_path)
            
            try:
                lots_config_data = json.loads(file_bytes.decode('utf-8'))
            except UnicodeDecodeError:
                lots_config_data = json.loads(file_bytes.decode('utf-8-sig'))
            
            if not isinstance(lots_config_data, list):
                bot.send_message(m.chat.id, "❌ JSON должен содержать массив объектов.", reply_markup=_kb_cancel())
                return
            
            storage = _get_storage()
            existing_config = storage.load_lots_config()
            
            added_count = 0
            updated_count = 0
            
            for new_lot in lots_config_data:
                if not isinstance(new_lot, dict):
                    continue
                
                lot_name = new_lot.get("lot_name", "").strip()
                if not lot_name:
                    continue
                
                found = False
                for i, existing_lot in enumerate(existing_config):
                    if existing_lot.get("lot_name", "").strip() == lot_name:
                        existing_config[i] = new_lot
                        updated_count += 1
                        found = True
                        break
                
                if not found:
                    existing_config.append(new_lot)
                    added_count += 1
            
            storage.save_lots_config(existing_config)
            
            verify_config = storage.load_lots_config()
            total_lots = len(verify_config) 
            logger.info(f"{LOGGER_PREFIX} [DEBUG] Проверка сохранения: сохранено {len(existing_config)} лотов, загружено {len(verify_config)} лотов")
            
            tg.clear_state(m.chat.id, m.from_user.id, True)
            text = (
                f"✅ <b>Конфигурация лотов обновлена</b>\n\n"
                f"📎 <b>Файл:</b> <code>{m.document.file_name}</code>\n"
                f"📊 <b>Всего лотов в конфиге:</b> {total_lots}\n\n"
            )
            
            if added_count > 0:
                text += f"➕ <b>Добавлено:</b> {added_count} лот(ов)\n"
            if updated_count > 0:
                text += f"🔄 <b>Обновлено:</b> {updated_count} лот(ов)\n"
            
            bot.send_message(m.chat.id, text, reply_markup=_kb_back(), parse_mode="HTML")
            logger.info(f"{LOGGER_PREFIX} Конфигурация лотов обновлена из файла {m.document.file_name}: добавлено {added_count}, обновлено {updated_count}, всего {total_lots}")
            
        except json.JSONDecodeError as e:
            error_msg = f"❌ Ошибка парсинга JSON:\n<code>{str(e)}</code>"
            bot.send_message(m.chat.id, error_msg, reply_markup=_kb_cancel(), parse_mode="HTML")
            logger.error(f"{LOGGER_PREFIX} Ошибка парсинга JSON файла: {e}", exc_info=True)
        except Exception as e:
            error_msg = f"❌ Ошибка при обработке файла:\n<code>{str(e)}</code>"
            bot.send_message(m.chat.id, error_msg, reply_markup=_kb_cancel(), parse_mode="HTML")
            logger.error(f"{LOGGER_PREFIX} Ошибка при обработке JSON файла: {e}", exc_info=True)
    
    def games_list(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        try:
            parts = c.data.split(":")
            page = int(parts[1]) if len(parts) > 1 else 0
            search_query = parts[2] if len(parts) > 2 else None
        except Exception:
            page = 0
            search_query = None
        
        lots_config = storage.load_lots_config()
        
        if search_query:
            search_lower = search_query.lower()
            lots_config = [l for l in lots_config if search_lower in l.get('lot_name', '').lower()]
        
        page_size = 10
        start = page * page_size
        end = start + page_size
        slice_lots = lots_config[start:end]
        
        if not slice_lots and page != 0:
            page = 0
            start = 0
            end = page_size
            slice_lots = lots_config[start:end]
        
        text = (
            f"📜 <b>Список лотов</b>\n\n"
            f"📊 <b>Всего лотов в конфиге:</b> {len(lots_config)}\n"
        )
        if search_query:
            text += f"🔍 <b>Поиск:</b> <code>{search_query}</code>\n\n"
        
        if not slice_lots:
            text += "\n📝 Конфигурация лотов пуста.\n"
            text += "Используйте кнопку '➕ Добавить лот' для загрузки конфигурации."
        else:
            text += "\n"
            for lot in slice_lots:
                lot_name = lot.get("lot_name", "Без названия")
                lot_type = lot.get("type", "Unknown")
                game_name = lot.get("game_name", "N/A")
                
                type_emoji = "🎁" if "steam" in lot_type.lower() else "📱" if "mobile" in lot_type.lower() else "❓"
                
                text += f"{type_emoji} <b>{lot_name}</b>\n"
                text += f"   Тип: {lot_type}\n"
                text += f"   Игра: {game_name}\n"
                
                if lot_type.lower() == "steam gift":
                    region = lot.get("region", "N/A")
                    text += f"   Регион: {region}\n"
                elif lot_type.lower() == "mobile refill":
                    amount = lot.get("amount", "N/A")
                    text += f"   Количество: {amount}\n"
                
                text += "\n"
        
        kb = K()
        if slice_lots:
            row = []
            for idx, lot in enumerate(slice_lots, start=start):
                lot_name = lot.get("lot_name", f"Лот {idx+1}")
                lot_type = lot.get("type", "Unknown")
                type_emoji = "🎁" if "steam" in lot_type.lower() else "📱" if "mobile" in lot_type.lower() else "❓"
                row.append(B(f"{type_emoji} {lot_name}", callback_data=f"AS_LOT_VIEW:{idx}"))
                if len(row) == 3:
                    kb.row(*row)
                    row = []
            if row:
                kb.row(*row)
        
        if end < len(lots_config):
            callback_data = f"AS_GAMES_LIST:{page+1}"
            if search_query:
                callback_data += f":{search_query}"
            kb.add(B("⏩ Далее", callback_data=callback_data))
        if page > 0:
            callback_data = f"AS_GAMES_LIST:{page-1}"
            if search_query:
                callback_data += f":{search_query}"
            kb.add(B("⏪ Назад", callback_data=callback_data))
        
        if not search_query:
            kb.add(B("🔍 Поиск", callback_data="AS_SEARCH_GAMES"))
        else:
            kb.add(B("🔍 Новый поиск", callback_data="AS_SEARCH_GAMES"))
            kb.add(B("❌ Очистить поиск", callback_data="AS_GAMES_LIST:0"))
        
        kb.add(B("🔙 Назад", callback_data=CB_OPEN_GAMES))
        
        _safe_edit(c, text, kb, parse_mode="HTML")
    
    def edit_markup(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        result = bot.send_message(c.message.chat.id, "✏️ Введите процент наценки (например: 15.5):", reply_markup=_kb_cancel())
        tg.set_state(c.message.chat.id, result.id, c.from_user.id, STATE_SET_MARKUP, {})
    
    def state_set_markup(m: Message):
        state = tg.get_state(m.chat.id, m.from_user.id)
        if not state or state["state"] != STATE_SET_MARKUP:
            return
        
        try:
            markup = float((m.text or "").strip())
            if markup < 0 or markup > 1000:
                bot.send_message(m.chat.id, "❌ Наценка должна быть от 0 до 1000%.", reply_markup=_kb_cancel())
                return
            
            settings = storage.load_settings()
            settings["markup_percent"] = markup
            storage.save_settings(settings)
            
            tg.clear_state(m.chat.id, m.from_user.id, True)
            text = (
                f"✅ <b>Наценка установлена</b>\n\n"
                f"📊 <b>Новая наценка:</b> {markup}%"
            )
            bot.send_message(m.chat.id, text, reply_markup=_kb_back(), parse_mode="HTML")
            logger.info(f"{LOGGER_PREFIX} Наценка изменена на: {markup}%")
        except ValueError:
            bot.send_message(m.chat.id, "❌ Неверный формат. Введите число (например: 15.5)", reply_markup=_kb_cancel())
    
    def toggle_auto_markup(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        settings = storage.load_settings()
        settings["auto_markup_enabled"] = not bool(settings.get("auto_markup_enabled", True))
        storage.save_settings(settings)
        logger.info(f"{LOGGER_PREFIX} Автонаценка: {settings['auto_markup_enabled']}")
        open_settings(c)
    
    def toggle_balance_threshold(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        settings = storage.load_settings()
        settings["balance_threshold_enabled"] = not bool(settings.get("balance_threshold_enabled", True))
        storage.save_settings(settings)
        logger.info(f"{LOGGER_PREFIX} Мониторинг порога цены: {settings['balance_threshold_enabled']}")
        open_settings(c)
    
    def edit_api_key(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        result = bot.send_message(c.message.chat.id, "✏️ Введите API ключ DesslyHub:", reply_markup=_kb_cancel())
        tg.set_state(c.message.chat.id, result.id, c.from_user.id, "AS_SET_API_KEY", {})
    
    def state_set_api_key(m: Message):
        state = tg.get_state(m.chat.id, m.from_user.id)
        if not state or state["state"] != "AS_SET_API_KEY":
            return
        
        api_key = (m.text or "").strip()
        settings = storage.load_settings()
        settings["desslyhub_api_key"] = api_key
        storage.save_settings(settings)
        
        tg.clear_state(m.chat.id, m.from_user.id, True)
        text = (
            f"✅ <b>API ключ сохранен</b>\n\n"
            f"🔑 API ключ DesslyHub успешно сохранен"
        )
        bot.send_message(m.chat.id, text, reply_markup=_kb_back(), parse_mode="HTML")
        logger.info(f"{LOGGER_PREFIX} API ключ обновлен")
    
    def handle_cancel(c: CallbackQuery | Message):
        chat_id = c.message.chat.id if hasattr(c, "message") else c.chat.id
        user_id = c.from_user.id if hasattr(c, "from_user") else c.from_user.id
        tg.clear_state(chat_id, user_id, True)
        if hasattr(c, "data"):
            bot.answer_callback_query(c.id)
        open_main(c if hasattr(c, "data") else c)
    
    tg.msg_handler(open_main, commands=["steam"])
    
    tg.cbq_handler(open_main, lambda c: c.data == CB_OPEN_MAIN)
    tg.cbq_handler(toggle_active, lambda c: c.data == CB_TOGGLE_ACTIVE)
    tg.cbq_handler(license_recheck, lambda c: c.data == CB_LICENSE_RECHECK)
    tg.cbq_handler(open_games, lambda c: c.data == CB_OPEN_GAMES)
    tg.cbq_handler(open_settings, lambda c: c.data == CB_OPEN_SETTINGS)
    tg.cbq_handler(open_templates, lambda c: c.data == CB_OPEN_TEMPLATES)
    tg.cbq_handler(manual_sync, lambda c: c.data == CB_MANUAL_SYNC)
    tg.cbq_handler(auto_list_all, lambda c: c.data == CB_AUTO_LIST_ALL)
    tg.cbq_handler(handle_back, lambda c: c.data == CB_BACK)
    tg.cbq_handler(handle_cancel, lambda c: c.data == CB_CANCEL)
    
    tg.cbq_handler(add_game, lambda c: c.data == "AS_ADD_GAME")
    tg.cbq_handler(games_list, lambda c: c.data.startswith("AS_GAMES_LIST:"))
    
    def search_games(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        result = bot.send_message(c.message.chat.id, "🔍 Введите название лота для поиска:", reply_markup=_kb_cancel())
        tg.set_state(c.message.chat.id, result.id, c.from_user.id, STATE_SEARCH_GAMES, {})
    
    def state_search_games(m: Message):
        state = tg.get_state(m.chat.id, m.from_user.id)
        if not state or state["state"] != STATE_SEARCH_GAMES:
            return
        
        search_query = (m.text or "").strip()
        if not search_query:
            bot.send_message(m.chat.id, "❌ Поисковый запрос не может быть пустым.", reply_markup=_kb_cancel())
            return
        
        tg.clear_state(m.chat.id, m.from_user.id, True)
        games_list_callback = type('obj', (object,), {
            'data': f"AS_GAMES_LIST:0:{search_query}",
            'id': m.message_id,
            'message': m
        })()
        games_list(games_list_callback)
    
    def edit_balance_threshold(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        result = bot.send_message(c.message.chat.id, "💰 Введите порог уведомления о низком балансе в USD (например: 30.0):", reply_markup=_kb_cancel())
        tg.set_state(c.message.chat.id, result.id, c.from_user.id, STATE_SET_BALANCE_THRESHOLD, {})
    
    def state_set_balance_threshold(m: Message):
        state = tg.get_state(m.chat.id, m.from_user.id)
        if not state or state["state"] != STATE_SET_BALANCE_THRESHOLD:
            return
        
        try:
            threshold = float((m.text or "").strip())
            if threshold < 0:
                bot.send_message(m.chat.id, "❌ Порог должен быть положительным числом.", reply_markup=_kb_cancel())
                return
            
            settings = storage.load_settings()
            settings["balance_threshold"] = threshold
            storage.save_settings(settings)
            
            tg.clear_state(m.chat.id, m.from_user.id, True)
            text = (
                f"✅ <b>Порог уведомления установлен</b>\n\n"
                f"💰 <b>Новый порог:</b> {threshold} USD"
            )
            bot.send_message(m.chat.id, text, reply_markup=_kb_back(), parse_mode="HTML")
            logger.info(f"{LOGGER_PREFIX} Порог уведомления о низком балансе установлен: {threshold} USD")
        except ValueError:
            bot.send_message(m.chat.id, "❌ Неверный формат. Введите число (например: 30.0).", reply_markup=_kb_cancel())
    
    tg.cbq_handler(search_games, lambda c: c.data == "AS_SEARCH_GAMES")
    tg.cbq_handler(edit_balance_threshold, lambda c: c.data == "AS_EDIT_BALANCE_THRESHOLD")
    
    tg.cbq_handler(edit_markup, lambda c: c.data == "AS_EDIT_MARKUP")
    tg.cbq_handler(toggle_auto_markup, lambda c: c.data == "AS_TOGGLE_AUTO_MARKUP")
    tg.cbq_handler(toggle_balance_threshold, lambda c: c.data == "AS_TOGGLE_BALANCE_THRESHOLD")
    tg.cbq_handler(edit_api_key, lambda c: c.data == "AS_EDIT_API_KEY")
    
    def set_admin_id(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        settings = storage.load_settings()
        settings["admin_id"] = str(c.from_user.id)
        storage.save_settings(settings)
        logger.info(f"{LOGGER_PREFIX} Admin ID установлен: {c.from_user.id} (пользователь: @{c.from_user.username or 'N/A'})")
        open_settings(c)
    
    tg.cbq_handler(set_admin_id, lambda c: c.data == "AS_SET_ADMIN_ID")
    
    def edit_template_welcome_steam(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        result = bot.send_message(c.message.chat.id, "✏️ Введите шаблон приветственного сообщения для Steam Gift:\n\nДоступные переменные: {game_name}, {region}", reply_markup=_kb_cancel())
        tg.set_state(c.message.chat.id, result.id, c.from_user.id, STATE_EDIT_TEMPLATE_WELCOME_STEAM, {})
    
    def state_edit_template_welcome_steam(m: Message):
        state = tg.get_state(m.chat.id, m.from_user.id)
        if not state or state["state"] != STATE_EDIT_TEMPLATE_WELCOME_STEAM:
            return
        
        template = (m.text or "").strip()
        templates = storage.load_templates()
        templates["welcome_steam_template"] = template
        storage.save_templates(templates)
        
        tg.clear_state(m.chat.id, m.from_user.id, True)
        text = f"✅ <b>Шаблон сохранен</b>\n\n📝 <b>Приветствие Steam:</b>\n<code>{template[:200]}{'...' if len(template) > 200 else ''}</code>"
        bot.send_message(m.chat.id, text, reply_markup=_kb_back(), parse_mode="HTML")
        logger.info(f"{LOGGER_PREFIX} Шаблон приветствия Steam обновлен")
    
    def edit_template_welcome_mobile(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        result = bot.send_message(c.message.chat.id, "✏️ Введите шаблон приветственного сообщения для Mobile Refill:\n\nДоступные переменные: {game_name}, {position_name}, {position_price}, {field_name}", reply_markup=_kb_cancel())
        tg.set_state(c.message.chat.id, result.id, c.from_user.id, STATE_EDIT_TEMPLATE_WELCOME_MOBILE, {})
    
    def state_edit_template_welcome_mobile(m: Message):
        state = tg.get_state(m.chat.id, m.from_user.id)
        if not state or state["state"] != STATE_EDIT_TEMPLATE_WELCOME_MOBILE:
            return
        
        template = (m.text or "").strip()
        templates = storage.load_templates()
        templates["welcome_mobile_template"] = template
        storage.save_templates(templates)
        
        tg.clear_state(m.chat.id, m.from_user.id, True)
        text = f"✅ <b>Шаблон сохранен</b>\n\n📝 <b>Приветствие Mobile:</b>\n<code>{template[:200]}{'...' if len(template) > 200 else ''}</code>"
        bot.send_message(m.chat.id, text, reply_markup=_kb_back(), parse_mode="HTML")
        logger.info(f"{LOGGER_PREFIX} Шаблон приветствия Mobile обновлен")
    
    def edit_template_success_steam(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        result = bot.send_message(c.message.chat.id, "✏️ Введите шаблон сообщения об успехе для Steam Gift:\n\nДоступные переменные: {game_name}, {region_name}, {price}, {transaction_id}, {status}, {order_link}", reply_markup=_kb_cancel())
        tg.set_state(c.message.chat.id, result.id, c.from_user.id, STATE_EDIT_TEMPLATE_SUCCESS_STEAM, {})
    
    def state_edit_template_success_steam(m: Message):
        state = tg.get_state(m.chat.id, m.from_user.id)
        if not state or state["state"] != STATE_EDIT_TEMPLATE_SUCCESS_STEAM:
            return
        
        template = (m.text or "").strip()
        templates = storage.load_templates()
        templates["success_steam_template"] = template
        storage.save_templates(templates)
        
        tg.clear_state(m.chat.id, m.from_user.id, True)
        text = f"✅ <b>Шаблон сохранен</b>\n\n✅ <b>Успех Steam:</b>\n<code>{template[:200]}{'...' if len(template) > 200 else ''}</code>"
        bot.send_message(m.chat.id, text, reply_markup=_kb_back(), parse_mode="HTML")
        logger.info(f"{LOGGER_PREFIX} Шаблон успеха Steam обновлен")
    
    def edit_template_success_mobile(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        result = bot.send_message(c.message.chat.id, "✏️ Введите шаблон сообщения об успехе для Mobile Refill:\n\nДоступные переменные: {game_name}, {position_name}, {field_labels}, {server_text}, {transaction_id}, {status}, {order_link}, {admin_call_message}", reply_markup=_kb_cancel())
        tg.set_state(c.message.chat.id, result.id, c.from_user.id, STATE_EDIT_TEMPLATE_SUCCESS_MOBILE, {})
    
    def state_edit_template_success_mobile(m: Message):
        state = tg.get_state(m.chat.id, m.from_user.id)
        if not state or state["state"] != STATE_EDIT_TEMPLATE_SUCCESS_MOBILE:
            return
        
        template = (m.text or "").strip()
        templates = storage.load_templates()
        templates["success_mobile_template"] = template
        storage.save_templates(templates)
        
        tg.clear_state(m.chat.id, m.from_user.id, True)
        text = f"✅ <b>Шаблон сохранен</b>\n\n✅ <b>Успех Mobile:</b>\n<code>{template[:200]}{'...' if len(template) > 200 else ''}</code>"
        bot.send_message(m.chat.id, text, reply_markup=_kb_back(), parse_mode="HTML")
        logger.info(f"{LOGGER_PREFIX} Шаблон успеха Mobile обновлен")
    
    tg.cbq_handler(edit_template_welcome_steam, lambda c: c.data == "AS_EDIT_TEMPLATE_WELCOME_STEAM")
    tg.cbq_handler(edit_template_welcome_mobile, lambda c: c.data == "AS_EDIT_TEMPLATE_WELCOME_MOBILE")
    tg.cbq_handler(edit_template_success_steam, lambda c: c.data == "AS_EDIT_TEMPLATE_SUCCESS_STEAM")
    tg.cbq_handler(edit_template_success_mobile, lambda c: c.data == "AS_EDIT_TEMPLATE_SUCCESS_MOBILE")
    
    def test_purchase(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        settings = storage.load_settings()
        api_key = settings.get("desslyhub_api_key", "")
        
        if not api_key:
            text = (
                f"🧪 <b>Тест покупки</b>\n\n"
                f"❌ <b>API ключ не установлен</b>\n\n"
                f"Для тестирования покупки необходимо установить API ключ DesslyHub в настройках."
            )
            kb = K()
            kb.add(B("🔙 Назад", callback_data=CB_BACK))
            _safe_edit(c, text, kb, parse_mode="HTML")
            return
        
        test_uuid = str(_uuid.uuid4())
        _test_purchases[test_uuid] = {
            "chat_id": None,
            "chat_name": None,
            "game_name": "UBERMOSH Collection",
            "app_id": None,
            "status": "pending",
            "created_at": time.time()
        }
        
        logger.info(f"{LOGGER_PREFIX} [TEST] Сгенерирован UUID для тестовой покупки: {test_uuid}")
        logger.info(f"{LOGGER_PREFIX} [TEST] Всего активных UUID: {len(_test_purchases)}, список: {list(_test_purchases.keys())}")
        
        text = (
            f"🧪 <b>Тест покупки</b>\n\n"
            f"📋 <b>Инструкция:</b>\n"
            f"1. Откройте чат на FunPay\n"
            f"2. Напишите команду:\n"
            f"<code>!автовыда {test_uuid}</code>\n\n"
            f"🔑 <b>UUID:</b> <code>{test_uuid}</code>\n\n"
            f"⏱ <b>Время жизни:</b> 10 минут\n\n"
            f"💡 <i>После отправки команды в чат FunPay, плагин создаст тестовый заказ и начнет процесс покупки</i>"
        )
        kb = K()
        kb.add(B("🔄 Сгенерировать новый", callback_data=CB_TEST_PURCHASE))
        kb.add(B("🔙 Назад", callback_data=CB_BACK))
        _safe_edit(c, text, kb, parse_mode="HTML")
    
    tg.cbq_handler(test_purchase, lambda c: c.data == CB_TEST_PURCHASE)
    
    def open_mobile(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        text = (
            f"📱 <b>Mobile Refill</b>\n\n"
            f"🔹 <b>Функционал:</b>\n"
            f"   • Тестовая покупка мобильных игр\n"
            f"   • Пополнение через DesslyHub API\n\n"
            f"💡 <i>Выберите действие:</i>"
        )
        _safe_edit(c, text, _kb_mobile_menu(), parse_mode="HTML")
    
    def test_mobile(c: CallbackQuery):
        bot.answer_callback_query(c.id)
        storage = _get_storage()
        settings = storage.load_settings()
        api_key = settings.get("desslyhub_api_key", "")
        
        if not api_key:
            text = "❌ Ошибка: API ключ DesslyHub не установлен"
            _safe_edit(c, text, _kb_back())
            return
        
        test_uuid = str(_uuid.uuid4())
        _test_purchases[test_uuid] = {
            "chat_id": None,
            "chat_name": None,
            "game_name": "PUBG Mobile (RU)",
            "game_id": None,
            "position_id": None,
            "type": "mobile",
            "status": "pending",
            "created_at": time.time()
        }
        
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Сгенерирован UUID для тестовой покупки Mobile: {test_uuid}")
        
        text = (
            f"🧪 <b>Тест покупки Mobile</b>\n\n"
            f"📋 <b>Инструкция:</b>\n"
            f"1. Откройте чат на FunPay\n"
            f"2. Напишите команду:\n"
            f"<code>!автовыда {test_uuid}</code>\n\n"
            f"🔑 <b>UUID:</b> <code>{test_uuid}</code>\n\n"
            f"⏱ <b>Время жизни:</b> 10 минут\n\n"
            f"💡 <i>После отправки команды в чат FunPay, плагин создаст тестовый заказ и начнет процесс пополнения</i>"
        )
        kb = K()
        kb.add(B("🔄 Сгенерировать новый", callback_data=CB_TEST_MOBILE))
        kb.add(B("🔙 Назад", callback_data=CB_OPEN_MOBILE))
        _safe_edit(c, text, kb, parse_mode="HTML")
    
    def open_blacklist(c: "CallbackQuery"):
        bot.answer_callback_query(c.id)
        storage = _get_storage()
        settings = storage.load_settings()
        blacklist_enabled = settings.get("blacklist_enabled", True)
        black_list = storage.load_black_list()
        
        text = f"🚫 <b>Черный список</b>\n\n"
        text += f"Статус: {'✅ Включен' if blacklist_enabled else '❌ Выключен'}\n\n"
        
        if not black_list:
            text += "Список пуст."
        else:
            text += f"Всего пользователей: <b>{len(black_list)}</b>\n\n"
            text += "Нажмите на имя пользователя для удаления:"
        
        kb = K()
        kb.add(B(f"{'🟢 Выключить' if blacklist_enabled else '🔴 Включить'}", callback_data="AS_TOGGLE_BLACKLIST"))
        kb.add(B("➕ Добавить никнейм", callback_data="AS_BLACKLIST_ADD"))
        
        if black_list:
            displayed_list = black_list[:50]
            for username in displayed_list:
                kb.add(B(f"❌ {username}", callback_data=f"AS_BLACKLIST_REMOVE:{username}"))
            
            if len(black_list) > 50:
                text += f"\n\n... и еще {len(black_list) - 50} пользователей (показаны первые 50)"
            
            kb.add(B("🗑 Очистить все", callback_data="AS_BLACKLIST_CLEAR"))
        
        kb.add(B("🔙 Назад", callback_data=CB_OPEN_MAIN))
        _safe_edit(c, text, kb, parse_mode="HTML")
    
    def blacklist_add(c: "CallbackQuery"):
        bot.answer_callback_query(c.id)
        text = (
            "➕ <b>Добавить в черный список</b>\n\n"
            "Отправьте никнейм пользователя FunPay для добавления в черный список.\n\n"
            "Плагин не будет обрабатывать заказы и сообщения от этого пользователя.\n\n"
            "Пример: <code>baduser123</code>"
        )
        kb = K()
        kb.add(B("🔙 Назад", callback_data=CB_OPEN_BLACKLIST))
        result = bot.send_message(c.message.chat.id, text, reply_markup=kb, parse_mode="HTML")
        tg.set_state(c.message.chat.id, result.id, c.from_user.id, "AS_BLACKLIST_ADD", {})
    
    def blacklist_clear(c: "CallbackQuery"):
        bot.answer_callback_query(c.id)
        storage = _get_storage()
        storage.save_black_list([])
        text = "✅ Черный список очищен."
        kb = K()
        kb.add(B("🔙 Назад", callback_data=CB_OPEN_BLACKLIST))
        _safe_edit(c, text, kb, parse_mode="HTML")
    
    def toggle_blacklist(c: "CallbackQuery"):
        bot.answer_callback_query(c.id)
        storage = _get_storage()
        settings = storage.load_settings()
        settings["blacklist_enabled"] = not bool(settings.get("blacklist_enabled", True))
        storage.save_settings(settings)
        logger.info(f"{LOGGER_PREFIX} Черный список: {settings['blacklist_enabled']}")
        open_blacklist(c)
    
    def state_blacklist_add(m: "Message"):
        state = tg.get_state(m.chat.id, m.from_user.id)
        if not state or state["state"] != "AS_BLACKLIST_ADD":
            return
        
        try:
            username = m.text.strip()
            if not username:
                bot.send_message(m.chat.id, "❌ Никнейм не может быть пустым.", reply_markup=_kb_cancel())
                return
            
            storage = _get_storage()
            black_list = storage.load_black_list()
            
            username_lower = username.lower()
            if username_lower in [u.lower() for u in black_list]:
                bot.send_message(m.chat.id, f"❌ Пользователь <code>{username}</code> уже есть в черном списке.", parse_mode="HTML", reply_markup=_kb_back())
                tg.clear_state(m.chat.id, m.from_user.id, True)
                return
            
            black_list.append(username)
            storage.save_black_list(black_list)
            
            tg.clear_state(m.chat.id, m.from_user.id, True)
            
            settings = storage.load_settings()
            blacklist_enabled = settings.get("blacklist_enabled", True)
            
            text = f"✅ Пользователь <code>{username}</code> добавлен в черный список.\n\n"
            text += f"Статус: {'✅ Включен' if blacklist_enabled else '❌ Выключен'}\n"
            text += f"Всего в списке: <b>{len(black_list)}</b>"
            
            kb = K()
            kb.add(B("🔙 К черному списку", callback_data=CB_OPEN_BLACKLIST))
            
            bot.send_message(m.chat.id, text, parse_mode="HTML", reply_markup=kb)
            logger.info(f"{LOGGER_PREFIX} Пользователь {username} добавлен в черный список")
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Ошибка добавления в черный список: {e}")
            bot.send_message(m.chat.id, "❌ Ошибка при добавлении в черный список.", reply_markup=_kb_back())
            tg.clear_state(m.chat.id, m.from_user.id, True)
    
    def blacklist_remove(c: "CallbackQuery"):
        bot.answer_callback_query(c.id)
        try:
            if not c.data or not c.data.startswith("AS_BLACKLIST_REMOVE:"):
                return
            
            username = c.data.replace("AS_BLACKLIST_REMOVE:", "", 1)
            if not username:
                return
            
            storage = _get_storage()
            black_list = storage.load_black_list()
            
            username_lower = username.lower()
            original_username = None
            for u in black_list:
                if u.lower() == username_lower:
                    original_username = u
                    break
            
            if not original_username:
                bot.send_message(c.message.chat.id, f"❌ Пользователь <code>{username}</code> не найден в черном списке.", parse_mode="HTML", reply_markup=_kb_back())
                return
            
            black_list = [u for u in black_list if u.lower() != username_lower]
            storage.save_black_list(black_list)
            
            logger.info(f"{LOGGER_PREFIX} Пользователь {original_username} удален из черного списка")
            open_blacklist(c)
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Ошибка удаления из черного списка: {e}")
            bot.send_message(c.message.chat.id, "❌ Ошибка при удалении из черного списка.", reply_markup=_kb_back())
    
    tg.cbq_handler(blacklist_add, lambda c: c.data == "AS_BLACKLIST_ADD")
    tg.cbq_handler(blacklist_clear, lambda c: c.data == "AS_BLACKLIST_CLEAR")
    tg.cbq_handler(toggle_blacklist, lambda c: c.data == "AS_TOGGLE_BLACKLIST")
    tg.cbq_handler(blacklist_remove, lambda c: c.data and c.data.startswith("AS_BLACKLIST_REMOVE:"))
    tg.msg_handler(state_blacklist_add, func=lambda m: tg.check_state(m.chat.id, m.from_user.id, "AS_BLACKLIST_ADD"))
    
    def open_statistics(c: "CallbackQuery"):
        storage = _get_storage()
        orders = storage.load_orders()
        settings = storage.load_settings()
        lots_config = storage.load_lots_config()
        
        now = time.time()
        day_ago = now - 86400
        week_ago = now - 604800
        month_ago = now - 2592000
        
        all_success = [o for o in orders if o.get("status") == "success"]
        day_orders = [o for o in all_success if o.get("timestamp", 0) >= day_ago]
        week_orders = [o for o in all_success if o.get("timestamp", 0) >= week_ago]
        month_orders = [o for o in all_success if o.get("timestamp", 0) >= month_ago]
        
        def calculate_revenue(orders_list):
            total = 0.0
            for o in orders_list:
                price = o.get("price")
                if price:
                    try:
                        total += float(price)
                    except (ValueError, TypeError):
                        pass
            return total
        
        day_revenue = calculate_revenue(day_orders)
        week_revenue = calculate_revenue(week_orders)
        month_revenue = calculate_revenue(month_orders)
        total_revenue = calculate_revenue(all_success)
        
        steam_count = len([o for o in all_success if o.get("type") == "steam_gift"])
        mobile_count = len([o for o in all_success if o.get("type") == "mobile_refill"])
        
        api_key = settings.get("desslyhub_api_key", "")
        balance_text = "Не установлен"
        if api_key:
            balance_data = _get_desslyhub_balance(api_key)
            if balance_data:
                balance = balance_data.get("balance", 0.0)
                currency = balance_data.get("currency", "USD")
                balance_text = f"{balance:.2f} {currency}"
        
        text = (
            f"📊 <b>Статистика продаж</b>\n\n"
            f"📅 <b>За сегодня:</b>\n"
            f"   • Заказов: <b>{len(day_orders)}</b>\n"
            f"   • Доход: <b>{day_revenue:.2f} USD</b>\n\n"
            f"📅 <b>За неделю:</b>\n"
            f"   • Заказов: <b>{len(week_orders)}</b>\n"
            f"   • Доход: <b>{week_revenue:.2f} USD</b>\n\n"
            f"📅 <b>За месяц:</b>\n"
            f"   • Заказов: <b>{len(month_orders)}</b>\n"
            f"   • Доход: <b>{month_revenue:.2f} USD</b>\n\n"
            f"📦 <b>Всего:</b>\n"
            f"   • Заказов: <b>{len(all_success)}</b>\n"
            f"   • Доход: <b>{total_revenue:.2f} USD</b>\n\n"
            f"🎮 <b>По типам:</b>\n"
            f"   • 🎁 Steam Gift: <b>{steam_count}</b>\n"
            f"   • 📱 Mobile Refill: <b>{mobile_count}</b>\n\n"
            f"⚙️ <b>Настройки:</b>\n"
            f"   • Лотов в конфиге: <b>{len(lots_config)}</b>\n"
            f"   • Наценка: <b>{settings.get('markup_percent', 10.0)}%</b>\n"
            f"   • Баланс DesslyHub: <b>{balance_text}</b>\n"
        )
        
        kb = K()
        kb.add(B("🔙 Назад", callback_data=CB_OPEN_MAIN))
        _safe_edit(c, text, kb, parse_mode="HTML")
    
    def open_orders_history(c: "CallbackQuery"):
        bot.answer_callback_query(c.id)
        try:
            if c.data.startswith("AS_ORDERS_HISTORY:"):
                page = int(c.data.split(":")[1]) if ":" in c.data and len(c.data.split(":")) > 1 else 0
            else:
                page = 0
        except Exception:
            page = 0
        
        storage = _get_storage()
        orders = storage.load_orders()
        
        if not orders:
            text = "📜 <b>История заказов</b>\n\nИстория пуста."
            kb = K()
            kb.add(B("🔙 Назад", callback_data=CB_OPEN_MAIN))
            _safe_edit(c, text, kb, parse_mode="HTML")
            return
        
        sorted_orders = sorted(orders, key=lambda x: x.get("timestamp", 0), reverse=True)
        page_size = 5
        start = page * page_size
        end = start + page_size
        page_orders = sorted_orders[start:end]
        
        if not page_orders and page != 0:
            page = 0
            start = 0
            end = page_size
            page_orders = sorted_orders[start:end]
        
        text = f"📜 <b>История заказов</b>\n\n"
        text += f"📊 <b>Всего заказов:</b> {len(sorted_orders)}\n"
        text += f"📄 <b>Страница:</b> {page + 1} из {(len(sorted_orders) + page_size - 1) // page_size}\n\n"
        text += "━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for order in page_orders:
            order_id = order.get("order_id", "N/A")
            order_type = order.get("type", "unknown")
            game_name = order.get("game_name", "N/A")
            status = order.get("status", "unknown")
            timestamp = order.get("timestamp", 0)
            transaction_id = order.get("transaction_id", "")
            player_id = order.get("player_id", "")
            position_name = order.get("position_name", "")
            region = order.get("region", "")
            
            if timestamp:
                dt = datetime.fromtimestamp(timestamp)
                date_str = dt.strftime("%d.%m.%Y")
                time_str = dt.strftime("%H:%M:%S")
            else:
                date_str = "N/A"
                time_str = "N/A"
            
            type_emoji = "🎁" if order_type == "steam_gift" else "📱" if order_type == "mobile_refill" else "❓"
            type_name = "Steam Gift" if order_type == "steam_gift" else "Mobile Refill" if order_type == "mobile_refill" else "Unknown"
            status_emoji = "✅" if status == "success" else "❌"
            status_name = "Успешно" if status == "success" else "Ошибка"
            
            text += f"{type_emoji} <b>{type_name}</b> {status_emoji} {status_name}\n"
            text += f"🆔 <code>{order_id}</code>\n"
            text += f"🎮 <b>Игра:</b> {game_name}\n"
            
            if position_name and position_name != "N/A":
                text += f"💎 <b>Позиция:</b> {position_name}\n"
            if region:
                text += f"🌍 <b>Регион:</b> {region}\n"
            if player_id:
                text += f"👤 <b>Player ID:</b> <code>{player_id}</code>\n"
            if transaction_id:
                text += f"🔑 <b>Transaction ID:</b> <code>{transaction_id}</code>\n"
            
            text += f"📅 <b>Дата:</b> {date_str} {time_str}\n"
            text += "━━━━━━━━━━━━━━━━━━━━\n\n"
        
        kb = K()
        if end < len(sorted_orders):
            kb.add(B("⏩ Далее", callback_data=f"AS_ORDERS_HISTORY:{page+1}"))
        if page > 0:
            kb.add(B("⏪ Назад", callback_data=f"AS_ORDERS_HISTORY:{page-1}"))
        kb.add(B("🔙 Назад", callback_data=CB_OPEN_MAIN))
        _safe_edit(c, text, kb, parse_mode="HTML")
    
    tg.cbq_handler(open_mobile, lambda c: c.data == CB_OPEN_MOBILE)
    tg.cbq_handler(open_blacklist, lambda c: c.data == CB_OPEN_BLACKLIST)
    tg.cbq_handler(open_statistics, lambda c: c.data == CB_OPEN_STATISTICS)
    tg.cbq_handler(open_orders_history, lambda c: c.data == CB_OPEN_ORDERS_HISTORY or c.data.startswith("AS_ORDERS_HISTORY:"))
    

    tg.file_handler(STATE_ADD_GAME, state_add_game)

    tg.msg_handler(state_add_game, func=lambda m: tg.check_state(m.chat.id, m.from_user.id, STATE_ADD_GAME) and (not hasattr(m, 'document') or not m.document))
    tg.msg_handler(state_set_markup, func=lambda m: tg.check_state(m.chat.id, m.from_user.id, STATE_SET_MARKUP))
    tg.msg_handler(state_set_balance_threshold, func=lambda m: tg.check_state(m.chat.id, m.from_user.id, STATE_SET_BALANCE_THRESHOLD))
    tg.msg_handler(state_search_games, func=lambda m: tg.check_state(m.chat.id, m.from_user.id, STATE_SEARCH_GAMES))
    tg.msg_handler(state_set_api_key, func=lambda m: tg.check_state(m.chat.id, m.from_user.id, "AS_SET_API_KEY"))
    tg.msg_handler(state_edit_template_welcome_steam, func=lambda m: tg.check_state(m.chat.id, m.from_user.id, STATE_EDIT_TEMPLATE_WELCOME_STEAM))
    tg.msg_handler(state_edit_template_welcome_mobile, func=lambda m: tg.check_state(m.chat.id, m.from_user.id, STATE_EDIT_TEMPLATE_WELCOME_MOBILE))
    tg.msg_handler(state_edit_template_success_steam, func=lambda m: tg.check_state(m.chat.id, m.from_user.id, STATE_EDIT_TEMPLATE_SUCCESS_STEAM))
    tg.msg_handler(state_edit_template_success_mobile, func=lambda m: tg.check_state(m.chat.id, m.from_user.id, STATE_EDIT_TEMPLATE_SUCCESS_MOBILE))
    
    cardinal.add_telegram_commands(UUID, [("steam", "Открыть панель AUTOSTEAM", True)])
    
    logger.info(f"{LOGGER_PREFIX} Плагин успешно инициализирован")


def handle_test_purchase_message(cardinal: "Cardinal", event: NewMessageEvent) -> None:
    try:
        logger.info(f"{LOGGER_PREFIX} [TEST] Обработчик вызван: chat_id={event.message.chat_id}, author_id={event.message.author_id}, author={event.message.author}, text='{str(event.message)[:50]}'")
        
        if not cardinal or not hasattr(cardinal, 'account'):
            logger.warning(f"{LOGGER_PREFIX} [TEST] Cardinal или account недоступен")
            return
        
        if not hasattr(cardinal.account, 'id'):
            logger.warning(f"{LOGGER_PREFIX} [TEST] cardinal.account.id недоступен")
            return
        
        if event.message.author_id == 0:
            return
        
        if event.message.author_id == cardinal.account.id:
            return
        
        username = event.message.author
        if username and _check_blacklist_username(username):
            logger.warning(f"{LOGGER_PREFIX} [TEST] Пользователь '{username}' в черном списке, игнорируем сообщение")
            return
        
        message_type = None
        if hasattr(event.message, "type"):
            message_type = event.message.type
        elif hasattr(event.message, "get_message_type"):
            message_type = event.message.get_message_type()
        
        if message_type and message_type != MessageTypes.NON_SYSTEM:
            return
        
        message_text = str(event.message).strip()
        
        if (message_text.startswith("❌ Неверный формат ссылки!") or 
            message_text.startswith("❌ Это пример ссылки") or
            message_text.startswith("🎮 Спасибо за покупку!") or
            message_text.startswith("✅ Подарок успешно отправлен!") or
            message_text.startswith("✅ Пополнение успешно отправлено!")):
            return
        
        if ("xxxx-xxxx" in message_text or 
            "Правильный формат:" in message_text or 
            "Пример:" in message_text or
            "Отправьте ссылку на добавление в друзья:" in message_text or
            "https://s.team/p/..." in message_text or
            "https://s.team/p/xxxx" in message_text or
            "/p/..." in message_text or
            re.search(r'https?://s\.team/p/\.\.\.', message_text, re.IGNORECASE) or
            "Ожидание ссылки..." in message_text):
            return
        
        logger.info(f"{LOGGER_PREFIX} [TEST] Обработка сообщения: author_id={event.message.author_id}")
        logger.info(f"{LOGGER_PREFIX} [TEST] Текст сообщения: '{message_text[:100]}'")
        
        if not message_text.startswith("!автовыда"):
            logger.info(f"{LOGGER_PREFIX} [TEST] Сообщение не начинается с '!автовыда', пропускаем")
            return
        
        logger.info(f"{LOGGER_PREFIX} [TEST] Получена команда тестовой покупки: {message_text}")
        
        parts = message_text.split()
        if len(parts) < 2:
            logger.warning(f"{LOGGER_PREFIX} [TEST] UUID не указан в команде")
            return
        
        uuid_value = parts[1].strip()
        uuid_value = re.sub(r'[^\w\-]', '', uuid_value)
        
        logger.info(f"{LOGGER_PREFIX} [TEST] Извлеченный UUID: '{uuid_value}', длина: {len(uuid_value)}")
        logger.info(f"{LOGGER_PREFIX} [TEST] Доступные UUID в _test_purchases: {list(_test_purchases.keys())}")
        
        found_uuid = None
        for stored_uuid in _test_purchases.keys():
            if stored_uuid.replace('-', '') == uuid_value.replace('-', ''):
                found_uuid = stored_uuid
                logger.info(f"{LOGGER_PREFIX} [TEST] Найден совпадающий UUID: '{stored_uuid}' для '{uuid_value}'")
                break
        
        if not found_uuid:
            logger.warning(f"{LOGGER_PREFIX} [TEST] Невалидный UUID: '{uuid_value}'. Доступные UUID: {list(_test_purchases.keys())}")
            return
        
        uuid_value = found_uuid
        
        test_data = _test_purchases[uuid_value]
        if test_data["status"] != "pending":
            logger.warning(f"{LOGGER_PREFIX} [TEST] UUID уже использован: {uuid_value}, статус: {test_data['status']}")
            return
        
        if time.time() - test_data["created_at"] > 600:
            logger.warning(f"{LOGGER_PREFIX} [TEST] UUID истек: {uuid_value}")
            del _test_purchases[uuid_value]
            return
        
        chat_id = event.message.chat_id
        chat_name = event.message.chat_name
        username = event.message.author
        
        logger.info(f"{LOGGER_PREFIX} [TEST] Начало обработки тестовой покупки: UUID={uuid_value}, chat_id={chat_id}, username={username}")
        
        test_data["chat_id"] = chat_id
        test_data["chat_name"] = chat_name
        test_data["status"] = "processing"
        
        settings = _get_storage().load_settings()
        api_key = settings.get("desslyhub_api_key", "")
        
        if not api_key:
            logger.error(f"{LOGGER_PREFIX} [TEST] API ключ не установлен")
            cardinal.send_message(chat_id, "❌ Ошибка: API ключ не установлен", chat_name)
            del _test_purchases[uuid_value]
            return
        
        purchase_type = test_data.get("type", "steam")
        game_name = test_data["game_name"]
        
        if purchase_type == "mobile":
            logger.info(f"{LOGGER_PREFIX} [MOBILE] Поиск game_id для мобильной игры '{game_name}'")
            
            game_id = _get_mobile_game_id_by_name(game_name, api_key)
            if not game_id:
                logger.error(f"{LOGGER_PREFIX} [MOBILE] Не удалось найти game_id для игры '{game_name}'")
                cardinal.send_message(chat_id, f"❌ Ошибка: Игра '{game_name}' не найдена в каталоге", chat_name)
                del _test_purchases[uuid_value]
                return
            
            test_data["game_id"] = game_id
            logger.info(f"{LOGGER_PREFIX} [MOBILE] Найден game_id={game_id} для игры '{game_name}'")
            
            game_info = _get_mobile_game_by_id(api_key, game_id)
            if not game_info:
                logger.error(f"{LOGGER_PREFIX} [MOBILE] Не удалось получить информацию об игре game_id={game_id}")
                cardinal.send_message(chat_id, f"❌ Ошибка: Не удалось получить информацию об игре '{game_name}'", chat_name)
                del _test_purchases[uuid_value]
                return
            
            positions = game_info.get("positions", [])
            if not positions:
                logger.error(f"{LOGGER_PREFIX} [MOBILE] Нет доступных позиций для игры game_id={game_id}")
                cardinal.send_message(chat_id, f"❌ Ошибка: Нет доступных позиций для игры '{game_name}'", chat_name)
                del _test_purchases[uuid_value]
                return
            
            selected_position = None
            game_name_lower = game_name.lower().strip()
            
            if "pubg" in game_name_lower and "mobile" in game_name_lower:
                logger.info(f"{LOGGER_PREFIX} [MOBILE] Поиск позиции для PUBG Mobile: 60 us")
                
                for pos in positions:
                    pos_name = pos.get("name", "")
                    pos_name_lower = pos_name.lower()
                    
                    if "60" in pos_name and ("us" in pos_name_lower or "uc" in pos_name_lower or "unknown cash" in pos_name_lower):
                        numbers = re.findall(r'\d+', pos_name)
                        if numbers:
                            first_number = int(numbers[0])
                            if first_number == 60:
                                selected_position = pos
                                logger.info(f"{LOGGER_PREFIX} [MOBILE] Найдена позиция: {pos.get('name')} (id={pos.get('id')})")
                                break
                
                if not selected_position:
                    logger.warning(f"{LOGGER_PREFIX} [MOBILE] Позиция '60 us' не найдена, используем первую доступную")
                    logger.info(f"{LOGGER_PREFIX} [MOBILE] Доступные позиции: {[p.get('name') for p in positions[:10]]}")
                    selected_position = positions[0]
            else:
                selected_position = positions[0]
            
            logger.info(f"{LOGGER_PREFIX} [MOBILE] Выбрана позиция: {selected_position.get('name')} (id={selected_position.get('id')})")
            
            position_id = selected_position.get("id")
            position_name = selected_position.get("name", "")
            position_price = selected_position.get("price", "0")
            
            test_data["position_id"] = position_id
            test_data["position_name"] = position_name
            test_data["position_price"] = position_price
            
            fields_info = game_info.get("fields", {})
            servers_info = game_info.get("servers", {})
            
            fields_config = _get_mobile_game_fields_config(game_name, fields_info, servers_info)
            fields_to_request = fields_config.get("fields_to_request", [])
            auto_server = fields_config.get("auto_server")
            
            if not fields_to_request:
                field_name = list(fields_info.keys())[0] if fields_info else "Player ID"
                fields_to_request = [field_name]
            
            test_data["fields_to_request"] = fields_to_request
            test_data["current_field_index"] = 0
            test_data["fields_data"] = {}
            test_data["auto_server"] = auto_server
            
            if auto_server:
                test_data["server"] = auto_server
                logger.info(f"{LOGGER_PREFIX} [MOBILE] Автоматически установлен сервер: {auto_server}")
            
            field_name = fields_to_request[0]
            
            storage = _get_storage()
            templates = storage.load_templates()
            welcome_template = templates.get("welcome_mobile_template", 
                "🎮 Спасибо за покупку!\n\n📦 Игра: {game_name}\n💎 Позиция: {position_name}\n\n📝 Отправьте {field_name}:\n\n⏱ Ожидание данных...")
            
            message = _format_template(welcome_template,
                game_name=game_name,
                position_name=position_name,
                position_price=position_price,
                field_name=field_name
            )
            
            result = cardinal.send_message(chat_id, message, chat_name)
            if result:
                logger.info(f"{LOGGER_PREFIX} [MOBILE] Сообщение с запросом {field_name} успешно отправлено в чат {chat_id}")
                test_data["status"] = "waiting_player_id"
            else:
                logger.error(f"{LOGGER_PREFIX} [MOBILE] Не удалось отправить сообщение в чат {chat_id}")
                test_data["status"] = "failed"
                del _test_purchases[uuid_value]
            return
        
        logger.info(f"{LOGGER_PREFIX} [TEST] Поиск app_id для игры '{game_name}'")
        
        app_id = _get_game_app_id_by_name(game_name, api_key)
        if not app_id:
            logger.error(f"{LOGGER_PREFIX} [TEST] Не удалось найти app_id для игры '{game_name}'")
            cardinal.send_message(chat_id, f"❌ Ошибка: Игра '{game_name}' не найдена в каталоге", chat_name)
            del _test_purchases[uuid_value]
            return
        
        test_data["app_id"] = app_id
        test_data["game_name"] = game_name
        logger.info(f"{LOGGER_PREFIX} [TEST] Найден app_id={app_id} для игры '{game_name}'")
        
        date = datetime.now()
        date_text = date.strftime("%H:%M")
        
        ORDER_HTML_TEMPLATE = """<a href="https://funpay.com/orders/ADTEST/" class="tc-item">
   <div class="tc-date" bis_skin_checked="1">
      <div class="tc-date-time" bis_skin_checked="1">сегодня, $date</div>
      <div class="tc-date-left" bis_skin_checked="1">только что</div>
   </div>
   <div class="tc-order" bis_skin_checked="1">#ADTEST</div>
   <div class="order-desc" bis_skin_checked="1">
      <div bis_skin_checked="1">$lot_name</div>
      <div class="text-muted" bis_skin_checked="1">Автовыдача, Тест</div>
   </div>
   <div class="tc-user" bis_skin_checked="1">
      <div class="media media-user offline" bis_skin_checked="1">
         <div class="media-left" bis_skin_checked="1">
            <div class="avatar-photo pseudo-a" tabindex="0" data-href="https://funpay.com/users/000000/" style="background-image: url(/img/layout/avatar.png);" bis_skin_checked="1"></div>
         </div>
         <div class="media-body" bis_skin_checked="1">
            <div class="media-user-name" bis_skin_checked="1">
               <span class="pseudo-a" tabindex="0" data-href="https://funpay.com/users/000000/">$username</span>
            </div>
            <div class="media-user-status" bis_skin_checked="1">был 1.000.000 лет назад</div>
         </div>
      </div>
   </div>
   <div class="tc-status text-primary" bis_skin_checked="1">Оплачен</div>
   <div class="tc-price text-nowrap tc-seller-sum" bis_skin_checked="1">999999.0 <span class="unit">₽</span></div>
</a>"""
        
        html = ORDER_HTML_TEMPLATE.replace("$username", chat_name).replace("$lot_name", game_name).replace("$date", date_text)
        
        fake_order = types.OrderShortcut("ADTEST", game_name, 0.0, Currency.UNKNOWN, chat_name, 000000, chat_id,
                                       types.OrderStatuses.PAID, date, "Авто-выдача, Тест", None, html)
        
        fake_event = NewOrderEvent("test", fake_order)
        logger.info(f"{LOGGER_PREFIX} [TEST] Создан тестовый заказ для игры '{game_name}'")
        
        message = (
            f"🎮 Спасибо за покупку!\n\n"
            f"📦 Игра: {game_name}\n"
            f"🌍 Регион: Казахстан\n\n"
            f"🔗 Отправьте ссылку на добавление в друзья:\n"
            f"https://s.team/p/...\n\n"
            f"⏱ Ожидание ссылки..."
        )
        
        logger.info(f"{LOGGER_PREFIX} [TEST] Отправка сообщения в чат {chat_id} с запросом ссылки")
        result = cardinal.send_message(chat_id, message, chat_name)
        if result:
            logger.info(f"{LOGGER_PREFIX} [TEST] Сообщение с запросом ссылки успешно отправлено в чат {chat_id}")
            test_data["status"] = "waiting_link"
        else:
            logger.error(f"{LOGGER_PREFIX} [TEST] Не удалось отправить сообщение в чат {chat_id}")
            test_data["status"] = "failed"
            del _test_purchases[uuid_value]
        
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [TEST] Критическая ошибка при обработке команды тестовой покупки: {e}", exc_info=True)


def _check_blacklist_username(username: str) -> bool:
    """Проверяет, находится ли пользователь в черном списке"""
    try:
        storage = _get_storage()
        settings = storage.load_settings()
        
        if not settings.get("blacklist_enabled", True):
            return False
        
        black_list = storage.load_black_list()
        if not black_list:
            return False
        
        username_lower = username.lower().strip()
        for blacklisted_username in black_list:
            if blacklisted_username.lower().strip() == username_lower:
                logger.warning(f"{LOGGER_PREFIX} Пользователь '{username}' находится в черном списке")
                return True
        
        return False
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} Ошибка проверки черного списка: {e}")
        return False


def handle_friend_link_message(cardinal: "Cardinal", event: NewMessageEvent) -> None:
    try:
        if not cardinal or not hasattr(cardinal, 'account'):
            return
        
        if event.message.author_id == 0:
            return
        
        if event.message.author_id == cardinal.account.id:
            return
        
        message_type = None
        if hasattr(event.message, "type"):
            message_type = event.message.type
        elif hasattr(event.message, "get_message_type"):
            message_type = event.message.get_message_type()
        
        if message_type and message_type != MessageTypes.NON_SYSTEM:
            return
        
        chat_id = str(event.message.chat_id)
        chat_name = event.message.chat_name
        message_text = str(event.message).strip()
        logger.debug(f"{LOGGER_PREFIX} [TEST] Обработка сообщения Steam: chat_id={chat_id} (тип: {type(chat_id).__name__})")
        
        if (message_text.startswith("❌ Неверный формат ссылки!") or 
            message_text.startswith("❌ Это пример ссылки") or
            message_text.startswith("🎮 Спасибо за покупку!") or
            message_text.startswith("✅ Подарок успешно отправлен!") or
            message_text.startswith("✅ Пополнение успешно отправлено!")):
            return
        
        if ("xxxx-xxxx" in message_text or 
            "Правильный формат:" in message_text or 
            "Пример:" in message_text or
            "Отправьте ссылку на добавление в друзья:" in message_text or
            "Отправьте" in message_text and ":" in message_text and ("Player ID" in message_text or "ссылку" in message_text)):
            return
        
        if ("https://s.team/p/..." in message_text or 
            "https://s.team/p/xxxx" in message_text or
            "/p/..." in message_text or
            re.search(r'https?://s\.team/p/\.\.\.', message_text, re.IGNORECASE) or
            "Ожидание ссылки..." in message_text):
            return
        
        if ("🎮 Спасибо за покупку!" in message_text):
            if ("https://s.team/p/..." in message_text or 
                "Ожидание ссылки..." in message_text or 
                "📦 Игра:" in message_text or
                "🔗 Отправьте ссылку" in message_text):
                return
        
        if ("📦 Игра:" in message_text and "🌍 Регион:" in message_text):
            if ("https://s.team/p/..." in message_text or 
                "Ожидание ссылки..." in message_text or
                "🔗 Отправьте ссылку" in message_text):
                return
        
        if ("🔗 Отправьте ссылку на добавление в друзья:" in message_text and 
            ("https://s.team/p/..." in message_text or "Ожидание ссылки..." in message_text)):
            return
        
        if ("🎮 Спасибо за покупку!" in message_text and 
            "📦 Игра:" in message_text and 
            "🌍 Регион:" in message_text and
            ("https://s.team/p/..." in message_text or "Ожидание ссылки..." in message_text)):
            return
        
        username = event.message.author
        if username and _check_blacklist_username(username):
            logger.warning(f"{LOGGER_PREFIX} [TEST] Пользователь '{username}' в черном списке, игнорируем сообщение: {chat_id}")
            return
        
        logger.info(f"{LOGGER_PREFIX} [TEST] Проверка сообщения на ссылку Steam: chat_id={chat_id}, text='{message_text[:100]}'")
        
        test_data = None
        test_uuid = None
        order_data = None
        order_id = None
        
        for uuid_key, data in _test_purchases.items():
            data_chat_id = str(data.get("chat_id", ""))
            if (data_chat_id == chat_id and 
                data.get("status") == "waiting_link" and
                data.get("type") != "mobile" and
                time.time() - data.get("created_at", 0) < 600):
                test_data = data
                test_uuid = uuid_key
                logger.info(f"{LOGGER_PREFIX} [TEST] Найдена тестовая покупка {test_uuid} в ожидании ссылки Steam")
                break
        
        if not test_data:
            with _order_lock:
                logger.info(f"{LOGGER_PREFIX} [ORDER] Поиск активного заказа в ожидании ссылки для chat_id={chat_id}, всего заказов: {len(_active_orders)}")
                for oid, data in _active_orders.items():
                    data_chat_id = str(data.get('chat_id', ''))
                    logger.info(f"{LOGGER_PREFIX} [ORDER] Проверка заказа {oid}: chat_id={data_chat_id} (тип: {type(data_chat_id).__name__}), ищем={chat_id} (тип: {type(chat_id).__name__}), status={data.get('status')}, type={data.get('type')}")
                    if (data_chat_id == str(chat_id) and 
                        data.get("status") == "waiting_link" and
                        data.get("type") == "steam"):
                        order_data = data
                        order_id = oid
                        logger.info(f"{LOGGER_PREFIX} [ORDER] Найден активный заказ {order_id} в ожидании ссылки Steam")
                        break
        
        if not test_data and not order_data:
            logger.debug(f"{LOGGER_PREFIX} [TEST] Нет активной покупки Steam в ожидании ссылки для чата {chat_id}")
            return
        
        if order_data:
            logger.info(f"{LOGGER_PREFIX} [ORDER] Найден активный заказ {order_id} в ожидании ссылки Steam")
        
        if test_data and test_data.get("status") in ("completed", "failed"):
            logger.warning(f"{LOGGER_PREFIX} [TEST] Заказ уже завершен, статус: {test_data.get('status')}")
            return
        
        if order_data and order_data.get("status") in ("completed", "failed"):
            logger.warning(f"{LOGGER_PREFIX} [ORDER] Заказ уже завершен, статус: {order_data.get('status')}")
            return
        
        logger.info(f"{LOGGER_PREFIX} [TEST] Получено сообщение в чате {chat_id}, проверка на ссылку Steam")
        
        steam_link_patterns = [
            r'https?://s\.team/[^\s\)]+',
            r'https?://steamcommunity\.com/(?:profiles|id)/[^\s\)]+',
            r'https?://steamcommunity\.com/friends/add/[^\s\)]+'
        ]
        
        friend_link = None
        for pattern in steam_link_patterns:
            match = re.search(pattern, message_text, re.IGNORECASE)
            if match:
                potential_link = match.group(0)
                
                if 'xxxx-xxxx' in potential_link or 'xxxxx' in potential_link:
                    continue
                
                if '/p/...' in potential_link or potential_link.endswith('/p/...'):
                    continue
                
                match_end = match.end()
                if match_end < len(message_text) and message_text[match_end:match_end+1] == ')':
                    continue
                
                if potential_link.endswith('...') or potential_link.endswith('...)'):
                    continue
                
                if '...' in potential_link:
                    continue
                
                if potential_link.endswith(')'):
                    continue
                
                if '/p/' in potential_link or '/profiles/' in potential_link or '/id/' in potential_link:
                    path_part = potential_link.split('/')[-1] if '/' in potential_link else ''
                    if path_part and path_part != '...' and len(path_part) > 2 and '...' not in path_part:
                        if not path_part.endswith(')'):
                            if len(path_part) > 5 and 'xxxx' not in path_part.lower():
                                friend_link = potential_link
                                break
        
        if not friend_link:
            logger.warning(f"{LOGGER_PREFIX} [TEST] В сообщении не найдена валидная ссылка Steam: {message_text[:100]}")
            if test_data or order_data:
                error_msg = (
                    f"❌ Неверный формат ссылки!\n\n"
                    f"📝 Правильный формат:\n"
                    f"https://s.team/p/xxxx-xxxx/xxxxx\n\n"
                    f"🔗 Пример:\n"
                    f"https://s.team/p/jwkn-dphc/mtmbdmjp\n\n"
                    f"⏱ Пожалуйста, отправьте корректную ссылку."
                )
                cardinal.send_message(chat_id, error_msg, chat_name)
            return
        
        friend_link = _clean_steam_link(friend_link)
        
        if friend_link.endswith('...') or friend_link.endswith('...)') or '...' in friend_link or friend_link.endswith(')'):
            logger.warning(f"{LOGGER_PREFIX} [TEST] Ссылка является примером, а не реальной ссылкой: {friend_link}")
            return
        
        if not _validate_steam_friend_link(friend_link):
            logger.warning(f"{LOGGER_PREFIX} [TEST] Невалидная ссылка Steam: {friend_link}")
            error_msg = (
                f"❌ Неверный формат ссылки!\n\n"
                f"📝 Правильный формат:\n"
                f"https://s.team/p/xxxx-xxxx/xxxxx\n\n"
                f"🔗 Пример:\n"
                f"https://s.team/p/jwkn-dphc/mtmbdmjp\n\n"
                f"⏱ Пожалуйста, отправьте корректную ссылку."
            )
            cardinal.send_message(chat_id, error_msg, chat_name)
            return
        
        logger.info(f"{LOGGER_PREFIX} [TEST] Найдена валидная ссылка Steam: {friend_link}")
        
        settings = _get_storage().load_settings()
        api_key = settings.get("desslyhub_api_key", "")
        
        if not api_key:
            logger.error(f"{LOGGER_PREFIX} [TEST] API ключ не установлен при обработке ссылки")
            cardinal.send_message(chat_id, "❌ Ошибка: API ключ не установлен", chat_name)
            del _test_purchases[test_uuid]
            return
        
        if test_data:
            app_id = test_data.get("app_id")
            if not app_id:
                logger.error(f"{LOGGER_PREFIX} [TEST] app_id не найден в данных тестовой покупки")
                cardinal.send_message(chat_id, "❌ Ошибка: app_id игры не найден", chat_name)
                if test_uuid:
                    del _test_purchases[test_uuid]
                return
            
            game_name = test_data.get("game_name", "UBERMOSH Collection")
            region = test_data.get("region", "KZ")
        elif order_data:
            app_id = order_data.get("app_id")
            if not app_id:
                logger.error(f"{LOGGER_PREFIX} [ORDER] app_id не найден в данных заказа")
                cardinal.send_message(chat_id, "❌ Ошибка: app_id игры не найден", chat_name)
                with _order_lock:
                    if order_id in _active_orders:
                        del _active_orders[order_id]
                return
            
            game_name = order_data.get("game_name", "")
            region = order_data.get("region", "KZ")
            lot_config = order_data.get("lot_config", {})
        else:
            return
        
        # Получаем lot_name для определения типа издания
        lot_name = lot_config.get("lot_name", "") if lot_config else ""
        
        logger.info(f"{LOGGER_PREFIX} {'[TEST]' if test_data else '[ORDER]'} Отправка подарка через DesslyHub: app_id={app_id}, friend_link={friend_link}, region={region}, lot_name={lot_name}")
        
        package_info = _get_package_id_by_app_id(api_key, app_id, region=region, game_name=game_name, lot_name=lot_name)
        game_price = package_info.get("price") if package_info else None
        
        balance_data = _get_desslyhub_balance(api_key)
        if balance_data:
            current_balance = balance_data.get("balance", 0.0)
            currency = balance_data.get("currency", "USD")
            logger.info(f"{LOGGER_PREFIX} [TEST] Текущий баланс: {current_balance:.4f} {currency}, цена игры: {game_price if game_price else 'N/A'} USD")
            if game_price:
                estimated_final = float(game_price) * 10
                logger.info(f"{LOGGER_PREFIX} [TEST] Примечание: финальная сумма может быть больше из-за комиссии (примерная оценка: до {estimated_final:.2f} USD)")
        else:
            logger.warning(f"{LOGGER_PREFIX} [TEST] Не удалось получить баланс перед отправкой")
        
        if test_data:
            test_data["status"] = "sending_gift"
        elif order_data:
            with _order_lock:
                if order_id in _active_orders:
                    _active_orders[order_id]["status"] = "sending_gift"
        
        result = _send_steam_gift(api_key, app_id, friend_link, region=region, game_name=game_name, lot_name=lot_name)
        
        if result and result.get("error_code") is None:
            transaction_id = result.get("transaction_id")
            status = result.get("status")
            logger.info(f"{LOGGER_PREFIX} [TEST] Подарок успешно отправлен: transaction_id={transaction_id}, status={status}")
            
            final_amount = None
            try:
                api = DesslyHubAPI(api_key)
                transaction_info = api.get_transaction(transaction_id)
                if transaction_info and isinstance(transaction_info, dict):
                    final_amount = transaction_info.get("final_amount")
                    if final_amount:
                        try:
                            final_amount = float(final_amount)
                            logger.info(f"{LOGGER_PREFIX} [TEST] Получен final_amount из транзакции: {final_amount:.4f} USD")
                            if game_price:
                                commission = final_amount - float(game_price)
                                logger.info(f"{LOGGER_PREFIX} [TEST] Цена игры: {game_price:.4f} USD, Комиссия: {commission:.4f} USD, Итого: {final_amount:.4f} USD")
                        except (ValueError, TypeError):
                            final_amount = None
            except Exception as e:
                logger.warning(f"{LOGGER_PREFIX} [TEST] Не удалось получить информацию о транзакции: {e}")
            
            region_name = "Казахстан" if region == "KZ" else region
            
            order_link_text = ""
            admin_call_message = ""
            if order_id:
                order_url = f"https://funpay.com/orders/{order_id}/"
                order_link_text = f"Проверь и подтверди заказ тут: {order_url}\n\n"
                admin_call_message = (
                    "\n\n💬 Если возникла проблема или ошибка, позовите администратора командой: !позвать\n"
                    "Он ответит вам в ближайшее время и решит вашу проблему.\n"
                    "Не делайте поспешных выводов, не стоит оставлять плохой отзыв - я обязательно вам помогу! 🙏"
                )
            
            storage = _get_storage()
            templates = storage.load_templates()
            success_template = templates.get("success_steam_template",
                "✅ Подарок успешно отправлен!\n\n🎮 Игра: {game_name}\n🌍 Регион: {region_name}\n🆔 Transaction ID: {transaction_id}\n📊 Статус: {status}\n\n{order_link}🙏 Спасибо за покупку! Приятной игры!{admin_call_message}")
            
            success_message = _format_template(success_template,
                game_name=game_name,
                region_name=region_name,
                transaction_id=transaction_id,
                status=status,
                order_link=order_link_text,
                admin_call_message=admin_call_message
            )
            
            cardinal.send_message(chat_id, success_message, chat_name)
            
            if test_data:
                test_data["status"] = "completed"
                test_data["transaction_id"] = transaction_id
                logger.info(f"{LOGGER_PREFIX} [TEST] Тестовая покупка успешно завершена: UUID={test_uuid}")
                if test_uuid in _test_purchases:
                    del _test_purchases[test_uuid]
            elif order_data:
                with _order_lock:
                    if order_id in _active_orders:
                        _active_orders[order_id]["status"] = "completed"
                        _active_orders[order_id]["transaction_id"] = transaction_id
                logger.info(f"{LOGGER_PREFIX} [ORDER] Заказ {order_id} успешно завершен")
            
            try:
                storage = _get_storage()
                orders = storage.load_orders()
                order_info = {
                    "order_id": order_id if order_data else f"TEST-{test_uuid[:8] if test_uuid else 'UNKNOWN'}",
                    "type": "steam_gift",
                    "game_name": game_name,
                    "price": float(final_amount) if final_amount else (float(game_price) if game_price else None),
                    "chat_id": chat_id,
                    "chat_name": chat_name,
                    "transaction_id": transaction_id,
                    "status": "success",
                    "timestamp": time.time(),
                    "uuid": test_uuid if test_uuid else None
                }
                orders.append(order_info)
                storage.save_orders(orders)
                logger.info(f"{LOGGER_PREFIX} {'[ORDER]' if order_data else '[TEST]'} Заказ сохранен в историю")
            except Exception as e:
                logger.error(f"{LOGGER_PREFIX} {'[ORDER]' if order_data else '[TEST]'} Ошибка сохранения заказа в историю: {e}")
            
            if order_data:
                with _order_lock:
                    if order_id in _active_orders:
                        del _active_orders[order_id]
                        logger.info(f"{LOGGER_PREFIX} [ORDER] Заказ {order_id} удален из _active_orders после успешной обработки")
            
            if test_uuid and test_uuid in _test_purchases:
                del _test_purchases[test_uuid]
                logger.info(f"{LOGGER_PREFIX} [TEST] UUID {test_uuid} удален после успешной обработки")
        else:
            error_code = result.get("error_code") if result else None
            game_price = result.get("price") if result else None
            
            error_descriptions = {
                -2: "❌ Ошибка при обработке заказа.",
                -4: "❌ Неправильный формат запроса. Проверьте настройки.",
                -51: "❌ Неверная ссылка для добавления в друзья. Проверьте формат ссылки.",
                -52: "❌ Неправильный app ID игры.",
                -53: "❌ Информация об игре не найдена.",
                -57: "❌ Неправильно указан регион клиента.",
                -58: "❌ Регион получателя недоступен для подарка.",
                -59: "❌ Пользователь не добавил/удалил бота из списка друзей."
            }
            
            if error_code in error_descriptions:
                error_message = error_descriptions[error_code]
            else:
                error_message = f"❌ Ошибка при отправке подарка (код: {error_code if error_code else 'неизвестно'}). Проверьте логи для деталей."
            
            if error_code == -2:
                balance_data = _get_desslyhub_balance(api_key)
                current_balance = balance_data.get("balance", 0.0) if balance_data else None
                currency = balance_data.get("currency", "USD") if balance_data else "USD"
                logger.error(f"{LOGGER_PREFIX} [TEST] Недостаточно средств на балансе. Текущий баланс: {current_balance:.4f} {currency if current_balance is not None else ''}, требуется: {game_price} USD" if game_price else f"{LOGGER_PREFIX} [TEST] Недостаточно средств на балансе. Текущий баланс: {current_balance:.4f} {currency if current_balance is not None else ''}")
                if hasattr(cardinal, 'telegram') and hasattr(cardinal.telegram, 'bot'):
                    try:
                        storage = _get_storage()
                        admin_id = storage.load_settings().get("admin_id")
                        if admin_id:
                            balance_msg = f"⚠️ Недостаточно средств на балансе"
                            if current_balance is not None:
                                balance_msg += f"\n💰 Текущий баланс: {current_balance:.4f} {currency}"
                            if game_price:
                                balance_msg += f"\n💰 Требуется: {game_price} USD"
                            balance_msg += f"\n\nПожалуйста, пополните баланс."
                            cardinal.telegram.bot.send_message(int(admin_id), balance_msg)
                    except Exception as e:
                        logger.error(f"{LOGGER_PREFIX} {'[ORDER]' if order_data else '[TEST]'} Не удалось отправить уведомление о балансе в Telegram: {e}")
            
            logger.error(f"{LOGGER_PREFIX} {'[ORDER]' if order_data else '[TEST]'} Не удалось отправить подарок: error_code={error_code}, price={game_price}")
            cardinal.send_message(chat_id, error_message, chat_name)
            
            if test_data:
                test_data["status"] = "failed"
                if test_uuid in _test_purchases:
                    del _test_purchases[test_uuid]
            elif order_data:
                with _order_lock:
                    if order_id in _active_orders:
                        _active_orders[order_id]["status"] = "failed"
                        logger.info(f"{LOGGER_PREFIX} [ORDER] Заказ {order_id} помечен как failed")
        
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [TEST] Критическая ошибка при обработке ссылки Steam: {e}", exc_info=True)
        if test_uuid and test_uuid in _test_purchases:
            del _test_purchases[test_uuid]


BIND_TO_PRE_INIT = [init_autosteam_cp]
def handle_mobile_player_id_message(cardinal: "Cardinal", event: NewMessageEvent) -> None:
    try:
        if not cardinal or not hasattr(cardinal, 'account'):
            return
        
        if event.message.author_id == 0:
            return
        
        if event.message.author_id == cardinal.account.id:
            return
        
        message_type = None
        if hasattr(event.message, "type"):
            message_type = event.message.type
        elif hasattr(event.message, "get_message_type"):
            message_type = event.message.get_message_type()
        
        if message_type and message_type != MessageTypes.NON_SYSTEM:
            return
        
        chat_id = str(event.message.chat_id)
        chat_name = event.message.chat_name
        message_text = str(event.message).strip()
        logger.debug(f"{LOGGER_PREFIX} [MOBILE] Обработка сообщения Mobile: chat_id={chat_id} (тип: {type(chat_id).__name__})")
        
        if (message_text.startswith("❌ Неверный формат ссылки!") or 
            message_text.startswith("❌ Это пример ссылки") or
            message_text.startswith("🎮 Спасибо за покупку!") or
            message_text.startswith("✅ Подарок успешно отправлен!") or
            message_text.startswith("✅ Пополнение успешно отправлено!")):
            return
        
        if ("xxxx-xxxx" in message_text or 
            "Правильный формат:" in message_text or 
            "Пример:" in message_text or
            "Отправьте ссылку на добавление в друзья:" in message_text or
            "Отправьте" in message_text and ":" in message_text and ("Player ID" in message_text or "ссылку" in message_text)):
            return
        
        if ("https://s.team/p/..." in message_text or 
            "https://s.team/p/xxxx" in message_text or
            "/p/..." in message_text or
            re.search(r'https?://s\.team/p/\.\.\.', message_text, re.IGNORECASE) or
            "Ожидание ссылки..." in message_text or
            "Ожидание данных..." in message_text):
            return
        
        if ("🎮 Спасибо за покупку!" in message_text):
            if ("Ожидание данных..." in message_text or 
                "📦 Игра:" in message_text or
                "💎 Позиция:" in message_text or
                "📝 Отправьте" in message_text):
                return
        
        if ("📦 Игра:" in message_text and "💎 Позиция:" in message_text):
            if ("Ожидание данных..." in message_text or
                "📝 Отправьте" in message_text):
                return
        
        if ("🎮 Спасибо за покупку!" in message_text and 
            "📦 Игра:" in message_text and 
            "💎 Позиция:" in message_text and
            ("Ожидание данных..." in message_text or "📝 Отправьте" in message_text)):
            return
        
        if message_text.startswith("!автовыда") or message_text.startswith("!автовыдача"):
            logger.info(f"{LOGGER_PREFIX} [MOBILE] Игнорируем команду '!автовыда' в обработчике Player ID")
            return
        
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Проверка сообщения на Player ID: chat_id={chat_id}, text='{message_text[:100]}'")
        
        test_data = None
        test_uuid = None
        order_data = None
        order_id = None
        
        for uuid_key, data in _test_purchases.items():
            if (data.get("chat_id") == chat_id and 
                data.get("status") == "waiting_player_id" and
                data.get("type") == "mobile" and
                time.time() - data.get("created_at", 0) < 600):
                test_data = data
                test_uuid = uuid_key
                break
        
        if not test_data:
            with _order_lock:
                for oid, data in _active_orders.items():
                    if (data.get("chat_id") == chat_id and 
                        data.get("status") == "waiting_player_id" and
                        data.get("type") == "mobile"):
                        order_data = data
                        order_id = oid
                        break
        
        if not test_data and not order_data:
            logger.info(f"{LOGGER_PREFIX} [MOBILE] Нет активной покупки Mobile в ожидании Player ID для чата {chat_id}")
            return
        
        if test_data and test_data.get("status") in ("completed", "failed"):
            logger.warning(f"{LOGGER_PREFIX} [MOBILE] Заказ уже завершен, статус: {test_data.get('status')}")
            return
        
        if order_data and order_data.get("status") in ("completed", "failed"):
            logger.warning(f"{LOGGER_PREFIX} [MOBILE] Заказ уже завершен, статус: {order_data.get('status')}")
            return
        
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Получено сообщение в чате {chat_id}, проверка на данные пользователя")
        
        user_data = message_text.strip()
        user_data = re.sub(r'[\u200B-\u200D\uFEFF\u2060]', '', user_data)
        user_data = user_data.strip()
        
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Очистка данных: исходная длина={len(message_text)}, очищенная длина={len(user_data)}")
        
        if not user_data:
            logger.warning(f"{LOGGER_PREFIX} [MOBILE] Пустые данные")
            error_msg = (
                f"❌ Данные не могут быть пустыми!\n\n"
                f"📝 Пожалуйста, отправьте корректные данные.\n\n"
                f"⏱ Ожидание данных..."
            )
            cardinal.send_message(chat_id, error_msg, chat_name)
            return
        
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Получены данные: {user_data}")
        
        settings = _get_storage().load_settings()
        api_key = settings.get("desslyhub_api_key", "")
        
        if not api_key:
            logger.error(f"{LOGGER_PREFIX} [MOBILE] API ключ не установлен")
            cardinal.send_message(chat_id, "❌ Ошибка: API ключ не установлен", chat_name)
            if test_uuid:
                del _test_purchases[test_uuid]
            elif order_id:
                with _order_lock:
                    if order_id in _active_orders:
                        del _active_orders[order_id]
            return
        
        if test_data:
            game_name = test_data.get("game_name", "PUBG Mobile (RU)")
            position_id = test_data.get("position_id")
            fields_to_request = test_data.get("fields_to_request", [])
            current_field_index = test_data.get("current_field_index", 0)
            fields_data = test_data.get("fields_data", {})
            server = test_data.get("server")
            game_id = test_data.get("game_id")
        elif order_data:
            game_name = order_data.get("game_name", "")
            position_id = order_data.get("position_id")
            fields_to_request = order_data.get("fields_to_request", [])
            current_field_index = order_data.get("current_field_index", 0)
            fields_data = order_data.get("fields_data", {})
            server = order_data.get("server")
            game_id = order_data.get("game_id")
        else:
            return
        
        if not fields_to_request:
            field_name = "Player ID"
            fields_to_request = [field_name]
            current_field_index = 0
            fields_data = {}
        
        if current_field_index >= len(fields_to_request):
            logger.warning(f"{LOGGER_PREFIX} [MOBILE] Все поля уже собраны, но получено еще одно сообщение")
            return
        
        current_field_name = fields_to_request[current_field_index]
        fields_data[current_field_name] = user_data
        
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Сохранено поле '{current_field_name}': {user_data}, индекс: {current_field_index + 1}/{len(fields_to_request)}")
        
        if test_data:
            test_data["fields_data"] = fields_data
            test_data["current_field_index"] = current_field_index + 1
        elif order_data:
            with _order_lock:
                if order_id in _active_orders:
                    _active_orders[order_id]["fields_data"] = fields_data
                    _active_orders[order_id]["current_field_index"] = current_field_index + 1
        
        if current_field_index + 1 < len(fields_to_request):
            next_field_name = fields_to_request[current_field_index + 1]
            message = f"📝 Отправьте {next_field_name}:\n\n⏱ Ожидание данных..."
            cardinal.send_message(chat_id, message, chat_name)
            logger.info(f"{LOGGER_PREFIX} [MOBILE] Запрошено следующее поле: {next_field_name}")
            return
        
        field_name = fields_to_request[0]
        
        if not position_id:
            logger.error(f"{LOGGER_PREFIX} [MOBILE] position_id не найден")
            cardinal.send_message(chat_id, "❌ Ошибка: Позиция не найдена", chat_name)
            if test_uuid:
                del _test_purchases[test_uuid]
            elif order_id:
                with _order_lock:
                    if order_id in _active_orders:
                        del _active_orders[order_id]
            return
        
        fields = {}
        
        if server:
            server_field_name = None
            if test_data:
                server_field_name = test_data.get("server_field_name")
            elif order_data:
                server_field_name = order_data.get("server_field_name")
            
            if server_field_name:
                fields[server_field_name] = server
                logger.info(f"{LOGGER_PREFIX} [MOBILE] Добавлен сервер '{server}' в поле '{server_field_name}'")
            else:
                game_info = _get_mobile_game_by_id(api_key, game_id) if game_id else None
                if game_info:
                    fields_info = game_info.get("fields", {})
                    servers_info = game_info.get("servers", {})
                    
                    if servers_info:
                        server_field_name = None
                        for field_key in fields_info.keys():
                            field_key_lower = field_key.lower()
                            if "server" in field_key_lower or "region" in field_key_lower or "сервер" in field_key_lower:
                                server_field_name = field_key
                                break
                        
                        if server_field_name:
                            fields[server_field_name] = server
                            logger.info(f"{LOGGER_PREFIX} [MOBILE] Добавлен сервер '{server}' в поле '{server_field_name}'")
                        else:
                            logger.warning(f"{LOGGER_PREFIX} [MOBILE] Поле для сервера не найдено в fields, доступные поля: {list(fields_info.keys())}")
                    else:
                        logger.info(f"{LOGGER_PREFIX} [MOBILE] Серверы не требуются для этой игры")
        
        test_uuid_ref = test_uuid
        
        logger.info(f"{LOGGER_PREFIX} [MOBILE] Отправка пополнения через DesslyHub: position_id={position_id}, fields={fields}, reference={test_uuid_ref}")
        
        if test_data:
            test_data["status"] = "sending_refill"
            reference = test_uuid_ref
        elif order_data:
            with _order_lock:
                if order_id in _active_orders:
                    _active_orders[order_id]["status"] = "sending_refill"
            reference = order_id
        else:
            return
        
        result = _send_mobile_refill(api_key, position_id, fields, reference=reference)
        
        if result and result.get("error_code") is None:
            transaction_id = result.get("transaction_id")
            status = result.get("status")
            logger.info(f"{LOGGER_PREFIX} [MOBILE] Пополнение успешно отправлено: transaction_id={transaction_id}, status={status}")
            
            final_amount = None
            position_price = (test_data.get("position_price", "0") if test_data 
                            else (order_data.get("position_price", "0") if order_data else "0"))
            try:
                position_price_float = float(position_price) if position_price and position_price != "N/A" else None
            except:
                position_price_float = None
            
            try:
                api = DesslyHubAPI(api_key)
                transaction_info = api.get_transaction(transaction_id)
                if transaction_info and isinstance(transaction_info, dict):
                    final_amount = transaction_info.get("final_amount")
                    if final_amount:
                        try:
                            final_amount = float(final_amount)
                            logger.info(f"{LOGGER_PREFIX} [MOBILE] Получен final_amount из транзакции: {final_amount:.4f} USD")
                            if position_price_float:
                                commission = final_amount - position_price_float
                                logger.info(f"{LOGGER_PREFIX} [MOBILE] Цена позиции: {position_price_float:.4f} USD, Комиссия: {commission:.4f} USD, Итого: {final_amount:.4f} USD")
                        except (ValueError, TypeError):
                            final_amount = None
            except Exception as e:
                logger.warning(f"{LOGGER_PREFIX} [MOBILE] Не удалось получить информацию о транзакции: {e}")
            
            fields_data_to_show = fields_data if fields_data else {field_name: user_data}
            
            field_labels = []
            for field_key, field_value in fields_data_to_show.items():
                field_labels.append(f"🆔 {field_key}: {field_value}")
            field_label_text = "\n".join(field_labels) + "\n" if field_labels else ""
            
            server_text = ""
            if server:
                server_text = f"🌍 Сервер: {server}\n"
            
            position_name = (test_data.get('position_name', 'N/A') if test_data 
                           else (order_data.get('position_name', 'N/A') if order_data else 'N/A'))
            
            order_link_text = ""
            admin_call_message = ""
            if order_id:
                order_url = f"https://funpay.com/orders/{order_id}/"
                order_link_text = f"Проверь и подтверди заказ тут: {order_url}\n\n"
                admin_call_message = (
                    "\n\n💬 Если возникла проблема или ошибка, позовите администратора командой: !позвать\n"
                    "Он ответит вам в ближайшее время и решит вашу проблему.\n"
                    "Не делайте поспешных выводов, не стоит оставлять плохой отзыв - я обязательно вам помогу! 🙏"
                )
            
            storage = _get_storage()
            templates = storage.load_templates()
            success_template = templates.get("success_mobile_template",
                "✅ Пополнение успешно отправлено!\n\n🎮 Игра: {game_name}\n💎 Позиция: {position_name}\n{field_labels}{server_text}🆔 Transaction ID: {transaction_id}\n📊 Статус: {status}\n\n{order_link}🙏 Спасибо за покупку! Приятной игры!{admin_call_message}")
            
            success_message = _format_template(success_template,
                game_name=game_name,
                position_name=position_name,
                field_labels=field_label_text,
                server_text=server_text,
                transaction_id=transaction_id,
                status=status,
                order_link=order_link_text,
                admin_call_message=admin_call_message
            )
            
            cardinal.send_message(chat_id, success_message, chat_name)
            
            if test_data:
                test_data["status"] = "completed"
                test_data["transaction_id"] = transaction_id
                logger.info(f"{LOGGER_PREFIX} [MOBILE] Тестовая покупка Mobile успешно завершена: UUID={test_uuid}")
            elif order_data:
                with _order_lock:
                    if order_id in _active_orders:
                        _active_orders[order_id]["status"] = "completed"
                        _active_orders[order_id]["transaction_id"] = transaction_id
                logger.info(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Заказ {order_id} успешно завершен")
            
            
            try:    
                storage = _get_storage()
                orders = storage.load_orders()
                
                player_id_value = list(fields_data.values())[0] if fields_data else user_data
                
                order_info = {
                    "order_id": order_id if order_id else f"MOBILE-{test_uuid[:8] if test_uuid else 'UNKNOWN'}",
                    "type": "mobile_refill",
                    "game_name": game_name,
                    "position_name": position_name,
                    "price": float(final_amount) if final_amount else (position_price_float if position_price_float else None),
                    "player_id": player_id_value,
                    "fields_data": fields_data,
                    "chat_id": chat_id,
                    "chat_name": chat_name,
                    "transaction_id": transaction_id,
                    "status": "success",
                    "timestamp": time.time(),
                    "uuid": test_uuid if test_uuid else None
                }
                orders.append(order_info)
                storage.save_orders(orders)
                logger.info(f"{LOGGER_PREFIX} [MOBILE] Заказ сохранен в историю")
            except Exception as e:
                logger.error(f"{LOGGER_PREFIX} [MOBILE] Ошибка сохранения заказа в историю: {e}")
            
            if test_uuid and test_uuid in _test_purchases:
                del _test_purchases[test_uuid]
                logger.info(f"{LOGGER_PREFIX} [MOBILE] UUID {test_uuid} удален после успешной обработки")
        else:
            error_code = result.get("error_code") if result else None
            
            error_descriptions = {
                -200: "❌ Мобильная игра не найдена.",
                -201: "❌ Позиция мобильной игры не найдена.",
                -2: "❌ Ошибка при обработке заказа."
            }
            
            if error_code in error_descriptions:
                error_message = error_descriptions[error_code]
            else:
                error_message = f"❌ Ошибка при отправке пополнения (код: {error_code if error_code else 'неизвестно'}). Проверьте логи для деталей."
            
            if error_code == -2:
                position_price = (test_data.get("position_price", "N/A") if test_data 
                                else (order_data.get("position_price", "N/A") if order_data else "N/A"))
                logger.error(f"{LOGGER_PREFIX} [MOBILE] Недостаточно средств на балансе. Требуется: {position_price} USD")
                if hasattr(cardinal, 'telegram') and hasattr(cardinal.telegram, 'bot'):
                    try:
                        storage = _get_storage()
                        admin_id = storage.load_settings().get("admin_id")
                        if admin_id:
                            balance_msg = f"⚠️ Недостаточно средств на балансе"
                            if position_price and position_price != "N/A":
                                balance_msg += f"\n💰 Требуется: {position_price} USD"
                            balance_msg += f"\n\nПожалуйста, пополните баланс."
                            cardinal.telegram.bot.send_message(int(admin_id), balance_msg)
                    except Exception as e:
                        logger.error(f"{LOGGER_PREFIX} [MOBILE] Не удалось отправить уведомление о балансе в Telegram: {e}")
            
            logger.error(f"{LOGGER_PREFIX} [MOBILE] Не удалось отправить пополнение: error_code={error_code}")
            if test_uuid and test_uuid in _test_purchases:
                test_data["status"] = "failed"
                del _test_purchases[test_uuid]
            elif order_id:
                with _order_lock:
                    if order_id in _active_orders:
                        _active_orders[order_id]["status"] = "failed"
        
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [MOBILE] Критическая ошибка при обработке Player ID: {e}", exc_info=True)


_active_orders = {}
_order_lock = threading.Lock()


def handle_new_order(cardinal: "Cardinal", event: NewOrderEvent) -> None:
    """Обработчик новых заказов"""
    global LICENSE_OK
    if not LICENSE_OK:
        logger.warning(f"{LOGGER_PREFIX} [ORDER] Попытка обработать заказ без валидной лицензии, order_id={event.order.id}")
        return
    try:
        order = event.order
        order_id = order.id
        
        logger.info(f"{LOGGER_PREFIX} [ORDER] Получен новый заказ: ID={order_id}, название={order.description}")
        
        buyer_username = getattr(order, 'buyer_username', '')
        if buyer_username and _check_blacklist_username(buyer_username):
            logger.info(f"{LOGGER_PREFIX} [ORDER] Покупатель '{buyer_username}' в черном списке, игнорируем заказ {order_id}")
            return
        
        storage = _get_storage()
        settings = storage.load_settings()
        
        if not settings.get("active", False):
            logger.info(f"{LOGGER_PREFIX} [ORDER] Плагин неактивен, пропускаем заказ {order_id}")
            return
        
        api_key = settings.get("desslyhub_api_key", "")
        if not api_key:
            logger.warning(f"{LOGGER_PREFIX} [ORDER] API ключ не установлен, пропускаем заказ {order_id}")
            return
        
        lots_config = storage.load_lots_config()
        if not lots_config:
            logger.info(f"{LOGGER_PREFIX} [ORDER] Конфиг лотов пуст, пропускаем заказ {order_id}")
            return
        
        lot_name = order.description or ""
        if not lot_name:
            logger.warning(f"{LOGGER_PREFIX} [ORDER] Не удалось определить название лота для заказа {order_id}")
            return
        
        lot_name_lower = lot_name.lower().strip()
        lot_config = None
        
        exact_match = None
        partial_matches = []
        
        for config in lots_config:
            config_lot_name = config.get("lot_name", "").strip()
            if not config_lot_name:
                continue
            
            config_lot_name_lower = config_lot_name.lower()
            
            if config_lot_name_lower == lot_name_lower:
                exact_match = config
                logger.info(f"{LOGGER_PREFIX} [ORDER] Найдено точное совпадение: '{config_lot_name}' == '{lot_name}'")
                break
            elif config_lot_name_lower in lot_name_lower:
                partial_matches.append((len(config_lot_name), config, config_lot_name))
                logger.debug(f"{LOGGER_PREFIX} [ORDER] Найдено частичное совпадение: '{config_lot_name}' содержится в '{lot_name}' (длина: {len(config_lot_name)})")
            elif lot_name_lower in config_lot_name_lower:
                partial_matches.append((len(config_lot_name), config, config_lot_name))
                logger.debug(f"{LOGGER_PREFIX} [ORDER] Найдено частичное совпадение: '{lot_name}' содержится в '{config_lot_name}' (длина: {len(config_lot_name)})")
        
        if exact_match:
            lot_config = exact_match
            logger.info(f"{LOGGER_PREFIX} [ORDER] Используется точное совпадение: '{lot_config.get('lot_name')}'")
        elif partial_matches:
            partial_matches.sort(key=lambda x: x[0], reverse=True)
            lot_config = partial_matches[0][1]
            matched_name = partial_matches[0][2]
            logger.info(f"{LOGGER_PREFIX} [ORDER] Используется самое длинное частичное совпадение: '{matched_name}' (длина: {partial_matches[0][0]}) из {len(partial_matches)} совпадений")
            if len(partial_matches) > 1:
                logger.info(f"{LOGGER_PREFIX} [ORDER] Другие совпадения: {[m[2] for m in partial_matches[1:]]}")
        
        if not lot_config:
            logger.info(f"{LOGGER_PREFIX} [ORDER] Конфиг для лота '{lot_name}' не найден, пропускаем заказ {order_id}")
            return
        
        with _order_lock:
            if order_id in _active_orders:
                logger.warning(f"{LOGGER_PREFIX} [ORDER] Заказ {order_id} уже обрабатывается")
                return
            _active_orders[order_id] = {
                "status": "processing",
                "started_at": time.time()
            }
        
        thread = threading.Thread(
            target=_process_order_thread,
            args=(cardinal, order, lot_config, api_key, storage),
            daemon=True,
            name=f"Order-{order_id}"
        )
        thread.start()
        logger.info(f"{LOGGER_PREFIX} [ORDER] Заказ {order_id} поставлен в очередь обработки")
        
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [ORDER] Критическая ошибка при обработке заказа: {e}", exc_info=True)


def _process_order_thread(cardinal: "Cardinal", order, lot_config: dict, api_key: str, storage: Storage) -> None:
    """Обработка заказа в отдельном потоке"""
    order_id = order.id
    try:
        logger.info(f"{LOGGER_PREFIX} [ORDER] Начало обработки заказа {order_id} в потоке")
        
        purchase_type = lot_config.get("type", "").lower()
        game_name = lot_config.get("game_name", "")
        
        chat_id = None
        chat_name = order.buyer_username
        
        for attempt in range(3):
            try:
                chat_obj = cardinal.account.get_chat_by_name(chat_name, True)
                numeric_chat_id = getattr(chat_obj, "id", None)
                if numeric_chat_id:
                    chat_id = str(numeric_chat_id)
                    break
            except Exception as e:
                if attempt < 2:
                    logger.warning(f"{LOGGER_PREFIX} [ORDER] Попытка {attempt+1}/3 получить chat_id для {chat_name} не удалась: {e}")
                    time.sleep(0.5 * (attempt + 1))
                else:
                    logger.error(f"{LOGGER_PREFIX} [ORDER] Не удалось получить chat_id для {chat_name} после 3 попыток: {e}")
        
        if not chat_id:
            logger.warning(f"{LOGGER_PREFIX} [ORDER] Используем fallback - попытка найти chat_id через историю чатов")
            try:
                chats_response = cardinal.account.get_chats()
                if hasattr(chats_response, 'chats'):
                    for chat in chats_response.chats:
                        if hasattr(chat, 'name') and chat.name == chat_name:
                            chat_id = str(chat.id)
                            logger.info(f"{LOGGER_PREFIX} [ORDER] Chat_id найден через историю: {chat_id}")
                            break
            except Exception as fallback_ex:
                logger.error(f"{LOGGER_PREFIX} [ORDER] Fallback метод поиска chat_id тоже не сработал: {fallback_ex}")
        
        if not chat_id:
            order_chat_id = str(order.chat_id)
            if order_chat_id.startswith("users-"):
                logger.warning(f"{LOGGER_PREFIX} [ORDER] Не удалось получить числовой chat_id, используем order.chat_id: {order_chat_id}")
                chat_id = order_chat_id
            else:
                chat_id = order_chat_id
        
        logger.info(f"{LOGGER_PREFIX} [ORDER] Заказ {order_id}: chat_id={chat_id}, chat_name={chat_name}")
        
        if purchase_type == "steam gift":
            region = lot_config.get("region", "KZ")
            _process_steam_gift_order(cardinal, order, lot_config, game_name, region, api_key, chat_id, chat_name, storage)
        elif purchase_type == "mobile refill":
            amount = lot_config.get("amount", "")
            _process_mobile_refill_order(cardinal, order, lot_config, game_name, amount, api_key, chat_id, chat_name, storage)
        else:
            logger.error(f"{LOGGER_PREFIX} [ORDER] Неизвестный тип покупки: {purchase_type} для заказа {order_id}")
            cardinal.send_message(chat_id, "❌ Ошибка: Неизвестный тип покупки", chat_name)
        
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [ORDER] Ошибка обработки заказа {order_id}: {e}", exc_info=True)
        with _order_lock:
            if order_id in _active_orders:
                _active_orders[order_id]["status"] = "failed"
        logger.info(f"{LOGGER_PREFIX} [ORDER] Завершена обработка заказа {order_id} с ошибкой")


def _process_steam_gift_order(cardinal: "Cardinal", order, lot_config: dict, game_name: str, region: str, 
                               api_key: str, chat_id: str, chat_name: str, storage: Storage) -> None:
    """Обработка заказа Steam Gift"""
    order_id = order.id
    try:
        logger.info(f"{LOGGER_PREFIX} [ORDER] [STEAM] Обработка заказа {order_id}: игра={game_name}, регион={region}")
        
        app_id = _get_game_app_id_by_name(game_name, api_key)
        if not app_id:
            logger.error(f"{LOGGER_PREFIX} [ORDER] [STEAM] Не удалось найти app_id для игры '{game_name}'")
            cardinal.send_message(chat_id, f"❌ Ошибка: Игра '{game_name}' не найдена", chat_name)
            return
        
        storage = _get_storage()
        templates = storage.load_templates()
        welcome_template = templates.get("welcome_steam_template",
            "🎮 Спасибо за покупку!\n\n📦 Игра: {game_name}\n🌍 Регион: {region}\n\n🔗 Отправьте ссылку на добавление в друзья:\nhttps://s.team/p/...\n\n⏱ Ожидание ссылки...")
        
        message = _format_template(welcome_template,
            game_name=game_name,
            region=region
        )
        
        result = cardinal.send_message(chat_id, message, chat_name)
        if result:
            logger.info(f"{LOGGER_PREFIX} [ORDER] [STEAM] Сообщение с запросом ссылки отправлено для заказа {order_id}")
            
            order_data = {
                "order_id": order_id,
                "chat_id": str(chat_id),
                "chat_name": chat_name,
                "type": "steam",
                "game_name": game_name,
                "app_id": app_id,
                "region": region,
                "status": "waiting_link",
                "created_at": time.time(),
                "lot_config": lot_config
            }
            
            with _order_lock:
                _active_orders[order_id] = order_data
                logger.info(f"{LOGGER_PREFIX} [ORDER] Заказ {order_id} сохранен в _active_orders: chat_id={order_data['chat_id']}, status={order_data['status']}, всего заказов: {len(_active_orders)}")
                logger.info(f"{LOGGER_PREFIX} [ORDER] Содержимое _active_orders: {list(_active_orders.keys())}")
        else:
            logger.error(f"{LOGGER_PREFIX} [ORDER] [STEAM] Не удалось отправить сообщение для заказа {order_id}")
            with _order_lock:
                if order_id in _active_orders:
                    _active_orders[order_id]["status"] = "failed"
                    logger.info(f"{LOGGER_PREFIX} [ORDER] Заказ {order_id} помечен как failed - не удалось отправить сообщение")
            
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [ORDER] [STEAM] Ошибка обработки заказа {order_id}: {e}", exc_info=True)
        with _order_lock:
            if order_id in _active_orders:
                _active_orders[order_id]["status"] = "failed"
                logger.info(f"{LOGGER_PREFIX} [ORDER] Заказ {order_id} помечен как failed из-за исключения")


def _process_mobile_refill_order(cardinal: "Cardinal", order, lot_config: dict, game_name: str, amount: str,
                                  api_key: str, chat_id: str, chat_name: str, storage: Storage) -> None:
    """Обработка заказа Mobile Refill"""
    order_id = order.id
    try:
        logger.info(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Обработка заказа {order_id}: игра={game_name}, количество={amount}")
        
        game_id = _get_mobile_game_id_by_name(game_name, api_key)
        if not game_id:
            logger.error(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Не удалось найти game_id для игры '{game_name}'")
            cardinal.send_message(chat_id, f"❌ Ошибка: Игра '{game_name}' не найдена", chat_name)
            return
        
        game_info = _get_mobile_game_by_id(api_key, game_id)
        if not game_info:
            logger.error(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Не удалось получить информацию об игре game_id={game_id}")
            cardinal.send_message(chat_id, f"❌ Ошибка: Не удалось получить информацию об игре '{game_name}'", chat_name)
            return
        
        positions = game_info.get("positions", [])
        if not positions:
            logger.error(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Нет доступных позиций для игры game_id={game_id}")
            cardinal.send_message(chat_id, f"❌ Ошибка: Нет доступных позиций для игры '{game_name}'", chat_name)
            return
        
        selected_position = None
        game_name_lower = game_name.lower().strip()
        
        if amount:
            amount_lower = amount.lower()
            amount_clean = amount_lower.strip()
            
            if "pubg" in game_name_lower and "mobile" in game_name_lower:
                logger.info(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Поиск позиции для PUBG Mobile: {amount}")
                
                for pos in positions:
                    pos_name = pos.get("name", "")
                    pos_name_lower = pos_name.lower()
                    
                    if amount_clean in pos_name_lower or amount in pos.get("name", ""):
                        if "60" in amount_clean and ("us" in pos_name_lower or "uc" in pos_name_lower or "unknown cash" in pos_name_lower):
                            numbers = re.findall(r'\d+', pos_name)
                            if numbers:
                                first_number = int(numbers[0])
                                if first_number == 60:
                                    selected_position = pos
                                    logger.info(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Найдена позиция: {pos.get('name')} (id={pos.get('id')})")
                                    break
                        else:
                            selected_position = pos
                            logger.info(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Найдена позиция: {pos.get('name')} (id={pos.get('id')})")
                            break
            else:
                for pos in positions:
                    pos_name = pos.get("name", "").lower()
                    if amount_clean in pos_name or amount in pos.get("name", ""):
                        selected_position = pos
                        logger.info(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Найдена позиция: {pos.get('name')} (id={pos.get('id')})")
                        break
        
        if not selected_position:
            selected_position = positions[0]
            logger.warning(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Позиция с количеством '{amount}' не найдена, используем первую доступную")
            logger.info(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Доступные позиции: {[p.get('name') for p in positions[:10]]}")
        
        position_id = selected_position.get("id")
        position_name = selected_position.get("name", "")
        position_price = selected_position.get("price", "0")
        
        fields_info = game_info.get("fields", {})
        servers_info = game_info.get("servers", {})
        
        fields_config = _get_mobile_game_fields_config(game_name, fields_info, servers_info)
        fields_to_request = fields_config.get("fields_to_request", [])
        auto_server = fields_config.get("auto_server")
        
        if not fields_to_request:
            field_name = list(fields_info.keys())[0] if fields_info else "Player ID"
            fields_to_request = [field_name]
        
        field_name = fields_to_request[0]
        
        templates = storage.load_templates()
        welcome_template = templates.get("welcome_mobile_template",
            "🎮 Спасибо за покупку!\n\n📦 Игра: {game_name}\n💎 Позиция: {position_name}\n\n📝 Отправьте {field_name}:\n\n⏱ Ожидание данных...")
        
        message = _format_template(welcome_template,
            game_name=game_name,
            position_name=position_name,
            field_name=field_name
        )
        
        result = cardinal.send_message(chat_id, message, chat_name)
        if result:
            logger.info(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Сообщение с запросом {field_name} отправлено для заказа {order_id}")
            
            order_data = {
                "order_id": order_id,
                "chat_id": chat_id,
                "chat_name": chat_name,
                "type": "mobile",
                "game_name": game_name,
                "game_id": game_id,
                "position_id": position_id,
                "position_name": position_name,
                "position_price": position_price,
                "fields_to_request": fields_to_request,
                "current_field_index": 0,
                "fields_data": {},
                "auto_server": auto_server,
                "status": "waiting_player_id",
                "created_at": time.time(),
                "lot_config": lot_config
            }
            
            if auto_server:
                order_data["server"] = auto_server
                logger.info(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Автоматически установлен сервер: {auto_server}")
            
            with _order_lock:
                _active_orders[order_id] = order_data
        else:
            logger.error(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Не удалось отправить сообщение для заказа {order_id}")
            
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [ORDER] [MOBILE] Ошибка обработки заказа {order_id}: {e}", exc_info=True)


def handle_admin_call_message(cardinal: "Cardinal", event: NewMessageEvent) -> None:
    try:
        if not cardinal or not hasattr(cardinal, 'account'):
            return
        
        if event.message.author_id == cardinal.account.id:
            return
        
        message_type = getattr(event.message, 'type', None)
        if message_type and message_type != MessageTypes.NON_SYSTEM:
            return
        
        message_text = str(event.message).strip()
        if not message_text:
            return
        
        message_lower = message_text.lower().strip()
        if message_lower != "!позвать" and message_lower != "!позвать ":
            return
        
        chat_id = event.message.chat_id
        chat_name = event.message.chat_name
        username = event.message.author
        
        if username and _check_blacklist_username(username):
            logger.warning(f"{LOGGER_PREFIX} [ADMIN_CALL] Пользователь '{username}' в черном списке, игнорируем команду !позвать")
            return
        
        storage = _get_storage()
        settings = storage.load_settings()
        admin_id = settings.get("admin_id", "")
        
        if not admin_id:
            logger.warning(f"{LOGGER_PREFIX} [ADMIN_CALL] Admin ID не установлен")
            return
        
        try:
            chat_url = f"https://funpay.com/chats/?node={chat_id}"
            admin_username = None
            if hasattr(cardinal, 'telegram') and hasattr(cardinal.telegram, 'bot'):
                try:
                    user_info = cardinal.telegram.bot.get_chat(int(admin_id))
                    if hasattr(user_info, 'username') and user_info.username:
                        admin_username = user_info.username
                except:
                    try:
                        user_info = cardinal.telegram.bot.get_chat_member(int(admin_id), int(admin_id))
                        if hasattr(user_info, 'user') and hasattr(user_info.user, 'username'):
                            admin_username = user_info.user.username
                    except:
                        pass
            
            admin_mention = f"@{admin_username}" if admin_username else f"ID: {admin_id}"
            
            message = (
                f"🔔 <b>Вас позвал покупатель</b>\n\n"
                f"👤 <b>Покупатель:</b> <a href=\"{chat_url}\">{username}</a>\n"
                f"💬 <b>Чат:</b> <a href=\"{chat_url}\">{chat_name or chat_id}</a>\n"
                f"📅 <b>Время:</b> <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>\n\n"
                f"{admin_mention}"
            )
            
            cardinal.telegram.bot.send_message(int(admin_id), message, parse_mode="HTML")
            logger.info(f"{LOGGER_PREFIX} [ADMIN_CALL] Отправлено уведомление админу о вызове от {username} (chat_id={chat_id})")
            
            cardinal.send_message(chat_id, "✅ Ваш запрос отправлен администратору. Он ответит вам в ближайшее время.", chat_name)
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} [ADMIN_CALL] Ошибка отправки уведомления админу: {e}")
    
    except Exception as e:
        logger.error(f"{LOGGER_PREFIX} [ADMIN_CALL] Критическая ошибка: {e}", exc_info=True)


BIND_TO_NEW_MESSAGE = [handle_test_purchase_message, handle_friend_link_message, handle_mobile_player_id_message, handle_admin_call_message]
BIND_TO_NEW_ORDER = [handle_new_order]
