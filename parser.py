import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, Callable, List
from datetime import datetime
import logging

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UniversalGiftParser:
    def __init__(self, source_name: str, base_url: str, start_num: int = 1):
        self.source_name = source_name
        self.base_url = base_url
        self.num = start_num
        self.last_quantity: int = 0
        self.current_info: Optional[Dict[str, Any]] = None
        self.is_running = False
        self.mode = "new"  # "new" или "range"
        self.max_concurrent = 10  # Максимальное количество одновременных запросов
        self.retry_attempts = 3  # Количество попыток повтора при ошибке
        self.retry_delay = 1  # Задержка между попытками в секундах
        
    async def fetch_html(self, session: aiohttp.ClientSession, num: int) -> Optional[Dict[str, Any]]:
        """Получение и парсинг HTML страницы с повторными попытками"""
        url = self.base_url.format(num)
        
        for attempt in range(self.retry_attempts):
            try:
                async with session.get(url, headers=HEADERS, timeout=15) as resp:
                    if resp.status == 404:
                        # Не существует - не пытаемся повторять
                        return None
                    if resp.status != 200:
                        logger.warning(f"HTTP {resp.status} for {url}, attempt {attempt + 1}")
                        if attempt < self.retry_attempts - 1:
                            await asyncio.sleep(self.retry_delay * (attempt + 1))
                        continue
                    
                    html = await resp.text()
                    info = self.parse_html(html)
                    if info is None:
                        return None
                    
                    info['url'] = url
                    info['num'] = num
                    info['source'] = self.source_name
                    return info
                    
            except asyncio.TimeoutError:
                logger.warning(f"Timeout for {url}, attempt {attempt + 1}")
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
            except Exception as e:
                logger.error(f"Fetch error for {url}: {e}, attempt {attempt + 1}")
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
        
        return None

    def parse_html(self, html: str) -> Optional[Dict[str, str]]:
        """Парсинг HTML и извлечение данных NFT"""
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", class_="tgme_gift_table")
        if not table:
            return None

        data: Dict[str, str] = {}
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                data[th.text.strip()] = td.text.strip()

        # Парсинг quantity
        qty_str = data.get("Quantity", "")
        quantity: Optional[int] = None
        if qty_str:
            part = qty_str.split("/")[0]
            num_str = "".join(ch for ch in part if ch.isdigit())
            if num_str:
                quantity = int(num_str)

        return {
            "owner": data.get("Owner"),
            "model": data.get("Model"),
            "backdrop": data.get("Backdrop"),
            "symbol": data.get("Symbol"),
            "quantity": quantity
        }

    async def fetch_batch_concurrent(self, session: aiohttp.ClientSession, numbers: List[int]) -> List[Dict[str, Any]]:
        """Пакетная обработка нескольких номеров одновременно"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def fetch_one(num):
            async with semaphore:
                return await self.fetch_html(session, num)
        
        tasks = [fetch_one(num) for num in numbers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем успешные результаты
        successful_results = []
        for result in results:
            if isinstance(result, dict) and result is not None:
                successful_results.append(result)
        
        return successful_results

    async def run_new_mode(self, update_callback: Callable):
        """Режим 1: Мониторинг новых подарков с улучшенной производительностью"""
        self.mode = "new"
        self.is_running = True
        
        async with aiohttp.ClientSession() as session:
            # Инициализация: найти первую валидную
            logger.info(f"[{self.source_name}] Поиск начального подарка...")
            found_initial = False
            
            while self.is_running and not found_initial:
                info = await self.fetch_html(session, self.num)
                if info and info["quantity"] is not None and info["quantity"] > 0:
                    self.last_quantity = info["quantity"]
                    self.current_info = info
                    await update_callback(info)
                    logger.info(f"[{self.source_name}] Найден начальный: #{info['num']}, quantity={info['quantity']}")
                    found_initial = True
                    break
                
                # Увеличиваем номер для поиска следующего
                self.num += 1
                await asyncio.sleep(0.02)

            if not found_initial:
                logger.warning(f"[{self.source_name}] Не удалось найти начальный подарок")
                return

            # Основной цикл мониторинга
            consecutive_empty = 0
            max_consecutive_empty = 50  # Максимальное количество пустых подряд
            
            while self.is_running:
                # Проверяем следующий номер
                next_num = self.last_quantity + 1
                info = await self.fetch_html(session, next_num)
                
                if info and info["quantity"] is not None:
                    qty = info["quantity"]
                    if qty > self.last_quantity:
                        # Найден новый подарок!
                        self.last_quantity = qty
                        self.current_info = info
                        await update_callback(info)
                        logger.info(f"[{self.source_name}] Новый подарок! #{info['num']}, quantity={qty}")
                        consecutive_empty = 0  # Сброс счетчика пустых
                    else:
                        consecutive_empty += 1
                else:
                    consecutive_empty += 1
                
                # Если слишком много пустых подряд, делаем большую паузу
                if consecutive_empty >= max_consecutive_empty:
                    logger.info(f"[{self.source_name}] Много пустых подряд, делаем паузу...")
                    await asyncio.sleep(5)
                    consecutive_empty = 0
                else:
                    await asyncio.sleep(0.02)

    async def run_range_mode(self, start: int, end: int, update_callback: Callable, progress_callback: Optional[Callable] = None):
        """Режим 2: Парсинг диапазона с конкурентной обработкой"""
        self.mode = "range"
        self.is_running = True
        
        total = end - start + 1
        parsed = 0
        batch_size = self.max_concurrent * 2  # Размер пакета для обработки
        
        async with aiohttp.ClientSession() as session:
            logger.info(f"[{self.source_name}] Парсинг диапазона {start}-{end} ({total} подарков) с конкурентностью {self.max_concurrent}")
            
            # Обрабатываем диапазон пакетами
            for batch_start in range(start, end + 1, batch_size):
                if not self.is_running:
                    break
                
                batch_end = min(batch_start + batch_size - 1, end)
                numbers = list(range(batch_start, batch_end + 1))
                
                # Конкурентная обработка пакета
                results = await self.fetch_batch_concurrent(session, numbers)
                
                # Обработка результатов
                for info in results:
                    await update_callback(info)
                    parsed += 1
                
                # Прогресс
                if progress_callback:
                    await progress_callback(parsed, total)
                
                # Небольшая пауза между пакетами
                await asyncio.sleep(0.1)
            
            if progress_callback:
                await progress_callback(parsed, total)
            
            logger.info(f"[{self.source_name}] Парсинг завершён: {parsed}/{total}")
    
    def stop(self):
        """Остановка парсера"""
        self.is_running = False
        print(f"[{self.source_name}] Парсер остановлен")


class ParserManager:
    """Менеджер для управления несколькими парсерами одновременно"""
    def __init__(self):
        self.parsers: Dict[str, UniversalGiftParser] = {}
        self.tasks: Dict[str, asyncio.Task] = {}
    
    def add_parser(self, source_name: str, base_url: str, start_num: int = 1):
        """Добавление нового парсера"""
        if source_name not in self.parsers:
            self.parsers[source_name] = UniversalGiftParser(source_name, base_url, start_num)
            print(f"[Manager] Парсер {source_name} добавлен")
    
    async def start_parser(self, source_name: str, mode: str, update_callback: Callable, **kwargs):
        """Запуск парсера в нужном режиме"""
        if source_name not in self.parsers:
            raise ValueError(f"Parser {source_name} not found")
        
        parser = self.parsers[source_name]
        
        # Остановить, если уже запущен
        if source_name in self.tasks:
            await self.stop_parser(source_name)
        
        # Запуск в зависимости от режима
        if mode == "new":
            task = asyncio.create_task(parser.run_new_mode(update_callback))
        elif mode == "range":
            start = kwargs.get('start', 1)
            end = kwargs.get('end', 100)
            progress_callback = kwargs.get('progress_callback')
            task = asyncio.create_task(
                parser.run_range_mode(start, end, update_callback, progress_callback)
            )
        else:
            raise ValueError(f"Unknown mode: {mode}")
        
        self.tasks[source_name] = task
        print(f"[Manager] Парсер {source_name} запущен в режиме {mode}")
    
    async def stop_parser(self, source_name: str):
        """Остановка парсера"""
        if source_name in self.parsers:
            self.parsers[source_name].stop()
        
        if source_name in self.tasks:
            task = self.tasks[source_name]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            del self.tasks[source_name]
            print(f"[Manager] Парсер {source_name} остановлен")
    
    async def stop_all(self):
        """Остановка всех парсеров"""
        for source_name in list(self.tasks.keys()):
            await self.stop_parser(source_name)
    
    def get_parser_status(self, source_name: str) -> Dict[str, Any]:
        """Получение статуса парсера"""
        if source_name not in self.parsers:
            return {"status": "not_found"}
        
        parser = self.parsers[source_name]
        is_running = source_name in self.tasks and not self.tasks[source_name].done()
        
        return {
            "status": "running" if is_running else "stopped",
            "mode": parser.mode,
            "current_num": parser.num,
            "last_quantity": parser.last_quantity,
            "current_info": parser.current_info
        }