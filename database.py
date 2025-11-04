import sqlite3
import asyncio
from datetime import datetime
from typing import List, Dict, Optional, Any
from contextlib import asynccontextmanager
import aiosqlite

class Database:
    def __init__(self, db_path: str = "data.db"):
        self.db_path = db_path
        self.unique_values_cache = {field: set() for field in ["symbol", "model", "backdrop", "owner"]}
        
    async def init_db(self):
        """Инициализация базовых таблиц"""
        async with aiosqlite.connect(self.db_path) as db:
            # Таблица источников NFT
            await db.execute("""
                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    base_url TEXT NOT NULL,
                    start_num INTEGER DEFAULT 1,
                    current_num INTEGER DEFAULT 1,
                    last_quantity INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 1,
                    mode TEXT DEFAULT 'new',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Таблица администраторов
            await db.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            await db.commit()
    
    async def create_nft_table(self, source_name: str):
        """Создание таблицы для конкретного типа NFT"""
        table_name = f"nft_{source_name.lower()}"
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    num INTEGER UNIQUE NOT NULL,
                    owner TEXT,
                    model TEXT,
                    backdrop TEXT,
                    symbol TEXT,
                    quantity INTEGER,
                    url TEXT,
                    time_parsed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Индексы для быстрого поиска
            await db.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_num ON {table_name}(num)")
            await db.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_model ON {table_name}(model)")
            await db.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_owner ON {table_name}(owner)")
            await db.commit()
    
    async def add_source(self, name: str, base_url: str, start_num: int = 1) -> bool:
        """Добавление нового источника NFT"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "INSERT INTO sources (name, base_url, start_num, current_num) VALUES (?, ?, ?, ?)",
                    (name, base_url, start_num, start_num)
                )
                await db.commit()
            await self.create_nft_table(name)
            return True
        except sqlite3.IntegrityError:
            return False
    
    async def get_sources(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Получение списка источников"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            query = "SELECT * FROM sources"
            if active_only:
                query += " WHERE is_active = 1"
            async with db.execute(query) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    async def get_source(self, name: str) -> Optional[Dict[str, Any]]:
        """Получение конкретного источника"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM sources WHERE name = ?", (name,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None
    
    async def update_source_state(self, name: str, current_num: int, last_quantity: int):
        """Обновление состояния парсинга источника"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE sources SET current_num = ?, last_quantity = ? WHERE name = ?",
                (current_num, last_quantity, name)
            )
            await db.commit()
    
    async def get_global_unique_values(self, field: str) -> List[str]:
        """Получение уникальных значений по всем источникам с кэшированием"""
        if field not in self.unique_values_cache:
            self.unique_values_cache[field] = set()
        
        # Если кэш пустой, загрузить из БД
        if not self.unique_values_cache[field]:
            sources = await self.get_sources(active_only=False)
            async with aiosqlite.connect(self.db_path) as db:
                for source in sources:
                    table_name = f"nft_{source['name'].lower()}"
                    try:
                        async with db.execute(f"SELECT DISTINCT {field} FROM {table_name} WHERE {field} IS NOT NULL AND {field} != ''") as cursor:
                            rows = await cursor.fetchall()
                            self.unique_values_cache[field].update(row[0] for row in rows)
                    except sqlite3.OperationalError:
                        continue
        
        return sorted(list(self.unique_values_cache[field]))
    
    async def get_search_suggestions(self, source_name: str, field: str, query: str) -> List[str]:
        """Получение предложений для автозаполнения"""
        table_name = f"nft_{source_name.lower()}"
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(f"SELECT DISTINCT {field} FROM {table_name} WHERE {field} LIKE ? AND {field} IS NOT NULL AND {field} != '' ORDER BY {field} LIMIT 10", (f"%{query}%",)) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    
    async def toggle_source(self, name: str, is_active: bool) -> bool:
        """Включение/выключение источника"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE sources SET is_active = ? WHERE name = ?",
                (1 if is_active else 0, name)
            )
            await db.commit()
            return True
    
    async def save_nft(self, source_name: str, nft_data: Dict[str, Any]) -> bool:
        """Сохранение данных NFT с обновлением кэша"""
        table_name = f"nft_{source_name.lower()}"
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Проверяем, существует ли запись с таким же номером
                async with db.execute(f"SELECT * FROM {table_name} WHERE num = ?", (nft_data['num'],)) as cursor:
                    existing = await cursor.fetchone()
                
                if existing:
                    # Если запись существует, удаляем старую
                    await db.execute(f"DELETE FROM {table_name} WHERE num = ?", (nft_data['num'],))
                
                # Вставляем новую запись
                await db.execute(f"""
                    INSERT INTO {table_name} 
                    (num, owner, model, backdrop, symbol, quantity, url)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    nft_data['num'],
                    nft_data.get('owner'),
                    nft_data.get('model'),
                    nft_data.get('backdrop'),
                    nft_data.get('symbol'),
                    nft_data.get('quantity'),
                    nft_data.get('url')
                ))
                await db.commit()
            
            # Обновление кэша для новых значений
            for field in ["owner", "model", "backdrop", "symbol"]:
                value = nft_data.get(field)
                if value and value not in self.unique_values_cache.get(field, set()):
                    self.unique_values_cache.setdefault(field, set()).add(value)
            
            return True
        except Exception as e:
            print(f"Error saving NFT: {e}")
            return False
    
    async def add_or_update_nft(self, source_name: str, nft_data: Dict[str, Any]) -> bool:
        """Алиас для save_nft для обратной совместимости"""
        return await self.save_nft(source_name, nft_data)
    
    async def get_latest_nfts(self, source_name: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Получение последних NFT"""
        table_name = f"nft_{source_name.lower()}"
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                f"SELECT * FROM {table_name} ORDER BY num DESC LIMIT ?",
                (limit,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    async def search_nfts(self, source_name: str, query: str, field: str = "all", exact: bool = False) -> List[Dict[str, Any]]:
        """Поиск NFT по различным полям с улучшенной точностью"""
        table_name = f"nft_{source_name.lower()}"
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            if field == "all":
                # Для поиска по всем полям используем точное совпадение для номера
                if exact:
                    sql = f"SELECT * FROM {table_name} WHERE model = ? OR owner = ? OR symbol = ? OR backdrop = ? OR num = ? ORDER BY num DESC"
                    params = (query, query, query, query, query)
                else:
                    sql = f"SELECT * FROM {table_name} WHERE model LIKE ? OR owner LIKE ? OR symbol LIKE ? OR backdrop LIKE ? OR num = ? ORDER BY num DESC"
                    params = (f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", query)
            elif field == "num":
                # Точный поиск по номеру
                sql = f"SELECT * FROM {table_name} WHERE num = ? ORDER BY num DESC"
                params = (int(query) if query.isdigit() else -1,)
            elif field == "backdrop":
                # Поиск по фону
                sql = f"SELECT * FROM {table_name} WHERE backdrop LIKE ? ORDER BY num DESC"
                params = (f"%{query}%",)
            else:
                # Поиск по другим полям
                if exact:
                    sql = f"SELECT * FROM {table_name} WHERE {field} = ? ORDER BY num DESC"
                    params = (query,)
                else:
                    sql = f"SELECT * FROM {table_name} WHERE {field} LIKE ? ORDER BY num DESC"
                    params = (f"%{query}%",)
            
            async with db.execute(sql, params) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    async def get_stats(self, source_name: str) -> Dict[str, Any]:
        """Получение статистики по источнику"""
        table_name = f"nft_{source_name.lower()}"
        async with aiosqlite.connect(self.db_path) as db:
            # Общее количество
            async with db.execute(f"SELECT COUNT(*) FROM {table_name}") as cursor:
                total = (await cursor.fetchone())[0]
            
            # Последний добавленный
            async with db.execute(f"SELECT MAX(num) FROM {table_name}") as cursor:
                last_num = (await cursor.fetchone())[0]
            
            # Уникальные модели
            async with db.execute(f"SELECT COUNT(DISTINCT model) FROM {table_name}") as cursor:
                unique_models = (await cursor.fetchone())[0]
            
            return {
                "total": total,
                "last_num": last_num,
                "unique_models": unique_models
            }
    
    async def is_admin(self, user_id: int) -> bool:
        """Проверка, является ли пользователь администратором"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,)) as cursor:
                return await cursor.fetchone() is not None
    
    async def add_admin(self, user_id: int, username: str = None) -> bool:
        """Добавление администратора"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "INSERT INTO admins (user_id, username) VALUES (?, ?)",
                    (user_id, username)
                )
                await db.commit()
                return True
        except sqlite3.IntegrityError:
            return False
    
    async def get_unique_values(self, source_name: str, field: str) -> List[str]:
        """Получение уникальных значений для автозаполнения"""
        table_name = f"nft_{source_name.lower()}"
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(f"SELECT DISTINCT {field} FROM {table_name} WHERE {field} IS NOT NULL AND {field} != '' ORDER BY {field}") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]