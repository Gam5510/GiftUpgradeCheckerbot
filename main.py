import asyncio
import os
import signal
from database import Database
from parser import ParserManager
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from rapidfuzz import fuzz
# Создаем основное FastAPI приложение
app = FastAPI(title="NFT Gift Monitor")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Инициализация компонентов
db = Database()
parser_manager = ParserManager()

# Глобальные переменные для бота
bot_task = None

# === API эндпоинты из webapp.py ===

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Главная страница - просмотр последних NFT"""
    sources = await db.get_sources()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "sources": sources
    })

@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request):
    """Страница поиска"""
    sources = await db.get_sources()
    return templates.TemplateResponse("search.html", {
        "request": request,
        "sources": sources
    })

@app.get("/api/latest/{source_name}")
async def get_latest(source_name: str, limit: int = 20):
    """Получение последних NFT по источнику"""
    try:
        nfts = await db.get_latest_nfts(source_name, limit)
        return {"success": True, "data": nfts}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/search/{source_name}")
async def search_nfts(
    source_name: str, 
    query: str = Query(..., min_length=1),
    field: str = "all",
    exact: bool = Query(False, description="Точное совпадение")
):
    """Поиск NFT"""
    try:
        nfts = await db.search_nfts(source_name, query, field, exact)
        return {"success": True, "data": nfts}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/stats/{source_name}")
async def get_stats(source_name: str):
    """Статистика по источнику"""
    try:
        stats = await db.get_stats(source_name)
        parser_status = parser_manager.get_parser_status(source_name)
        # Проверяем, что парсер действительно запущен
        is_running = bool(parser_status and parser_status.get('status') == 'running')
        return {
            "success": True, 
            "data": {**stats, "parser_status": {"status": "running" if is_running else "stopped"}}
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/sources")
async def get_sources():
    """Список всех источников"""
    try:
        sources = await db.get_sources()
        return {"success": True, "data": sources}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/get_autocomplete_data")
async def get_autocomplete_data(query: str = Query(None), field: str = Query("all")):
    if not query:
        return {"suggestions": []}
    
    if field == "all": 
        unique_values = ( 
            await db.get_global_unique_values("symbol") + 
            await db.get_global_unique_values("model") + 
            await db.get_global_unique_values("backdrop") + 
            await db.get_global_unique_values("owner") 
        ) 
        field_type_map = { 
            "symbol": await db.get_global_unique_values("symbol"), 
            "model": await db.get_global_unique_values("model"), 
            "backdrop": await db.get_global_unique_values("backdrop"), 
            "owner": await db.get_global_unique_values("owner") 
        } 
    else: 
        unique_values = await db.get_global_unique_values(field) 
        field_type_map = {field: unique_values} 
    
    # Fuzzy matching 
    suggestions = [] 
    for item in unique_values: 
        ratio = fuzz.partial_ratio(query.lower(), item.lower()) 
        if ratio > 70:  # Порог совпадения 
            item_type = next((ftype for ftype, vals in field_type_map.items() if item in vals), "unknown") 
            suggestions.append({"value": item, "type": item_type}) 
    
    # Сортируем по релевантности 
    suggestions.sort(key=lambda x: fuzz.partial_ratio(query.lower(), x['value'].lower()), reverse=True) 
    
    return {"suggestions": suggestions[:10]}  # Лимит 10

async def init_database():
    """Инициализация базы данных"""
    db = Database()
    await db.init_db()
    print("✅ База данных инициализирована")



@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    global bot_task
    
    await db.init_db()
    print("✅ База данных инициализирована")
    
    # Запуск Telegram бота в фоновом режиме
    try:
        bot_task = asyncio.create_task(run_bot())
        print("✅ Telegram бот запущен в фоновом режиме")
    except Exception as e:
        print(f"❌ Ошибка запуска бота: {e}")
    
    # Загрузка источников и автоматический запуск парсеров
    sources = await db.get_sources()
    for source in sources:
        parser_manager.add_parser(
            source['name'], 
            source['base_url'], 
            source['current_num']
        )
        # Автоматический запуск парсера в режиме мониторинга новых подарков
        try:
            await parser_manager.start_parser(
                source['name'], 
                "new", 
                lambda info, source_name=source['name']: save_nft_info(info, source_name)
            )
            print(f"✅ Парсер {source['name']} запущен в режиме мониторинга")
        except Exception as e:
            print(f"❌ Ошибка запуска парсера {source['name']}: {e}")
    
    print(f"✅ Загружено и запущено {len(sources)} источников")

async def save_nft_info(info: dict, source_name: str):
    """Функция обратного вызова для сохранения NFT информации"""
    try:
        await db.save_nft(source_name, info)
        print(f"🎁 Сохранен подарок #{info['num']} из {source_name}")
    except Exception as e:
        print(f"❌ Ошибка сохранения подарка #{info['num']} из {source_name}: {e}")

async def run_bot():
    """Запуск Telegram бота в фоновом режиме"""
    try:
        from bot import dp, bot, db as bot_db, parser_manager as bot_parser_manager, ADMIN_IDS
        import asyncio
        
        print("🤖 Инициализация Telegram бота...")
        
        # Инициализация БД для бота
        await bot_db.init_db()
        print("✅ База данных для бота инициализирована")
        
        # Добавление админов из переменных окружения
        for admin_id in ADMIN_IDS:
            await bot_db.add_admin(admin_id)
        
        # Загрузка источников и парсеров
        sources = await bot_db.get_sources()
        for source in sources:
            bot_parser_manager.add_parser(source['name'], source['base_url'], source['current_num'])
        
        print(f"✅ Загружено {len(sources)} источников для бота")
        print("🤖 Telegram бот запущен и готов к работе!")
        
        # Запускаем polling в фоновом режиме
        await dp.start_polling(bot)
        
    except Exception as e:
        print(f"❌ Ошибка запуска бота: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Остановка всех компонентов при выключении"""
    global bot_task
    
    print("🛑 Остановка всех компонентов...")
    
    # Останавливаем парсеры
    await parser_manager.stop_all()
    print("✅ Все парсеры остановлены")
    
    # Останавливаем бота
    if bot_task and not bot_task.done():
        bot_task.cancel()
        try:
            await bot_task
        except asyncio.CancelledError:
            pass
        print("✅ Telegram бот остановлен")

