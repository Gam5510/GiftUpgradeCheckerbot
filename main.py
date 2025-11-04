import asyncio
import os
import signal
from multiprocessing import Process
from database import Database
from parser import ParserManager

# Глобальные переменные для корректной остановки
bot_process = None
webapp_process = None

def run_bot():
    """Запуск Telegram бота в отдельном процессе"""
    import asyncio
    from bot import main as bot_main
    asyncio.run(bot_main())

def run_webapp():
    """Запуск веб-приложения в отдельном процессе"""
    import uvicorn
    from webapp import app
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

async def init_database():
    """Инициализация базы данных"""
    db = Database()
    await db.init_db()
    print("✅ База данных инициализирована")

def signal_handler(signum, frame):
    """Обработчик сигналов для корректной остановки"""
    print("\n🛑 Получен сигнал остановки, завершаем процессы...")
    
    if bot_process and bot_process.is_alive():
        bot_process.terminate()
        bot_process.join(timeout=5)
        print("✅ Бот остановлен")
    
    if webapp_process and webapp_process.is_alive():
        webapp_process.terminate()
        webapp_process.join(timeout=5)
        print("✅ Веб-приложение остановлено")
    
    exit(0)

async def main():
    """Главная функция запуска всего приложения"""
    global bot_process, webapp_process
    
    print("="*50)
    print("🎁 NFT Gift Monitor - Запуск системы")
    print("="*50)

    
    # Инициализация БД
    await init_database()
    
    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Запуск бота в отдельном процессе
        print("\n🤖 Запуск Telegram бота...")
        bot_process = Process(target=run_bot)
        bot_process.start()
        await asyncio.sleep(2)
        print("✅ Бот запущен")
        
        # Запуск веб-приложения в отдельном процессе
        print("\n🌐 Запуск веб-приложения...")
        webapp_process = Process(target=run_webapp)
        webapp_process.start()
        await asyncio.sleep(2)
        
        port = int(os.getenv("PORT", 8000))
        web_url = os.getenv("WEB_APP_URL", f"http://localhost:{port}")
        
        print(f"✅ Веб-приложение запущено: {web_url}")
        print("\n" + "="*50)
        print("✅ Система полностью запущена!")
        print("="*50)
        print(f"\n📱 Telegram бот: работает")
        print(f"🌐 Веб-интерфейс: {web_url}")
        print(f"\n💡 Для остановки нажмите Ctrl+C")
        print("="*50 + "\n")
        
        # Ожидание завершения процессов
        while bot_process.is_alive() or webapp_process.is_alive():
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Получен сигнал остановки...")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
    finally:
        # Остановка процессов
        if bot_process and bot_process.is_alive():
            print("🛑 Остановка бота...")
            bot_process.terminate()
            bot_process.join(timeout=5)
        
        if webapp_process and webapp_process.is_alive():
            print("🛑 Остановка веб-приложения...")
            webapp_process.terminate()
            webapp_process.join(timeout=5)
        
        print("✅ Все процессы остановлены")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 До свидания!")