import asyncio
import os
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from database import Database
from parser import ParserManager

# Конфигурация
BOT_TOKEN = os.getenv("BOT_TOKEN", "8458352134:AAE8Z9VrDK9xzUcBFrPzgfrUMYI0V-pH4Dg")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "5699915010").split(",") if x]
WEB_APP_URL = os.getenv("WEB_APP_URL", "https://t.me/GiftUpgradeCheckerbot/app")

# FSM состояния
class AddSourceStates(StatesGroup):
    waiting_name = State()
    waiting_url = State()
    waiting_start_num = State()

class ParseRangeStates(StatesGroup):
    waiting_source = State()
    waiting_start = State()
    waiting_end = State()

class StartMonitoringStates(StatesGroup):
    waiting_source = State()

# Инициализация
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
db = Database()
parser_manager = ParserManager()

# === Клавиатуры ===

def get_main_keyboard() -> InlineKeyboardMarkup:
    """Главная клавиатура для обычных пользователей"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Открыть Web App", url=WEB_APP_URL)],
        [InlineKeyboardButton(text="💬 Поддержка", url="https://t.me/your_support_username")]
    ])

def get_admin_main_keyboard() -> InlineKeyboardMarkup:
    """Главная клавиатура с админ-панелью"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Открыть Web App", url=WEB_APP_URL)],
        [InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin:menu")],
        [InlineKeyboardButton(text="💬 Техподдержка", url="https://t.me/gam5510")]
    ])

def get_admin_menu_keyboard() -> InlineKeyboardMarkup:
    """Меню админ-панели"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить NFT источник", callback_data="admin:add_source")],
        [InlineKeyboardButton(text="🚀 Запустить парсинг диапазона", callback_data="admin:parse_range")],
        [InlineKeyboardButton(text="🔄 Запустить мониторинг", callback_data="admin:start_monitoring")],
        [InlineKeyboardButton(text="🔄 Мониторинг всех моделей", callback_data="admin:start_all_monitoring")],
        [InlineKeyboardButton(text="⏹ Остановить парсер", callback_data="admin:stop_parser")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton(text="📋 Список источников", callback_data="admin:list_sources")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin:close")]
    ])

async def get_sources_keyboard(action_prefix: str) -> InlineKeyboardMarkup:
    """Клавиатура со списком источников"""
    sources = await db.get_sources()
    buttons = []
    
    for source in sources:
        status = "✅" if source['is_active'] else "❌"
        buttons.append([
            InlineKeyboardButton(
                text=f"{status} {source['name']}", 
                callback_data=f"{action_prefix}:{source['name']}"
            )
        ])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# === Обработчики команд ===

@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Команда /start"""
    user_id = message.from_user.id
    is_admin = user_id in ADMIN_IDS or await db.is_admin(user_id)
    
    welcome_text = (
        f"👋 Привет, <b>{message.from_user.first_name}</b>!\n\n"
        "🎁 Добро пожаловать в NFT Gift Monitor\n\n"
        "Здесь вы можете отслеживать новые NFT подарки в реальном времени.\n\n"
        "Нажмите кнопку ниже, чтобы открыть веб-интерфейс:"
    )
    
    keyboard = get_admin_main_keyboard() if is_admin else get_main_keyboard()
    await message.answer(welcome_text, reply_markup=keyboard, parse_mode="HTML")

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    """Команда /admin - быстрый доступ к админ-панели"""
    user_id = message.from_user.id
    is_admin = user_id in ADMIN_IDS or await db.is_admin(user_id)
    
    if not is_admin:
        await message.answer("❌ У вас нет доступа к админ-панели")
        return
    
    await message.answer(
        "⚙️ <b>Админ-панель</b>\n\nВыберите действие:",
        reply_markup=get_admin_menu_keyboard(),
        parse_mode="HTML"
    )

# === Обработчики callback ===

@dp.callback_query(F.data == "admin:menu")
async def admin_menu(callback: CallbackQuery):
    """Открытие админ-меню"""
    user_id = callback.from_user.id
    is_admin = user_id in ADMIN_IDS or await db.is_admin(user_id)
    
    if not is_admin:
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⚙️ <b>Админ-панель</b>\n\nВыберите действие:",
        reply_markup=get_admin_menu_keyboard(),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "admin:close")
async def admin_close(callback: CallbackQuery):
    """Закрытие админ-меню"""
    await callback.message.delete()
    await callback.answer()

# === Добавление источника ===

@dp.callback_query(F.data == "admin:add_source")
async def admin_add_source_start(callback: CallbackQuery, state: FSMContext):
    """Начало добавления источника"""
    await callback.message.edit_text(
        "➕ <b>Добавление нового NFT источника</b>\n\n"
        "Введите название источника (например: MoneyPot):",
        parse_mode="HTML"
    )
    await state.set_state(AddSourceStates.waiting_name)
    await callback.answer()

@dp.message(AddSourceStates.waiting_name)
async def admin_add_source_name(message: Message, state: FSMContext):
    """Получение названия источника"""
    name = message.text.strip()
    await state.update_data(name=name)
    
    await message.answer(
        f"✅ Название: <b>{name}</b>\n\n"
        "Теперь введите базовый URL с плейсхолдером {}\n"
        "Например: https://t.me/nft/MoneyPot-{}",
        parse_mode="HTML"
    )
    await state.set_state(AddSourceStates.waiting_url)

@dp.message(AddSourceStates.waiting_url)
async def admin_add_source_url(message: Message, state: FSMContext):
    """Получение URL источника"""
    url = message.text.strip()
    
    if "{}" not in url:
        await message.answer("❌ URL должен содержать плейсхолдер {}\nПопробуйте снова:")
        return
    
    await state.update_data(url=url)
    
    await message.answer(
        "✅ URL принят\n\n"
        "Введите начальный номер для парсинга (по умолчанию 1):"
    )
    await state.set_state(AddSourceStates.waiting_start_num)

@dp.message(AddSourceStates.waiting_start_num)
async def admin_add_source_finish(message: Message, state: FSMContext):
    """Завершение добавления источника"""
    try:
        start_num = int(message.text.strip())
    except ValueError:
        start_num = 1
    
    data = await state.get_data()
    name = data['name']
    url = data['url']
    
    # Добавляем в БД
    success = await db.add_source(name, url, start_num)
    
    if success:
        # Добавляем парсер
        parser_manager.add_parser(name, url, start_num)
        
        await message.answer(
            f"✅ <b>Источник добавлен!</b>\n\n"
            f"📝 Название: {name}\n"
            f"🔗 URL: {url}\n"
            f"🔢 Начальный номер: {start_num}\n\n"
            f"Теперь вы можете запустить парсинг через админ-панель.",
            reply_markup=get_admin_menu_keyboard(),
            parse_mode="HTML"
        )
    else:
        await message.answer(
            f"❌ Ошибка! Источник с названием <b>{name}</b> уже существует.",
            parse_mode="HTML"
        )
    
    await state.clear()

# === Парсинг диапазона ===

@dp.callback_query(F.data == "admin:parse_range")
async def admin_parse_range_start(callback: CallbackQuery, state: FSMContext):
    """Начало парсинга диапазона"""
    keyboard = await get_sources_keyboard("parse_range_select")
    await callback.message.edit_text(
        "🚀 <b>Парсинг диапазона</b>\n\n"
        "Выберите источник:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await state.set_state(ParseRangeStates.waiting_source)
    await callback.answer()

@dp.callback_query(F.data.startswith("parse_range_select:"))
async def admin_parse_range_source(callback: CallbackQuery, state: FSMContext):
    """Выбор источника для парсинга"""
    source_name = callback.data.split(":")[1]
    await state.update_data(source=source_name)
    
    await callback.message.edit_text(
        f"📋 Источник: <b>{source_name}</b>\n\n"
        "Введите начальный номер:",
        parse_mode="HTML"
    )
    await state.set_state(ParseRangeStates.waiting_start)
    await callback.answer()

@dp.message(ParseRangeStates.waiting_start)
async def admin_parse_range_start_num(message: Message, state: FSMContext):
    """Получение начального номера"""
    try:
        start = int(message.text.strip())
        await state.update_data(start=start)
        await message.answer(f"✅ Начало: {start}\n\nВведите конечный номер:")
        await state.set_state(ParseRangeStates.waiting_end)
    except ValueError:
        await message.answer("❌ Введите корректное число:")

@dp.message(ParseRangeStates.waiting_end)
async def admin_parse_range_execute(message: Message, state: FSMContext):
    """Запуск парсинга диапазона"""
    try:
        end = int(message.text.strip())
        data = await state.get_data()
        source = data['source']
        start = data['start']
        
        if end < start:
            await message.answer("❌ Конечный номер должен быть больше начального!")
            return
        
        status_msg = await message.answer(
            f"⏳ Запуск парсинга...\n\n"
            f"📋 Источник: {source}\n"
            f"📊 Диапазон: {start} - {end}\n"
            f"📈 Прогресс: 0/{end - start + 1}"
        )
        
        # Callback для сохранения
        async def save_callback(nft_data):
            await db.save_nft(source, nft_data)
        
        # Callback для прогресса
        async def progress_callback(parsed, total):
            try:
                await status_msg.edit_text(
                    f"⏳ Парсинг в процессе...\n\n"
                    f"📋 Источник: {source}\n"
                    f"📊 Диапазон: {start} - {end}\n"
                    f"📈 Прогресс: {parsed}/{total}"
                )
            except:
                pass
        
        # Запускаем парсинг
        await parser_manager.start_parser(
            source, 
            "range", 
            save_callback,
            start=start,
            end=end,
            progress_callback=progress_callback
        )
        
        await asyncio.sleep(2)  # Даём время на завершение
        
        await status_msg.edit_text(
            f"✅ <b>Парсинг завершён!</b>\n\n"
            f"📋 Источник: {source}\n"
            f"📊 Диапазон: {start} - {end}",
            reply_markup=get_admin_menu_keyboard(),
            parse_mode="HTML"
        )
        
    except ValueError:
        await message.answer("❌ Введите корректное число:")
        return
    
    await state.clear()

# === Запуск мониторинга ===

@dp.callback_query(F.data == "admin:start_monitoring")
async def admin_start_monitoring(callback: CallbackQuery, state: FSMContext):
    """Выбор источника для мониторинга"""
    keyboard = await get_sources_keyboard("monitoring_select")
    await callback.message.edit_text(
        "🔄 <b>Запуск мониторинга</b>\n\n"
        "Выберите источник:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await state.set_state(StartMonitoringStates.waiting_source)
    await callback.answer()

@dp.callback_query(F.data.startswith("monitoring_select:"))
async def admin_monitoring_execute(callback: CallbackQuery, state: FSMContext):
    """Запуск мониторинга"""
    source_name = callback.data.split(":")[1]
    
    # Получаем данные источника
    source = await db.get_source(source_name)
    if not source:
        await callback.answer("❌ Источник не найден", show_alert=True)
        return
    
    # Callback для сохранения
    async def save_callback(nft_data):
        await db.save_nft(source_name, nft_data)
        await db.update_source_state(source_name, nft_data['num'], nft_data['quantity'])
    
    # Запускаем мониторинг
    await parser_manager.start_parser(source_name, "new", save_callback)
    
    await callback.message.edit_text(
        f"✅ <b>Мониторинг запущен!</b>\n\n"
        f"📋 Источник: {source_name}\n"
        f"🔄 Режим: Поиск новых подарков\n\n"
        f"Парсер работает в фоновом режиме.",
        reply_markup=get_admin_menu_keyboard(),
        parse_mode="HTML"
    )
    
    await state.clear()
    await callback.answer()

# === Мониторинг всех моделей ===

@dp.callback_query(F.data == "admin:start_all_monitoring")
async def admin_start_all_monitoring(callback: CallbackQuery):
    """Запуск мониторинга для всех активных источников"""
    sources = await db.get_sources()
    started_count = 0
    for source in sources:
        if not source['is_active']:
            continue
        source_name = source['name']
        # Проверка, запущен ли уже
        status = parser_manager.get_parser_status(source_name)
        if status and status['status'] == 'running':
            continue  # Уже запущен, пропускаем
        # Callback для сохранения с обработкой ошибок
        async def save_callback(nft_data):
            try:
                await db.save_nft(source_name, nft_data)
                await db.update_source_state(source_name, nft_data['num'], nft_data['quantity'])
            except Exception as e:
                logger.error(f"Ошибка сохранения NFT для {source_name}: {e}")
        # Запускаем мониторинг
        await parser_manager.start_parser(source_name, "new", save_callback)
        started_count += 1
    await callback.message.edit_text(
        f"✅ <b>Мониторинг запущен для {started_count} моделей!</b>",
        reply_markup=get_admin_menu_keyboard(),
        parse_mode="HTML"
    )
    await callback.answer()

# === Остановка парсера ===

@dp.callback_query(F.data == "admin:stop_parser")
async def admin_stop_parser(callback: CallbackQuery):
    """Выбор парсера для остановки"""
    keyboard = await get_sources_keyboard("stop_parser_select")
    await callback.message.edit_text(
        "⏹ <b>Остановка парсера</b>\n\n"
        "Выберите источник:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("stop_parser_select:"))
async def admin_stop_parser_execute(callback: CallbackQuery):
    """Остановка выбранного парсера"""
    source_name = callback.data.split(":")[1]
    await parser_manager.stop_parser(source_name)
    
    await callback.message.edit_text(
        f"✅ Парсер <b>{source_name}</b> остановлен",
        reply_markup=get_admin_menu_keyboard(),
        parse_mode="HTML"
    )
    await callback.answer()

# === Статистика ===

@dp.callback_query(F.data == "admin:stats")
async def admin_stats(callback: CallbackQuery):
    """Показать статистику"""
    sources = await db.get_sources()
    
    if not sources:
        await callback.message.edit_text(
            "📊 <b>Статистика</b>\n\n"
            "Источников пока нет.",
            reply_markup=get_admin_menu_keyboard(),
            parse_mode="HTML"
        )
        await callback.answer()
        return
    
    stats_text = "📊 <b>Статистика по источникам</b>\n\n"
    
    for source in sources:
        source_name = source['name']
        stats = await db.get_stats(source_name)
        status = parser_manager.get_parser_status(source_name)
        
        is_running = status and status.get('status') == 'running'
        stats_text += (
            f"📋 <b>{source_name}</b>\n"
            f"├ Всего: {stats['total']}\n"
            f"├ Последний: #{stats['last_num'] or 0}\n"
            f"├ Моделей: {stats['unique_models']}\n"
            f"└ Статус: {'🟢 Работает' if is_running else '🔴 Остановлен'}\n\n"
        )
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=get_admin_menu_keyboard(),
        parse_mode="HTML"
    )
    await callback.answer()

# === Список источников ===

@dp.callback_query(F.data == "admin:list_sources")
async def admin_list_sources(callback: CallbackQuery):
    """Список всех источников"""
    sources = await db.get_sources(active_only=False)
    
    if not sources:
        await callback.message.edit_text(
            "📋 <b>Список источников</b>\n\n"
            "Источников пока нет.",
            reply_markup=get_admin_menu_keyboard(),
            parse_mode="HTML"
        )
        await callback.answer()
        return
    
    text = "📋 <b>Список источников</b>\n\n"
    
    for source in sources:
        status = "✅" if source['is_active'] else "❌"
        text += (
            f"{status} <b>{source['name']}</b>\n"
            f"├ URL: <code>{source['base_url']}</code>\n"
            f"├ Начало: {source['start_num']}\n"
            f"└ Текущий: {source['current_num']}\n\n"
        )
    
    await callback.message.edit_text(
        text,
        reply_markup=get_admin_menu_keyboard(),
        parse_mode="HTML"
    )
    await callback.answer()

# === Запуск бота ===

async def main():
    """Главная функция запуска бота"""
    try:
        # Инициализация БД
        await db.init_db()
        print("✅ База данных инициализирована")
        
        # Добавление админов из переменных окружения
        for admin_id in ADMIN_IDS:
            await db.add_admin(admin_id)
        
        # Загрузка источников и парсеров
        sources = await db.get_sources()
        for source in sources:
            parser_manager.add_parser(source['name'], source['base_url'], source['current_num'])
        
        print(f"✅ Загружено {len(sources)} источников")
        print("🤖 Telegram бот запущен и готов к работе!")
        
        # Запускаем polling
        await dp.start_polling(bot)
        
    except Exception as e:
        print(f"❌ Ошибка при запуске бота: {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
