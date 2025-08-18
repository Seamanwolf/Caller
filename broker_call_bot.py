import os
import logging
import requests
import pandas as pd
import matplotlib.pyplot as plt
import calendar
from datetime import datetime, timedelta
from io import BytesIO
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
import json
import re
from tqdm import tqdm
from prettytable import PrettyTable
from colorama import init, Fore, Style
import numpy as np
import matplotlib
matplotlib.use('Agg')
import asyncio
# Импортирую EmployeeDataProvider
from employee_data_provider import EmployeeDataProvider

# Инициализация colorama
init(autoreset=True)

# Функция для коррекции системного времени (если на сервере неправильная дата)
def get_actual_now():
    """
    Возвращает актуальную текущую дату.
    """
    return datetime.now()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Настройки API ВАТС
API_KEY = "d1b0ef65-e491-43f9-967b-df67d4657dbb"
API_URL = "https://leto.megapbx.ru/crmapi/v1"

# Список разрешенных пользователей
ALLOWED_USERS = [194530, 368752085, 261337953, 702018715]

# ID пользователя для автоматических отчетов
AUTO_REPORT_USER_ID = 194530

# Глобальная переменная для приложения бота (нужна для планировщика)
bot_application = None

# Инициализация провайдера сотрудников (глобально)
EMPLOYEE_API_TOKEN = "a4d4a75094d8f9d8597085ac0ac12a51"
employee_provider = EmployeeDataProvider(EMPLOYEE_API_TOKEN)

def setup_logging():
    logger.info("Настройка логирования")
    logging.getLogger().addHandler(logging.NullHandler())
    return logging.getLogger()

def get_department_numbers(department):
    logger.debug(f"Обработка отдела: {department}")
    if not department:
        return None
    try:
        if isinstance(department, (int, float)):
            return str(int(department))
        
        department = str(department)
        department = re.sub(r'[^\d]', '', department)
        result = department if department else None
        logger.debug(f"Результат обработки отдела: {result}")
        return result
    except Exception as e:
        logger.error(f"Ошибка при обработке отдела '{department}': {str(e)}")
        return None

async def start(update, context):
    user = update.effective_user
    user_id = user.id
    logger.info(f"Пользователь {user.first_name} (ID: {user_id}) запустил бота")
    if user_id not in ALLOWED_USERS:
        logger.warning(f"Попытка несанкционированного доступа от пользователя с ID {user_id}")
        await update.message.reply_text("⛔ Извините, у вас нет доступа к этому боту.")
        return
    welcome_text = "👋 Добро пожаловать в бот статистики отделов!\n\nВыберите опцию:"
    keyboard = [
        [InlineKeyboardButton("📋 Отчёт по всем отделам", callback_data="report:all")],
        [InlineKeyboardButton("🏢 Отчёт по отделу", callback_data="report:by")],
        [InlineKeyboardButton("🔄 Обновить сотрудников", callback_data="update_employees")]
    ]
    if update.message:
        await update.message.reply_text(welcome_text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        query = update.callback_query
        await query.edit_message_text(welcome_text, reply_markup=InlineKeyboardMarkup(keyboard))

async def show_main_menu(update, context):
    welcome_text = "👋 Выберите опцию:"
    keyboard = [
        [InlineKeyboardButton("📋 Отчёт по всем отделам", callback_data="report:all")],
        [InlineKeyboardButton("🏢 Отчёт по отделу", callback_data="report:by")],
        [InlineKeyboardButton("🔄 Обновить сотрудников", callback_data="update_employees")]
    ]
    if update.message:
        await update.message.reply_text(welcome_text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        query = update.callback_query
        await query.edit_message_text(welcome_text, reply_markup=InlineKeyboardMarkup(keyboard))

async def button_callback(update, context):
    query = update.callback_query
    data = query.data
    logger.info(f"Callback received: {data}")
    # Всегда отвечаем на callback, чтобы кнопки не "висели"
    try:
        await query.answer()
    except Exception:
        pass
    # Удалена устаревшая логика с "report_all"/"report_by_department" и выбором квартала
    if data == "back_to_main":
        await show_main_menu(update, context)
        return
    if data == "update_employees":
        await query.edit_message_text("🔄 Обновляю кэш сотрудников...")
        try:
            employee_provider.update_cache(force=True)
            await query.edit_message_text("✅ Кэш сотрудников успешно обновлён!", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main")]
            ]))
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка обновления кэша: {e}", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main")]
            ]))
        return
    if data == "report:all":
        context.user_data["report_type"] = "all"
        context.user_data["dept_number"] = "all"
        await show_period_selection(query, context, sheet_type=None, report_type="all")
        return
    if data == "report:by":
        context.user_data["report_type"] = "by"
        # Для отчета по отделам сначала показываем список отделов
        await show_department_list(query, context, sheet_type=None, report_type="by")
        return
    if data == "quarter:3sheets":
        # Квартальный отчет доступен только после выбора отчета по отделу и самого отдела
        if not (context.user_data.get("report_type") == "by" and context.user_data.get("dept_number")):
            context.user_data["report_type"] = "by"
            await show_department_list(query, context, context.user_data.get("sheet_type", ""), "by")
            return
        context.user_data["report_type"] = "quarter_3sheets"
        await show_year_selection(query, context)
        return
    if data == "quarter:1sheet":
        if not (context.user_data.get("report_type") == "by" and context.user_data.get("dept_number")):
            context.user_data["report_type"] = "by"
            await show_department_list(query, context, context.user_data.get("sheet_type", ""), "by")
            return
        context.user_data["report_type"] = "quarter_1sheet"
        await show_year_selection(query, context)
        return
    if data.startswith("dept:"):
        dept_data = data.split(":")
        if len(dept_data) >= 2:
            dept_number = dept_data[1]
            context.user_data["dept_number"] = str(dept_number)
            context.user_data["selected_dept_number"] = str(dept_number)
            await show_period_selection(query, context, sheet_type=None, report_type="by")
            return
    # Остальные callback — без изменений, только возвраты теперь к back_to_main
    if data == "back_to_report_selection":
        await show_main_menu(update, context)
        return
    if data == "back_to_sheet_selection":
        # Возврат к выбору типа отчёта по текущему листу
        sheet_type = context.user_data.get("sheet_type", "")
        await show_report_selection(query, context, sheet_type)
        return
    if data == "back_to_department_selection":
        # Возвращаемся к выбору отдела
        sheet_type = context.user_data.get("sheet_type", "")
        report_type = context.user_data.get("report_type", "")
        await show_department_list(query, context, sheet_type, report_type)
        return
    if data == "back_to_period_selection":
        # Возвращаемся к выбору периода
        sheet_type = context.user_data.get("sheet_type", "")
        report_type = context.user_data.get("report_type", "")
        await show_period_selection(query, context, sheet_type, report_type)
        return
    if data == "confirm_custom_period":
        # Обрабатываем выбор произвольного периода
        start_date = context.user_data.get("custom_start_date")
        end_date = context.user_data.get("custom_end_date")
        
        if not start_date or not end_date:
            await query.edit_message_text(
                "❌ Пожалуйста, укажите обе даты (начало и конец периода)",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_period_selection")]])
            )
            return
            
        context.user_data["period"] = "custom"
        
        # Извлекаем sheet_type, report_type и dept_number из контекста
        sheet_type = context.user_data.get("sheet_type", "")
        report_type = context.user_data.get("report_type", "")
        dept_number = context.user_data.get("dept_number", "all")
        
        logger.info(f"Подтвержден произвольный период: {start_date} - {end_date}")
        
        await show_format_selection(query, context, sheet_type, report_type, dept_number, "custom")
        return
        
    # Проверяем, содержит ли callback данные о периоде
    if data.startswith("period:"):
        period = data.split(":")[1]
        
        # Извлекаем sheet_type, report_type и dept_number из контекста
        sheet_type = context.user_data.get("sheet_type", "")
        report_type = context.user_data.get("report_type", "")
        dept_number = context.user_data.get("dept_number", "all")
        
        logger.info(f"Выбран период: {period}, sheet_type: {sheet_type}, report_type: {report_type}, dept_number: {dept_number}")
        
        if period == "custom_range":
            await query.edit_message_text(
                "📆 Введите даты начала и конца периода в формате ДД.ММ.ГГГГ ДД.ММ.ГГГГ",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_custom_period")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_period_selection")]
                ])
            )
            # Устанавливаем состояние для ожидания ввода дат
            context.user_data["waiting_for_dates"] = True
            context.user_data["date_input_type"] = "range"
            return
        
        elif period == "custom_date":
            await query.edit_message_text(
                "📆 Введите дату в формате ДД.ММ.ГГГГ",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_custom_period")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_period_selection")]
                ])
            )
            # Устанавливаем состояние для ожидания ввода даты
            context.user_data["waiting_for_dates"] = True
            context.user_data["date_input_type"] = "single"
            return
            
        # Сохраняем выбранный период в контексте пользователя
        context.user_data["period"] = period
        
        await show_format_selection(query, context, sheet_type, report_type, dept_number, period)
        return
        
    # Проверяем, содержит ли callback данные о формате
    if data.startswith("format:"):
        format_type = data.split(":")[1]
        
        # Извлекаем sheet_type, report_type, dept_number и period из контекста
        sheet_type = context.user_data.get("sheet_type", "")
        report_type = context.user_data.get("report_type", "")
        dept_number = context.user_data.get("dept_number", "all")
        period = context.user_data.get("period", "")
        
        logger.info(f"Выбран формат: {format_type}, sheet_type: {sheet_type}, report_type: {report_type}, dept_number: {dept_number}, period: {period}")
        
        if format_type == "incoming":
            await handle_incoming_numbers_excel(query, context, sheet_type, dept_number, period)
        else:
            await handle_report_format(query, context, sheet_type, dept_number, period, format_type)
        return
        
    # Проверяем, содержит ли callback данные о департаменте (дублирующий обработчик удален)
        
    # Обработка выбора года для квартального отчета
    if data.startswith("year:"):
        year = int(data.split(":")[1])
        context.user_data["selected_year"] = year
        await show_quarter_selection(query, context, year)
        return
    
    # Обработка выбора квартала
    if data.startswith("quarter:"):
        quarter_data = data.split(":")
        if len(quarter_data) == 3:
            # Выбор конкретного квартала
            year = int(quarter_data[1])
            quarter = int(quarter_data[2])
            await generate_quarter_report(query, context, year, quarter)
            return
        elif len(quarter_data) == 2:
            # Возврат к выбору типа квартального отчета (из экрана выбора квартала)
            sheets_key = quarter_data[1]
            if sheets_key == "3sheets":
                context.user_data["report_type"] = "quarter_3sheets"
                # dept_number не трогаем
                await show_year_selection(query, context)
            elif sheets_key == "1sheet":
                context.user_data["report_type"] = "quarter_1sheet"
                await show_year_selection(query, context)
            return
        
    # Проверяем, содержит ли callback данные о типе отчета
    if data.startswith("report:"):
        report_type = data.split(":")[1]
        
        # Сохраняем тип отчета в контексте
        context.user_data["report_type"] = report_type
        
        # Извлекаем sheet_type из контекста
        sheet_type = context.user_data.get("sheet_type", "")
        
        logger.info(f"Выбран тип отчета: {report_type}, sheet_type: {sheet_type}")
        
        if report_type == "all":
            # Для отчета по всем отделам сразу переходим к выбору периода
            context.user_data["dept_number"] = "all"
            context.user_data["selected_dept_number"] = "all"
            await show_period_selection(query, context, sheet_type, report_type)
        else:
            # Для отчета по отделам показываем список отделов
            await show_department_list(query, context, sheet_type, report_type)
        return
        
    # Проверяем, содержит ли callback данные о типе листа
    if data.startswith("sheet:"):
        sheet_type = data.split(":")[1]
        
        # Сохраняем тип листа в контексте
        context.user_data["sheet_type"] = sheet_type
        
        # Очищаем предыдущие выборы департамента
        if "dept_number" in context.user_data:
            del context.user_data["dept_number"]
        if "selected_dept_number" in context.user_data:
            del context.user_data["selected_dept_number"]
            
        logger.info(f"Выбран тип листа: {sheet_type}")
        
        await show_report_selection(query, context, sheet_type)
        return
        
    if data == "update_employees":
        await query.edit_message_text("🔄 Обновляю кэш сотрудников...")
        try:
            employee_provider.update_cache(force=True)
            await query.edit_message_text("✅ Кэш сотрудников успешно обновлён!", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main")]
            ]))
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка обновления кэша: {e}", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main")]
            ]))
        return
    
    await query.edit_message_text(
        f"⚠️ Неизвестный callback: {data}",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
    )

async def show_report_selection(query, context, sheet_type):
    logger.info(f"Показываю выбор типа отчета для sheet_type={sheet_type}")
    sheet_name = "Вторичка" if sheet_type == "vtorichka" else "Загородка"
    
    keyboard = [
        [InlineKeyboardButton("📊 По всем отделам", callback_data=f"report:all")],
        [InlineKeyboardButton("📋 По отделам", callback_data=f"report:by")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_sheet_selection")]
    ]
    
    try:
        await query.edit_message_text(
            f"📋 Выберите тип отчета для листа '{sheet_name}':",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        if 'Message is not modified' not in str(e):
            raise

async def show_period_selection(query, context, sheet_type, report_type):
    logger.info(f"Показываю выбор периода для sheet_type={sheet_type}, report_type={report_type}")
    sheet_name = "Вторичка" if sheet_type == "vtorichka" else "Загородка"
    
    keyboard = [
        [InlineKeyboardButton("📅 Сегодня", callback_data="period:today")],
        [InlineKeyboardButton("📅 Текущий месяц", callback_data="period:current_month")],
        [InlineKeyboardButton("📅 Предыдущий месяц", callback_data="period:previous_month")],
        [InlineKeyboardButton("📅 За 7 дней", callback_data="period:week")],
        [InlineKeyboardButton("📅 За 30 дней", callback_data="period:month")],
        [InlineKeyboardButton("📅 Произвольный период", callback_data="period:custom_range")],
        [InlineKeyboardButton("📅 Конкретная дата", callback_data="period:custom_date")],
    ]

    # Если отчет по отделу — добавляем квартальные варианты внутрь этого меню
    if report_type == "by":
        keyboard.extend([
            [InlineKeyboardButton("📊 Квартал (3 листа)", callback_data="quarter:3sheets")],
            [InlineKeyboardButton("📈 Квартал (1 лист)", callback_data="quarter:1sheet")],
        ])

    keyboard.extend([
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_department_selection" if report_type == "by" else "back_to_report_selection")]
    ])
    
    try:
        await query.edit_message_text(
            f"📅 Выберите период для отчёта:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        if 'Message is not modified' not in str(e):
            raise

# Добавляю функцию анимированного прогресса
async def show_loading_animation(query, context, base_text="Получаю данные сотрудников"):
    animation = ["", ".", "..", "...", " ....", " .....", " ......"]
    try:
        for i in range(60):  # максимум 30 секунд
            await asyncio.sleep(0.5)
            await query.edit_message_text(f"{base_text}{animation[i % len(animation)]}")
    except asyncio.CancelledError:
        pass
    except Exception:
        pass

async def show_department_list(query, context, sheet_type, report_type):
    logger.info(f"Показываю список отделов для sheet_type={sheet_type}, report_type={report_type}")
    loading_task = asyncio.create_task(show_loading_animation(query, context))
    try:
        # Получаем сотрудников из кэша
        employees = employee_provider.get_employees()
        filtered = employees
        # Группируем по отделам
        departments = {}
        import re
        for emp in filtered:
            dept_raw = emp['department']
            match = re.search(r'(\d+)', str(dept_raw))
            if match:
                dept = str(int(match.group(1)))
                if dept not in departments:
                    departments[dept] = []
                departments[dept].append({
                    'phone': emp['sim'],
                    'name': f"{emp['last_name']} {emp['first_name']}",
                    'department': emp['department']
                })
        loading_task.cancel()
        try:
            await loading_task
        except asyncio.CancelledError:
            pass
        if not departments:
            logger.error("Не найдено отделов с номерами")
            await query.edit_message_text("❌ Не найдено отделов с номерами", 
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_report_selection")]])
            )
            return
        keyboard = []
        for dept_number in sorted(departments.keys(), key=int):
            callback_data = f"dept:{dept_number}"
            keyboard.append([InlineKeyboardButton(f"Отдел {dept_number} ({len(departments[dept_number])} сотрудников)", callback_data=callback_data)])
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_report_selection")])
        try:
            await query.edit_message_text(
                f"📋 Выберите отдел для создания отчета:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            if 'Message is not modified' not in str(e):
                raise
    except Exception as e:
        loading_task.cancel()
        logger.error(f"Ошибка при показе списка отделов: {str(e)}")
        await query.edit_message_text(f"❌ Произошла ошибка: {str(e)}", 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_report_selection")]])
        )

async def show_format_selection(query, context, sheet_type, report_type, dept_number, period):
    logger.info(f"Показываю выбор формата для sheet_type={sheet_type}, report_type={report_type}, dept_number={dept_number}, period={period}")
    sheet_name = "Вторичка" if sheet_type == "vtorichka" else "Загородка"
    
    keyboard = [
        [InlineKeyboardButton("📊 График", callback_data="format:plot")],
        [InlineKeyboardButton("📋 Таблица", callback_data="format:table")],
        [InlineKeyboardButton("📑 Excel", callback_data="format:excel")],
        [InlineKeyboardButton("📞 Входящие номера", callback_data="format:incoming")],
        [InlineKeyboardButton("📊 Все форматы", callback_data="format:all")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_period_selection")]
    ]
    
    try:
        await query.edit_message_text(
            f"📋 Выберите формат отчета для листа '{sheet_name}':",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        if 'Message is not modified' not in str(e):
            raise

async def initiate_custom_period(query, context, sheet_type, report_type):
    for key in ["custom_start_year", "custom_start_month", "custom_start_day", "custom_start_date",
                "custom_end_year", "custom_end_month", "custom_end_day", "custom_end_date"]:
        context.user_data.pop(key, None)
    current_year = datetime.now().year
    keyboard = []
    for y in range(2020, current_year + 3):
        keyboard.append([InlineKeyboardButton(str(y), callback_data=f"custom|start|year|{y}|{sheet_type}|{report_type}")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data=f"period_{sheet_type}_{report_type}")])
    try:
        await query.edit_message_text("Выберите год начала периода:", reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        if 'Message is not modified' not in str(e):
            raise

async def handle_custom_period(query, context):
    data = query.data
    parts = data.split("|")
    if len(parts) != 8:
        await query.edit_message_text("Ошибка в данных выбора.")
        return
    
    _, phase, field, value, sheet_type, report_type = parts
    
    if phase == "start":
        if field == "year":
            context.user_data["custom_start_year"] = value
            keyboard = []
            row = []
            for m in range(1, 13):
                m_str = f"{m:02d}"
                row.append(InlineKeyboardButton(m_str, callback_data=f"custom|start|month|{m_str}|{sheet_type}|{report_type}"))
                if len(row) == 4:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            try:
                await query.edit_message_text("Выберите месяц начала периода:", reply_markup=InlineKeyboardMarkup(keyboard))
            except Exception as e:
                if 'Message is not modified' not in str(e):
                    raise
        elif field == "month":
            context.user_data["custom_start_month"] = value
            year = int(context.user_data.get("custom_start_year"))
            month = int(value)
            _, num_days = calendar.monthrange(year, month)
            keyboard = []
            row = []
            for d in range(1, num_days + 1):
                d_str = f"{d:02d}"
                row.append(InlineKeyboardButton(d_str, callback_data=f"custom|start|day|{d_str}|{sheet_type}|{report_type}"))
                if len(row) == 5:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            try:
                await query.edit_message_text("Выберите день начала периода:", reply_markup=InlineKeyboardMarkup(keyboard))
            except Exception as e:
                if 'Message is not modified' not in str(e):
                    raise
        elif field == "day":
            context.user_data["custom_start_day"] = value
            start_date_str = (context.user_data["custom_start_year"] +
                            context.user_data["custom_start_month"] +
                            context.user_data["custom_start_day"])
            context.user_data["custom_start_date"] = start_date_str
            keyboard = []
            current_year = datetime.now().year
            for y in range(2020, current_year + 3):
                keyboard.append([InlineKeyboardButton(str(y), callback_data=f"custom|end|year|{y}|{sheet_type}|{report_type}")])
            try:
                await query.edit_message_text("Выберите год окончания периода:", reply_markup=InlineKeyboardMarkup(keyboard))
            except Exception as e:
                if 'Message is not modified' not in str(e):
                    raise
    elif phase == "end":
        if field == "year":
            context.user_data["custom_end_year"] = value
            keyboard = []
            row = []
            for m in range(1, 13):
                m_str = f"{m:02d}"
                row.append(InlineKeyboardButton(m_str, callback_data=f"custom|end|month|{m_str}|{sheet_type}|{report_type}"))
                if len(row) == 4:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            try:
                await query.edit_message_text("Выберите месяц окончания периода:", reply_markup=InlineKeyboardMarkup(keyboard))
            except Exception as e:
                if 'Message is not modified' not in str(e):
                    raise
        elif field == "month":
            context.user_data["custom_end_month"] = value
            year = int(context.user_data.get("custom_end_year"))
            month = int(value)
            _, num_days = calendar.monthrange(year, month)
            keyboard = []
            row = []
            for d in range(1, num_days + 1):
                d_str = f"{d:02d}"
                row.append(InlineKeyboardButton(d_str, callback_data=f"custom|end|day|{d_str}|{sheet_type}|{report_type}"))
                if len(row) == 5:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            try:
                await query.edit_message_text("Выберите день окончания периода:", reply_markup=InlineKeyboardMarkup(keyboard))
            except Exception as e:
                if 'Message is not modified' not in str(e):
                    raise
        elif field == "day":
            context.user_data["custom_end_day"] = value
            end_date_str = (context.user_data["custom_end_year"] +
                          context.user_data["custom_end_month"] +
                          context.user_data["custom_end_day"])
            context.user_data["custom_end_date"] = end_date_str
            
            if context.user_data["custom_end_date"] < context.user_data["custom_start_date"]:
                await query.edit_message_text("Дата окончания не может быть раньше даты начала. Пожалуйста, выберите дату окончания снова.")
                return
                
            await show_format_selection(query, context, sheet_type, report_type, "custom")

async def handle_report_format(query, context, sheet_type, dept_number, period, format_type):
    sheet_name = 'Все отделы'
    logger.info(f"Обработка отчета: тип={sheet_type}, отдел={dept_number}, период={period}, формат={format_type}")
    
    # Дополнительная проверка номера отдела из context.user_data
    if dept_number == "None" or not dept_number or dept_number == "undefined":
        if context.user_data.get("selected_dept_number"):
            dept_number = context.user_data.get("selected_dept_number")
            logger.info(f"Восстановлен номер отдела из context.user_data['selected_dept_number']: {dept_number}")
        elif context.user_data.get("dept_number"):
            dept_number = context.user_data.get("dept_number")
            logger.info(f"Восстановлен номер отдела из context.user_data['dept_number']: {dept_number}")
    
    logger.info(f"Окончательный номер отдела для использования: {dept_number}")
    
    try:
        await query.edit_message_text("🔄 Получаю данные из Google Sheets...", reply_markup=None)
        
        # Получаем сотрудников из кэша
        employees = employee_provider.get_employees()
        filtered = employees
        
        if not filtered:
            logger.error("Не найдено сотрудников для отчета")
            await query.edit_message_text("❌ Нет данных для создания отчета", 
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
            )
            return

        logger.info(f"Найдено {len(filtered)} сотрудников для отчета")
        
        await query.edit_message_text("🔄 Формирую список номеров...", reply_markup=None)

        # Группируем сотрудников по отделам
        departments = {}
        import re
        for employee in filtered:
            dept_raw = employee['department']
            match = re.search(r'(\d+)', str(dept_raw))
            if match:
                dept = str(int(match.group(1)))
                if dept not in departments:
                    departments[dept] = []
                departments[dept].append({
                    'phone': employee['sim'],
                    'name': f"{employee['last_name']} {employee['first_name']}",
                    'department': employee['department']
                })

        logger.info(f"Найдено {len(departments)} отделов: {list(departments.keys())}")
        
        if not departments:
            logger.error("Не найдено отделов с номерами")
            await query.edit_message_text("❌ Не найдено отделов с номерами", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
            )
            return

        await query.edit_message_text("🔄 Получаю статистику звонков...", reply_markup=None)

        try:
            actual_period = context.user_data.get("period", period)
            logger.info(f"Используемый период: {actual_period}")
            
            # Получаем строки дат в формате YYYYMMDD
            start_date_str, end_date_str = get_period_dates(actual_period, context)
            logger.info(f"Получены даты периода: {start_date_str} - {end_date_str}")
            
            # Не преобразуем даты в этой функции, так как fetch_call_history
            # самостоятельно преобразует даты в нужный формат YYYY-MM-DD для API
            # Это гарантирует единообразие работы для всех типов периодов
            
            logger.info(f"Отправка запроса с датами: {start_date_str} - {end_date_str}")
            
        except ValueError as e:
            logger.error(f"Ошибка при определении периода: {str(e)}")
            await query.edit_message_text(f"❌ Ошибка при определении периода: {str(e)}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
            )
            return
        
        # Получаем тип отчета из context.user_data
        report_type = context.user_data.get("report_type", "all")
        logger.info(f"Тип отчета в handle_report_format: {report_type}")
        
        # Проверяем и нормализуем номер отдела
        if not dept_number or dept_number == "None" or dept_number == "all" or dept_number == "undefined":
            # Если отдел не указан или "all", то для report_type "by" это ошибка
            if report_type != "all":
                logger.error(f"Не указан номер отдела для отчета типа {report_type}")
                await query.edit_message_text("❌ Не указан номер отдела для отчета", 
                                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
                )
                return
            dept_number = "all"
        
        logger.info(f"Используемый номер отдела: {dept_number}")
        
        # Проверка существования отдела
        if dept_number != "all" and dept_number not in departments:
            logger.error(f"Отдел {dept_number} не найден среди доступных отделов: {list(departments.keys())}")
            await query.edit_message_text(f"❌ Отдел {dept_number} не найден", 
                                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
            )
            return
            
        # Собираем статистику по номерам
        all_stats = []
        
        # Определяем, какие номера нужно обработать
        numbers_to_process = []
        if report_type == "all" or dept_number == "all":
            # Для всех отделов обрабатываем все номера
            for dept_id, employees in departments.items():
                logger.info(f"Добавляем всех сотрудников отдела {dept_id} ({len(employees)} человек)")
                numbers_to_process.extend(employees)
        else:
            # Для отчета по отделам обрабатываем только номера выбранного отдела
            if dept_number in departments:
                logger.info(f"Добавляем сотрудников только отдела {dept_number} ({len(departments[dept_number])} человек)")
                numbers_to_process = departments[dept_number]
                # Сохраняем номер отдела для последующего использования
                context.user_data["selected_dept_number"] = dept_number
                context.user_data["dept_number"] = dept_number
            else:
                logger.error(f"Отдел {dept_number} не найден среди доступных отделов: {list(departments.keys())}")
                await query.edit_message_text(f"❌ Отдел {dept_number} не найден", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
                )
                return
        
        logger.info(f"Всего для обработки: {len(numbers_to_process)} номеров")
        
        # Подсчитываем общее количество номеров для прогресс-бара
        total_numbers = len(numbers_to_process)
        processed_numbers = 0
        total_calls_found = 0
        total_incoming_calls = 0
        total_missed_calls = 0
        calls_per_employee = {}
        
        # Текущий год для проверки дат
        current_year = datetime.now().year
        
        # Обрабатываем номера
        for employee in numbers_to_process:
            try:
                # Обновляем прогресс-бар
                processed_numbers += 1
                progress = (processed_numbers / total_numbers) * 100
                progress_bar = "█" * int(progress / 2) + "░" * (50 - int(progress / 2))
                await query.edit_message_text(
                    f"🔄 Получаю статистику звонков...\n"
                    f"Прогресс: {progress_bar} {progress:.1f}%\n"
                    f"Обработано: {processed_numbers}/{total_numbers} номеров\n"
                    f"Найдено звонков: {total_calls_found} (вх: {total_incoming_calls}, проп: {total_missed_calls})",
                    reply_markup=None
                )
                
                logger.info(f"Запрос данных для {employee['name']} ({employee['phone']})")
                data = fetch_call_history(start_date_str, end_date_str, employee['phone'])
                if not data:
                    logger.info(f"Нет данных для {employee['phone']}")
                    continue
                
                total_calls_found += len(data)
                calls_per_employee[employee['phone']] = len(data)
                logger.info(f"Получено {len(data)} звонков для {employee['phone']}")
                
                # Проверяем и исправляем даты в данных
                cleaned_data = []
                for call in data:
                    if 'start' in call and call['start']:
                        try:
                            # Получаем исходную дату и преобразуем ее
                            raw_dt = call['start']
                            dt = datetime.fromisoformat(raw_dt.replace('Z', '+00:00'))
                            
                            # Убираем проверку года в будущем
                            cleaned_data.append(call)
                        except Exception as e:
                            logger.error(f"Ошибка при обработке даты звонка: {str(e)}, пропускаем запись")
                            # Добавляем звонок без изменений, если не удалось обработать дату
                            cleaned_data.append(call)
                    else:
                        cleaned_data.append(call)
                
                if cleaned_data:
                    df = pd.DataFrame(cleaned_data)
                else:
                    logger.warning(f"После проверки дат не осталось данных для {employee['phone']}")
                    continue
                    
                if not df.empty:
                    # Подробное логирование
                    type_counts = df['type'].value_counts().to_dict()
                    logger.info(f"Статистика по типам для {employee['phone']}: {type_counts}")
                    
                    # Считаем входящие и пропущенные
                    incoming_types = ['in', 'incoming', 'received', 'inbound', 'входящий']
                    outgoing_types = ['out', 'outgoing', 'исходящий']
                    missed_statuses = ['noanswer', 'missed', 'пропущен', 'неотвечен', 'нет ответа']

                    incoming_count = df[df['type'].str.lower().isin(incoming_types)].shape[0]
                    outgoing_count = df[df['type'].str.lower().isin(outgoing_types)].shape[0]
                    missed_count = df[df['status'].str.lower().isin(missed_statuses)] if 'status' in df.columns else 0
                    missed_count = missed_count.shape[0] if hasattr(missed_count, 'shape') else 0
                    total_incoming_calls += incoming_count
                    total_missed_calls += missed_count
                    
                    # Создаем запись статистики для этого номера
                    outgoing_count = df[df['type'] == 'out'].shape[0]
                    stats_dict = {
                        'Сотрудник': employee['name'],
                        'Отдел': get_department_numbers(employee['department']),
                        'Входящие 📞': incoming_count,
                        'Исходящие 📤': outgoing_count,
                        'Пропущенные ❌': missed_count,
                        'Всего звонков': len(cleaned_data)
                    }
                    all_stats.append(stats_dict)
                    
                    # Дополнительное логирование для периода "сегодня"
                    if actual_period == "today":
                        logger.info(f"ПЕРИОД СЕГОДНЯ - детали для {employee['name']}: входящих: {incoming_count}, пропущенных: {missed_count}")
                        if len(data) > 0:
                            sample_call = data[0]
                            logger.info(f"Пример данных звонка для периода СЕГОДНЯ: {sample_call}")
                            if 'start' in sample_call:
                                logger.info(f"Дата звонка в примере для СЕГОДНЯ: {sample_call['start']}")
            except Exception as e:
                logger.error(f"Ошибка получения данных для {employee['name']} ({employee['phone']}): {str(e)}")
                continue
        
        if not all_stats:
            logger.error("Не найдено данных для создания отчета")
            await query.edit_message_text("❌ Нет данных для создания отчета", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )
            return

        # Создаем DataFrame для дальнейшей обработки
        df_stats = pd.DataFrame(all_stats)
        
        # Фильтруем данные, исключая все итоговые строки
        df_stats = df_stats[~df_stats['Сотрудник'].str.contains('ИТОГО')]

        logger.info(f"Подготовлен DataFrame с {len(df_stats)} строками данных")
        logger.info(f"Общее количество обработанных звонков: {total_calls_found}")

        # Считаем итоговую статистику по отчету
        total_incoming = df_stats['Входящие 📞'].sum() if 'Входящие 📞' in df_stats.columns else 0
        total_outgoing = df_stats['Исходящие 📤'].sum() if 'Исходящие 📤' in df_stats.columns else 0
        total_missed = df_stats['Пропущенные ❌'].sum() if 'Пропущенные ❌' in df_stats.columns else 0
        logger.info(f"Итого во всем отчете: Входящие - {total_incoming}, Исходящие - {total_outgoing}, Пропущенные - {total_missed}, Всего: {total_calls_found}")
        
        # Дополнительная отладочная информация для периода "сегодня"
        if actual_period == "today":
            logger.info(f"СВОДНАЯ СТАТИСТИКА ДЛЯ ПЕРИОДА СЕГОДНЯ: Всего звонков: {total_calls_found}")
            logger.info(f"Звонки по сотрудникам для периода СЕГОДНЯ: {calls_per_employee}")
            logger.info(f"Всего входящих для СЕГОДНЯ: {total_incoming}, всего пропущенных: {total_missed}")

        # Сохраняем период в контексте для использования в функциях обработки форматов
        context.user_data["period"] = actual_period

        # Обрабатываем только выбранный формат
        if format_type == "plot":
            await handle_plot_format(query, context, df_stats, sheet_name)
        elif format_type == "table":
            await handle_table_format(query, context, all_stats, sheet_name)
        elif format_type == "excel":
            await handle_excel_format(query, context, df_stats, sheet_name, actual_period)
        elif format_type == "all":
            # Для формата "all" отправляем все форматы последовательно
            await handle_table_format(query, context, all_stats, sheet_name)
            await handle_plot_format(query, context, df_stats, sheet_name)
            await handle_excel_format(query, context, df_stats, sheet_name, actual_period)
            period_info = get_period_dates_info(actual_period, context)
            await query.edit_message_text(f"✅ Все форматы отчета отправлены! ({period_info})", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )
        
    except Exception as e:
        logger.error(f"Ошибка при обработке отчета: {str(e)}")
        await query.edit_message_text(f"❌ Произошла ошибка: {str(e)}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )

async def handle_plot_format(query, context, df_stats, sheet_name=None):
    if not sheet_name:
        sheet_name = 'Все отделы'
    # Проверяем тип отчета
    report_type = context.user_data.get("report_type", "all")
    logger.info(f"Тип отчета в handle_plot_format: {report_type}")
    
    # Получаем номер отдела из context.user_data
    dept_number = context.user_data.get("selected_dept_number", "all")
    logger.info(f"Номер отдела в handle_plot_format: {dept_number}")
    
    # Получаем информацию о периоде
    period = context.user_data.get("period", "current_month")
    period_dates = get_period_dates_info(period, context)
    period_str = f"({period_dates})"
    
    try:
        if report_type == "all":
            logger.info("Обработка графика для всех отделов")
            if df_stats.empty:
                logger.error("Нет данных для создания графика")
                await query.edit_message_text("❌ Нет данных для создания графика", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]]))
                return
            fig, ax = plt.subplots(figsize=(15, 8), dpi=150)
            required_columns = ['Отдел', 'Входящие 📞', 'Исходящие 📤', 'Пропущенные ❌']
            for column in required_columns:
                if column not in df_stats.columns:
                    df_stats[column] = 0
            # Группируем данные по отделам
            dept_totals = df_stats.groupby('Отдел').agg({
                'Входящие 📞': 'sum',
                'Исходящие 📤': 'sum',
                'Пропущенные ❌': 'sum'
            }).reset_index()
            # Считаем количество сотрудников в каждом отделе
            dept_counts = df_stats.groupby('Отдел').size().reset_index(name='num_employees')
            dept_totals = dept_totals.merge(dept_counts, on='Отдел', how='left')
            dept_totals['num_employees'] = dept_totals['num_employees'].replace(0, 1)
            # Считаем среднее на сотрудника
            incoming = (dept_totals['Входящие 📞'] / dept_totals['num_employees']).round(1)
            outgoing = (dept_totals['Исходящие 📤'] / dept_totals['num_employees']).round(1)
            missed = (dept_totals['Пропущенные ❌'] / dept_totals['num_employees']).round(1)
            x = range(len(dept_totals))
            width = 0.2
            bars1 = ax.bar([i - width for i in x], incoming, width, color='#080835', alpha=0.7, label='Входящие')
            bars2 = ax.bar(x, outgoing, width, color='#45B7D1', alpha=0.7, label='Исходящие')
            bars3 = ax.bar([i + width for i in x], missed, width, color='#e74c3c', alpha=0.4, label='Пропущенные')
            # --- Исправляю подписи на графике по всем отделам ---
            incoming_total = dept_totals['Входящие 📞'].fillna(0).astype(int)
            outgoing_total = dept_totals['Исходящие 📤'].fillna(0).astype(int)
            missed_total = dept_totals['Пропущенные ❌'].fillna(0).astype(int)
            for i, bar in enumerate(bars1):
                height = bar.get_height()
                total = incoming_total.iloc[i]
                avg = incoming.iloc[i]
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., height, f'{total} ({avg})', ha='center', va='bottom', fontsize=8)
            for i, bar in enumerate(bars2):
                height = bar.get_height()
                total = outgoing_total.iloc[i]
                avg = outgoing.iloc[i]
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., height, f'{total} ({avg})', ha='center', va='bottom', fontsize=8)
            for i, bar in enumerate(bars3):
                height = bar.get_height()
                total = missed_total.iloc[i]
                avg = missed.iloc[i]
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., height, f'{total} ({avg})', ha='center', va='bottom', fontsize=8)
            ax.set_title(f"Статистика звонков по отделам (общее (среднее на сотрудника)) {sheet_name} {period_str}", pad=20, fontsize=14, fontweight='bold', color='#2c3e50')
            ax.set_xlabel("Отделы", fontsize=10, color='#2c3e50')
            ax.set_ylabel("Количество звонков", fontsize=10, color='#2c3e50')
            ax.set_xticks(x)
            if not dept_totals.empty:
                x_labels = [str(label)[:10] for label in dept_totals['Отдел']]
                ax.set_xticklabels(x_labels, rotation=45, ha='right')
            legend = ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0, frameon=True, facecolor='white', edgecolor='#95a5a6')
            for text in legend.get_texts():
                text.set_color('#2c3e50')
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.spines['left'].set_color('#95a5a6')
            ax.spines['bottom'].set_color('#95a5a6')
            plt.subplots_adjust(right=0.85)
            plt.tight_layout()
            await send_plot(plt.gcf(), query.message.chat_id, context)
            await query.edit_message_text("✅ График отправлен!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]]))
            return
        else:
            logger.info(f"Обработка графика для отдела: {dept_number}")
            
            # Проверяем, есть ли номер отдела
            if not dept_number or dept_number == "all":
                logger.error("Не удалось получить номер отдела")
                await query.edit_message_text("❌ Ошибка: не удалось определить отдел", 
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
                )
                return
                
            # Фильтруем данные по выбранному отделу
            filtered_df = df_stats[df_stats['Отдел'] == dept_number]
            logger.info(f"Отфильтровано {len(filtered_df)} записей для отдела {dept_number}")
            
            if filtered_df.empty:
                logger.error(f"Нет данных для отдела {dept_number}")
                await query.edit_message_text(f"❌ Нет данных для отдела {dept_number}", 
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
                )
                return
            
            # Ограничиваем количество записей на одном графике
            max_records_per_graph = 30
            num_plots = (len(filtered_df) + max_records_per_graph - 1) // max_records_per_graph  # Округляем вверх
            logger.info(f"Будет создано {num_plots} графиков")
            
            # Создаем словарь цветов для отделов
            dept_colors = {}
            colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEEAD', '#D4A5A5', '#9B59B6', '#3498DB', '#E67E22', '#1ABC9C']
            for idx, dept in enumerate(filtered_df['Отдел'].unique()):
                dept_colors[dept] = colors[idx % len(colors)]
            
            for plot_num in range(num_plots):
                start_idx = plot_num * max_records_per_graph
                end_idx = min((plot_num + 1) * max_records_per_graph, len(filtered_df))
                plot_data = filtered_df.iloc[start_idx:end_idx]
                plt.style.use('bmh')
                plt.rcParams['figure.facecolor'] = 'white'
                plt.rcParams['axes.facecolor'] = 'white'
                plt.rcParams['grid.color'] = '#95a5a6'
                plt.rcParams['grid.alpha'] = 0.3
                fig, ax = plt.subplots(figsize=(15, 8), dpi=150)
                x = range(len(plot_data))
                width = 0.2
                incoming = plot_data['Входящие 📞'].fillna(0).astype(int)
                outgoing = plot_data['Исходящие 📤'].fillna(0).astype(int) if 'Исходящие 📤' in plot_data.columns else [0]*len(plot_data)
                missed = plot_data['Пропущенные ❌'].fillna(0).astype(int)
                if 'Сотрудник' in plot_data.columns:
                    x_labels = [str(name)[:20] for name in plot_data['Сотрудник']]
                else:
                    x_labels = [str(num)[:15] for num in plot_data['Номер']]
                bars1 = ax.bar([i - width for i in x], incoming, width, color='#080835', alpha=0.7, label='Входящие')
                bars2 = ax.bar(x, outgoing, width, color='#45B7D1', alpha=0.7, label='Исходящие')
                bars3 = ax.bar([i + width for i in x], missed, width, color='#e74c3c', alpha=0.4, label='Пропущенные')
                for bars in [bars1, bars2, bars3]:
                    for bar in bars:
                        height = bar.get_height()
                        if height > 0:
                            ax.text(bar.get_x() + bar.get_width()/2., height, f'{int(height)}', ha='center', va='bottom', fontsize=8)
                ax.set_title(f"Статистика звонков по сотрудникам отдела {dept_number} ({sheet_name}) {period_str} - График {plot_num + 1}/{num_plots}", 
                            pad=20, fontsize=14, fontweight='bold', color='#2c3e50')
                ax.set_xlabel("Сотрудники", fontsize=10, color='#2c3e50')
                ax.set_ylabel("Количество звонков", fontsize=10, color='#2c3e50')
                ax.set_xticks(x)
                ax.set_xticklabels(x_labels, rotation=45, ha='right')
                legend = ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0, frameon=True, facecolor='white', edgecolor='#95a5a6')
                for text in legend.get_texts():
                    text.set_color('#2c3e50')
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.spines['left'].set_color('#95a5a6')
                ax.spines['bottom'].set_color('#95a5a6')
                plt.subplots_adjust(right=0.85)
                plt.tight_layout()
                await send_plot(plt.gcf(), query.message.chat_id, context)
        
        await query.edit_message_text("✅ Графики отправлены!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )
    
    except Exception as e:
        logger.error(f"Ошибка при создании графика: {str(e)}")
        await query.edit_message_text(f"❌ Ошибка при создании графика: {str(e)}", 
                                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )

async def handle_table_format(query, context, all_stats, sheet_name=None):
    if not sheet_name:
        sheet_name = 'Все отделы'
    logger.info("Начало формирования таблицы")
    
    try:
        # Проверяем тип отчета
        report_type = context.user_data.get("report_type", "all")
        logger.info(f"Тип отчета в handle_table_format: {report_type}")
        
        # Получаем номер отдела из context.user_data
        dept_number = context.user_data.get("selected_dept_number", "all")
        logger.info(f"Номер отдела в handle_table_format: {dept_number}")
        
        # Получаем информацию о периоде
        period = context.user_data.get("period", "current_month")
        period_info = get_period_dates_info(period, context)
        
        # Проверяем наличие данных
        if not all_stats:
            logger.error("Нет данных для создания таблицы")
            await query.edit_message_text("❌ Нет данных для создания таблицы", 
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )
            return
        
        # Формируем таблицу
        message_parts = []
        current_part = f"📞 Статистика звонков по {sheet_name} ({period_info}):\n\n"
        
        # Группируем статистику по отделам
        departments = {}
        import re
        for stat in all_stats:
            if stat and isinstance(stat, dict) and 'Номер' in stat and not stat['Номер'].startswith('ИТОГО'):
                dept_raw = stat.get('Отдел')
                if dept_raw:
                    match = re.search(r'(\d+)', str(dept_raw))
                    if match:
                        dept = str(int(match.group(1)))
                        if dept not in departments:
                            departments[dept] = []
                        departments[dept].append(stat)
        
        if not departments:
            logger.error("Нет данных для группировки по отделам")
            await query.edit_message_text("❌ Нет данных для создания таблицы", 
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )
            return
        
        logger.info(f"Сгруппировано {len(departments)} отделов")
        
        # Фильтруем отделы, если выбран конкретный отдел
        if report_type != "all" and dept_number and dept_number != "all":
            if dept_number in departments:
                filtered_departments = {dept_number: departments[dept_number]}
                departments = filtered_departments
                logger.info(f"Отфильтрован отдел {dept_number} с {len(departments[dept_number])} сотрудниками")
            else:
                logger.error(f"Отдел {dept_number} не найден среди доступных отделов: {list(departments.keys())}")
                await query.edit_message_text(f"❌ Отдел {dept_number} не найден", 
                                         reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
                )
                return
        
        if report_type == "all":
            for dept_number, dept_stats in departments.items():
                num_employees = len(dept_stats) if dept_stats else 1
                dept_incoming = sum(s.get('Входящие 📞', 0) for s in dept_stats)
                dept_outgoing = sum(s.get('Исходящие 📤', 0) for s in dept_stats)
                dept_missed = sum(s.get('Пропущенные ❌', 0) for s in dept_stats)
                avg_incoming = round(dept_incoming / num_employees, 1) if num_employees else 0
                avg_outgoing = round(dept_outgoing / num_employees, 1) if num_employees else 0
                avg_missed = round(dept_missed / num_employees, 1) if num_employees else 0
                current_part += f"<b>Отдел {dept_number}</b>:\n"
                current_part += f"  Входящие: {dept_incoming} ({avg_incoming})\n"
                current_part += f"  Исходящие: {dept_outgoing} ({avg_outgoing})\n"
                current_part += f"  Пропущенные: {dept_missed} ({avg_missed})\n\n"
                if len(current_part) > 4000:
                    message_parts.append(current_part)
                    current_part = ""
        else:
            # Для отчета по отделам показываем детальную информацию по каждому номеру
            for dept_number, dept_stats in departments.items():
                current_part += f"<b>Отдел {dept_number}</b>:\n"
                for stat in dept_stats:
                    if 'Сотрудник' in stat and 'Номер' in stat:
                        current_part += f"  {stat['Сотрудник']} ({stat['Номер']}):\n"
                        current_part += f"    Входящие: {stat.get('Входящие 📞', 0)}\n"
                        current_part += f"    Исходящие: {stat.get('Исходящие 📤', 0)}\n"
                        current_part += f"    Пропущенные: {stat.get('Пропущенные ❌', 0)}\n"
                dept_incoming = sum(s.get('Входящие 📞', 0) for s in dept_stats)
                dept_outgoing = sum(s.get('Исходящие 📤', 0) for s in dept_stats)
                dept_missed = sum(s.get('Пропущенные ❌', 0) for s in dept_stats)
                current_part += f"\n<b>Итого по отделу {dept_number}:</b>\n"
                current_part += f"  Входящие: {dept_incoming}\n"
                current_part += f"  Исходящие: {dept_outgoing}\n"
                current_part += f"  Пропущенные: {dept_missed}\n\n"
                if len(current_part) > 4000:
                    message_parts.append(current_part)
                    current_part = ""
        
        if current_part:
            message_parts.append(current_part)
        
        logger.info(f"Таблица сформирована: {len(message_parts)} частей")
        
        # Проверяем, есть ли что отправлять
        if not message_parts:
            logger.error("Нет данных для отправки")
            await query.edit_message_text("❌ Не удалось сформировать таблицу", 
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )
            return
        
        # Отправляем сообщение частями
        if len(message_parts) > 1:
            for i, part in enumerate(message_parts):
                if i == 0:
                    await query.edit_message_text(part, parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
                )
                else:
                    await context.bot.send_message(chat_id=query.message.chat_id, text=part, parse_mode="HTML")
        else:
            await query.edit_message_text(message_parts[0], parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )
    
    except Exception as e:
        logger.error(f"Ошибка при формировании таблицы: {str(e)}")
        await query.edit_message_text(f"❌ Ошибка при формировании таблицы: {str(e)}", 
                                   reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )

def fetch_call_history(start_date: str, end_date: str, phone_number: str):
    """
    Получает историю звонков из API ВАТС.
    
    Args:
        start_date (str): Начальная дата в формате YYYY-MM-DD или YYYYMMDD
        end_date (str): Конечная дата в формате YYYY-MM-DD или YYYYMMDD
        phone_number (str): Номер телефона для запроса
        
    Returns:
        list: Список звонков или пустой список в случае ошибки
    """
    logger.info(f"Запрос истории звонков для номера {phone_number}, период {start_date} - {end_date}")
    headers = {"X-API-KEY": API_KEY, "Content-Type": "application/json"}
    
    # Форматируем номер телефона в формат +7XXXXXXXXXX
    phone = phone_number.strip()
    if phone and len(phone) >= 10:
        phone = re.sub(r'\D', '', phone)
        if len(phone) == 10:
            phone = '+7' + phone
        elif len(phone) == 11:
            if phone.startswith('7') or phone.startswith('8'):
                phone = '+7' + phone[1:]
            else:
                phone = '+' + phone
        else:
            phone = '+' + phone
        
        logger.debug(f"Отформатирован номер: {phone_number} -> {phone}")
    else:
        logger.error(f"Некорректный номер телефона: {phone_number}")
        return []
    
    try:
        # Преобразуем даты в объекты datetime
        if re.match(r'^\d{4}-\d{2}-\d{2}$', start_date) and re.match(r'^\d{4}-\d{2}-\d{2}$', end_date):
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        elif re.match(r'^\d{8}$', start_date) and re.match(r'^\d{8}$', end_date):
            start_date_obj = datetime.strptime(start_date, "%Y%m%d")
            end_date_obj = datetime.strptime(end_date, "%Y%m%d")
        else:
            start_date_obj = datetime.strptime(start_date, "%Y%m%d")
            end_date_obj = datetime.strptime(end_date, "%Y%m%d")
        
        # Устанавливаем время: начало дня для start_date, конец дня для end_date
        start_date_obj = start_date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_obj = end_date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # Форматируем даты в формат YYYYmmddTHHMMSSZ
        start_date_formatted = start_date_obj.strftime("%Y%m%dT%H%M%SZ")
        end_date_formatted = end_date_obj.strftime("%Y%m%dT%H%M%SZ")
        
        logger.info(f"Форматированные даты для API: {start_date_formatted} - {end_date_formatted}")
        
    except Exception as e:
        logger.error(f"Ошибка при форматировании дат: {str(e)}")
        return []
    
    params = {
        "start": start_date_formatted,
        "end": end_date_formatted,
        "diversion": phone,
        "type": "all",  # Получаем все типы звонков
        "limit": 1000
    }
    
    logger.info(f"Параметры запроса к API: {params}")
    
    try:
        response = requests.get(f"{API_URL}/history/json", headers=headers, params=params)
        
        # Добавляем полное логирование запроса и ответа
        logger.info(f"API запрос: {API_URL}/history/json")
        logger.info(f"Заголовки: {headers}")
        logger.info(f"Параметры: {params}")
        logger.info(f"Статус ответа: {response.status_code}")
        logger.info(f"Полный URL запроса: {response.url}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                # Проверяем, что data не None и является списком
                if data is None:
                    logger.info(f"API вернул пустой ответ для номера {phone}")
                    return []
                
                if not isinstance(data, list):
                    logger.error(f"Неожиданный формат ответа API: {type(data)}")
                    return []
                
                logger.info(f"Получено {len(data)} записей звонков для номера {phone}")
                
                # Подробная информация о типах звонков
                call_types = {}
                for call in data:
                    call_type = call.get('type', 'unknown')
                    call_types[call_type] = call_types.get(call_type, 0) + 1
                
                logger.info(f"Распределение звонков по типам: {call_types}")
                
                # Выводим пример данных для диагностики
                if data and len(data) > 0:
                    logger.info(f"Пример данных звонка: {data[0]}")
                    if 'start' in data[0]:
                        logger.info(f"Дата звонка в примере: {data[0]['start']}")
                else:
                    logger.info(f"Нет данных звонков в ответе API для номера {phone} за период {start_date_formatted} - {end_date_formatted}")
                
                return data
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка декодирования JSON: {str(e)}, содержимое ответа: {response.text[:500]}")
                return []
        else:
            try:
                logger.error(f"Полный ответ API с ошибкой: {response.text}")
                error_data = response.json()
                error_msg = error_data.get("message", "Неизвестная ошибка")
            except Exception:
                error_msg = f"HTTP-статус: {response.status_code}, содержимое: {response.text[:500]}"
            logger.error(f"Ошибка API: {response.status_code}, {error_msg}")
            return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к API: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Неизвестная ошибка: {str(e)}")
        return []

def process_json_data(json_data, account_display, start_date_str, end_date_str):
    """
    Обрабатывает данные звонков и формирует статистику.
    
    Args:
        json_data (list): Список звонков из API
        account_display (str): Отображаемое имя аккаунта
        start_date_str (str): Начальная дата в формате YYYY-MM-DD
        end_date_str (str): Конечная дата в формате YYYY-MM-DD
        
    Returns:
        tuple: (DataFrame со статистикой, список входящих номеров)
    """
    logger.info(f"Обработка данных для периода {start_date_str} - {end_date_str}")
    
    # Преобразуем строки дат в объекты datetime для корректного сравнения
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999)
    
    logger.info(f"Диапазон дат для фильтрации: {start_date} - {end_date}")
    
    # Фильтруем звонки по дате
    filtered_calls = []
    incoming_numbers = set()
    
    for call in json_data:
        try:
            # Преобразуем дату звонка в datetime
            call_date = datetime.strptime(call['start'], "%Y-%m-%d %H:%M:%S")
            
            # Проверяем, попадает ли звонок в указанный период
            if start_date <= call_date <= end_date:
                filtered_calls.append(call)
                # Добавляем входящий номер в множество
                if call.get('type') == 'incoming':
                    incoming_number = call.get('diversion', '')
                    if incoming_number:
                        incoming_numbers.add(incoming_number)
        except (KeyError, ValueError) as e:
            logger.error(f"Ошибка при обработке звонка: {str(e)}, данные звонка: {call}")
            continue
    
    logger.info(f"Отфильтровано звонков: {len(filtered_calls)} из {len(json_data)}")
    logger.info(f"Найдено уникальных входящих номеров: {len(incoming_numbers)}")
    
    # Создаем DataFrame из отфильтрованных звонков
    df = pd.DataFrame(filtered_calls)
    
    if df.empty:
        logger.warning("Нет данных для формирования статистики")
        return pd.DataFrame(), list(incoming_numbers)
    
    # Добавляем колонку с именем аккаунта
    df['account_display'] = account_display
    
    # Преобразуем даты в datetime
    df['start'] = pd.to_datetime(df['start'])
    df['end'] = pd.to_datetime(df['end'])
    
    # Рассчитываем длительность звонков в секундах
    df['duration'] = (df['end'] - df['start']).dt.total_seconds()
    
    # Группируем данные по типу звонка и рассчитываем статистику
    stats = df.groupby('type').agg({
        'duration': ['count', 'sum', 'mean'],
        'account_display': 'first'
    }).reset_index()
    
    # Переименовываем колонки
    stats.columns = ['type', 'count', 'total_duration', 'avg_duration', 'account_display']
    
    # Добавляем информацию о периоде
    stats['start_date'] = start_date_str
    stats['end_date'] = end_date_str
    
    logger.info(f"Сформирована статистика: {stats.to_dict('records')}")
    
    return stats, list(incoming_numbers)

async def send_excel(df, filename, chat_id, context: ContextTypes.DEFAULT_TYPE):
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)
    await context.bot.send_document(chat_id=chat_id, document=buffer, filename=filename)

async def send_plot(fig, chat_id, context: ContextTypes.DEFAULT_TYPE):
    buffer = BytesIO()
    fig.savefig(buffer, format="png", dpi=150)
    buffer.seek(0)
    await context.bot.send_photo(chat_id=chat_id, photo=buffer)
    plt.close(fig)

def get_period_dates(period, context):
    today = get_actual_now()
    
    if period == "today":
        # Сегодняшний день - расширяем интервал для полного захвата данных
        start_date = today.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = today.replace(hour=23, minute=59, second=59, microsecond=999999)
        # Форматируем даты сразу в строки в формате YYYYMMDD для последующего использования
        return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
    
    if period == "custom":
        # Извлекаем даты из context.user_data
        start_date_str = context.user_data.get("custom_start_date")
        end_date_str = context.user_data.get("custom_end_date")
        
        if not start_date_str or not end_date_str:
            raise ValueError("Не указаны даты для произвольного периода")
            
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            
            # Убираем проверки на будущие даты
            
            if end_date < start_date:
                raise ValueError("Дата окончания не может быть раньше даты начала")
                
            # Устанавливаем время для начала и конца дня
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # Форматируем даты в строки в формате YYYYMMDD для последующего использования
            return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
        except ValueError:
            raise ValueError("Неверный формат даты")
            
    elif period == "current_month":
        # Текущий месяц
        start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if today.month == 12:
            end_date = today.replace(year=today.year + 1, month=1, day=1, hour=23, minute=59, second=59) - timedelta(days=1)
        else:
            end_date = today.replace(month=today.month + 1, day=1, hour=23, minute=59, second=59) - timedelta(days=1)
        # Форматируем даты в строки в формате YYYYMMDD для последующего использования
        return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
        
    elif period == "previous_month":
        # Предыдущий месяц
        if today.month == 1:
            start_date = today.replace(year=today.year - 1, month=12, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_date = today.replace(year=today.year, month=1, day=1, hour=0, minute=0, second=0) - timedelta(seconds=1)
        else:
            start_date = today.replace(month=today.month - 1, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_date = today.replace(month=today.month, day=1, hour=0, minute=0, second=0) - timedelta(seconds=1)
        # Форматируем даты в строки в формате YYYYMMDD для последующего использования
        return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
        
    elif period == "week":
        # За последние 7 дней
        end_date = today.replace(hour=23, minute=59, second=59)
        start_date = (end_date - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        # Форматируем даты в строки в формате YYYYMMDD для последующего использования
        return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
        
    elif period == "month":
        # За последние 30 дней
        end_date = today.replace(hour=23, minute=59, second=59)
        start_date = (end_date - timedelta(days=29)).replace(hour=0, minute=0, second=0, microsecond=0)
        # Форматируем даты в строки в формате YYYYMMDD для последующего использования
        return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
    
    elif period.startswith("quarter_"):
        # Квартальные периоды: quarter_2024_1, quarter_2024_2, etc.
        parts = period.split("_")
        if len(parts) == 3:
            year = int(parts[1])
            quarter = int(parts[2])
            
            # Определяем месяцы квартала
            quarter_months = {
                1: (1, 3),   # Январь-Март
                2: (4, 6),   # Апрель-Июнь
                3: (7, 9),   # Июль-Сентябрь
                4: (10, 12)  # Октябрь-Декабрь
            }
            
            if quarter not in quarter_months:
                raise ValueError(f"Неверный квартал: {quarter}")
                
            start_month, end_month = quarter_months[quarter]
            
            # Начало квартала
            start_date = datetime(year, start_month, 1, 0, 0, 0)
            
            # Конец квартала
            if end_month == 12:
                end_date = datetime(year + 1, 1, 1, 23, 59, 59) - timedelta(seconds=1)
            else:
                end_date = datetime(year, end_month + 1, 1, 23, 59, 59) - timedelta(seconds=1)
                
            return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
        
    else:
        raise ValueError(f"Неизвестный период: {period}")

def get_period_dates_info(period, context):
    """Возвращает строку с информацией о периоде в читаемом формате"""
    try:
        # Получаем даты из get_period_dates, теперь в формате строк YYYYMMDD
        start_date_str, end_date_str = get_period_dates(period, context)
        
        # Преобразуем строки формата YYYYMMDD в объекты datetime для форматирования
        start_date = datetime.strptime(start_date_str, "%Y%m%d")
        end_date = datetime.strptime(end_date_str, "%Y%m%d")
        
        # Форматируем даты для человекочитаемого отображения
        start_str = start_date.strftime("%d.%m.%Y")
        end_str = end_date.strftime("%d.%m.%Y")
        
        if period == "today":
            return f"Сегодня: {start_str}"
        elif period == "current_month":
            return f"Текущий месяц: {start_str} - {end_str}"
        elif period == "previous_month":
            return f"Предыдущий месяц: {start_str} - {end_str}"
        elif period == "week":
            return f"Последние 7 дней: {start_str} - {end_str}"
        elif period == "month":
            return f"Последние 30 дней: {start_str} - {end_str}"
        elif period == "custom":
            return f"Произвольный период: {start_str} - {end_str}"
        elif period.startswith("quarter_"):
            # Квартальные периоды
            parts = period.split("_")
            if len(parts) == 3:
                year = int(parts[1])
                quarter = int(parts[2])
                quarter_names = {1: "I", 2: "II", 3: "III", 4: "IV"}
                quarter_name = quarter_names.get(quarter, str(quarter))
                return f"Квартал {quarter_name} {year}: {start_str} - {end_str}"
        else:
            return f"Период: {start_str} - {end_str}"
            
    except Exception as e:
        logger.error(f"Ошибка при получении информации о периоде: {str(e)}")
        return f"Период: {period}"

async def message_handler(update, context):
    # Проверяем доступ пользователя
    user_id = update.effective_user.id
    if user_id not in ALLOWED_USERS:
        logger.warning(f"Попытка несанкционированного доступа к message_handler от пользователя с ID {user_id}")
        await update.message.reply_text("⛔ Извините, у вас нет доступа к этому боту.")
        return
        
    message_text = update.message.text
    
    # Обрабатываем ожидание произвольных дат
    if context.user_data.get("waiting_for_dates", False):
        logger.info(f"Получен ввод дат: {message_text}")
        
        date_input_type = context.user_data.get("date_input_type", "range")
        
        if date_input_type == "range":
            # Разделяем сообщение на две даты
            date_parts = message_text.strip().split()
            
            if len(date_parts) != 2:
                await update.message.reply_text(
                    "❌ Пожалуйста, введите две даты в формате ДД.ММ.ГГГГ ДД.ММ.ГГГГ",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_custom_period")],
                        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_period_selection")]
                    ])
                )
                return
                
            start_date_str, end_date_str = date_parts
            
            # Проверяем формат дат
            try:
                start_date = datetime.strptime(start_date_str, "%d.%m.%Y")
                end_date = datetime.strptime(end_date_str, "%d.%m.%Y")
                
                # Убраем проверку на год в будущем
                
                if end_date < start_date:
                    await update.message.reply_text(
                        "❌ Дата окончания не может быть раньше даты начала",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_custom_period")],
                            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_period_selection")]
                        ])
                    )
                    return
                
                # Сохраняем даты в контексте в формате YYYY-MM-DD для API
                context.user_data["custom_start_date"] = start_date.strftime("%Y-%m-%d")
                context.user_data["custom_end_date"] = end_date.strftime("%Y-%m-%d")
                
                await update.message.reply_text(
                    f"✅ Период установлен: с {start_date_str} по {end_date_str}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_custom_period")],
                        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_period_selection")]
                    ])
                )
                
            except ValueError:
                await update.message.reply_text(
                    "❌ Неверный формат даты. Используйте формат ДД.ММ.ГГГГ ДД.ММ.ГГГГ",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_custom_period")],
                        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_period_selection")]
                    ])
                )
        
        else:  # date_input_type == "single"
            try:
                single_date = datetime.strptime(message_text.strip(), "%d.%m.%Y")
                
                # Убираем проверку на год в будущем
                
                # Сохраняем одну и ту же дату как начало и конец периода
                date_str = single_date.strftime("%Y-%m-%d")
                context.user_data["custom_start_date"] = date_str
                context.user_data["custom_end_date"] = date_str
                
                await update.message.reply_text(
                    f"✅ Дата установлена: {single_date.strftime('%d.%m.%Y')}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_custom_period")],
                        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_period_selection")]
                    ])
                )
                
            except ValueError:
                await update.message.reply_text(
                    "❌ Неверный формат даты. Используйте формат ДД.ММ.ГГГГ",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_custom_period")],
                        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_period_selection")]
                    ])
                )
    else:
        # Если мы не ожидаем ввода дат, предлагаем начать работу с ботом
        await start(update, context)

async def error_handler(update, context):
    """Обработчик ошибок для логирования"""
    logger.error(f"Update {update} вызвал ошибку {context.error}")
    
    # Отправляем сообщение пользователю об ошибке
    if update and update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"❌ Произошла ошибка: {str(context.error)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )

def fetch_call_details(start_date: str, end_date: str, phone_number: str):
    logger.info(f"Запрос детальной информации о звонках для номера {phone_number}, период {start_date} - {end_date}")
    headers = {"X-API-KEY": API_KEY, "Content-Type": "application/json"}
    
    # Форматируем номер телефона в формат +7XXXXXXXXXX
    phone = phone_number.strip()
    if phone and len(phone) >= 10:
        phone = re.sub(r'\D', '', phone)
        if len(phone) == 10:
            phone = '+7' + phone
        elif len(phone) == 11:
            if phone.startswith('7') or phone.startswith('8'):
                phone = '+7' + phone[1:]
            else:
                phone = '+' + phone
        else:
            phone = '+' + phone
        
        logger.debug(f"Отформатирован номер: {phone_number} -> {phone}")
    else:
        logger.error(f"Некорректный номер телефона: {phone_number}")
        return []
    
    # Проверяем и форматируем даты для API в формате YYYY-MM-DD
    start_date_formatted = start_date
    end_date_formatted = end_date
    
    try:
        # Если даты уже в формате YYYY-MM-DD, оставляем как есть
        if re.match(r'^\d{4}-\d{2}-\d{2}$', start_date) and re.match(r'^\d{4}-\d{2}-\d{2}$', end_date):
            start_date_formatted = start_date
            end_date_formatted = end_date
        # Если даты в формате YYYYMMDD, преобразуем в YYYY-MM-DD
        elif re.match(r'^\d{8}$', start_date) and re.match(r'^\d{8}$', end_date):
            start_date_formatted = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
            end_date_formatted = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
        else:
            # Пытаемся распознать формат и преобразовать
            try:
                start_date_obj = datetime.strptime(start_date, "%Y%m%d")
                end_date_obj = datetime.strptime(end_date, "%Y%m%d")
                start_date_formatted = start_date_obj.strftime("%Y-%m-%d")
                end_date_formatted = end_date_obj.strftime("%Y-%m-%d")
            except ValueError:
                logger.error(f"Неизвестный формат дат: {start_date}, {end_date}")
                # Если не удалось распознать формат, оставляем как есть
    except Exception as e:
        logger.error(f"Ошибка при форматировании дат: {str(e)}")
        # В случае любой ошибки используем исходные даты
    
    logger.info(f"Форматированные даты для API: {start_date_formatted} - {end_date_formatted}")
    
    params = {
        "start": start_date_formatted,
        "end": end_date_formatted,
        "diversion": phone,
        "limit": 1000
    }
    
    logger.info(f"Параметры запроса к API: {params}")
    
    try:
        response = requests.get(f"{API_URL}/history/json", headers=headers, params=params)
        
        # Добавляем полное логирование запроса и ответа
        logger.info(f"API запрос: {API_URL}/history/json")
        logger.info(f"Заголовки: {headers}")
        logger.info(f"Параметры: {params}")
        logger.info(f"Статус ответа: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                logger.info(f"Получено {len(data)} записей звонков для номера {phone}")
                
                # Логируем все данные по звонкам для периода текущего месяца или конкретной даты
                if 'current_month' in str(params) or '2025-04-07' in str(params) or '2025-04-08' in str(params):
                    logger.info(f"ПОЛНЫЙ ОТВЕТ API для даты 2025-04: {data}")
                
                # Подробная информация о типах звонков
                call_types = {}
                for call in data:
                    call_type = call.get('type', 'unknown')
                    call_types[call_type] = call_types.get(call_type, 0) + 1
                
                logger.info(f"Распределение звонков по типам: {call_types}")
                
                # Выводим пример данных для диагностики
                if data and len(data) > 0:
                    logger.info(f"Пример данных звонка: {data[0]}")
                else:
                    logger.warning(f"Нет данных звонков в ответе API для номера {phone} за период {start_date_formatted} - {end_date_formatted}")
                
                return data
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка декодирования JSON: {str(e)}, содержимое ответа: {response.text[:500]}")
                return []
        else:
            try:
                logger.error(f"Полный ответ API с ошибкой: {response.text}")
                error_data = response.json()
                error_msg = error_data.get("message", "Неизвестная ошибка")
            except Exception:
                error_msg = f"HTTP-статус: {response.status_code}, содержимое: {response.text[:500]}"
            logger.error(f"Ошибка API: {response.status_code}, {error_msg}")
            return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к API: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Неизвестная ошибка: {str(e)}")
        return []

async def handle_incoming_numbers_excel(query, context, sheet_type, dept_number, period):
    logger.info(f"Начало выгрузки входящих номеров: тип={sheet_type}, отдел={dept_number}, период={period}")
    
    try:
        # Имя листа для отображения и имени файла
        sheet_name = "Вторичка" if sheet_type == "vtorichka" else "Загородка"

        await query.edit_message_text("🔄 Получаю данные из Google Sheets...", reply_markup=None)
        
        # Получаем сотрудников из кэша
        employees = employee_provider.get_employees()
        filtered = employees
        
        if not filtered:
            logger.error("Не найдено сотрудников для отчета")
            await query.edit_message_text("❌ Нет данных для создания отчета", 
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
            )
            return

        # Получаем даты периода
        try:
            # Получаем строки дат в формате YYYYMMDD
            start_date_str, end_date_str = get_period_dates(period, context)
            logger.info(f"Получены даты периода: {start_date_str} - {end_date_str}")
            
            # Не преобразуем даты в этой функции, так как fetch_call_history
            # самостоятельно преобразует даты в нужный формат YYYY-MM-DD для API
            # Это гарантирует единообразие работы для всех типов периодов
            
            logger.info(f"Отправка запроса с датами: {start_date_str} - {end_date_str}")
                
        except ValueError as e:
            logger.error(f"Ошибка при определении периода: {str(e)}")
            await query.edit_message_text(f"❌ Ошибка при определении периода: {str(e)}", 
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
            )
            return
        
        # Проверка dept_number
        if dept_number == "None" or not dept_number or dept_number == "undefined":
            if context.user_data.get("selected_dept_number"):
                dept_number = context.user_data.get("selected_dept_number")
                logger.info(f"Восстановлен номер отдела из context.user_data['selected_dept_number']: {dept_number}")
            elif context.user_data.get("dept_number"):
                dept_number = context.user_data.get("dept_number")
                logger.info(f"Восстановлен номер отдела из context.user_data['dept_number']: {dept_number}")
        
        logger.info(f"Используемый номер отдела: {dept_number}")
        
        # Группируем сотрудников по отделам
        departments = {}
        for employee in filtered:
            dept = get_department_numbers(employee['department'])
            if dept:
                if dept not in departments:
                    departments[dept] = []
                if employee['sim'] and employee['sim'] != 'Нет данных':
                    departments[dept].append({
                        'phone': employee['sim'],
                        'name': f"{employee['last_name']} {employee['first_name']}",
                        'department': employee['department']
                    })
        
        if not departments:
            logger.error("Не найдено отделов с номерами")
            await query.edit_message_text("❌ Не найдено отделов с номерами", 
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
            )
            return

        # Проверяем существование отдела, если выбран конкретный отдел
        if dept_number != "all" and dept_number not in departments:
            logger.error(f"Отдел {dept_number} не найден среди доступных отделов: {list(departments.keys())}")
            await query.edit_message_text(f"❌ Отдел {dept_number} не найден", 
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
            )
            return

        await query.edit_message_text("🔄 Сбор данных о входящих и пропущенных звонках...", reply_markup=None)
        
        # Создаем одномерный список для хранения данных о входящих и пропущенных звонках
        all_incoming_numbers = []
        all_phones_processed = 0
        phones_with_data = 0
        total_calls_found = 0  # Общее количество найденных звонков
        total_incoming_calls = 0  # Количество входящих звонков
        total_missed_calls = 0  # Количество пропущенных звонков
        possible_phone_fields = ['phone', 'caller', 'caller_id', 'source', 'from', 'from_number', 'number', 'client']
        
        # Текущий год для проверки дат
        current_year = get_actual_now().year
        
        # Логируем информацию о параметрах запроса ВАТС
        logger.info(f"Тип формируемого отчета: входящие и пропущенные номера")
        logger.info(f"Параметры запроса к ВАТС - период: {period} ({start_date_str} - {end_date_str}), лист: {sheet_name}, отдел: {dept_number}")
        
        if dept_number == "all":
            # Для всех отделов собираем входящие номера по каждому отделу
            total_departments = len(departments)
            processed_departments = 0
            
            for dept_num, employees in departments.items():
                # Обновляем прогресс
                processed_departments += 1
                progress = (processed_departments / total_departments) * 100
                progress_bar = "█" * int(progress / 2) + "░" * (50 - int(progress / 2))
                await query.edit_message_text(
                    f"🔄 Сбор данных о входящих и пропущенных звонках...\n"
                    f"Прогресс: {progress_bar} {progress:.1f}%\n"
                    f"Обработано: {processed_departments}/{total_departments} отделов\n"
                    f"Телефонов с данными: {phones_with_data}/{all_phones_processed}\n"
                    f"Найдено звонков: {total_calls_found}, из них входящих: {total_incoming_calls}, пропущенных: {total_missed_calls}",
                    reply_markup=None
                )
                
                logger.info(f"Обрабатываем отдел {dept_num} с {len(employees)} сотрудниками")
                
                for employee in employees:
                    all_phones_processed += 1
                    logger.info(f"Запрос данных для {employee['name']} ({employee['phone']})")
                    calls = fetch_call_history(start_date_str, end_date_str, employee['phone'])
                    if not calls:
                        logger.info(f"Нет данных для {employee['phone']}")
                        continue
                        
                    total_calls_found += len(calls)
                    phones_with_data += 1
                    logger.info(f"Получено {len(calls)} звонков для {employee['phone']}")
                    
                    # Считаем типы звонков
                    call_types = {}
                    for call in calls:
                        call_type = call.get('type', 'unknown')
                        call_types[call_type] = call_types.get(call_type, 0) + 1
                    
                    logger.info(f"Распределение звонков по типам для {employee['phone']}: {call_types}")
                    
                    for call in calls:
                        # Логируем содержимое звонка для диагностики
                        logger.debug(f"Данные о звонке: {call}")
                        
                        # Ищем номер входящего звонка во всех возможных полях
                        incoming_number = None
                        for field in possible_phone_fields:
                            if field in call and call[field]:
                                incoming_number = call[field]
                                logger.debug(f"Найден номер в поле {field}: {incoming_number}")
                                break
                        
                        # Проверяем тип звонка
                        call_type = str(call.get('type', '')).lower()
                        
                        is_incoming = (
                            call_type in ['in', 'incoming', 'received', 'inbound', 'входящий'] or 
                            call.get('direction', '').lower() in ['in', 'incoming', 'received', 'inbound', 'входящий']
                        )
                        
                        is_missed = call_type in ['missed', 'пропущенный']
                        
                        # Обрабатываем входящие и пропущенные звонки
                        if incoming_number and (is_incoming or is_missed):
                            if is_incoming:
                                total_incoming_calls += 1
                                call_type_display = "Входящий 📞"
                            else:
                                total_missed_calls += 1
                                call_type_display = "Пропущенный ❌"
                                
                            logger.debug(f"Добавлен номер: {incoming_number}, тип: {call_type_display}")
                            
                            # Добавляем запись разговора, если она есть
                            record_url = call.get('record', '')
                            user_name = call.get('user_name', '')
                            
                            # Получаем и проверяем дату и время звонка
                            call_datetime = ""
                            if 'start' in call and call['start']:
                                try:
                                    # Парсим дату из строки, предполагая формат ISO
                                    raw_dt = call['start']
                                    logger.debug(f"Исходная дата из API: {raw_dt}")
                                    
                                    # Преобразуем из ISO формата в datetime
                                    dt = datetime.fromisoformat(raw_dt.replace('Z', '+00:00'))
                                    
                                    # Убираем проверку года в будущем
                                    
                                    # Форматируем дату в удобный для пользователя вид
                                    call_datetime = dt.strftime("%d.%m.%Y %H:%M")
                                    logger.debug(f"Преобразованная дата звонка: {call_datetime}")
                                except Exception as e:
                                    logger.error(f"Ошибка парсинга даты звонка: {str(e)}")
                                    call_datetime = str(call.get('start', ''))
                            
                            logger.debug(f"Ссылка на запись разговора: {record_url}")
                            logger.debug(f"Имя пользователя: {user_name}")
                            logger.debug(f"Дата и время звонка: {call_datetime}")
                            
                            all_incoming_numbers.append({
                                'Отдел/Номер': f"Отдел {dept_num}",
                                'Пользователь': user_name,
                                'Номер принявшего': employee['phone'],
                                'Входящий номер': incoming_number,
                                'Тип звонка': call_type_display,
                                'Дата звонка': call_datetime,
                                'Запись разговора': record_url
                            })
        else:
            # Для конкретного отдела собираем номера по каждому сотруднику
            employees = departments.get(dept_number, [])
            if not employees:
                logger.error(f"Нет сотрудников для отдела {dept_number}")
                await query.edit_message_text(f"❌ Нет сотрудников для отдела {dept_number}", 
                                           reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
                )
                return
            
            logger.info(f"Обрабатываем отдел {dept_number} с {len(employees)} сотрудниками")
            
            total_employees = len(employees)
            processed_employees = 0
            
            for employee in employees:
                # Обновляем прогресс
                processed_employees += 1
                all_phones_processed += 1
                progress = (processed_employees / total_employees) * 100
                progress_bar = "█" * int(progress / 2) + "░" * (50 - int(progress / 2))
                await query.edit_message_text(
                    f"🔄 Сбор данных о входящих и пропущенных звонках для отдела {dept_number}...\n"
                    f"Прогресс: {progress_bar} {progress:.1f}%\n"
                    f"Обработано: {processed_employees}/{total_employees} номеров\n"
                    f"Телефонов с данными: {phones_with_data}/{all_phones_processed}\n"
                    f"Найдено звонков: {total_calls_found}, из них входящих: {total_incoming_calls}, пропущенных: {total_missed_calls}",
                    reply_markup=None
                )
                
                logger.info(f"Запрос данных для {employee['name']} ({employee['phone']})")
                calls = fetch_call_history(start_date_str, end_date_str, employee['phone'])
                if not calls:
                    logger.info(f"Нет данных для {employee['phone']}")
                    continue
                
                total_calls_found += len(calls)
                phones_with_data += 1
                logger.info(f"Получено {len(calls)} звонков для {employee['phone']}")
                
                # Считаем типы звонков
                call_types = {}
                for call in calls:
                    call_type = call.get('type', 'unknown')
                    call_types[call_type] = call_types.get(call_type, 0) + 1
                
                logger.info(f"Распределение звонков по типам для {employee['phone']}: {call_types}")
                
                for call in calls:
                    # Логируем содержимое звонка для диагностики
                    logger.debug(f"Данные о звонке: {call}")
                    
                    # Ищем номер входящего звонка во всех возможных полях
                    incoming_number = None
                    for field in possible_phone_fields:
                        if field in call and call[field]:
                            incoming_number = call[field]
                            logger.debug(f"Найден номер в поле {field}: {incoming_number}")
                            break
                    
                    # Проверяем тип звонка
                    call_type = str(call.get('type', '')).lower()
                    
                    is_incoming = (
                        call_type in ['in', 'incoming', 'received', 'inbound', 'входящий'] or 
                        call.get('direction', '').lower() in ['in', 'incoming', 'received', 'inbound', 'входящий']
                    )
                    
                    is_missed = call_type in ['missed', 'пропущенный']
                    
                    # Обрабатываем входящие и пропущенные звонки
                    if incoming_number and (is_incoming or is_missed):
                        if is_incoming:
                            total_incoming_calls += 1
                            call_type_display = "Входящий 📞"
                        else:
                            total_missed_calls += 1
                            call_type_display = "Пропущенный ❌"
                            
                        logger.debug(f"Добавлен номер: {incoming_number}, тип: {call_type_display}")
                        
                        # Добавляем запись разговора, если она есть
                        record_url = call.get('record', '')
                        user_name = call.get('user_name', '')
                        
                        # Получаем и проверяем дату и время звонка
                        call_datetime = ""
                        if 'start' in call and call['start']:
                            try:
                                # Парсим дату из строки, предполагая формат ISO
                                raw_dt = call['start']
                                logger.debug(f"Исходная дата из API: {raw_dt}")
                                
                                # Преобразуем из ISO формата в datetime
                                dt = datetime.fromisoformat(raw_dt.replace('Z', '+00:00'))
                                
                                # Убираем проверку года в будущем
                                
                                # Форматируем дату в удобный для пользователя вид
                                call_datetime = dt.strftime("%d.%m.%Y %H:%M")
                                logger.debug(f"Преобразованная дата звонка: {call_datetime}")
                            except Exception as e:
                                logger.error(f"Ошибка парсинга даты звонка: {str(e)}")
                                call_datetime = str(call.get('start', ''))
                        
                        logger.debug(f"Ссылка на запись разговора: {record_url}")
                        logger.debug(f"Имя пользователя: {user_name}")
                        logger.debug(f"Дата и время звонка: {call_datetime}")
                        
                        employee_label = f"{employee['name']} ({employee['phone']})"
                        all_incoming_numbers.append({
                            'Отдел/Номер': employee_label,
                            'Пользователь': user_name,
                            'Номер принявшего': employee['phone'],
                            'Входящий номер': incoming_number,
                            'Тип звонка': call_type_display,
                            'Дата звонка': call_datetime,
                            'Запись разговора': record_url
                        })
        
        logger.info(f"Обработано телефонов: {all_phones_processed}, с данными: {phones_with_data}")
        logger.info(f"Найдено звонков: {total_calls_found}, из них входящих: {total_incoming_calls}, пропущенных: {total_missed_calls}")
        logger.info(f"Добавлено в отчет: {len(all_incoming_numbers)}")
        
        if not all_incoming_numbers:
            logger.error("Не найдено входящих или пропущенных номеров для создания Excel-файла")
            # Более информативное сообщение для пользователя
            await query.edit_message_text(
                f"❌ Не найдено входящих или пропущенных номеров для создания отчета\n\n"
                f"Проверено телефонов: {all_phones_processed}\n"
                f"Телефонов с данными: {phones_with_data}\n"
                f"Найдено звонков: {total_calls_found}, из них входящих: {total_incoming_calls}, пропущенных: {total_missed_calls}\n"
                f"Проверьте правильность выбранного периода: {get_period_dates_info(period, context)}", 
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
            )
            return
        
        # Создаем DataFrame из собранных данных
        df = pd.DataFrame(all_incoming_numbers)
        
        # Создаем Excel-файл с одним листом
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Входящие и пропущенные", index=False)
            
            # Делаем ссылки кликабельными
            workbook = writer.book
            worksheet = writer.sheets["Входящие и пропущенные"]
            
            # Применяем гиперссылки к столбцу "Запись разговора"
            for idx, url in enumerate(df["Запись разговора"], start=2):  # start=2 потому что Excel начинается с 1 и есть заголовок
                if url and isinstance(url, str) and (url.startswith("http://") or url.startswith("https://")):
                    cell = worksheet.cell(row=idx, column=df.columns.get_loc("Запись разговора") + 1)
                    cell.hyperlink = url
                    cell.style = "Hyperlink"
        
        buffer.seek(0)
        period_info = get_period_dates_info(period, context)
        # Формируем имя файла без спецсимволов
        filename = f"calls_{sheet_name.lower()}_{period_info.replace(':', '').replace(' ', '_').replace('/', '_')}.xlsx"
        
        await query.edit_message_text("🔄 Отправка Excel-файла...", reply_markup=None)
        await context.bot.send_document(chat_id=query.message.chat_id, document=buffer, filename=filename)
        await query.edit_message_text(f"✅ Excel-файл с входящими и пропущенными звонками отправлен ({period_info})!", 
                                   reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )
        
    except Exception as e:
        logger.error(f"Ошибка при выгрузке звонков: {str(e)}")
        await query.edit_message_text(f"❌ Произошла ошибка: {str(e)}", 
                                   reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )

async def handle_excel_format(query, context, df_stats, sheet_name, period):
    logger.info("Начало формирования Excel-файла")
    
    try:
        # Проверяем тип отчета
        report_type = context.user_data.get("report_type", "all")
        logger.info(f"Тип отчета в handle_excel_format: {report_type}")
        
        # Получаем номер отдела из context.user_data
        dept_number = context.user_data.get("selected_dept_number", "all")
        logger.info(f"Номер отдела в handle_excel_format: {dept_number}")
        
        # Получаем информацию о периоде
        period_info = get_period_dates_info(period, context)
        
        # Проверяем наличие данных
        if df_stats.empty:
            logger.error("Нет данных для создания Excel-файла")
            await query.edit_message_text("❌ Нет данных для создания Excel-файла", 
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
            )
            return
        
        # Фильтруем данные по отделу, если выбран конкретный отдел
        if report_type != "all" and dept_number and dept_number != "all":
            filtered_df = df_stats[df_stats['Отдел'] == dept_number]
            logger.info(f"Отфильтровано {len(filtered_df)} записей для отдела {dept_number}")
            
            if filtered_df.empty:
                logger.error(f"Нет данных для отдела {dept_number}")
                await query.edit_message_text(f"❌ Нет данных для отдела {dept_number}", 
                                           reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
                )
                return
            df_stats = filtered_df
        
        if report_type == "all":
            excel_df = pd.DataFrame()
            required_columns = ['Отдел', 'Входящие 📞', 'Исходящие 📤', 'Пропущенные ❌']
            for column in required_columns:
                if column not in df_stats.columns:
                    df_stats[column] = 0
            for dept in df_stats['Отдел'].unique():
                dept_data = df_stats[df_stats['Отдел'] == dept]
                dept_total = dept_data[['Входящие 📞', 'Исходящие 📤', 'Пропущенные ❌']].sum()
                num_employees = len(dept_data) if len(dept_data) else 1
                excel_df = pd.concat([excel_df, pd.DataFrame([{
                    'Отдел': dept,
                    'Входящие 📞': f"{dept_total['Входящие 📞']} ({round(dept_total['Входящие 📞']/num_employees, 1)})",
                    'Исходящие 📤': f"{dept_total['Исходящие 📤']} ({round(dept_total['Исходящие 📤']/num_employees, 1)})",
                    'Пропущенные ❌': f"{dept_total['Пропущенные ❌']} ({round(dept_total['Пропущенные ❌']/num_employees, 1)})"
                }])], ignore_index=True)
            total = df_stats[['Входящие 📞', 'Исходящие 📤', 'Пропущенные ❌']].sum()
            excel_df = pd.concat([excel_df, pd.DataFrame([{
                'Отдел': 'ИТОГО ВСЕГО',
                'Входящие 📞': total['Входящие 📞'],
                'Исходящие 📤': total['Исходящие 📤'],
                'Пропущенные ❌': total['Пропущенные ❌']
            }])], ignore_index=True)
        else:
            excel_df = df_stats.copy()
            # Переупорядочиваем колонки для отчета по отделам
            # Первым столбцом должна быть фамилия и имя сотрудника
            column_order = ['Сотрудник', 'Отдел', 'Входящие 📞', 'Исходящие 📤', 'Пропущенные ❌', 'Всего звонков']
            # Добавляем колонки, которых может не быть
            for col in column_order:
                if col not in excel_df.columns:
                    excel_df[col] = 0
            # Переупорядочиваем колонки
            excel_df = excel_df[column_order]
            
            for dept in df_stats['Отдел'].unique():
                dept_data = df_stats[df_stats['Отдел'] == dept]
                dept_total = dept_data[['Входящие 📞', 'Исходящие 📤', 'Пропущенные ❌']].sum()
                excel_df = pd.concat([excel_df, pd.DataFrame([{
                    'Сотрудник': f'ИТОГО {dept}',
                    'Отдел': dept,
                    'Входящие 📞': dept_total['Входящие 📞'],
                    'Исходящие 📤': dept_total['Исходящие 📤'],
                    'Пропущенные ❌': dept_total['Пропущенные ❌'],
                    'Всего звонков': dept_total['Входящие 📞'] + dept_total['Исходящие 📤'] + dept_total['Пропущенные ❌']
                }])], ignore_index=True)
            total = df_stats[['Входящие 📞', 'Исходящие 📤', 'Пропущенные ❌']].sum()
            excel_df = pd.concat([excel_df, pd.DataFrame([{
                'Сотрудник': 'ИТОГО ВСЕГО',
                'Отдел': '',
                'Входящие 📞': total['Входящие 📞'],
                'Исходящие 📤': total['Исходящие 📤'],
                'Пропущенные ❌': total['Пропущенные ❌'],
                'Всего звонков': total['Входящие 📞'] + total['Исходящие 📤'] + total['Пропущенные ❌']
            }])], ignore_index=True)
        
        # Добавляем информацию о периоде в имя файла
        filename = f"calls_stats_{sheet_name.lower()}_{period_info.replace(':', '').replace(' ', '_').replace('/', '_')}.xlsx"
        await send_excel(excel_df, filename, query.message.chat_id, context)
        await query.edit_message_text(f"✅ Excel-файл отправлен ({period_info})!", 
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )
    
    except Exception as e:
        logger.error(f"Ошибка при создании Excel-файла: {str(e)}")
        await query.edit_message_text(f"❌ Ошибка при создании Excel-файла: {str(e)}", 
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )

# Функции для работы с квартальными отчетами
async def show_year_selection(query, context):
    """Показать выбор года для квартального отчета"""
    current_year = datetime.now().year
    keyboard = []
    
    # Добавляем кнопки для последних 3 лет
    for year in range(current_year, current_year - 3, -1):
        keyboard.append([InlineKeyboardButton(f"📅 {year} год", callback_data=f"year:{year}")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])
    
    await query.edit_message_text(
        "📅 Выберите год для квартального отчета:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_quarter_selection(query, context, year):
    """Показать выбор квартала с цветными индикаторами"""
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_quarter = (current_month - 1) // 3 + 1
    
    keyboard = []
    
    for quarter in range(1, 5):
        # Определяем статус квартала
        if year < current_year:
            # Прошедший год - все кварталы зеленые
            icon = "✅"
        elif year == current_year:
            if quarter < current_quarter:
                # Прошедший квартал
                icon = "✅"
            elif quarter == current_quarter:
                # Текущий квартал
                icon = "🟢"
            else:
                # Будущий квартал
                icon = "❌"
        else:
            # Будущий год - все кварталы красные
            icon = "❌"
        
        quarter_name = f"{quarter} квартал"
        keyboard.append([InlineKeyboardButton(f"{icon} {quarter_name}", callback_data=f"quarter:{year}:{quarter}")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"quarter:{context.user_data.get('report_type', 'quarter_3sheets')}")])
    
    await query.edit_message_text(
        f"📊 Выберите квартал для {year} года:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def generate_quarter_report(query, context, year, quarter):
    """Генерировать квартальный отчет с реальными данными"""
    try:
        report_type = context.user_data.get("report_type")
        sheet_type = context.user_data.get("sheet_type", "")
        dept_number = context.user_data.get("dept_number", "all")
        
        # Создаем период для квартала
        period = f"quarter_{year}_{quarter}"
        
        logger.info(f"Генерация квартального отчета: год={year}, квартал={quarter}, отдел={dept_number}, лист={sheet_type}")
        
        # Используем handle_report_format для создания отчета по сотрудникам
        await handle_report_format(query, context, sheet_type, dept_number, period, "excel")
            
    except Exception as e:
        logger.error(f"Ошибка при генерации квартального отчета: {str(e)}")
        await query.edit_message_text(
            f"❌ Ошибка при генерации отчета: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )

# Добавляю/перемещаю функцию update_employees_command выше main
async def update_employees_command(update, context):
    user_id = update.effective_user.id
    if user_id not in ALLOWED_USERS:
        await update.message.reply_text("⛔ У вас нет доступа к этой команде.")
        return
    await update.message.reply_text("🔄 Обновляю кэш сотрудников...")
    try:
        employee_provider.update_cache(force=True)
        await update.message.reply_text("✅ Кэш сотрудников успешно обновлён!")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка обновления кэша: {e}")

def main():
    global bot_application
    
    application = Application.builder().token("8083344307:AAEwLJNPEoPRKxEUXJaXoHgqpTa6k3lA5_k").build()
    bot_application = application
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("update_employees", update_employees_command))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    application.add_error_handler(error_handler)
    
    async def on_startup(app):
        # Только обновление кэша сотрудников при старте
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, employee_provider.update_cache, True)
            logger.info("✅ Кэш сотрудников обновлён при старте бота")
        except Exception as e:
            logger.error(f"❌ Не удалось обновить кэш сотрудников при старте: {e}")
    
    application.post_init = on_startup
    
    application.run_polling()

if __name__ == "__main__":
    main() 