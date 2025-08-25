#!/usr/bin/env python3
"""
Финальный скрипт для замены всех оставшихся вызовов query.edit_message_text
"""

def fix_all_remaining():
    """Заменяет все оставшиеся вызовы edit_message_text на safe_edit_message"""
    
    # Читаем файл
    with open('broker_call_bot.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Заменяем все оставшиеся вызовы
    replacements = [
        ('await query.edit_message_text("❌ Нет данных для создания отчета"', 'await safe_edit_message(query, "❌ Нет данных для создания отчета"'),
        ('await query.edit_message_text("📊 График отправлен!"', 'await safe_edit_message(query, "📊 График отправлен!"'),
        ('await query.edit_message_text("❌ Нет данных для создания Excel-файла"', 'await safe_edit_message(query, "❌ Нет данных для создания Excel-файла"'),
        ('await query.edit_message_text(f"❌ Нет данных для отдела {dept_number}"', 'await safe_edit_message(query, f"❌ Нет данных для отдела {dept_number}"'),
        ('await query.edit_message_text("❌ Не указан номер отдела для отчета"', 'await safe_edit_message(query, "❌ Не указан номер отдела для отчета"'),
        ('await query.edit_message_text(f"❌ Отдел {dept_number} не найден"', 'await safe_edit_message(query, f"❌ Отдел {dept_number} не найден"'),
        ('await query.edit_message_text("❌ Не удалось получить данные сотрудников"', 'await safe_edit_message(query, "❌ Не удалось получить данные сотрудников"'),
        ('await query.edit_message_text("❌ Нет данных сотрудников"', 'await safe_edit_message(query, "❌ Нет данных сотрудников"'),
        ('await query.edit_message_text("❌ Нет входящих звонков за указанный период"', 'await safe_edit_message(query, "❌ Нет входящих звонков за указанный период"'),
    ]
    
    count = 0
    for old, new in replacements:
        if old in content:
            content = content.replace(old, new)
            count += 1
    
    # Записываем обратно
    with open('broker_call_bot.py', 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✅ Заменено {count} вызовов edit_message_text на safe_edit_message")

if __name__ == "__main__":
    fix_all_remaining()
