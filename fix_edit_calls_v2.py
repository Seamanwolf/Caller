#!/usr/bin/env python3
"""
Улучшенный скрипт для замены всех вызовов query.edit_message_text на safe_edit_message
"""

import re

def fix_edit_calls():
    """Заменяет все оставшиеся вызовы edit_message_text на safe_edit_message"""
    
    # Читаем файл
    with open('broker_call_bot.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Счетчик замен
    replacements = 0
    
    # Паттерны для замены (более точные)
    patterns = [
        # Простые вызовы с обычными строками
        (r'await query\.edit_message_text\("([^"]+)"\)', r'await safe_edit_message(query, "\1")'),
        
        # Вызовы с f-строками
        (r'await query\.edit_message_text\(f"([^"]+)"\)', r'await safe_edit_message(query, f"\1")'),
        
        # Вызовы с обычными строками и reply_markup
        (r'await query\.edit_message_text\("([^"]+)", reply_markup=([^)]+)\)', r'await safe_edit_message(query, "\1", reply_markup=\2)'),
        
        # Вызовы с f-строками и reply_markup
        (r'await query\.edit_message_text\(f"([^"]+)", reply_markup=([^)]+)\)', r'await safe_edit_message(query, f"\1", reply_markup=\2)'),
        
        # Вызовы с обычными строками и parse_mode
        (r'await query\.edit_message_text\("([^"]+)", parse_mode=([^)]+)\)', r'await safe_edit_message(query, "\1", parse_mode=\2)'),
        
        # Вызовы с f-строками и parse_mode
        (r'await query\.edit_message_text\(f"([^"]+)", parse_mode=([^)]+)\)', r'await safe_edit_message(query, f"\1", parse_mode=\2)'),
        
        # Вызовы с обычными строками, reply_markup и parse_mode
        (r'await query\.edit_message_text\("([^"]+)", reply_markup=([^,]+), parse_mode=([^)]+)\)', r'await safe_edit_message(query, "\1", reply_markup=\2, parse_mode=\3)'),
        
        # Вызовы с f-строками, reply_markup и parse_mode
        (r'await query\.edit_message_text\(f"([^"]+)", reply_markup=([^,]+), parse_mode=([^)]+)\)', r'await safe_edit_message(query, f"\1", reply_markup=\2, parse_mode=\3)'),
        
        # Вызовы с переменными (без кавычек)
        (r'await query\.edit_message_text\(([^,]+)\)', r'await safe_edit_message(query, \1)'),
        
        # Вызовы с переменными и reply_markup
        (r'await query\.edit_message_text\(([^,]+), reply_markup=([^)]+)\)', r'await safe_edit_message(query, \1, reply_markup=\2)'),
    ]
    
    # Применяем замены
    modified_content = content
    for pattern, replacement in patterns:
        new_content = re.sub(pattern, replacement, modified_content)
        if new_content != modified_content:
            replacements += 1
        modified_content = new_content
    
    # Записываем обратно
    with open('broker_call_bot.py', 'w', encoding='utf-8') as f:
        f.write(modified_content)
    
    print(f"✅ Заменено {replacements} вызовов edit_message_text на safe_edit_message")

if __name__ == "__main__":
    fix_edit_calls()
