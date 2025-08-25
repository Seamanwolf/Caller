#!/usr/bin/env python3
"""
Скрипт для запуска бота с предварительной проверкой
"""

import os
import sys
import subprocess
from dotenv import load_dotenv

def check_environment():
    """Проверка переменных окружения"""
    print("🔍 Проверка переменных окружения...")
    
    # Загружаем переменные окружения
    load_dotenv()
    
    required_vars = [
        "API_KEY",
        "API_URL", 
        "TELEGRAM_BOT_TOKEN",
        "EMPLOYEE_API_TOKEN",
        "ALLOWED_USERS",
        "AUTO_REPORT_USER_ID"
    ]
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("📝 Создайте файл .env на основе env_example.txt")
        return False
    
    print("✅ Переменные окружения настроены")
    return True

def check_dependencies():
    """Проверка зависимостей"""
    print("🔍 Проверка зависимостей...")
    
    try:
        import requests
        import pandas
        import matplotlib
        from telegram import Bot
        from employee_data_provider import EmployeeDataProvider
        print("✅ Все зависимости установлены")
        return True
    except ImportError as e:
        print(f"❌ Отсутствуют зависимости: {e}")
        print("📦 Установите зависимости: pip install -r requirements.txt")
        return False

def run_tests():
    """Запуск тестов"""
    print("🔍 Запуск тестов...")
    
    try:
        result = subprocess.run([sys.executable, "test_bot.py"], 
                              capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print("✅ Тесты пройдены")
            return True
        else:
            print("❌ Тесты не пройдены")
            print("Вывод тестов:")
            print(result.stdout)
            if result.stderr:
                print("Ошибки:")
                print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Тесты превысили время ожидания")
        return False
    except Exception as e:
        print(f"❌ Ошибка запуска тестов: {e}")
        return False

def start_bot():
    """Запуск бота"""
    print("🚀 Запуск бота...")
    
    try:
        # Запускаем бота
        subprocess.run([sys.executable, "broker_call_bot.py"])
    except KeyboardInterrupt:
        print("\n⏹ Остановка бота...")
    except Exception as e:
        print(f"❌ Ошибка запуска бота: {e}")

def main():
    """Основная функция"""
    print("🤖 Запуск бота статистики звонков")
    print("=" * 40)
    
    # Проверяем переменные окружения
    if not check_environment():
        sys.exit(1)
    
    # Проверяем зависимости
    if not check_dependencies():
        sys.exit(1)
    
    # Запускаем тесты (опционально)
    if len(sys.argv) > 1 and sys.argv[1] == "--skip-tests":
        print("⏭ Пропуск тестов")
    else:
        if not run_tests():
            print("⚠️ Тесты не пройдены, но продолжаем запуск...")
            response = input("Продолжить запуск? (y/N): ")
            if response.lower() != 'y':
                sys.exit(1)
    
    print("\n" + "=" * 40)
    start_bot()

if __name__ == "__main__":
    main()
