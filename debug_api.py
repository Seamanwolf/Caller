#!/usr/bin/env python3
"""
Скрипт для отладки API запросов
"""

import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

def test_api_directly():
    """Прямое тестирование API"""
    print("🔍 Прямое тестирование API...")
    
    API_KEY = os.getenv("API_KEY")
    API_URL = os.getenv("API_URL")
    
    print(f"API_KEY: {API_KEY[:20]}...")
    print(f"API_URL: {API_URL}")
    
    # Тестовые данные
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    test_phone = "79384523568"
    
    print(f"📅 Период: {start_date} - {end_date}")
    print(f"📞 Телефон: {test_phone}")
    
    # Вариант 1: Параметры в URL
    url1 = f"{API_URL}/calls"
    params1 = {
        'api_key': API_KEY,
        'start_date': start_date,
        'end_date': end_date,
        'phone': test_phone,
        'limit': 1000
    }
    
    print(f"\n🔍 Тест 1: Параметры в URL")
    print(f"URL: {url1}")
    print(f"Params: {params1}")
    
    try:
        response1 = requests.get(url1, params=params1, timeout=10)
        print(f"Status: {response1.status_code}")
        print(f"Response: {response1.text[:200]}...")
    except Exception as e:
        print(f"Error: {e}")
    
    # Вариант 2: API ключ в заголовке
    url2 = f"{API_URL}/calls"
    params2 = {
        'start_date': start_date,
        'end_date': end_date,
        'phone': test_phone,
        'limit': 1000
    }
    headers2 = {
        'Authorization': f'Bearer {API_KEY}',
        'Content-Type': 'application/json'
    }
    
    print(f"\n🔍 Тест 2: API ключ в заголовке")
    print(f"URL: {url2}")
    print(f"Params: {params2}")
    print(f"Headers: {headers2}")
    
    try:
        response2 = requests.get(url2, params=params2, headers=headers2, timeout=10)
        print(f"Status: {response2.status_code}")
        print(f"Response: {response2.text[:200]}...")
    except Exception as e:
        print(f"Error: {e}")
    
    # Вариант 3: API ключ в URL
    url3 = f"{API_URL}/calls?api_key={API_KEY}"
    params3 = {
        'start_date': start_date,
        'end_date': end_date,
        'phone': test_phone,
        'limit': 1000
    }
    
    print(f"\n🔍 Тест 3: API ключ в URL")
    print(f"URL: {url3}")
    print(f"Params: {params3}")
    
    try:
        response3 = requests.get(url3, params=params3, timeout=10)
        print(f"Status: {response3.status_code}")
        print(f"Response: {response3.text[:200]}...")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_api_directly()
