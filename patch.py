import datetime
from broker_call_bot import fetch_call_history

# Список сотрудников: (ФИО, номер)
employees = [
    ("Мурашко Александр", "79384880338"),
    ("Панфёрова Мария", "79384184614"),
    ("Подъячев Тимофей", "79282335104"),
    ("Рыжов Станислав", "79053000331"),
    ("Слюсарь Анастасия", "79384520094"),
    ("Суржиков Николай", "79282330810"),
    ("Тимощенко Алена", "79384184665"),
    ("Черных Наталья", "79388745601"),
    ("Шевцов Антон", "79384520093"),
]

# Период: сегодня и вчера
today = datetime.datetime.now().date()
yesterday = today - datetime.timedelta(days=1)
start_date = yesterday.strftime("%Y-%m-%d")
end_date = today.strftime("%Y-%m-%d")

for name, phone in employees:
    print(f"\n--- {name} ({phone}) ---")
    try:
        calls = fetch_call_history(start_date, end_date, phone)
    except Exception as e:
        print(f"Ошибка запроса: {e}")
        continue
    if not calls:
        print("  Нет звонков")
        continue
    print(f"  Всего звонков: {len(calls)}")
    for i, call in enumerate(calls, 1):
        call_type = call.get('type', 'нет type')
        status = call.get('status', 'нет status')
        direction = call.get('direction', '')
        start = call.get('start', '')
        print(f"    {i}. type: {call_type}, status: {status}, direction: {direction}, start: {start}")
        # Если нужно — раскомментируйте для полного вывода:
        # print(call)
print("\nГотово! Скопируйте этот вывод и пришлите мне.")