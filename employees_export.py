import requests
import pandas as pd
from datetime import datetime
import os

class EmployeeExporter:
    def __init__(self, api_token):
        self.api_token = api_token
        self.base_url = f"https://leto.yucrm.ru/api/v1/{api_token}/employees"
        self.excel_filename = "employees.xlsx"
    
    def get_employees_data(self):
        """Получение данных сотрудников через API"""
        all_employees = []
        page = 1
        limit = 1000  # максимальное количество на страницу
        
        while True:
            try:
                # Параметры запроса
                params = {
                    'page': page,
                    'limit': limit,
                    'mode': 'short',  # краткая информация
                    'order_by': 'last_name',
                    'order_dir': 'asc'
                }
                
                url = f"{self.base_url}/list"
                print(f"Запрос к: {url}")
                
                response = requests.get(url, params=params)
                
                if response.status_code != 200:
                    print(f"Ошибка HTTP {response.status_code}: {response.text}")
                    break
                
                data = response.json()
                
                # Отладочная информация
                print(f"Тип данных ответа: {type(data)}")
                print(f"Ключи в ответе: {list(data.keys()) if isinstance(data, dict) else 'Не словарь'}")
                
                # Извлекаем данные сотрудников
                employees = []
                if isinstance(data, dict):
                    if 'result' in data:
                        result = data['result']
                        print(f"Тип result: {type(result)}")
                        
                        if isinstance(result, list):
                            employees = result
                        elif isinstance(result, dict):
                            # Ищем список сотрудников в различных возможных ключах
                            for key in ['employees', 'list', 'data', 'items']:
                                if key in result and isinstance(result[key], list):
                                    employees = result[key]
                                    print(f"Найден список сотрудников в ключе '{key}'")
                                    break
                            
                            # Если не нашли список, возможно result содержит отдельные записи
                            if not employees:
                                print(f"Содержимое result: {result}")
                                # Возможно, каждый ключ в result - это отдельный сотрудник
                                if all(isinstance(v, dict) for v in result.values()):
                                    employees = list(result.values())
                                    print(f"Извлечены сотрудники как значения словаря: {len(employees)}")
                    elif 'data' in data:
                        employees = data['data'] if isinstance(data['data'], list) else []
                elif isinstance(data, list):
                    employees = data
                
                print(f"Найдено сотрудников на странице {page}: {len(employees)}")
                
                # Отладочная информация о структуре первого элемента
                if employees:
                    print(f"Тип первого элемента: {type(employees[0])}")
                    if isinstance(employees[0], dict):
                        print(f"Ключи первого сотрудника: {list(employees[0].keys())}")
                    else:
                        print(f"Первый элемент: {employees[0]}")
                
                if not employees:
                    print("Список сотрудников пуст, завершаем")
                    break
                    
                all_employees.extend(employees)
                
                # Если получили меньше чем limit, значит это последняя страница
                if len(employees) < limit:
                    break
                    
                page += 1
                
            except requests.exceptions.RequestException as e:
                print(f"Ошибка при запросе к API: {e}")
                break
            except Exception as e:
                print(f"Ошибка при обработке данных: {e}")
                import traceback
                traceback.print_exc()
                break
        
        print(f"Всего получено сотрудников: {len(all_employees)}")
        return all_employees
    
    def process_employee_data(self, employees):
        """Обработка данных сотрудников для Excel"""
        processed_data = []
        
        # Список номеров отделов, которые нас интересуют (с 1 по 18)
        target_departments = list(range(1, 19))  # [1, 2, 3, ..., 18]
        
        # Отладочная информация для 9 отдела
        dept_9_total = 0
        dept_9_active = 0
        dept_9_inactive = 0
        dept_9_no_dept = 0
        dept_9_wrong_format = 0
        
        for emp in employees:
            # Получаем информацию об отделе
            department_info = emp.get('department')
            department_num = None
            department_name = 'Не указан'
            
            if isinstance(department_info, dict):
                department_name = department_info.get('name', 'Не указан')
                
                # Извлекаем номер отдела из названия (например, "4 отдел" -> 4)
                # Извлекаем номер отдела из названия (поддерживаем разные форматы)
                if department_name and isinstance(department_name, str):
                    # Ищем число в названии отдела (поддерживаем разные форматы)
                    import re
                    # Ищем число в начале: "9 отдел", "9-й отдел"
                    match = re.match(r'^(\d+)\s*(-?й\s*)?отдел', department_name, re.IGNORECASE)
                    if not match:
                        # Ищем число после "отдел": "отдел 9", "Отдел 9"
                        match = re.search(r'отдел\s*(\d+)', department_name, re.IGNORECASE)
                    
                    if match:
                        try:
                            department_num = int(match.group(1))
                        except ValueError:
                            department_num = None
            
            # Отладка для 9 отдела
            if department_num == 9:
                dept_9_total += 1
                is_active = emp.get('is_active', True)
                print(f"9 отдел - Сотрудник: {emp.get('last_name', '')} {emp.get('first_name', '')}, Активен: {is_active}, Отдел: {department_name}")
                
                if is_active:
                    dept_9_active += 1
                else:
                    dept_9_inactive += 1
            elif department_name and '9' in str(department_name):
                dept_9_wrong_format += 1
                print(f"9 отдел (неправильный формат) - Сотрудник: {emp.get('last_name', '')} {emp.get('first_name', '')}, Отдел: {department_name}")
            
            # Пропускаем неактивных сотрудников
            if not emp.get('is_active', True):
                continue
            
            # Фильтруем только отделы с номерами от 1 до 18
            if department_num not in target_departments:
                continue
                
            # Получаем номер телефона
            phone = emp.get('phone', '') or emp.get('second_phone', '') or str(emp.get('id', ''))
            
            # Нормализуем название отдела для единообразия
            normalized_dept_name = f"{department_num} отдел" if department_num else department_name
            
            # Формируем данные для таблицы
            employee_data = {
                'Фамилия': emp.get('last_name', ''),
                'Имя': emp.get('first_name', ''),
                'Номер': phone,
                'Отдел': normalized_dept_name  # Используем нормализованное название
            }
            
            processed_data.append(employee_data)
        
        # Выводим статистику по 9 отделу
        print(f"\nСтатистика по 9 отделу:")
        print(f"  Всего найдено: {dept_9_total}")
        print(f"  Активных: {dept_9_active}")
        print(f"  Неактивных: {dept_9_inactive}")
        print(f"  Неправильный формат названия: {dept_9_wrong_format}")
        
        return processed_data
    
    def create_excel_file(self, employee_data):
        """Создание Excel файла"""
        if not employee_data:
            print("Нет данных для экспорта")
            return
        
        # Создаем DataFrame
        df = pd.DataFrame(employee_data)
        
        # Сортируем по отделам, затем по фамилии
        df = df.sort_values(['Отдел', 'Фамилия'], ascending=[True, True])
        
        # Сбрасываем индекс
        df = df.reset_index(drop=True)
        
        try:
            # Создаем Excel файл с форматированием
            with pd.ExcelWriter(self.excel_filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Сотрудники', index=False)
                
                # Получаем рабочий лист для форматирования
                worksheet = writer.sheets['Сотрудники']
                
                # Автоматическая ширина колонок
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
                
                # Добавляем информацию об обновлении
                info_sheet = writer.book.create_sheet('Информация')
                info_sheet['A1'] = 'Дата последнего обновления:'
                info_sheet['B1'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                info_sheet['A2'] = 'Количество активных сотрудников:'
                info_sheet['B2'] = len(df)
            
            print(f"Excel файл '{self.excel_filename}' успешно создан/обновлен")
            print(f"Экспортировано активных сотрудников: {len(df)}")
            
            # Показываем статистику по отделам
            dept_stats = df['Отдел'].value_counts()
            print("\nСтатистика по отделам:")
            for dept, count in dept_stats.items():
                print(f"  {dept}: {count} сотрудников")
            
        except Exception as e:
            print(f"Ошибка при создании Excel файла: {e}")
    
    def export_employees(self):
        """Основной метод экспорта"""
        print("Начинаем экспорт сотрудников...")
        
        # Получаем данные из API
        print("Получение данных из API...")
        employees = self.get_employees_data()
        
        if not employees:
            print("Не удалось получить данные сотрудников")
            return
        
        print(f"Получено {len(employees)} записей сотрудников")
        
        # Обрабатываем данные
        print("Обработка данных (фильтрация отделов 1-18)...")
        processed_data = self.process_employee_data(employees)
        
        if not processed_data:
            print("Нет активных сотрудников в отделах 1-18 для экспорта")
            return
        
        print(f"Найдено {len(processed_data)} сотрудников в отделах 1-18")
        # Создаем Excel файл
        print("Создание Excel файла...")
        self.create_excel_file(processed_data)
        
        print("Экспорт завершен!")

def main():
    # API токен
    API_TOKEN = "a4d4a75094d8f9d8597085ac0ac12a51"
    
    # Создаем экспортер и запускаем
    exporter = EmployeeExporter(API_TOKEN)
    exporter.export_employees()

if __name__ == "__main__":
    main()