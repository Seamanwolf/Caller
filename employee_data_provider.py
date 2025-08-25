import requests
from datetime import datetime, timedelta
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class EmployeeDataProvider:
    def __init__(self, api_token, cache_ttl_minutes=10):
        self.api_token = api_token
        self.base_url = f"https://leto.yucrm.ru/api/v1/{api_token}/employees"
        self._cache = []
        self._cache_time = None
        self._cache_ttl = timedelta(minutes=cache_ttl_minutes)
        # Заменяем Lock на RLock для избежания дедлока
        self._lock = threading.RLock()

        # Надёжные HTTP-клиент/таймауты/ретраи
        self._session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        self._timeout = (10, 30)  # (connect, read)

    def _fetch_employees(self):
        all_employees = []
        page = 1
        limit = 1000
        while True:
            params = {
                'page': page,
                'limit': limit,
                'mode': 'short',
                'order_by': 'last_name',
                'order_dir': 'asc'
            }
            url = f"{self.base_url}/list"
            try:
                # Используем сессию с таймаутами и ретраями
                response = self._session.get(url, params=params, timeout=self._timeout)
                if response.status_code != 200:
                    break
                data = response.json()
                employees = []
                if isinstance(data, dict):
                    if 'result' in data:
                        result = data['result']
                        if isinstance(result, list):
                            employees = result
                        elif isinstance(result, dict):
                            for key in ['employees', 'list', 'data', 'items']:
                                if key in result and isinstance(result[key], list):
                                    employees = result[key]
                                    break
                            if not employees and all(isinstance(v, dict) for v in result.values()):
                                employees = list(result.values())
                    elif 'data' in data:
                        employees = data['data'] if isinstance(data['data'], list) else []
                elif isinstance(data, list):
                    employees = data
                if not employees:
                    break
                all_employees.extend(employees)
                if len(employees) < limit:
                    break
                page += 1
            except Exception:
                break
        return all_employees

    def _process_employees(self, employees):
        processed = []
        target_departments = list(range(1, 19))
        for emp in employees:
            department_info = emp.get('department')
            department_num = None
            department_name = 'Не указан'
            if isinstance(department_info, dict):
                department_name = str(department_info.get('name', 'Не указан') or 'Не указан')
                import re
                match = re.match(r'^(\d+)\s*(-?й\s*)?отдел', department_name, re.IGNORECASE)
                if not match:
                    match = re.search(r'отдел\s*(\d+)', department_name, re.IGNORECASE)
                if match:
                    try:
                        department_num = int(match.group(1))
                    except ValueError:
                        department_num = None
            if not emp.get('is_active', True):
                continue
            if department_num not in target_departments:
                continue
            phone = emp.get('phone', '') or emp.get('second_phone', '') or str(emp.get('id', ''))
            normalized_dept_name = f"{department_num}" if department_num else department_name
            processed.append({
                'last_name': emp.get('last_name', ''),
                'first_name': emp.get('first_name', ''),
                'department': normalized_dept_name,
                'sim': phone
            })
        return processed

    def update_cache(self, force=False):
        with self._lock:
            now = datetime.now()
            if not force and self._cache_time and now - self._cache_time < self._cache_ttl:
                return  # Кэш ещё актуален
        
        # Выносим сетевой запрос из-под лока
        employees = self._fetch_employees()
        
        with self._lock:
            self._cache = self._process_employees(employees)
            self._cache_time = now

    def get_employees(self):
        now = datetime.now()
        # Сначала проверяем, нужен ли рефреш
        with self._lock:
            needs_refresh = (
                not self._cache or
                not self._cache_time or
                (now - self._cache_time > self._cache_ttl)
            )

        if needs_refresh:
            # Обновляем кэш отдельно — update_cache сама возьмёт RLock
            self.update_cache(force=True)

        # Затем возвращаем копию кэша
        with self._lock:
            employees = list(self._cache)
            import re
            print('DEBUG: Отделы в кэше:', set(e['department'] for e in employees))
            print('DEBUG: Сотрудники отдела 9:', [e for e in employees if re.search(r'(\b9\b|^9$)', str(e['department']))])
            return employees

    def get_departments(self):
        employees = self.get_employees()
        departments = {}
        for emp in employees:
            dept = emp['department']
            if dept not in departments:
                departments[dept] = []
            departments[dept].append(emp)
        return departments 