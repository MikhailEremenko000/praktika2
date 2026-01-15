# import requests
import pandas as pd
import time
import json
import os
from typing import Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class APIDataCollector:
    
    def __init__(self, api_name: str):
        self.api_name = api_name
        self.session = requests.Session()
        self.setup_session()
    
    def setup_session(self):
        self.session.headers.update({
            'User-Agent': 'StudentDataCollector/1.0',
            'Accept': 'application/json',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8'
        })
    
    def get_base_url(self) -> str:
        from utils.config import API_CONFIG
        return API_CONFIG[self.api_name]['base_url']
    
    def get_api_key(self) -> str:
        return os.getenv('YANDEX_LOCATOR_API_KEY')
    
    def make_request(self, params: Dict = None) -> Dict:

        try:
            url = self.get_base_url()
            
            base_params = {
                'apikey': self.get_api_key(),
                'format': 'json',
                'lang': 'ru_RU'
            }
            
            if params:
                base_params.update(params)
            
            response = self.session.get(url, params=base_params, timeout=15)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе: {e}")
            if hasattr(e.response, 'text'):
                print(f"Ответ сервера: {e.response.text}")
            return {}
    
    def save_to_csv(self, data: List[Dict], filename: str):
        if not data:
            print("Нет данных для сохранения")
            return
        
        df = pd.DataFrame(data)
        
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Данные сохранены в {filename}")
        print(f"Всего записей: {len(df)}")
        
        print("\nПервые 5 записей:")
        print(df.head().to_string())
        print(f"\nКолонки: {', '.join(df.columns)}")


class YandexLocatorCollector(APIDataCollector):
    
    def __init__(self):
        super().__init__('yandex_locator')
    
    def find_by_coordinates(self, lat: float, lon: float, 
                           radius: int = 1000, 
                           results: int = 50) -> List[Dict]:
        params = {
            'll': f'{lon},{lat}',
            'spn': f'{radius/111000:.6f},{radius/111000:.6f}',
            'results': results,
            'type': 'biz'
        }
        
        data = self.make_request(params)
        return self._parse_locator_data(data, lat, lon)
    
    def find_by_query(self, query: str, lat: float, lon: float,
                     radius: int = 1000) -> List[Dict]:
        params = {
            'll': f'{lon},{lat}',
            'spn': f'{radius/111000:.6f},{radius/111000:.6f}',
            'text': query,
            'type': 'biz',
            'results': 50
        }
        
        data = self.make_request(params)
        return self._parse_locator_data(data, lat, lon, query)
    
    def _parse_locator_data(self, data: Dict, lat: float, lon: float, 
                           query: str = None) -> List[Dict]:
        objects = []
        
        if not data or 'results' not in data:
            print("Нет данных в ответе или некорректный ответ")
            return objects
        
        for item in data['results']:
            try:
                obj_data = {
                    'search_query': query or 'coordinates_search',
                    'search_lat': lat,
                    'search_lon': lon,
                    'object_name': item.get('name', 'Неизвестно'),
                    'object_address': item.get('address', 'Нет адреса'),
                    'object_lat': item.get('lat'),
                    'object_lon': item.get('lon'),
                    'object_type': ', '.join(item.get('type', [])),
                    'object_category': item.get('category', 'Неизвестно'),
                    'distance_meters': item.get('distance', 0),
                    'rating': item.get('rating', {}).get('value', 0),
                    'reviews_count': item.get('rating', {}).get('count', 0),
                    'working_hours': item.get('working_hours', 'Неизвестно'),
                    'phone': item.get('phone', 'Нет телефона'),
                    'url': item.get('url', 'Нет ссылки'),
                    'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                objects.append(obj_data)
                
            except Exception as e:
                print(f"Ошибка при парсинге объекта: {e}")
                continue
        
        print(f"Найдено объектов: {len(objects)}")
        return objects
    
    def collect_multiple_searches(self, searches: List[Dict]) -> List[Dict]:
        all_objects = []
        
        print("Начинается сбор данных через Яндекс.Локатор")
        
        for i, search in enumerate(searches, 1):
            print(f"\n[{i}/{len(searches)}] Поиск: {search['name']}")
            print(f"Запрос: '{search['query']}' в районе {search['ll']}")
            
            lon, lat = map(float, search['ll'].split(','))
            
            objects = self.find_by_query(
                query=search['query'],
                lat=lat,
                lon=lon,
                radius=2000
            )
            
            if objects:
                all_objects.extend(objects)
                print(f"Найдено: {len(objects)} объектов")
            
            time.sleep(1.5)
        
        print(f"\nИтого собрано объектов: {len(all_objects)}")
        return all_objects