import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from collecting_from_api.api_collector import YandexLocatorCollector
from utils.config import SEARCH_OBJECTS, TEST_COORDINATES

def main():
    api_key = os.getenv('YANDEX_LOCATOR_API_KEY')
    if not api_key:
        print("API ключ не найден в .env файле")
        print("Добавьте YANDEX_LOCATOR_API_KEY=ваш_ключ в файл .env")
        return
    
    print(f"API ключ обнаружен: {api_key[:10]}...")
    
    collector = YandexLocatorCollector()
    
    print("\nТест")
    
    test_results = []
    for name, coords in TEST_COORDINATES[:1]:
        print(f"\nТест: {name}")
        lon, lat = map(float, coords.split(','))
        objects = collector.find_by_coordinates(lat, lon, radius=500)
        test_results.extend(objects)
        time.sleep(1)
    
    if test_results:
        print(f"\nТестовый поиск успешен. Найдено объектов: {len(test_results)}")
    
    print("\nсбор данных")
    
    all_objects = collector.collect_multiple_searches(SEARCH_OBJECTS)

    if all_objects:
        collector.save_to_csv(
            all_objects,
            'collecting_from_api/data/yandex_locator_data.csv'
        )
        
        # Дополнительная информация
        df = pd.DataFrame(all_objects)
        print("\nстатистика")
        print(f"Всего уникальных объектов: {len(df)}")
        print(f"Категории поиска: {df['search_query'].unique()}")
        print(f"Города: {df['search_query'].value_counts()}")
    else:
        print("Не удалось собрать данные. так что проверьте API ключ и параметры.")

if __name__ == "__main__":
    import pandas as pd
    import time
    main()