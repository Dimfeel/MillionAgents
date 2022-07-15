import requests
import time
import enum
import pandas as pd
import math

from typing import List
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium import webdriver

class DetMirParser():
    """
    Парсит каталог https://www.detmir.ru/catalog/index/name/lego/ 
    и сохраняет в *.csv файл
    """
    
    class City(enum.Enum):
        RU_MOW = "Москва"
        RU_SPE = "Санкт-Петербург"
    
    # Адрес каталога, из которого считываются данные
    URL: str = "https://www.detmir.ru/catalog/index/name/lego/"
    # Адрес endpointа в API, 
    API_URL: str = "https://api.detmir.ru/v2/products"
    # Путь для сохранения данных
    CSV_PATH: str = "./data.csv"
    
    # Количество попыток считать определенную страницу в каталоге
    COUNT_ATTEMPTS = 10
    
    # Задержка между считываниями каталога
    SLEEP_TIME = 10
    
    # Путь к chromedriver для генерации cookies 
    CHROMEDRIVER_PATH: str
    
    #Словарь cookies
    cookies: dict
    
    def __init__(self):
        """
        Скачивает chromedriver и записывает путь к нему в CHROMEDRIVER_PATH
        """
        self.CHROMEDRIVER_PATH = ChromeDriverManager(path="./").install()
    
        
    def get_cookies(self) -> None:
        """Обновляет cookies в переменной self.cookies"""
        
        options = webdriver.ChromeOptions()

        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--headless")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Chrome(service=Service(self.CHROMEDRIVER_PATH), options=options)
        
        # Открываем нужный нам url
        driver.get(self.URL)
        
        # Запоминаем cookies этого url
        self.cookies = dict()
        for item in driver.get_cookies():
            self.cookies[item['name']] = item['value']
        
    
    def get_item_data(self, city: City, item: dict) -> tuple:
        """Считывает необходимые данные о товаре и записывает в tuple"""
        
        id = item["id"]
        title = item["title"]
        url = f"https://www.detmir.ru/product/index/id/{id}/"
        city = city.value
        if "old_price" in item and item["old_price"] is not None:
            price = item["old_price"]["price"]
            promo_price = item["price"]["price"]
        else:
            price = item["price"]["price"]
            promo_price = ""
            
        return (id, title, price, promo_price, url, city)
    
    
    def get_page_data(self, city: City, page_data: dict) -> List[tuple]:
        """Считывает необходимые данные со страницы товаров"""
        
        data = list()
        for item in page_data["items"]:
            data.append( self.get_item_data(city, item) )
        
        return data
        
    
    def parse_city_offset_data(self, offset: int, city: City) -> dict:
        """Парсит данные с определенной страницы для определенного города"""
        
        # Заголовки и параметры для get запроса
        headers = {
            'authority': 'api.detmir.ru',
            'accept': '*/*',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'content-type': 'application/json',
            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Mobile Safari/537.36',
            'x-requested-with': 'detmir-ui',
        }
        params = {
            "filter": f"categories[].alias:lego;promo:false;withregion:{city.name.replace('_', '-')}",
            "expand": "meta.facet.ages.adults,meta.facet.gender.adults,webp",
            "meta": "*",
            "limit":"30",
            "offset": str(offset),
            "sort":"popularity:desc"
        }
        
        # Пробуем не больше COUNT_ATTEMPTS раз достучаться до страницы 
        for _ in range(self.COUNT_ATTEMPTS):
            response = requests.get(
                url='https://api.detmir.ru/v2/products?', 
                cookies=self.cookies, 
                headers=headers, 
                params=params)
            if response.status_code == 200:
                return response.json()
            time.sleep(self.SLEEP_TIME)
            
        return None
      
        
    def parse_city_data(self, city: City) -> List[tuple]:
        """Парсит все страницы для определенного города"""
        
        print(f"Парсим товары с {self.URL} по городу {city.value}...")
        
        offset = 0
        # Получем количество товаров и страниц в каталоге
        page_data = self.parse_city_offset_data(offset, city)
        count_items = page_data["meta"]["length"]
        count_pages = math.ceil(count_items/30)
        
        print(f"Количество страниц: {count_pages}, количество товаров: {count_items}.")
        
        data = list()
        for page_num in range(0, count_pages):
            # Парсим необходимую страницу из каталога
            page_data = self.parse_city_offset_data(offset, city)
            # Небольшая задержка перед считыванием следующей страницы
            time.sleep(self.SLEEP_TIME)
            if page_data is None:
                print(f"Не удалось считать страницу под номером {page_num + 1}.")
            else:
                # Получаем необходимые данные о товарах со страницы
                data.extend(self.get_page_data(city, page_data))
                print(f"Страница под номером {page_num + 1} успешно считана.")
            offset += 30
                
        print(f"Парсинг товаров с {self.URL} по городу {city.value} завершен!")
            
        return data
    
    def parse_data(self) -> None:
        """
        Парсит все данные со страницы https://www.detmir.ru/catalog/index/name/lego/
        для городов в классе City и сохраняет в csv файл
        """
        
        data = list()
        
        for city in self.City:
            # Обновляем cookies перед парсингом нового города
            self.get_cookies()
            # Парсим каталог для выбранного города
            data.extend(self.parse_city_data(city))
        
        # Записываем в .csv файл
        self.write_to_csv(data)
        
            
    def write_to_csv(self, data: List[tuple]) -> None:
        """Записывает выходные данные в csv файл"""
        pd.DataFrame(
            data, 
            columns=[
                'id', 
                'title',
                'price',
                'promo_price',
                'url',
                'city']).to_csv(self.CSV_PATH, sep=';', encoding='cp1251', index=False)
        

if __name__ == "__main__":
    p = DetMirParser()
    p.parse_data()
    