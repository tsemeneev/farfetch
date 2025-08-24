import json
import csv
import time
import aiohttp
import asyncio
from aiohttp import TCPConnector

# === Настройки ===
API_SEARCH_URL = "https://api.farfetch.net/v1/search/products"
API_BASE_URL = "https://api.farfetch.net/v1/products"
CONCURRENT_REQUESTS = 100  # Максимальное количество одновременных запросов

HEADERS = {
    "Accept-Encoding": "gzip",
    "Accept-Language": "ru-RU",
    "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjFCMzk0QzY0Q0JCM0Y3RDIyNDY0OUVCNjQ5RkNBM0ZEM0I5NDhERTMiLCJ4NXQiOiJHemxNWk11ejk5SWtaSjYyU2Z5al9UdVVqZU0iLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwOi8vZmFyZmV0Y2guY29tIiwibmJmIjoxNzU1MTc5MzQwLCJpYXQiOjE3NTUxNzkzNDAsImV4cCI6MTc3MDk0NzM0MCwiYXVkIjpbImFwaSIsImNvbW1lcmNlLmJhZ3Mud3JpdGUiLCJjb21tZXJjZS5jYXRhbG9nIiwiY29tbWVyY2UubWVyY2hhbnRzIiwiY29tbWVyY2UucHJvbW9ldmFsdWF0aW9ucy5yZWFkIiwiY29tbWVyY2UucHJvbW90aW9ucyIsImNvbW1lcmNlLnNpemVwcmVkaWN0LnJlYWQiLCJjb21tZXJjZS51c2Vycy5wcm9tb2NvZGVzIiwiZGF0YS5lc3RpbWF0ZWRkZWxpdmVyeWRhdGUiLCJleHBlcmltZW50YXRpb24uZnRvZ2dsZS5yZWFkIiwibWt0LmNvbnRleHR1YWxtZXNzYWdlcy5yZWFkIiwic21zdmVyaWZpY2F0aW9uIiwidXNlci53cml0ZSIsImh0dHA6Ly9mYXJmZXRjaC5jb20vcmVzb3VyY2VzIl0sInNjb3BlIjpbImFwaSIsImNvbW1lcmNlLmJhZ3Mud3JpdGUiLCJjb21tZXJjZS5jYXRhbG9nIiwiY29tbWVyY2UubWVyY2hhbnRzIiwiY29tbWVyY2UucHJvbW9ldmFsdWF0aW9ucy5yZWFkIiwiY29tbWVyY2UucHJvbW90aW9ucyIsImNvbW1lcmNlLnNpemVwcmVkaWN0LnJlYWQiLCJjb21tZXJjZS51c2Vycy5wcm9tb2NvZGVzIiwiZGF0YS5lc3RpbWF0ZWRkZWxpdmVyeWRhdGUiLCJleHBlcmltZW50YXRpb24uZnRvZ2dsZS5yZWFkIiwibWt0LmNvbnRleHR1YWxtZXNzYWdlcy5yZWFkIiwic21zdmVyaWZpY2F0aW9uIiwidXNlci53cml0ZSJdLCJjbGllbnRfaWQiOiJDRjkzRDhGNEFGMzI0N0Y1OTRENUE5MTRBMjYxRDQwNyIsImNsaWVudF91aWQiOiIxMDAwNCIsImNsaWVudF90ZW5hbnRJZCI6IjEwMDAwIiwiY2xpZW50X2lzc190eXBlIjoiMSIsImNsaWVudF9ndWVzdCI6IjUwMDAwMzQxNjE2MjQ0NjMiLCJzaWQiOiIxMDVFN0VENENDQTM3MzMwNzdENEMwQUNDMTU1QUFCQSIsImp0aSI6IjlFMDZCNzdERDkxQTcxMTdFQkQwMThDOUU4MUNDMzRCIn0.CaSYP3fNWLJRkfVYWZExplj9fWGmbHKT0cIkNlD1Jjg3wUGGw60D7PHWus-p2C7fYMteHeQlnd0asYN90YSG4jdepWI4SOyIyvRQdksYSvLGyrPYMAXBXbMMQrEnF_SfbanKlp7Ry0n3A-empFpdl0yRaCEfXf9OrOQU-w937EcoCBJQ9jGBD0UXEHVspWXXzK-bppQ-5WBPE-TkKSFwmAh8Q34d37WczQiBXJWPhws7MyN638KVoFY5bYTXtQCJjM_b105wGxVDBO5rH6aIWHcLmRQ6Ot_0Vl58vtrGu3722-D6GMNfbsndN_Oae6j54KPmxEOwpXx0YvOVMs6EdFRDE9w6JX6re32lZtEc_oX2MqxAmaujyUWPCyPYQD1usQLLpQPQJeOBiEzhTjmflFPbzQopQGOeOFigcgmPBdO1r1792nGb5cHuHrh3Jj4_0EgQG4H4NTUpDDL769O6xYBPtcwnN4s3JXrLtmuJVnnD8y1FbMFPMjABjME5UchFYDXWV9GV5hnJTF3AkYNzYMbfU7b6G1tSYvYpZUuVZUlZCysNMLvZ7z54boTYHl3Lf4DNOGGADJfG85BezjZBQt7moWdIs4rGsBe-1S6_Zu4hRiIckS7FnJinfKVM6pIyeFH4nSiRiUDUqpooFipq3c91ZjpH9Cc_6NMqYmjIotM",
    "Cache-Control": "no-cache, no-store",
    "Connection": "Keep-Alive",
    "FF-Country": "AM",
    "FF-Currency": "USD",
    "Host": "api.farfetch.net",
    "traceparent": "00-00000000000000000000000000000000-0000000000000000-00",
    "User-Agent": "Farfetch/5.81.1 (samsung SM-S9260; Android 9; Scale/3.00)",
}

# Новые поля из import.csv
FIELDNAMES = [
    "ID", "Title", "Content", "Short Description", "Post Type", "Parent Product ID", 
    "Sku", "Price", "Regular Price", "Sale Price", "Stock Status", "Stock", 
    "Manage Stock", "Attribute Name (pa_artikul)", "Attribute Value (pa_artikul)", 
    "Attribute In Variations (pa_artikul)", "Attribute Is Visible (pa_artikul)", 
    "Attribute Is Taxonomy (pa_artikul)", "Attribute Name (pa_brend)", 
    "Attribute Value (pa_brend)", "Attribute In Variations (pa_brend)", 
    "Attribute Is Visible (pa_brend)", "Attribute Is Taxonomy (pa_brend)", 
    "Attribute Name (pa_cvet)", "Attribute Value (pa_cvet)", 
    "Attribute In Variations (pa_cvet)", "Attribute Is Visible (pa_cvet)", 
    "Attribute Is Taxonomy (pa_cvet)", "Attribute Name (pa_pol)", 
    "Attribute Value (pa_pol)", "Attribute In Variations (pa_pol)", 
    "Attribute Is Visible (pa_pol)", "Attribute Is Taxonomy (pa_pol)", 
    "Attribute Name (pa_razmer)", "Attribute Value (pa_razmer)", 
    "Attribute In Variations (pa_razmer)", "Attribute Is Visible (pa_razmer)", 
    "Attribute Is Taxonomy (pa_razmer)", "Attribute Name (pa_sezon)", 
    "Attribute Value (pa_sezon)", "Attribute In Variations (pa_sezon)", 
    "Attribute Is Visible (pa_sezon)", "Attribute Is Taxonomy (pa_sezon)", 
    "Product Type", "Product Visibility", "Image URL", "Image Featured", 
    "brend", "Categori", "Comment Status", "Default Attributes", "Product Attributes"
]


def extract_main_image(images):
    """Ищет изображение с size=1000"""
    for img in images.get("images", []):
        if img.get("size") == "1000":
            return img["url"]
    return ""


def build_category_path(categories):
    """Собирает категории с path, фильтруя по нужным"""
    if not categories:
        return "Обувь"  # По умолчанию
    
    if len(categories) > 1 and categories[1].get('parentId') == 0:
        return categories[2].get('name') if len(categories) > 2 else categories[1].get('name')
    return categories[1].get('name') if len(categories) > 1 else categories[0].get('name')


def get_gender_text(gender):
    """Преобразует пол в русский текст"""
    if "Man" in str(gender):
        return "Мужской"
    elif "Woman" in str(gender):
        return "Женский"
    else:
        return "Унисекс"


def get_color_text(color_name):
    """Преобразует название цвета в русский"""
    color_mapping = {
        "Black": "черный",
        "White": "белый",
        "Red": "красный",
        "Blue": "синий",
        "Green": "зеленый",
        "Yellow": "желтый",
        "Brown": "коричневый",
        "Gray": "серый",
        "Pink": "розовый",
        "Purple": "фиолетовый",
        "Orange": "оранжевый",
        "Navy": "темно-синий",
        "Beige": "бежевый",
        "Cream": "кремовый"
    }
    return color_mapping.get(color_name, color_name.lower())


def write_product_to_csv(product, writer):
    """Записывает товар (variable + variations) в CSV"""
    product_id = product["id"]
    name = product.get("shortDescription", "").strip()
    description = product.get("description", "").replace("\n", " ").replace('"', '""')
    brand = product.get("brand", {}).get("name", "Unknown")
    season = product.get("season", {}).get("code", "")
    
    # === Категории ===
    category_str = build_category_path(product.get("categories", []))

    # === Изображения ===
    main_image = extract_main_image(product.get("images", {}))

    # === Атрибуты (для родителя) ===
    variants = product.get("variants", [])
    sizes = []
    for var in variants:
        attrs = {a["type"]: a["value"] for a in var.get("attributes", [])}
        size = attrs.get("SizeDescription")
        if size:
            sizes.append(size)
    unique_sizes = sorted(set(sizes), key=sizes.index)  # сохраняем порядок
    size_values_str = "|".join(unique_sizes)  # Используем | как разделитель

    # === 1. Записываем родительский товар (variable) ===
    parent_row = {
        "ID": "",
        "Title": name,
        "Content": description,
        "Short Description": name,
        "Post Type": "product",
        "Parent Product ID": "0",
        "Sku": str(product_id),
        "Price": "",
        "Regular Price": "",
        "Sale Price": "",
        "Stock Status": "instock",
        "Stock": "",
        "Manage Stock": "no",
        "Attribute Name (pa_artikul)": "Артикул",
        "Attribute Value (pa_artikul)": str(product_id),
        "Attribute In Variations (pa_artikul)": "no",
        "Attribute Is Visible (pa_artikul)": "yes",
        "Attribute Is Taxonomy (pa_artikul)": "yes",
        "Attribute Name (pa_brend)": "Бренд",
        "Attribute Value (pa_brend)": brand,
        "Attribute In Variations (pa_brend)": "no",
        "Attribute Is Visible (pa_brend)": "yes",
        "Attribute Is Taxonomy (pa_brend)": "yes",
        "Attribute Name (pa_cvet)": "Цвет",
        "Attribute Value (pa_cvet)": "",
        "Attribute In Variations (pa_cvet)": "yes",
        "Attribute Is Visible (pa_cvet)": "yes",
        "Attribute Is Taxonomy (pa_cvet)": "yes",
        "Attribute Name (pa_pol)": "Пол",
        "Attribute Value (pa_pol)": get_gender_text(product.get("gender", "")),
        "Attribute In Variations (pa_pol)": "no",
        "Attribute Is Visible (pa_pol)": "yes",
        "Attribute Is Taxonomy (pa_pol)": "yes",
        "Attribute Name (pa_razmer)": "Размер",
        "Attribute Value (pa_razmer)": size_values_str,
        "Attribute In Variations (pa_razmer)": "yes",
        "Attribute Is Visible (pa_razmer)": "yes",
        "Attribute Is Taxonomy (pa_razmer)": "yes",
        "Attribute Name (pa_sezon)": "Сезон",
        "Attribute Value (pa_sezon)": season,
        "Attribute In Variations (pa_sezon)": "no",
        "Attribute Is Visible (pa_sezon)": "yes",
        "Attribute Is Taxonomy (pa_sezon)": "yes",
        "Product Type": "variable",
        "Product Visibility": "visible",
        "Image URL": main_image if main_image else "",
        "Image Featured": main_image if main_image else "",
        "brend": brand,
        "Categori": category_str,
        "Comment Status": "open",
        "Default Attributes": "",
        "Product Attributes": ""
    }

    writer.writerow(parent_row)

    # === 2. Записываем вариации (variation) ===
    for var in variants:
        var_attrs = {a["type"]: a["value"] for a in var.get("attributes", [])}
        size = var_attrs.get("SizeDescription")
        price_info = var.get("price", {})
        price = price_info.get("priceInclTaxes", "")
        stock = var.get("quantity", 1)

        if not size or not price:
            continue

        # Получаем цвет для вариации
        colors = [c["color"]["name"] for c in product.get("colors", []) if c.get("color")]
        color_value = get_color_text(colors[0]) if colors else ""

        variation_row = {
            "ID": "",
            "Title": f'{name} - {size}',
            "Content": description,
            "Short Description": f'{name} - {size}',
            "Post Type": "product",
            "Parent Product ID": str(product_id),
            "Sku": f"{product_id}_{size}",
            "Price": str(price),
            "Regular Price": str(price),
            "Sale Price": "",
            "Stock Status": "instock",
            "Stock": str(stock),
            "Manage Stock": "yes",
            "Attribute Name (pa_artikul)": "Артикул",
            "Attribute Value (pa_artikul)": "",
            "Attribute In Variations (pa_artikul)": "no",
            "Attribute Is Visible (pa_artikul)": "yes",
            "Attribute Is Taxonomy (pa_artikul)": "yes",
            "Attribute Name (pa_brend)": "Бренд",
            "Attribute Value (pa_brend)": "",
            "Attribute In Variations (pa_brend)": "no",
            "Attribute Is Visible (pa_brend)": "yes",
            "Attribute Is Taxonomy (pa_brend)": "yes",
            "Attribute Name (pa_cvet)": "Цвет",
            "Attribute Value (pa_cvet)": color_value,
            "Attribute In Variations (pa_cvet)": "yes",
            "Attribute Is Visible (pa_cvet)": "yes",
            "Attribute Is Taxonomy (pa_cvet)": "yes",
            "Attribute Name (pa_pol)": "Пол",
            "Attribute Value (pa_pol)": "",
            "Attribute In Variations (pa_pol)": "no",
            "Attribute Is Visible (pa_pol)": "yes",
            "Attribute Is Taxonomy (pa_pol)": "yes",
            "Attribute Name (pa_razmer)": "Размер",
            "Attribute Value (pa_razmer)": size,
            "Attribute In Variations (pa_razmer)": "yes",
            "Attribute Is Visible (pa_razmer)": "yes",
            "Attribute Is Taxonomy (pa_razmer)": "yes",
            "Attribute Name (pa_sezon)": "Сезон",
            "Attribute Value (pa_sezon)": "",
            "Attribute In Variations (pa_sezon)": "no",
            "Attribute Is Visible (pa_sezon)": "yes",
            "Attribute Is Taxonomy (pa_sezon)": "yes",
            "Product Type": "variable",
            "Product Visibility": "visible",
            "Image URL": main_image if main_image else "",
            "Image Featured": main_image if main_image else "",
            "brend": brand,
            "Categori": category_str,
            "Comment Status": "open",
            "Default Attributes": "",
            "Product Attributes": ""
        }

        writer.writerow(variation_row)


async def fetch_product_detail(session, product_id, semaphore):
    """Асинхронно получает данные товара по ID"""
    url = f"{API_BASE_URL}/{product_id}"
    try:
        async with semaphore:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
    except Exception as e:
        print(f"❌ Ошибка загрузки товара {product_id}: {e}")
        return None


async def get_item_ids(category_id):
    """Асинхронно получает ID товаров категории"""
    all_items = []
    search_params = {
        "page": 1,
        "pageSize": 100,
        "includeexplanation": "ranking",
        "imagessizes": 600,
        "sort": "ranking",
        "fields": "id",
        "contextfilters": f"categories:{category_id};pricetype:0,1"
    }
    
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = TCPConnector(limit=CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession(
        headers=HEADERS,
        connector=connector,
        raise_for_status=True
    ) as session:
        try:
            # Получаем первую страницу для определения общего количества страниц
            async with semaphore:
                async with session.get(API_SEARCH_URL, params=search_params) as response:
                    data = await response.json()
                    pages = data['products']['totalPages']
            
            print(f'Всего страниц: {pages} для категории {category_id}')
            
            # Создаем задачи для всех страниц
            tasks = []
            for page in range(1, pages + 1):
                params = search_params.copy()
                params['page'] = page
                tasks.append(fetch_search_page(session, params, semaphore))
            
            # Получаем результаты всех страниц
            results = await asyncio.gather(*tasks)
            
            # Собираем все ID
            for page_items in results:
                if page_items:
                    all_items.extend(page_items)
                    
        except Exception as e:
            print(f"❌ Ошибка получения ID для категории {category_id}: {e}")
    
    return all_items


async def fetch_search_page(session, params, semaphore):
    """Асинхронно получает одну страницу результатов поиска"""
    try:
        async with semaphore:
            async with session.get(API_SEARCH_URL, params=params) as response:
                data = await response.json()
                return [x.get('id') for x in data['products']['entries']]
    except Exception as e:
        print(f"❌ Ошибка загрузки страницы {params['page']}: {e}")
        return []


async def export_products_by_ids(product_ids, output_file):
    """Асинхронно экспортирует товары в CSV"""
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = TCPConnector(limit=CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession(
        headers=HEADERS,
        connector=connector,
        raise_for_status=True
    ) as session:
        # Создаем задачи для получения всех товаров
        tasks = []
        for product_id in product_ids:
            task = asyncio.create_task(
                fetch_product_detail(session, product_id, semaphore)
            )
            tasks.append(task)
        
        # Получаем результаты всех задач
        products_data = await asyncio.gather(*tasks)
    
    # Записываем результаты в CSV
    with open(output_file, "w", encoding="utf-8", newline="", errors="replace") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        
        successful_count = 0
        for product in products_data:
            if product:
                write_product_to_csv(product, writer)
                successful_count += 1
        
        print(f"✅ Успешно экспортировано {successful_count} из {len(product_ids)} товаров")


async def process_category(category):
    """Обрабатывает одну категорию"""
    category_id = category['id']
    name = category.get('name')
    gender = category.get('gender')
    
    print(f"🔄 Получение товаров для категории: {name} ({gender})")
    
    # Получаем ID товаров категории
    item_ids = await get_item_ids(category_id)
    
    if not item_ids:
        print(f"⚠️ Нет товаров для категории {name}")
        return
    
    # Формируем имя файла
    category_name = f"./products/{name.replace(' ', '_')}_{gender}_{category_id}"
    
    print(f"📦 Начинаем экспорт {len(item_ids)} товаров...")
    
    # Экспортируем товары
    await export_products_by_ids(item_ids, f"{category_name}.csv")
    
    print(f"✅ Категория {name} обработана")


async def main():
    start = time.time()
    """Основная асинхронная функция"""
    # Читаем категории из файла
    try:
        with open('parent_categories.json', 'r', encoding='utf-8') as f:
            categories = json.load(f)
    except FileNotFoundError:
        print("❌ Файл parent_categories.json не найден")
        return
    except json.JSONDecodeError:
        print("❌ Ошибка чтения JSON файла")
        return
    
    print(f"📋 Найдено {len(categories)} категорий для обработки")
    
    # Обрабатываем категории последовательно
    for category in categories[25:]:
        await process_category(category)
    
    print("Все категории обработаны!")
    print(f"Время выполнения: {(time.time() - start) / 60} минут")

if __name__ == "__main__":
    # Запускаем асинхронную обработку
    asyncio.run(main())
