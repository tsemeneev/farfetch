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

# Поля из вашего CSV (в точности как в wc-product-export-...)
FIELDNAMES = [
    "ID", "Тип", "Артикул", "GTIN, UPC, EAN или ISBN", "Имя", "Опубликован", "Рекомендуемый?",
    "Видимость в каталоге", "Краткое описание", "Описание", "Дата начала действия скидки",
    "Дата окончания действия скидки", "Статус налога", "Налоговый класс", "Наличие", "Запасы",
    "Величина малых запасов", "Возможен ли предзаказ?", "Продано индивидуально?",
    "Вес (кг)", "Длина (см)", "Ширина (см)", "Высота (см)", "Разрешить отзывы от клиентов?",
    "Примечание к покупке", "Акционная цена", "Базовая цена", "Категории", "Метки", "Класс доставки",
    "Изображения", "Лимит скачивания", "Дней срока скачивания", "Родительский", "Сгруппированные товары",
    "Апсэлы", "Кросселы", "Внешний URL", "Текст кнопки", "Позиция", "Бренды"
]

# Атрибуты (до 10)
for i in range(1, 11):
    FIELDNAMES += [
        f"Название атрибута {i}",
        f"Значения атрибутов {i}",
        f"Видимость атрибута {i}",
        f"Глобальный атрибут {i}"
    ]

# Дополнительные поля (если есть в вашем CSV)
FIELDNAMES += ["Метка Meta: _yoast_wpseo_focuskw", "Метка Meta: _yoast_wpseo_title"]


def extract_main_image(images):
    """Ищет изображение с size=1000"""
    for img in images.get("images", []):
        if img.get("size") == "1000":
            return img["url"]
    return ""


def extract_gallery_images(images):
    """Собирает все изображения (для галереи)"""
    urls = [img["url"] for img in images.get("images", []) if img.get("url")]
    return ", ".join(f'"{url}"' for url in urls) if urls else ""


def build_category_path(categories):
    """Собирает категории с path, фильтруя по нужным"""
    # Берём только те, где parentId != 0 и есть "Обувь"
    if not categories:
        return ""
    
    if len(categories) > 1 and categories[1].get('parentId') == 0:
        return categories[2].get('name') if len(categories) > 2 else categories[1].get('name')
    return categories[1].get('name') if len(categories) > 1 else categories[0].get('name')


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
    # gallery_images = extract_gallery_images(product.get("images", {}))

    # === Атрибуты (для родителя) ===
    variants = product.get("variants", [])
    sizes = []
    for var in variants:
        attrs = {a["type"]: a["value"] for a in var.get("attributes", [])}
        size = attrs.get("SizeDescription")
        if size:
            sizes.append(size)
    unique_sizes = sorted(set(sizes), key=sizes.index)  # сохраняем порядок
    size_values_str = ", ".join(unique_sizes)

    # === 1. Записываем родительский товар (variable) ===
    parent_row = {
        "ID": "",
        "Тип": "variable",
        "Артикул": str(product_id),
        "Имя": name,
        "Опубликован": 1,
        "Рекомендуемый?": 0,
        "Видимость в каталоге": "visible",
        "Краткое описание": name,
        "Описание": description,
        "Статус налога": "taxable",
        "Наличие": 1,
        "Возможен ли предзаказ?": 0,
        "Продано индивидуально?": 0,
        "Разрешить отзывы от клиентов?": 0,
        "Базовая цена": "",
        "Категории": category_str,
        "Изображения": main_image if main_image else "",
        "Бренды": brand,
    }

    attr_idx = 1

    # 1. Размер
    if unique_sizes:
        parent_row[f"Название атрибута {attr_idx}"] = "Размер"
        parent_row[f"Значения атрибутов {attr_idx}"] = size_values_str
        parent_row[f"Видимость атрибута {attr_idx}"] = 1
        parent_row[f"Глобальный атрибут {attr_idx}"] = 1
        attr_idx += 1

    # 2. Цвет
    colors = [c["color"]["name"] for c in product.get("colors", []) if c.get("color")]
    if colors:
        color_str = ", ".join(set(colors))
        parent_row[f"Название атрибута {attr_idx}"] = "Цвет"
        parent_row[f"Значения атрибутов {attr_idx}"] = color_str
        parent_row[f"Видимость атрибута {attr_idx}"] = 1
        parent_row[f"Глобальный атрибут {attr_idx}"] = 1
        attr_idx += 1

    # 3. Артикул
    try:
        sku_clean = product['variants'][0].get("sku", "").split()[0] if product.get('variants') else ""
    except IndexError:
        sku_clean = product_id
    if sku_clean:
        parent_row[f"Название атрибута {attr_idx}"] = "Артикул"
        parent_row[f"Значения атрибутов {attr_idx}"] = sku_clean
        parent_row[f"Видимость атрибута {attr_idx}"] = 1
        parent_row[f"Глобальный атрибут {attr_idx}"] = 1
        attr_idx += 1

    # 4. Пол
    gender = "Мужской" if "Man" in str(product.get("gender", "")) else "Унисекс"
    parent_row[f"Название атрибута {attr_idx}"] = "Пол"
    parent_row[f"Значения атрибутов {attr_idx}"] = gender
    parent_row[f"Видимость атрибута {attr_idx}"] = 1
    parent_row[f"Глобальный атрибут {attr_idx}"] = 1
    attr_idx += 1

    # 5. Сезон
    if season:
        parent_row[f"Название атрибута {attr_idx}"] = "Сезон"
        parent_row[f"Значения атрибутов {attr_idx}"] = season
        parent_row[f"Видимость атрибута {attr_idx}"] = 1
        parent_row[f"Глобальный атрибут {attr_idx}"] = 1
        attr_idx += 1

    # 6. Бренд
    parent_row[f"Название атрибута {attr_idx}"] = "Бренд"
    parent_row[f"Значения атрибутов {attr_idx}"] = brand
    parent_row[f"Видимость атрибута {attr_idx}"] = 1
    parent_row[f"Глобальный атрибут {attr_idx}"] = 1
    attr_idx += 1

    # Остальные атрибуты — пусто
    for i in range(attr_idx, 11):
        parent_row[f"Название атрибута {i}"] = ""
        parent_row[f"Значения атрибутов {i}"] = ""
        parent_row[f"Видимость атрибута {i}"] = ""
        parent_row[f"Глобальный атрибут {i}"] = ""

    writer.writerow(parent_row)

    # === 2. Записываем вариации (variation) ===
    for var in variants:
        var_attrs = {a["type"]: a["value"] for a in var.get("attributes", [])}
        size = var_attrs.get("SizeDescription")
        price_info = var.get("price", {})
        price = price_info.get("priceInclTaxes", "")

        if not size or not price:
            continue

        variation_row = {
            "ID": "",
            "Тип": "variation",
            "Артикул": f"{product_id}_{size}",
            "Родительский": str(product_id),
            "Имя": f'{name} - {size}',
            "Опубликован": 1,
            "Видимость в каталоге": "visible",
            "Наличие": 1,
            "Запасы": var.get("quantity", 1),
            "Возможен ли предзаказ?": 0,
            "Продано индивидуально?": 0,
            "Разрешить отзывы от клиентов?": 0,
            "Статус налога": "taxable",
            "Налоговый класс": "parent",
            "Базовая цена": str(price),
            "Изображения": main_image if main_image else "",
        }

        # Атрибуты вариации
        var_attr_idx = 1
        variation_row[f"Название атрибута {var_attr_idx}"] = "Размер"
        variation_row[f"Значения атрибутов {var_attr_idx}"] = size
        variation_row[f"Видимость атрибута {var_attr_idx}"] = 1
        variation_row[f"Глобальный атрибут {var_attr_idx}"] = 1
        var_attr_idx += 1

        if colors:
            variation_row[f"Название атрибута {var_attr_idx}"] = "Цвет"
            variation_row[f"Значения атрибутов {var_attr_idx}"] = colors[0]
            variation_row[f"Видимость атрибута {var_attr_idx}"] = 1
            variation_row[f"Глобальный атрибут {var_attr_idx}"] = 1
            var_attr_idx += 1

        # Остальные атрибуты — пусто (наследуются)
        for i in range(var_attr_idx, 11):
            variation_row[f"Название атрибута {i}"] = ""
            variation_row[f"Значения атрибутов {i}"] = ""
            variation_row[f"Видимость атрибута {i}"] = ""
            variation_row[f"Глобальный атрибут {i}"] = ""

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
        print("❌ Файл filtered_data.json не найден")
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