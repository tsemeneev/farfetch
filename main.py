import json
import requests
import csv

# URL с параметрами
url = "https://api.farfetch.net/v1/search/products"
detailUrl = "https://api.farfetch.net/v1/products"


params = {
    "page": 1,
    "pageSize": 100,
    "includeexplanation": "ranking",
    "imagessizes": 600,
    "sort": "ranking",
    "fields": "id,shortDescription,name,description,gender,images,merchantId,brand,quantity,tag,price,prices,priceWithoutDiscount,currencyIsoCode,priceType,isCustomizable,promotionPercentage,availableSizes,labels,promotions,explanation,categories,category,attributes",
    "contextfilters": "categories:137196;pricetype:0,1"
}

# Заголовки
headers = {
    "Accept-Encoding": "gzip",
    "Accept-Language": "ru-RU",
    "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjFCMzk0QzY0Q0JCM0Y3RDIyNDY0OUVCNjQ5RkNBM0ZEM0I5NDhERTMiLCJ4NXQiOiJHemxNWk11ejk5SWtaSjYyU2Z5al9UdVVqZU0iLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwOi8vZmFyZmV0Y2guY29tIiwibmJmIjoxNzU1MTc5MzQwLCJpYXQiOjE3NTUxNzkzNDAsImV4cCI6MTc3MDk0NzM0MCwiYXVkIjpbImFwaSIsImNvbW1lcmNlLmJhZ3Mud3JpdGUiLCJjb21tZXJjZS5jYXRhbG9nIiwiY29tbWVyY2UubWVyY2hhbnRzIiwiY29tbWVyY2UucHJvbW9ldmFsdWF0aW9ucy5yZWFkIiwiY29tbWVyY2UucHJvbW90aW9ucyIsImNvbW1lcmNlLnNpemVwcmVkaWN0LnJlYWQiLCJjb21tZXJjZS51c2Vycy5wcm9tb2NvZGVzIiwiZGF0YS5lc3RpbWF0ZWRkZWxpdmVyeWRhdGUiLCJleHBlcmltZW50YXRpb24uZnRvZ2dsZS5yZWFkIiwibWt0LmNvbnRleHR1YWxtZXNzYWdlcy5yZWFkIiwic21zdmVyaWZpY2F0aW9uIiwidXNlci53cml0ZSIsImh0dHA6Ly9mYXJmZXRjaC5jb20vcmVzb3VyY2VzIl0sInNjb3BlIjpbImFwaSIsImNvbW1lcmNlLmJhZ3Mud3JpdGUiLCJjb21tZXJjZS5jYXRhbG9nIiwiY29tbWVyY2UubWVyY2hhbnRzIiwiY29tbWVyY2UucHJvbW9ldmFsdWF0aW9ucy5yZWFkIiwiY29tbWVyY2UucHJvbW90aW9ucyIsImNvbW1lcmNlLnNpemVwcmVkaWN0LnJlYWQiLCJjb21tZXJjZS51c2Vycy5wcm9tb2NvZGVzIiwiZGF0YS5lc3RpbWF0ZWRkZWxpdmVyeWRhdGUiLCJleHBlcmltZW50YXRpb24uZnRvZ2dsZS5yZWFkIiwibWt0LmNvbnRleHR1YWxtZXNzYWdlcy5yZWFkIiwic21zdmVyaWZpY2F0aW9uIiwidXNlci53cml0ZSJdLCJjbGllbnRfaWQiOiJDRjkzRDhGNEFGMzI0N0Y1OTRENUE5MTRBMjYxRDQwNyIsImNsaWVudF91aWQiOiIxMDAwNCIsImNsaWVudF90ZW5hbnRJZCI6IjEwMDAwIiwiY2xpZW50X2lzc190eXBlIjoiMSIsImNsaWVudF9ndWVzdCI6IjUwMDAwMzQxNjE2MjQ0NjMiLCJzaWQiOiIxMDVFN0VENENDQTM3MzMwNzdENEMwQUNDMTU1QUFCQSIsImp0aSI6IjlFMDZCNzdERDkxQTcxMTdFQkQwMThDOUU4MUNDMzRCIn0.CaSYP3fNWLJRkfVYWZExplj9fWGmbHKT0cIkNlD1Jjg3wUGGw60D7PHWus-p2C7fYMteHeQlnd0asYN90YSG4jdepWI4SOyIyvRQdksYSvLGyrPYMAXBXbMMQrEnF_SfbanKlp7Ry0n3A-empFpdl0yRaCEfXf9OrOQU-w937EcoCBJQ9jGBD0UXEHVspWXXzK-bppQ-5WBPE-TkKSFwmAh8Q34d37WczQiBXJWPhws7MyN638KVoFY5bYTXtQCJjM_b105wGxVDBO5rH6aIWHcLmRQ6Ot_0Vl58vtrGu3722-D6GMNfbsndN_Oae6j54KPmxEOwpXx0YvOVMs6EdFRDE9w6JX6re32lZtEc_oX2MqxAmaujyUWPCyPYQD1usQLLpQPQJeOBiEzhTjmflFPbzQopQGOeOFigcgmPBdO1r1792nGb5cHuHrh3Jj4_0EgQG4H4NTUpDDL769O6xYBPtcwnN4s3JXrLtmuJVnnD8y1FbMFPMjABjME5UchFYDXWV9GV5hnJTF3AkYNzYMbfU7b6G1tSYvYpZUuVZUlZCysNMLvZ7z54boTYHl3Lf4DNOGGADJfG85BezjZBQt7moWdIs4rGsBe-1S6_Zu4hRiIckS7FnJinfKVM6pIyeFH4nSiRiUDUqpooFipq3c91ZjpH9Cc_6NMqYmjIotM",
    "Cache-Control": "no-cache, no-store",
    "Connection": "Keep-Alive",
    "FF-Country": "AM",
    "FF-Currency": "USD",
    # "FF-Omnitracking-CorrelationId": "61189a9a-67ec-46f5-8265-6ffb23d77eec",
    "Host": "api.farfetch.net",
    "traceparent": "00-00000000000000000000000000000000-0000000000000000-00",
    "User-Agent": "Farfetch/5.81.1 (samsung SM-S9260; Android 9; Scale/3.00)",
    # "X-FFBenefits": "eJyl0MEOgzAIBuB36VleYM+x27IDRVSSSpsW3dziu6+eZhaTHTzCH74At7fzrNyJXZfE7uJSju1EBoY98DNxFlZi1ziK7ZbjjBLQSxBbwC9gwhlEZ87GbR1ya/NLFnkx1BIK51mIy1erXRPtK9DFPKJJ1APg30773DPaSSIR0CCKgCFAjo+TXImdQcBJaTiQLKMWpO1yDEBxHHfvOQjrdm69fwBt3p6A"
}


def get_data():
    all_items = []
    # Выполнение GET-запроса
    response = requests.get(url, headers=headers, params=params)
    pages = response.json()['products']['totalPages']
    print('pages: ', pages)
    for page in range(1, 5):
        params['page'] = page
        response = requests.get(url, headers=headers, params=params)
        all_items.extend(response.json()['products']['entries'])
    # Печать статуса и ответа
    print(f"Status Code: {response.status_code}")
    # print(f"Response Body: {response.text}")
    print(len(all_items))
    with open("data.json", "w", encoding="utf-8") as d:
        json.dump(all_items, d, ensure_ascii=False, indent=2)

# get_data()

# def createcsv():
#     with open("data.csv", "w", encoding="utf-8") as d:
#         writer = csv.writer(d)
#         writer.writerow(["ID","Тип","Артикул","Имя","Описание","Изображение","Названия атрибутов","Значения атрибутов"])

# with open("data.json", "r", encoding="utf-8") as d:
#     data = json.load(d)
#     for item in data:
#         id = item['id']
#         gender = item['gender']
#         brand = item['brand']['name']
#         name = item['shortDescription']
#         description = item['explanation']
#         image = item['images'][0]['url']
#         attributes = item.get('attributes')
#         if  attributes:
#             attribute_names = []
#             attribute_values = []
#             for attribute in attributes:
#                 attribute_names.append(attribute['name'])
#                 attribute_values.append(attribute['values'][0]['value'])
#             attribute_names = ";".join(attribute_names)
#             attribute_values = ";".join(attribute_values)
#         else:
#             attribute_names = None
#             attribute_values = None

#         with open("data.csv", "a", encoding="utf-8") as d:
#             writer = csv.writer(d)
#             writer.writerow([id, gender, brand, name, description, image, attribute_names, attribute_values])
#             print(id, gender, brand, name, description, image, attribute_names, attribute_values)
#             print("______________________________________________________")
#             print("______________________________________________________")
#             print("______________________________________________________")



def create_woocommerce_csv():
    """
    Creates a CSV file with WooCommerce structure matching tets212.csv
    but only fills the fields we actually use from the API data
    """
    # WooCommerce CSV headers from tets212.csv
    csv_headers = [
        "ID", "Тип", "Артикул", "GTIN, UPC, EAN или ISBN", "Имя", "Опубликован", "Рекомендуемый?", 
        "Видимость в каталоге", "Краткое описание", "Описание", "Дата начала действия скидки", 
        "Дата окончания действия скидки", "Статус налога", "Налоговый класс", "Наличие", "Запасы", 
        "Величина малых запасов", "Возможен ли предзаказ?", "Продано индивидуально?", "Вес (кг)", 
        "Длина (см)", "Ширина (см)", "Высота (см)", "Разрешить отзывы от клиентов?", "Примечание к покупке", 
        "Акционная цена", "Базовая цена", "Категории", "Метки", "Класс доставки", "Изображения", 
        "Лимит скачивания", "Дней срока скачивания", "Родительский", "Сгруппированные товары", 
        "Апсэлы", "Кросселы", "Внешний URL", "Текст кнопки", "Позиция", "Бренды", "Мета: _4partners_id"
    ]
    
    # Add attribute columns (we'll use first 16 as in original)
    for i in range(1, 17):
        csv_headers.extend([
            f"Название атрибута {i}", f"Значения атрибутов {i}", 
            f"Видимость атрибута {i}", f"Глобальный атрибут {i}"
        ])
    
    # Add remaining meta fields from original
    meta_fields = [
        "Мета: _yfym_barcode", "Мета: wd_additional_variation_images_data", 
        "Мета: rank_math_internal_links_processed", "Мета: rank_math_seo_score", 
        "Мета: _wp_page_template", "Мета: woodmart_sguide_select", 
        "Мета: woodmart_price_unit_of_measure", "Мета: woodmart_total_stock_quantity",
        "Мета: _product_360_image_gallery", "Мета: _woodmart_whb_header",
        "Мета: _woodmart_main_layout", "Мета: _woodmart_sidebar_width",
        "Мета: _woodmart_custom_sidebar", "Мета: _woodmart_new_label",
        "Мета: _woodmart_new_label_date", "Мета: _woodmart_swatches_attribute",
        "Мета: _woodmart_related_off", "Мета: _woodmart_exclude_show_single_variation",
        "Мета: _woodmart_product_video", "Мета: _woodmart_product_hashtag",
        "Мета: _woodmart_single_product_style", "Мета: _woodmart_thums_position",
        "Мета: _woodmart_extra_content", "Мета: _woodmart_extra_position",
        "Мета: _woodmart_product_design", "Мета: _woodmart_product-background",
        "Мета: _woodmart_hide_tabs_titles", "Мета: _woodmart_product_custom_tab_title",
        "Мета: _woodmart_product_custom_tab_content_type", "Мета: _woodmart_product_custom_tab_content",
        "Мета: _woodmart_product_custom_tab_html_block", "Мета: _woodmart_product_custom_tab_title_2",
        "Мета: _woodmart_product_custom_tab_content_type_2", "Мета: _woodmart_product_custom_tab_content_2",
        "Мета: _woodmart_product_custom_tab_html_block_2", "Мета: rank_math_primary_product_brand",
        "Мета: rank_math_primary_product_cat", "Мета: _yfym_cargo_types",
        "Мета: _yfym_individual_vat", "Мета: _yfym_condition", "Мета: _yfym_quality",
        "Мета: _4partners_hash", "Мета: _4partners_primary_category", "Мета: sync_product",
        "Мета: rank_math_analytic_object_id", "Мета: yfym_individual_vat", "Мета: _rank_math_gtin_code"
    ]
    csv_headers.extend(meta_fields)
    
    # Create CSV with headers
    with open("woocommerce_products.csv", "w", encoding="utf-8", newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerow(csv_headers)
        
        # Process data from JSON
        with open("data.json", "r", encoding="utf-8") as d:
            data = json.load(d)
            
            for item in data:
                try:
                    id = item['id']
                    res = requests.get(detailUrl + f"/{id}", headers=headers)
                    
                    if res.status_code != 200:
                        print(f"Failed to get details for {id}")
                        continue
                        
                    detail_data = res.json()
                    type_category = detail_data.get('categories', [{}])[1].get('name', '') if len(detail_data.get('categories', [])) > 1 else ''
                    name = detail_data.get('shortDescription', '')
                    description = detail_data.get('description', '').replace('\n', ' ')
                    
                    # Get image URL
                    images = detail_data.get('images', {}).get('images', [])
                    imageurl = ''
                    for image in images:
                        if image.get('size') == "1000":
                            imageurl = image.get('url', '')
                            break
                    
                    # Get attributes
                    attributes_res = requests.get(detailUrl + f"/{id}/attributes", headers=headers)
                    attribute_names = []
                    attribute_values = []
                    
                    if attributes_res.status_code == 200:
                        for attribute in attributes_res.json():
                            attribute_names.append(attribute.get('name', ''))
                            values = attribute.get('values', [])
                            if values:
                                attribute_values.append(values[0].get('value', ''))
                            else:
                                attribute_values.append('')
                    
                    # Create row with empty values for all columns
                    row = [''] * len(csv_headers)
                    
                    # Fill only the fields we use
                    row[0] = id  # ID
                    row[1] = "variable"  # Тип (product type)
                    row[2] = id  # Артикул (SKU)
                    row[4] = name  # Имя
                    row[5] = "1"  # Опубликован
                    row[7] = "visible"  # Видимость в каталоге
                    row[9] = description  # Описание
                    row[12] = "taxable"  # Статус налога
                    row[14] = "1"  # Наличие
                    row[15] = "5"  # Запасы
                    row[27] = "Обувь"  # Категории
                    row[30] = imageurl  # Изображения
                    
                    # Fill attributes (first few attribute columns)
                    attr_start_idx = 42  # Start of attribute columns
                    for i, (attr_name, attr_value) in enumerate(zip(attribute_names[:16], attribute_values[:16])):
                        if i < 16:  # Limit to 16 attributes as in original
                            row[attr_start_idx + i*4] = attr_name  # Название атрибута
                            row[attr_start_idx + i*4 + 1] = attr_value  # Значения атрибутов
                            row[attr_start_idx + i*4 + 2] = "1"  # Видимость атрибута
                            row[attr_start_idx + i*4 + 3] = "1"  # Глобальный атрибут
                    
                    writer.writerow(row)
                    print(f"Processed: {id} - {name}")
                    
                except Exception as e:
                    print(f"Error processing item {item.get('id', 'unknown')}: {e}")
                    continue
    
    print("WooCommerce CSV file created: woocommerce_products.csv")

# Run the function
create_woocommerce_csv()