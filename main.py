import os
from dotenv import load_dotenv
from pymongo import MongoClient
import validators
import requests
import backblazeuploader
import datetime

load_dotenv()

client = MongoClient(os.getenv('ATLAS_URI'))
db = client[os.getenv('DB_NAME')]
collection_product = db[os.getenv('PRODUCTS_COLLECTION')]

backblaze_uploader = backblazeuploader.BackblazeUploader(os.getenv('B2_KEY_ID'), os.getenv('B2_APPLICATION_KEY'),
                                                         os.getenv('B2_BUCKET_NAME'))

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

CRAWLING = 1
CRAWLED = 2


def get_extension_from_url(url):
    if '?' in url:
        url = url.split('?')[0]
    return url.split('.')[-1]


def download_image_from_url(url):
    if validators.url(url):
        suffix = datetime.datetime.now().strftime("%y%m%d_%H%M%S")

        image_extension = get_extension_from_url(url)
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                local_filename = f'image_{suffix}.{image_extension}'
                with open(local_filename, 'wb') as f:
                    f.write(response.content)

                link_b2 = backblaze_uploader.upload_file(local_filename, f'image_{suffix}.{image_extension}')

                os.remove(local_filename)

                return link_b2
        except:
            print("An exception occurred")
            pass


def download_galleries(galleries: []):
    new_galleries = []
    for image_url in galleries:
        link_b2 = download_image_from_url(image_url)
        if link_b2:
            new_galleries.append(link_b2)

    return new_galleries


def download_variant_galleries(galleries: []):
    for image_url in galleries:
        download_image_from_url(image_url)
    pass


def download_thumbnail_url(thumbnail_url):
    return download_image_from_url(thumbnail_url)


class Crawler:
    def __init__(self, products=None):
        if products is None:
            products = []

        self.failed_urls = set()
        self.visited_urls = set()
        self.products = products
        self.to_visit_url = set()
        self.keywords = set()

    def run(self):
        for product in self.products:
            print("Crawling product: ", product['_id'])
            collection_product.update_one({'_id': product['_id']}, {"$set": {'crawled': CRAWLING}})
            new_galleries = download_galleries(product['galleries'])

            new_thumbnail_url = download_thumbnail_url(product['thumbnail_url'])
            collection_product.update_one({'_id': product['_id']}, {
                "$set": {'crawled': CRAWLED, 'new_galleries': new_galleries, 'new_thumbnail_url': new_thumbnail_url}})
            print("Crawled product: ", product['_id'])


if __name__ == '__main__':
    _products = collection_product.find({
        'synced_to_mysql': {'$in': [None, False]},
        'crawled': {'$in': [None, False]},
    })

    Crawler(products=_products).run()
