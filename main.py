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
collection_duplicate_image = db[os.getenv('DUPLICATE_IMAGE_COLLECTION')]

backblaze_uploader = backblazeuploader.BackblazeUploader(os.getenv('B2_KEY_ID'), os.getenv('B2_APPLICATION_KEY'),
                                                         os.getenv('B2_BUCKET_NAME'))

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}


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

                collection_duplicate_image.insert_one({'image_url': url, 'link_b2': link_b2})

                os.remove(local_filename)

                return link_b2
        except:
            print("An exception occurred")
            pass


def check_duplicate_image(image_url):
    return list(collection_duplicate_image.find({'image_url': image_url}))


def download_galleries(galleries: []):
    new_galleries = []
    for image_url in galleries:
        if not check_duplicate_image(image_url):
            link_b2 = download_image_from_url(image_url)
        else:
            link_b2 = check_duplicate_image(image_url)[0]['link_b2']

        if link_b2:
            new_galleries.append(link_b2)

    return new_galleries


def download_variant_galleries(galleries: []):
    for image_url in galleries:
        download_image_from_url(image_url)
    pass


def download_thumbnail_url(image_url):
    if not check_duplicate_image(image_url):
        link_b2 = download_image_from_url(image_url)
    else:
        link_b2 = check_duplicate_image(image_url)[0]['link_b2']

    return link_b2


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
            new_image_url = None
            if product['image_url']:
                new_image_url = download_thumbnail_url(product['image_url'])

            new_galleries = None
            if product['galleries']:
                new_galleries = download_galleries(product['galleries'])

            collection_product.update_one({'_id': product['_id']}, {
                "$set": {'status': 11, 'new_galleries': new_galleries, 'new_image_url': new_image_url}})
            print("Crawled product: ", product['_id'])


if __name__ == '__main__':
    _products = collection_product.find({
        'status': 10
    })
    # print(_products)
    Crawler(products=_products).run()
