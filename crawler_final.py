import re
import time
from typing import Iterator
import requests
import lxml.html
from pymongo import MongoClient

def main():
    """
    クローラーのメイン処理
    """

    client = MongoClient('localhost', 27017)
    collection = client.scraping.ebooks
    #ユニークキーを作成
    collection.create_index('key', unique=True)

    session = requests.Session() #複数ページをクロールするのでSessionを使う
    response = requests.get('https://gihyo.jp/dp')
    #ジェネレータを取得
    urls = scrape_list_page(response)
    #print([url for url in urls])
    for url in urls:
        key = extract_key(url) #URLからキーを取得する
        ebook = collection.find_one({'key':key})#Mongodbからkeyに該当するデータを探す
        print(key, 'hoge', ebook)


        if not ebook:
            time.sleep(1)
            response = session.get(url) #Sessionを使って詳細ページを取得する
            ebook = scrape_detail_page(response)
            collection.insert_one(ebook)
        
        print(ebook)
        #break




def scrape_list_page(response: requests.Response) -> Iterator[str]:
    """
    一覧ページのResponseから詳細ページのURLを抜き出すジェネレータ関数
    """

    html = lxml.html.fromstring(response.text)
    html.make_links_absolute(response.url)

    for a in html.cssselect('#listBook > li > a[itemprop="url"]'):
        url = a.get('href')
        yield url


def scrape_detail_page(response: requests.Response) -> dict:
    """
    詳細ページのResponseから電子書籍の情報をdictで取得する
    """

    html = lxml.html.fromstring(response.text)
    ebook = {
        'url':response.url,
        'key':extract_key(response.url),
        'title':html.cssselect('#bookTitle')[0].text_content(),
        'price':html.cssselect('.buy')[0].text.strip(),
        'content':[normalize_spaces(h3.text_content()) for h3 in html.cssselect('#content > h3')]
    }

    return ebook

def normalize_spaces(s: str) -> str:
    """
    連続する空白を一つのスペースで置き換え、前後の空白を削除した当たらしい文字列を取得する
    """

    return re.sub(r'\s+', ' ', s).strip()


def extract_key(url: str) -> str:
    """
    URLからキー（URLの末尾のISBN）を抜き出す
    """

    m = re.search(r'/([^/]+)$', url)
    return m.group(1)

if __name__ == "__main__":
    main()
