import re
import csv
import json
from collections import deque
from typing import Tuple, List
from urllib.parse import urlencode

import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from joblib import Parallel, delayed

VK_REGEXP = re.compile(r'<em class="pm_counter">([\d,]+)</em>')
FACEBOOK_REGEXP = re.compile(r'([\d,]+) people follow this')
TWITTER_URL_REGEXP = re.compile(r"https://twitter.com/(\w+)")


def extract_followers(provider: str, url: str) -> int:
    # Force english version where needed
    headers = {"Accept-Language": "en-US,en;q=0.5"}

    try:
        if provider == "vk":
            # Would be better to avoid parsing
            resp = requests.get(url, headers=headers)
            match = re.match(VK_REGEXP, resp.text)
            if match is None:
                return -1
            return int(match.group(1).replace(',', ''))
        elif provider == "instagram":
            # HACK: Request JSON data (official?)
            data = requests.get(url + "?__a=1").json()
            return data['graphql']['user']['edge_followed_by']['count']
        elif provider == "facebook":
            # Would be better to avoid parsing
            resp = requests.get(url, headers=headers)
            match = re.search(FACEBOOK_REGEXP, resp.text)
            if match is None:
                return -1
            return int(match.group(1).replace(',', ''))
        elif provider == "twitter":
            # HACK: Unofficial endpoint, unofficial/unsupported by twitter
            match = re.match(TWITTER_URL_REGEXP, url)
            if match is None:
                return -1
            screen_name = match.group(1)
            data = requests.get(
                f"https://cdn.syndication.twimg.com/widgets/followbutton/info.json?screen_names={screen_name}"
            ).json()
            return data[0]['followers_count']
    except Exception as e:
        print(e)
        return -1

    raise NotImplementedError


def find_links(soup: BeautifulSoup, domain: str):
    """
    Find all links in soup by domain
    :param soup: BeautifulSoup root node
    :param domain: domain which should be contained
    :return: All links with corresponding domain
    """
    results = []
    for link in soup.find_all('a'):
        if not link.has_attr('href'):
            continue
        if domain in link['href']:
            results.append(link['href'])
    return results


class SocialCrawler:
    MAX_ITER = 1  # Max pages to parse, set to 1 because it took too long to go deeper.

    social_providers = [
        ("vk", "vk.com"),
        ("facebook", "facebook.com"),
        ("twitter", "twitter.com"),
        ("instagram", "instagram.com")
    ]

    def __init__(self, url):
        self.url = url

    def crawl(self) -> List[Tuple[str, str, int]]:
        """
        Runs BFS on all pages of the website. Tries to find social urls.
        :return: List of tuples in format <provider, url>
        """
        cnt = 0
        was = set()
        q = deque()

        q.append(self.url)
        was.add(self.url)

        results = set()

        while len(q) > 0 and cnt < self.MAX_ITER:
            cnt += 1
            nxt = q.popleft()

            try:
                resp = requests.get(nxt, allow_redirects=True)
            except:
                continue

            if not resp.ok:
                continue

            soup = BeautifulSoup(resp.text, features="lxml")

            # Finding social accounts
            for name, domain in self.social_providers:
                for link in find_links(soup, domain):
                    results.add((name, link, extract_followers(name, link)))

            # Going deeper
            for link in find_links(soup, self.url.replace("http://", "").replace("https://", "")):
                if link not in was:
                    was.add(link)
                    q.append(link)

        return list(results)


def add_dict_prefix(prefix: str, data: dict) -> dict:
    """
    Adding prefix to all keys of a dictionary.
    :param prefix: Prefix to add
    :param data: Dictionary to transform
    :return: New dict where all keys are prefixed with `prefix`
    """
    return {prefix + k: v for k, v in data.items()}


def fetch_metadata(soup: BeautifulSoup) -> Tuple[str, List[str], List[str]]:
    """
    Extracting page metadata.
    :param soup: BeautifulSoup root node
    :return: Tuple of title, keywords and descriptions
    """
    title = getattr(soup.title, 'text', '')
    keywords = [item['content'] for item in soup.select('[name=Keywords][content], [name=keywords][content]')]
    descriptions = [item['content'] for item in soup.select('[name=Description][content], [name=description][content]')]

    return title, keywords, descriptions


def process(organization: dict) -> dict:
    """
    Adding website and social info.
    :param organization: Dict with organization data
    :return: Organization with added information from url
    """
    # Убеждаемся, что указана схема
    if not organization['site_url'].startswith('http'):
        organization['site_url'] = "http://" + organization['site_url']

    # Пытаемся подключиться
    try:
        resp = requests.get(organization['site_url'], allow_redirects=True)
    except:
        resp = None

    # Если не получилось - говорим, что сайт недоступен
    site_available = resp is not None and resp.ok

    if site_available:
        soup = BeautifulSoup(resp.text, features='lxml')

        # Метаданные
        title, keywords, descriptions = fetch_metadata(soup)

        # Соцсети
        social_spider = SocialCrawler(organization['site_url'])
        social_urls = social_spider.crawl()
    else:
        title, keywords, descriptions = "", [], []
        social_urls = []

    return {
        **organization,
        **add_dict_prefix("site_", {
            "available": site_available,
            "title": title,
            "keywords": keywords,
            "descriptions": descriptions,
        }),
        "social_urls": social_urls
    }


def main():
    # Создание URL
    LIMIT = 5000  # Как вариант - загружать количество данных по запросу и делать пагинацию
    params = {
        "orientation": "3,6",  # Техническая или естественнонаучная организация
        "perPage": LIMIT,  # Загружать не больше лимита
    }
    request_url = f"http://dop.edu.ru/organization/list?{urlencode(params)}"

    # Получение данных и обработка ошибок
    resp = requests.get(request_url)
    resp.raise_for_status()
    data = resp.json()
    if not data['success']:
        print("Не получилось запросить список")
        exit(-1)

    # Запись организаций в CSV файл
    organizations = data['data']['list']

    print("Fetching data...")
    organizations = Parallel(n_jobs=100, verbose=10)(delayed(process)(organization) for organization in organizations)

    print("Writing CSV...")
    with open("results.csv", "w", newline="") as f:
        fieldnames = list(organizations[0].keys())
        csv_writer = csv.DictWriter(f, fieldnames=fieldnames)

        csv_writer.writeheader()
        for organization in tqdm(organizations):
            csv_writer.writerow({field: organization[field] for field in fieldnames})


if __name__ == '__main__':
    main()
