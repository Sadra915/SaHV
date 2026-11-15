import asyncio
import aiohttp
import json
import time
import random

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone

# ==============================
# RUST WARFARE DUAL CRAWLER
# ==============================

URL = "http://127.0.0.1:8000"

CRAWLER_COUNT = 1000
MAX_PAGES = 1000
REQUEST_DELAY = 1

HEADERS = {
    "User-Agent": "Rust-Warfare-DualCrawler/1.0"
}

visited = set()
scheduled = {URL}
results = []

queue = asyncio.Queue()
stop_event = asyncio.Event()
request_lock = asyncio.Lock()

last_request = 0


def same_domain(url):
    return (
        urlparse(url).netloc ==
        urlparse(URL).netloc
    )


def extract_links(html, current_url):
    soup = BeautifulSoup(html, "html.parser")
    links = set()

    for tag in soup.find_all("a", href=True):
        link = urljoin(current_url, tag["href"])
        parsed = urlparse(link)

        if (
            parsed.scheme in ("http", "https")
            and same_domain(link)
            and parsed.path
            and not parsed.path.endswith((
                ".png", ".jpg", ".jpeg",
                ".gif", ".svg", ".pdf",
                ".zip", ".css", ".js"
            ))
        ):
            links.add(link)

    return list(links)


async def throttle():
    global last_request

    async with request_lock:
        now = time.monotonic()

        wait = REQUEST_DELAY - (now - last_request)

        if wait > 0:
            await asyncio.sleep(wait)

        last_request = time.monotonic()


async def inspect_page(session, url, worker_id):

    await throttle()

    print(f"[CRAWLER {worker_id}] VISIT: {url}")

    start = time.perf_counter()

    try:
        async with session.get(url) as response:

            content_type = response.headers.get(
                "Content-Type", ""
            )

            result = {
                "crawler": worker_id,
                "url": str(response.url),
                "status": response.status,
                "latency_ms": round(
                    (time.perf_counter() - start) * 1000,
                    2
                ),
                "server": response.headers.get("Server"),
                "cf_ray": response.headers.get("CF-Ray"),
                "content_type": content_type,
                "timestamp": datetime.now(
                    timezone.utc
                ).isoformat(),
                "title": None,
                "text": None,
                "links": []
            }

            if response.status == 429:
                result["error"] = "Rate limited"
                stop_event.set()
                return result, []

            if response.status != 200:
                return result, []

            if "text/html" not in content_type:
                return result, []

            html = await response.text()

            soup = BeautifulSoup(html, "html.parser")

            result["title"] = (
                soup.title.get_text(strip=True)
                if soup.title else None
            )

            for element in soup([
                "script", "style", "noscript"
            ]):
                element.decompose()

            result["text"] = soup.get_text(
                separator=" ",
                strip=True
            )[:10000]

            links = extract_links(html, str(response.url))

            result["links"] = links

            print(
                f"[CRAWLER {worker_id}] "
                f"HTTP {response.status} | "
                f"{result['latency_ms']} ms"
            )

            return result, links

    except Exception as error:

        return {
            "crawler": worker_id,
            "url": url,
            "error": str(error)
        }, []


async def crawler(worker_id, session):

    print(f"[CRAWLER {worker_id}] STARTED")

    while True:

        url = await queue.get()

        try:
            if stop_event.is_set():
                continue

            if len(visited) >= MAX_PAGES:
                continue

            visited.add(url)

            result, links = await inspect_page(
                session,
                url,
                worker_id
            )

            results.append(result)

            for link in links:

                if (
                    not stop_event.is_set()
                    and len(scheduled) < MAX_PAGES
                    and link not in scheduled
                ):
                    scheduled.add(link)
                    await queue.put(link)

        finally:
            queue.task_done()


async def main():

    print("=" * 45)
    print(" RUST WARFARE DUAL CRAWLER")
    print("=" * 45)

    timeout = aiohttp.ClientTimeout(total=20)

    connector = aiohttp.TCPConnector(limit=100)

    queue.put_nowait(URL)

    async with aiohttp.ClientSession(
        headers=HEADERS,
        timeout=timeout,
        connector=connector
    ) as session:

        workers = [
            asyncio.create_task(
                crawler(i + 1, session)
            )
            for i in range(CRAWLER_COUNT)
        ]

        await queue.join()

        for _ in workers:
            await queue.put(None)

        # Stop workers using sentinels
        for worker in workers:
            worker.cancel()

        await asyncio.gather(
            *workers,
            return_exceptions=True
        )

    report = {
        "project": "Rust Warfare Dual Crawler",
        "target": URL,
        "crawler_count": CRAWLER_COUNT,
        "pages_visited": len(visited),
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "pages": results
    }

    with open(
        "site_report.json",
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            report,
            file,
            ensure_ascii=False,
            indent=4
        )

    print("\n========== REPORT ==========")
    print("Crawlers:", CRAWLER_COUNT)
    print("Pages:", len(visited))
    print("Report: site_report.json")
    print("============================")


asyncio.run(main())
