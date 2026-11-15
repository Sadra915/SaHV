import asyncio
import aiohttp
import time
import json
import statistics

async def worker(session, url, semaphore, results):
    async with semaphore:
        start = time.perf_counter()

        try:
            async with session.get(url) as response:
                await response.read()

                latency = (
                    time.perf_counter() - start
                ) * 1000

                results.append({
                    "status": response.status,
                    "latency": round(latency, 2),
                    "success": 200 <= response.status < 400
                })

        except Exception as e:
            results.append({
                "success": False,
                "error": str(e)
            })


async def main():

    print("=== RUST WARFARE LOADLAB ===")

    url = input("Test URL: ").strip()
    count = int(input("Total requests (max 100): "))
    concurrency = int(input("Concurrency (max 5): "))

    if not url.startswith("https://"):
        raise ValueError("Use an HTTPS test endpoint")

    if not 1 <= count <= 100:
        raise ValueError("Request count must be 1-100")

    if not 1 <= concurrency <= 5:
        raise ValueError("Concurrency must be 1-5")

    results = []
    semaphore = asyncio.Semaphore(concurrency)

    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(
        timeout=timeout
    ) as session:

        start = time.perf_counter()

        tasks = [
            worker(session, url, semaphore, results)
            for _ in range(count)
        ]

        await asyncio.gather(*tasks)

        elapsed = time.perf_counter() - start

    successful = [
        x["latency"] for x in results
        if x.get("success") and "latency" in x
    ]

    report = {
        "target": url,
        "total": count,
        "successful": len(successful),
        "failed": count - len(successful),
        "duration": round(elapsed, 2),
        "average_latency": (
            round(statistics.mean(successful), 2)
            if successful else None
        ),
        "RPS": round(count / elapsed, 2)
    }

    print(json.dumps(report, indent=4))

    with open("report.json", "w") as f:
        json.dump(report, f, indent=4)


asyncio.run(main())
