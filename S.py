import asyncio
import aiohttp
import time
import json
from datetime import datetime, timezone

URL = "https://shukshuk.co.il"

async def main():
    timeout = aiohttp.ClientTimeout(total=15)

    headers = {
        "User-Agent": "Rust-Warfare-LoadLab/1.0"
    }

    async with aiohttp.ClientSession(
        timeout=timeout,
        headers=headers
    ) as session:

        start = time.perf_counter()

        try:
            async with session.get(
                URL,
                allow_redirects=False
            ) as response:

                elapsed = round(
                    (time.perf_counter() - start) * 1000, 2
                )

                report = {
                    "target": URL,
                    "timestamp": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "status": response.status,
                    "latency_ms": elapsed,
                    "server": response.headers.get("Server"),
                    "cf_ray": response.headers.get("CF-Ray"),
                    "retry_after": response.headers.get("Retry-After"),
                    "location": response.headers.get("Location"),
                    "content_type": response.headers.get("Content-Type")
                }

                print(json.dumps(report, indent=4))

                with open("report.json", "w") as f:
                    json.dump(report, f, indent=4)

        except Exception as error:
            print("Connection Error:", str(error))

asyncio.run(main())
