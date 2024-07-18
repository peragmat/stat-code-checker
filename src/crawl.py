import argparse
import asyncio
import logging
import sys
import httpx
from pydantic import BaseModel, HttpUrl, ValidationError


logger = logging.getLogger("crawl")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class UrlsModel(BaseModel):
    urls: list[HttpUrl]


async def get_current_task(task_id):
    async with httpx.AsyncClient() as client:
        response = await client.get(f"http://127.0.0.1:8888/api/v1/tasks/{task_id}")
        response.raise_for_status()
    task = response.json()
    return task


async def send_urls(urls: list[str]) -> httpx.Response:
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                "http://127.0.0.1:8888/api/v1/tasks/",
                json={"urls": urls},
            )
        except httpx.ConnectError as e:
            logger.error(e)
            sys.exit(0)

        response.raise_for_status()
        return response


def validate_urls(urls):
    try:
        UrlsModel(urls=urls)
    except ValidationError as e:
        logger.error(f"Invalid URLs: {e}", file=sys.stderr)
        sys.exit(0)


async def main(urls):
    validate_urls(urls)

    response = await send_urls(urls)
    logger.info("Urls sended. Waiting for processing to complete...")
    task = response.json()

    task_id = task["id"]

    while True:
        task = await get_current_task(task_id)
        if task["status"] == "ready":
            break
        else:
            await asyncio.sleep(0.1)

    print("Processed urls:")
    print(*task["result"], sep="\n")


if __name__ == "__main__":
    # python crawl.py https://httpbin.org/status/200 https://httpbin.org/status/404 https://httpbin.org/status/500

    parser = argparse.ArgumentParser(description="Submit URLs for processing")
    parser.add_argument("urls", nargs="+", help="List of URLs to submit")
    args = parser.parse_args()

    asyncio.run(main(args.urls))
