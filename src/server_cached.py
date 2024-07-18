import asyncio
from contextlib import asynccontextmanager
import json
import logging
from urllib.parse import urlparse
import uuid
import uvicorn
import httpx
import redis.asyncio as redis
from pydantic import BaseModel, Field, HttpUrl
from fastapi import BackgroundTasks, FastAPI, status
from fastapi.responses import JSONResponse

logger = logging.getLogger("crawl")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

client = redis.Redis()
name_url_hash = "urls_hash"
name_count_submit_domain = "count_submit"


async def clear_hash_data(cache_timeout: int = 60):
    await asyncio.sleep(cache_timeout)
    logger.info("Starting cache cleanup task")
    keys = await client.keys("*")
    for key in keys:
        await client.delete(key)
        logger.info(f"Deleted cache key: {key.decode('utf-8')}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(clear_hash_data(30))
    yield
    await client.aclose()


app = FastAPI(lifespan=lifespan)


class TaskRequest(BaseModel):
    urls: list[HttpUrl]


class Task(BaseModel):
    status: str
    result: list[str] | None = []
    id: uuid.UUID = Field(default_factory=lambda: uuid.uuid4())


tasks: dict[uuid.UUID, Task] = {}


def cache_query(func):
    async def wrapped(url: str):
        value_in_cache = await client.hget(name_url_hash, url)
        domain = urlparse(url).netloc
        await client.hincrby(name_count_submit_domain, domain, 1)

        if value_in_cache is not None:
            logger.info(f"Cache hit for URL: {url}")
            return value_in_cache

        logger.info(f"Cache miss for URL: {url}")
        result = await func(url)
        await client.hset(name_url_hash, url, json.dumps(result))

        return result

    return wrapped


@cache_query
async def get_response_code(url: str):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            status_code = response.status_code
            logger.info(f"Fetched URL: {url} with status code: {status_code}")
        except httpx.ConnectError:
            status_code = status.HTTP_404_NOT_FOUND
            logger.error(f"Failed to fetch URL: {url}")

    return status_code


async def pumping_urls(task_id: uuid.UUID, urls: list[HttpUrl]):
    task = tasks[task_id]
    results = await asyncio.gather(*[get_response_code(str(url)) for url in urls])
    task.result.extend(
        [f"{status_code}\t{url}" for status_code, url in zip(results, urls)]
    )
    task.status = "ready"
    logger.info(f"Completed processing task ID: {task_id}")

    domain_count_s = await client.hgetall(name_count_submit_domain)
    domain_count = {
        key.decode("utf-8"): int(value) for key, value in domain_count_s.items()
    }
    logger.info(f"Domain counts: {domain_count}")


@app.post(
    "/api/v1/tasks/",
)
async def create_task(
    task_request: TaskRequest,
    background_tasks: BackgroundTasks,
) -> Task:
    task = Task(status="running")
    tasks[task.id] = task
    background_tasks.add_task(pumping_urls, task_id=task.id, urls=task_request.urls)
    logger.info(f"Created new task ID: {task.id}")
    return task


@app.get("/api/v1/tasks/{received_task_id}")
async def get_status_task(
    received_task_id: uuid.UUID,
):
    if received_task_id not in tasks:
        return JSONResponse(
            f"Task with id={received_task_id} not found",
            status_code=status.HTTP_404_NOT_FOUND,
        )
    logger.info(f"Fetched status for task ID: {received_task_id}")
    return tasks[received_task_id]


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="localhost",
        port=8888,
    )
