import os
import re
import asyncio
import logging
import tempfile
import shutil
import requests

from datetime import datetime
from uuid import uuid4
from urllib.parse import urlencode, urlparse, parse_qs, quote_plus, unquote

from pyrogram import Client, filters
from pyrogram.types import Message

from pymongo import MongoClient, ASCENDING

from config import CHANNEL, DATABASE, TERABOX, QUEUE


log = logging.getLogger(__name__)


# ============================================================
# DATABASE
# ============================================================

mongo_client = MongoClient(
    DATABASE.URI,
    serverSelectionTimeoutMS=10000,
)

db = mongo_client[DATABASE.NAME]

queue_col = db["terabox_queue"]
last_upload_col = db["terabox_lastupload"]


queue_col.create_index(
    [("status", ASCENDING), ("created_at", ASCENDING)]
)

queue_col.create_index(
    [("url", ASCENDING)],
    unique=False,
)

last_upload_col.create_index(
    [("keys", ASCENDING)]
)


# ============================================================
# CONSTANTS
# ============================================================

TERABOX_REGEX = re.compile(
    r'https?://(?:www\.)?[^/\s]*tera[^/\s]*\.[a-zA-Z]+/[^\s<>]+',
    re.IGNORECASE,
)

API_DOWNLOAD_REGEX = re.compile(
    r'https?://terabox-api\.mn-bots\.workers\.dev/dl/[^\s]+',
    re.IGNORECASE,
)

REQUEST_TIMEOUT = 60

MAX_RETRIES = max(1, QUEUE.MAX_RETRIES)
WORKER_COUNT = max(1, QUEUE.WORKERS)

RETRY_DELAY = max(5, QUEUE.RETRY_DELAY)


# ============================================================
# HELPERS
# ============================================================

def utcnow():
    return datetime.utcnow()


def get_size(size):
    size = int(size or 0)

    if size >= 1024 ** 3:
        return f"{size / 1024 ** 3:.2f} GB"

    if size >= 1024 ** 2:
        return f"{size / 1024 ** 2:.2f} MB"

    if size >= 1024:
        return f"{size / 1024:.2f} KB"

    return f"{size} bytes"


def clean_filename(filename):
    """
    Only used for safe filesystem fallback.
    Telegram filename is kept separately.
    """

    filename = os.path.basename(filename or "download")

    return re.sub(
        r'[\\/:*?"<>|]+',
        "_",
        filename,
    ).strip() or "download"


def filename_from_response(response, fallback):
    content_disposition = response.headers.get(
        "content-disposition",
        "",
    )

    match = re.search(
        r'filename\*?=(?:UTF-8\'\')?["\']?([^"\';]+)',
        content_disposition,
        re.IGNORECASE,
    )

    if match:
        try:
            return unquote(match.group(1))
        except Exception:
            pass

    return fallback or "download"


def extract_urls(text):
    if not text:
        return []

    found = TERABOX_REGEX.findall(text)

    result = []

    for url in found:
        url = url.rstrip(".,)>]}\"'")

        if url not in result:
            result.append(url)

    return result


# ============================================================
# TERABOX RESOLVER
# ============================================================

def get_api_download_url(api_data):
    qualities = api_data.get("qualities") or {}

    best_quality = api_data.get("best_quality")

    if best_quality and qualities.get(best_quality):
        return qualities[best_quality]

    if qualities:
        return next(iter(qualities.values()))

    return (
        api_data.get("media_url")
        or api_data.get("direct_download_url")
        or ""
    )


def get_api_file_info(share_url):
    api_url = (
        f"{TERABOX.API_DOWNLOAD_ENDPOINT}"
        f"?url={quote_plus(share_url)}"
    )

    response = requests.get(
        api_url,
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()

    data = response.json()

    if not data.get("success"):
        raise ValueError(
            data.get("message")
            or data.get("error")
            or "TeraBox API download failed"
        )

    download_link = get_api_download_url(data)

    if not download_link:
        raise ValueError(
            "API response did not contain a download link"
        )

    size_bytes = int(data.get("size") or 0)

    return {
        "name": data.get("filename") or "download",
        "download_link": download_link,
        "size_bytes": size_bytes,
        "size_str": get_size(size_bytes),
        "resolved_url": data.get("resolved_url") or share_url,
        "original_url": share_url,
        "source": "api",
    }


def find_between(text, start, end):
    try:
        return text.split(start, 1)[1].split(end, 1)[0]
    except Exception:
        return ""


def get_scraper_headers():
    cookie = TERABOX.COOKIE.strip()

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "DNT": "1",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
    }

    if cookie:
        headers["Cookie"] = cookie

    return headers


def get_file_info(share_url):
    headers = get_scraper_headers()

    response = requests.get(
        share_url,
        headers=headers,
        allow_redirects=True,
        timeout=REQUEST_TIMEOUT,
    )

    if response.status_code != 200:
        raise ValueError(
            f"Failed to fetch TeraBox page "
            f"({response.status_code})"
        )

    final_url = response.url

    parsed = urlparse(final_url)

    surl = parse_qs(
        parsed.query
    ).get("surl", [None])[0]

    if not surl:
        raise ValueError(
            "Invalid TeraBox URL: missing surl"
        )

    page = requests.get(
        final_url,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )

    html = page.text

    js_token = find_between(
        html,
        'fn%28%22',
        '%22%29',
    )

    logid = find_between(
        html,
        'dp-logid=',
        '&',
    )

    bdstoken = find_between(
        html,
        'bdstoken":"',
        '"',
    )

    if not all([
        js_token,
        logid,
        bdstoken,
    ]):
        raise ValueError(
            "Failed to extract TeraBox authentication tokens"
        )

    params = {
        "app_id": "250528",
        "web": "1",
        "channel": "dubox",
        "clienttype": "0",
        "jsToken": js_token,
        "dp-logid": logid,
        "page": "1",
        "num": "20",
        "by": "name",
        "order": "asc",
        "site_referer": final_url,
        "shorturl": surl,
        "root": "1,",
    }

    info = requests.get(
        "https://www.terabox.app/share/list?"
        + urlencode(params),
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    ).json()

    if info.get("errno") or not info.get("list"):
        raise ValueError(
            f"TeraBox list API error: "
            f"{info.get('errmsg', 'Unknown error')}"
        )

    file_data = info["list"][0]

    size_bytes = int(
        file_data.get("size", 0)
    )

    return {
        "name": file_data.get(
            "server_filename",
            "download",
        ),
        "download_link": file_data.get(
            "dlink",
            "",
        ),
        "size_bytes": size_bytes,
        "size_str": get_size(size_bytes),
        "resolved_url": share_url,
        "original_url": share_url,
        "source": "scraper",
    }


def get_download_info(url):
    if API_DOWNLOAD_REGEX.match(url):
        return {
            "name": "download",
            "download_link": url,
            "size_bytes": 0,
            "size_str": get_size(0),
            "resolved_url": url,
            "original_url": url,
            "source": "direct",
        }

    if TERABOX.USE_API_DOWNLOAD:
        try:
            return get_api_file_info(url)

        except Exception as api_error:
            log.warning(
                "TeraBox API resolver failed: %s",
                api_error,
            )

            if not TERABOX.API_FALLBACK_TO_SCRAPER:
                raise

    return get_file_info(url)


# ============================================================
# CACHE
# ============================================================

def get_cache_keys(info):
    keys = [
        info.get("original_url"),
        info.get("resolved_url"),
        info.get("download_link"),
    ]

    return list(
        dict.fromkeys(
            key for key in keys if key
        )
    )


def get_cached_upload(info):
    keys = get_cache_keys(info)

    if not keys:
        return None

    return last_upload_col.find_one(
        {
            "keys": {
                "$in": keys,
            },
            "file_id": {
                "$exists": True,
            },
        }
    )


def save_cached_upload(info, file_id, message_id=None):
    if not file_id:
        return

    keys = get_cache_keys(info)

    if not keys:
        return

    data = {
        "file_id": file_id,
        "file_name": info.get(
            "name",
            "download",
        ),
        "size_str": info.get(
            "size_str",
            get_size(0),
        ),
        "resolved_url": info.get(
            "resolved_url"
        ),
        "updated_at": utcnow(),
    }

    if message_id:
        data["telegram_message_id"] = message_id

    existing = last_upload_col.find_one(
        {
            "keys": {
                "$in": keys,
            }
        }
    )

    if existing:
        last_upload_col.update_one(
            {
                "_id": existing["_id"],
            },
            {
                "$set": data,
                "$addToSet": {
                    "keys": {
                        "$each": keys,
                    }
                },
            },
        )

    else:
        data["keys"] = keys
        data["created_at"] = utcnow()

        last_upload_col.insert_one(data)


# ============================================================
# QUEUE DATABASE FUNCTIONS
# ============================================================

def create_job(
    url,
    user_id,
    source_message_id,
):
    existing = queue_col.find_one(
        {
            "url": url,
            "status": {
                "$in": [
                    "QUEUED",
                    "DOWNLOADING",
                    "UPLOADING",
                    "RETRYING",
                    "COMPLETED",
                ]
            },
        },
        sort=[
            ("created_at", -1)
        ],
    )

    if existing:
        return existing, False

    job_id = uuid4().hex

    job = {
        "job_id": job_id,
        "user_id": user_id,
        "source_message_id": source_message_id,
        "url": url,
        "status": "QUEUED",
        "created_at": utcnow(),
        "started_at": None,
        "completed_at": None,
        "retry_count": 0,
        "filename": None,
        "filesize": 0,
        "telegram_message_id": None,
        "error": None,
        "worker": None,
    }

    queue_col.insert_one(job)

    return job, True


def get_next_job():
    return queue_col.find_one_and_update(
        {
            "status": "QUEUED",
        },
        {
            "$set": {
                "status": "DOWNLOADING",
                "started_at": utcnow(),
                "worker": str(
                    os.getpid()
                ),
                "error": None,
            }
        },
        sort=[
            ("created_at", ASCENDING)
        ],
        return_document=True,
    )


def recover_jobs():
    result = queue_col.update_many(
        {
            "status": {
                "$in": [
                    "DOWNLOADING",
                    "UPLOADING",
                    "RETRYING",
                ]
            }
        },
        {
            "$set": {
                "status": "QUEUED",
                "worker": None,
                "error": "Recovered after bot restart",
            }
        },
    )

    log.info(
        "Recovered %s interrupted jobs",
        result.modified_count,
    )


def mark_uploading(job_id):
    queue_col.update_one(
        {
            "job_id": job_id,
        },
        {
            "$set": {
                "status": "UPLOADING",
            }
        },
    )


def mark_completed(
    job_id,
    filename,
    filesize,
    telegram_message_id,
):
    queue_col.update_one(
        {
            "job_id": job_id,
        },
        {
            "$set": {
                "status": "COMPLETED",
                "filename": filename,
                "filesize": filesize,
                "telegram_message_id": telegram_message_id,
                "completed_at": utcnow(),
                "error": None,
                "worker": None,
            }
        },
    )


def mark_retry(job_id, error):
    job = queue_col.find_one(
        {
            "job_id": job_id,
        }
    )

    if not job:
        return

    retry_count = int(
        job.get("retry_count", 0)
    ) + 1

    if retry_count >= MAX_RETRIES:
        queue_col.update_one(
            {
                "job_id": job_id,
            },
            {
                "$set": {
                    "status": "FAILED",
                    "error": str(error),
                    "retry_count": retry_count,
                    "worker": None,
                }
            },
        )

    else:
        queue_col.update_one(
            {
                "job_id": job_id,
            },
            {
                "$set": {
                    "status": "RETRYING",
                    "error": str(error),
                    "retry_count": retry_count,
                    "worker": None,
                }
            },
        )


def convert_retrying_to_queued():
    queue_col.update_many(
        {
            "status": "RETRYING",
        },
        {
            "$set": {
                "status": "QUEUED",
            }
        },
    )


# ============================================================
# DOWNLOAD
# ============================================================

def download_file(info, target_dir):
    download_url = info.get("download_link")

    if not download_url:
        raise ValueError(
            "No direct download URL available"
        )

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64)"
        ),
        "Accept": "*/*",
        "Connection": "keep-alive",
    }

    if TERABOX.COOKIE.strip():
        headers["Cookie"] = TERABOX.COOKIE.strip()

    response = requests.get(
        download_url,
        headers=headers,
        stream=True,
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()

    original_name = filename_from_response(
        response,
        info.get("name") or "download",
    )

    info["name"] = original_name

    # Never use original filename as actual temporary path.
    # This prevents filename collisions and filesystem issues.
    safe_name = clean_filename(
        original_name
    )

    temp_path = os.path.join(
        target_dir,
        f"{uuid4().hex}_{safe_name}",
    )

    downloaded = 0

    total = int(
        response.headers.get(
            "content-length",
            0,
        )
        or 0
    )

    with open(
        temp_path,
        "wb",
    ) as output:

        for chunk in response.iter_content(
            chunk_size=1024 * 1024
        ):
            if not chunk:
                continue

            output.write(chunk)
            downloaded += len(chunk)

    if total and downloaded != total:
        raise IOError(
            f"Incomplete download: "
            f"{downloaded}/{total} bytes"
        )

    if not os.path.exists(temp_path):
        raise IOError(
            "Downloaded file does not exist"
        )

    actual_size = os.path.getsize(
        temp_path
    )

    info["size_bytes"] = (
        actual_size
        if actual_size
        else info.get("size_bytes", 0)
    )

    info["size_str"] = get_size(
        info["size_bytes"]
    )

    return temp_path


# ============================================================
# CAPTION
# ============================================================

def build_caption(info):
    return (
        f"File Name: {info.get('name', 'download')}\n"
        f"File Size: {info.get('size_str', 'Unknown')}\n"
        f"Source: TeraBox"
    )


# ============================================================
# PROCESS ONE JOB
# ============================================================

async def process_job(client, job):
    job_id = job["job_id"]
    url = job["url"]

    log.info(
        "Processing job %s: %s",
        job_id,
        url,
    )

    info = None
    temp_dir = None

    try:
        # ----------------------------------------------------
        # Resolve
        # ----------------------------------------------------

        info = await asyncio.to_thread(
            get_download_info,
            url,
        )

        filename = info.get(
            "name",
            "download",
        )

        filesize = int(
            info.get(
                "size_bytes",
                0,
            )
            or 0
        )

        queue_col.update_one(
            {
                "job_id": job_id,
            },
            {
                "$set": {
                    "filename": filename,
                    "filesize": filesize,
                }
            },
        )

        # ----------------------------------------------------
        # Duplicate/cache check
        # ----------------------------------------------------

        cached = await asyncio.to_thread(
            get_cached_upload,
            info,
        )

        if cached and cached.get("file_id"):
            log.info(
                "Cached Telegram file found for %s",
                filename,
            )

            mark_uploading(job_id)

            sent = await client.send_document(
                chat_id=CHANNEL.ID,
                document=cached["file_id"],
                caption=build_caption(info),
                file_name=filename,
            )

            message_id = (
                sent.id
                if sent
                else None
            )

            mark_completed(
                job_id,
                filename,
                filesize,
                message_id,
            )

            return

        # ----------------------------------------------------
        # Temporary storage
        # ----------------------------------------------------

        temp_dir = tempfile.mkdtemp(
            prefix="tera_transfer_"
        )

        temp_path = await asyncio.to_thread(
            download_file,
            info,
            temp_dir,
        )

        # ----------------------------------------------------
        # Upload
        # ----------------------------------------------------

        mark_uploading(job_id)

        if not CHANNEL.ID:
            raise ValueError(
                "CHANNEL_ID is not configured"
            )

        sent = await client.send_document(
            chat_id=CHANNEL.ID,
            document=temp_path,
            caption=build_caption(info),
            file_name=filename,
        )

        if not sent:
            raise RuntimeError(
                "Telegram did not return upload message"
            )

        if not sent.document:
            raise RuntimeError(
                "Telegram upload did not produce a document"
            )

        message_id = sent.id

        # ----------------------------------------------------
        # Cache Telegram file_id
        # ----------------------------------------------------

        await asyncio.to_thread(
            save_cached_upload,
            info,
            sent.document.file_id,
            message_id,
        )

        # ----------------------------------------------------
        # Completed
        # ----------------------------------------------------

        mark_completed(
            job_id,
            filename,
            filesize,
            message_id,
        )

        log.info(
            "Completed job %s: %s",
            job_id,
            filename,
        )

    except Exception as error:

        log.exception(
            "Job %s failed",
            job_id,
        )

        mark_retry(
            job_id,
            error,
        )

    finally:

        # ----------------------------------------------------
        # ALWAYS cleanup temporary storage
        # ----------------------------------------------------

        if temp_dir and os.path.exists(
            temp_dir
        ):
            try:
                shutil.rmtree(
                    temp_dir,
                    ignore_errors=True,
                )
            except Exception:
                pass


# ============================================================
# WORKER
# ============================================================

async def queue_worker(client, worker_id):
    log.info(
        "Queue worker %s started",
        worker_id,
    )

    while True:

        try:

            convert_retrying_to_queued()

            job = await asyncio.to_thread(
                get_next_job
            )

            if not job:
                await asyncio.sleep(3)
                continue

            log.info(
                "Worker %s picked job %s",
                worker_id,
                job["job_id"],
            )

            await process_job(
                client,
                job,
            )

            await asyncio.sleep(1)

        except asyncio.CancelledError:
            raise

        except Exception:
            log.exception(
                "Queue worker %s crashed",
                worker_id,
            )

            await asyncio.sleep(5)


# ============================================================
# STARTUP
# ============================================================

_workers = []


async def start_queue_workers(client):

    recover_jobs()

    for index in range(
        WORKER_COUNT
    ):
        task = asyncio.create_task(
            queue_worker(
                client,
                index + 1,
            )
        )

        _workers.append(task)

    log.info(
        "Started %s queue worker(s)",
        WORKER_COUNT,
    )


# ============================================================
# START
# ============================================================

@Client.on_message(
    filters.private
    & filters.regex(TERABOX_REGEX)
)
async def handle_terabox(
    client: Client,
    message: Message,
):

    text = message.text or ""

    urls = extract_urls(text)

    if not urls:
        return

    added = 0
    duplicate = 0

    for url in urls:

        job, created = await asyncio.to_thread(
            create_job,
            url,
            message.from_user.id,
            message.id,
        )

        if created:
            added += 1
        else:
            duplicate += 1

    status = (
        "📥 <b>Queue added</b>\n\n"
        f"New files: <b>{added}</b>\n"
        f"Duplicates: <b>{duplicate}</b>\n"
        f"Total links: <b>{len(urls)}</b>"
    )

    await message.reply_text(
        status,
        disable_web_page_preview=True,
    )


# ============================================================
# /QUEUE
# ============================================================

@Client.on_message(
    filters.private
    & filters.command("queue")
)
async def queue_command(
    client: Client,
    message: Message,
):

    rows = await asyncio.to_thread(
        lambda: list(
            queue_col.find(
                {
                    "user_id":
                        message.from_user.id,
                    "status": {
                        "$in": [
                            "QUEUED",
                            "DOWNLOADING",
                            "UPLOADING",
                            "RETRYING",
                        ],
                    },
                }
            ).sort(
                "created_at",
                ASCENDING,
            ).limit(20)
        )
    )

    if not rows:
        await message.reply_text(
            "📭 Your queue is empty."
        )
        return

    lines = [
        "📋 <b>Your Queue</b>",
        "",
    ]

    for index, job in enumerate(
        rows,
        1,
    ):

        name = (
            job.get("filename")
            or "Resolving..."
        )

        status = job.get(
            "status",
            "UNKNOWN",
        )

        lines.append(
            f"{index}. <b>{name}</b> — "
            f"{status}"
        )

    await message.reply_text(
        "\n".join(lines)
    )


# ============================================================
# /STATUS
# ============================================================

@Client.on_message(
    filters.private
    & filters.command("status")
)
async def status_command(
    client: Client,
    message: Message,
):

    pipeline = [
        {
            "$match": {
                "user_id":
                    message.from_user.id
            }
        },
        {
            "$group": {
                "_id": "$status",
                "count": {
                    "$sum": 1
                },
            }
        },
    ]

    data = await asyncio.to_thread(
        lambda: list(
            queue_col.aggregate(
                pipeline
            )
        )
    )

    counts = {
        row["_id"]: row["count"]
        for row in data
    }

    text = (
        "📊 <b>Transfer Status</b>\n\n"
        f"Queued: {counts.get('QUEUED', 0)}\n"
        f"Downloading: {counts.get('DOWNLOADING', 0)}\n"
        f"Uploading: {counts.get('UPLOADING', 0)}\n"
        f"Retrying: {counts.get('RETRYING', 0)}\n"
        f"Completed: {counts.get('COMPLETED', 0)}\n"
        f"Failed: {counts.get('FAILED', 0)}\n"
        f"Cancelled: {counts.get('CANCELLED', 0)}"
    )

    await message.reply_text(text)


# ============================================================
# /CANCEL
# ============================================================

@Client.on_message(
    filters.private
    & filters.command("cancel")
)
async def cancel_command(
    client: Client,
    message: Message,
):

    parts = message.text.split(
        maxsplit=1
    )

    if len(parts) == 1:
        await message.reply_text(
            "Usage:\n/cancel JOB_ID"
        )
        return

    job_id = parts[1].strip()

    result = await asyncio.to_thread(
        lambda: queue_col.update_one(
            {
                "job_id": job_id,
                "user_id":
                    message.from_user.id,
                "status": {
                    "$in": [
                        "QUEUED",
                        "RETRYING",
                    ]
                },
            },
            {
                "$set": {
                    "status": "CANCELLED",
                    "error": "Cancelled by user",
                    "worker": None,
                }
            },
        )
    )

    if result.modified_count:
        await message.reply_text(
            f"🛑 Job `{job_id}` cancelled."
        )
    else:
        await message.reply_text(
            "❌ Job not found or already processing."
        )


# ============================================================
# /CLEAR
# ============================================================

@Client.on_message(
    filters.private
    & filters.command("clear")
)
async def clear_command(
    client: Client,
    message: Message,
):

    result = await asyncio.to_thread(
        lambda: queue_col.update_many(
            {
                "user_id":
                    message.from_user.id,
                "status": {
                    "$in": [
                        "QUEUED",
                        "RETRYING",
                    ],
                },
            },
            {
                "$set": {
                    "status": "CANCELLED",
                    "error": "Queue cleared by user",
                    "worker": None,
                }
            },
        )
    )

    await message.reply_text(
        f"🧹 Cancelled "
        f"<b>{result.modified_count}</b> queued jobs."
    )


# ============================================================
# /RETRY
# ============================================================

@Client.on_message(
    filters.private
    & filters.command("retry")
)
async def retry_command(
    client: Client,
    message: Message,
):

    result = await asyncio.to_thread(
        lambda: queue_col.update_many(
            {
                "user_id":
                    message.from_user.id,
                "status": "FAILED",
            },
            {
                "$set": {
                    "status": "QUEUED",
                    "retry_count": 0,
                    "error": None,
                    "worker": None,
                }
            },
        )
    )

    await message.reply_text(
        f"🔄 Re-queued "
        f"<b>{result.modified_count}</b> failed jobs."
    )
async def stop_queue_workers():
    """
    Gracefully stop all queue workers.
    """

    global _workers

    if not _workers:
        return

    logger.info(
        "Stopping %s TeraBox queue worker(s)...",
        len(_workers),
    )

    for task in _workers:
        if not task.done():
            task.cancel()

    for task in _workers:
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception(
                "Error while stopping queue worker."
            )

    _workers.clear()

    logger.info(
        "All TeraBox queue workers stopped."
    )
