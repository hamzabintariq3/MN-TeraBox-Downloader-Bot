from dotenv import load_dotenv
load_dotenv()

import os


class BOT:
    TOKEN = os.environ.get("TOKEN", "")


class API:
    HASH = os.environ.get("API_HASH", "")
    ID = int(os.environ.get("API_ID", 0))


class OWNER:
    ID = int(os.environ.get("OWNER", 0))


class CHANNEL:
    ID = int(os.environ.get("CHANNEL_ID", 0))


class WEB:
    PORT = int(
        os.environ.get(
            "PORT",
            8000,
        )
    )


class DATABASE:
    URI = os.environ.get(
        "DB_URI",
        "",
    )

    NAME = os.environ.get(
        "DB_NAME",
        "MN_Bot_DB",
    )


class TERABOX:

    API_DOWNLOAD_ENDPOINT = os.environ.get(
        "TERABOX_API_DOWNLOAD_ENDPOINT",
        "https://terabox-api.mn-bots.workers.dev/download",
    )

    USE_API_DOWNLOAD = (
        os.environ.get(
            "TERABOX_USE_API_DOWNLOAD",
            "true",
        ).lower()
        == "true"
    )

    API_FALLBACK_TO_SCRAPER = (
        os.environ.get(
            "TERABOX_API_FALLBACK",
            "true",
        ).lower()
        == "true"
    )

    # IMPORTANT:
    # Never hardcode TeraBox cookies here.
    COOKIE = os.environ.get(
        "TERABOX_COOKIE",
        "",
    )


class QUEUE:

    # Start safely with 1 worker.
    # Later this can be changed to 2-4.
    WORKERS = int(
        os.environ.get(
            "QUEUE_WORKERS",
            1,
        )
    )

    MAX_RETRIES = int(
        os.environ.get(
            "QUEUE_MAX_RETRIES",
            3,
        )
    )

    RETRY_DELAY = int(
        os.environ.get(
            "QUEUE_RETRY_DELAY",
            10,
        )
    )
