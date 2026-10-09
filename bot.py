
import asyncio
import logging
import os
import threading

from flask import Flask
from pyrogram import Client, idle
from pyrogram.errors import FloodWait

from config import BOT, API, OWNER

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logging.getLogger("pyrogram").setLevel(logging.WARNING)
logger = logging.getLogger("MN_BOT")

app = Flask(__name__)


@app.route("/")
def home():
    return "MnBot is running!"


@app.route("/health")
def health():
    return "OK"


def run_flask():
    port = int(os.environ.get("PORT", 8000))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
    )


class MN_Bot(Client):
    def __init__(self):
        super().__init__(
            "MN-Bot",
            api_id=API.ID,
            api_hash=API.HASH,
            bot_token=BOT.TOKEN,
            plugins=dict(root="plugins"),
            workers=16,
        )

    async def start(self):
        await super().start()

        me = await self.get_me()
        BOT.USERNAME = f"@{me.username}" if me.username else ""
        self.mention = me.mention
        self.username = me.username

        logger.info("Bot started successfully: @%s", me.username)

        if OWNER.ID:
            try:
                await self.send_message(
                    chat_id=OWNER.ID,
                    text=f"✅ {me.first_name} Bot started successfully.",
                )
            except Exception:
                logger.exception("Could not send startup message to owner.")

    async def stop(self, *args):
        await super().stop()
        logger.info("Bot stopped.")


async def main():
    while True:
        bot = MN_Bot()

        try:
            logger.info("Starting Telegram bot...")
            await bot.start()
            await idle()

            await bot.stop()
            return

        except FloodWait as exc:
            wait_seconds = max(int(exc.value), 1) + 5

            logger.error(
                "Telegram FloodWait: waiting %s seconds before retrying.",
                wait_seconds,
            )

            try:
                if bot.is_connected:
                    await bot.stop()
            except Exception:
                logger.warning(
                    "Cleanup after FloodWait was unsuccessful.",
                    exc_info=True,
                )

            await asyncio.sleep(wait_seconds)

        except Exception:
            logger.exception("Bot startup failed with an unexpected error.")

            try:
                if bot.is_connected:
                    await bot.stop()
            except Exception:
                logger.warning("Bot cleanup failed.", exc_info=True)

            raise


if __name__ == "__main__":
    flask_thread = threading.Thread(
        target=run_flask,
        daemon=True,
    )
    flask_thread.start()

    logger.info(
        "Health server started on port %s",
        os.environ.get("PORT", "8000"),
    )

    asyncio.run(main())
