import logging
import threading

from flask import Flask

from pyrogram import Client

from config import BOT, API, OWNER, WEB


# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logging.getLogger("pyrogram").setLevel(logging.WARNING)

logger = logging.getLogger("MN_BOT")


# ---------------------------------------------------------
# FLASK HEALTH CHECK
# ---------------------------------------------------------

app = Flask(__name__)


@app.route("/")
def home():
    return "MN TeraBox Bot is running!"


@app.route("/health")
def health():
    return "OK"


def run_flask():
    app.run(
        host="0.0.0.0",
        port=WEB.PORT,
        debug=False,
        use_reloader=False,
    )


# ---------------------------------------------------------
# TELEGRAM BOT
# ---------------------------------------------------------

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

        # Start Pyrogram
        await super().start()

        # Get bot information
        me = await self.get_me()

        BOT.USERNAME = f"@{me.username}" if me.username else ""

        self.mention = me.mention
        self.username = me.username

        logger.info(
            "Telegram bot started successfully: @%s",
            me.username,
        )

        # -------------------------------------------------
        # START TeraBox Queue Workers
        # -------------------------------------------------

        try:
            from plugins.terabox import start_queue_workers

            await start_queue_workers(self)

            logger.info(
                "TeraBox queue workers started successfully."
            )

        except Exception as e:
            logger.exception(
                "Failed to start TeraBox queue workers: %s",
                e,
            )

        # -------------------------------------------------
        # OWNER NOTIFICATION
        # -------------------------------------------------

        if OWNER.ID:
            try:
                await self.send_message(
                    chat_id=OWNER.ID,
                    text=(
                        "✅ <b>MN TeraBox Bot Started</b>\n\n"
                        "Queue system: <b>ONLINE</b>\n"
                        "TeraBox worker: <b>RUNNING</b>"
                    ),
                )
            except Exception as e:
                logger.warning(
                    "Could not send owner startup message: %s",
                    e,
                )

    async def stop(self, *args):

        # -------------------------------------------------
        # STOP QUEUE WORKERS FIRST
        # -------------------------------------------------

        try:
            from plugins.terabox import stop_queue_workers

            await stop_queue_workers()

            logger.info(
                "TeraBox queue workers stopped."
            )

        except Exception as e:
            logger.warning(
                "Queue worker shutdown warning: %s",
                e,
            )

        # Stop Pyrogram
        await super().stop()

        logger.info("MN Bot stopped.")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":

    # Start Flask health server in background
    flask_thread = threading.Thread(
        target=run_flask,
        daemon=True,
    )

    flask_thread.start()

    logger.info("Health server started on port %s", WEB.PORT)

    # Start Telegram bot
    MN_Bot().run()
