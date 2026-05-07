import os
import sys
import requests
from dotenv import load_dotenv


def main() -> int:
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID", file=sys.stderr)
        return 2

    text = " ".join(sys.argv[1:]).strip() or "(no message)"

    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        },
        timeout=15,
    )
    if r.status_code != 200:
        print("status:", r.status_code, file=sys.stderr)
        print(r.text, file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
