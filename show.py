import os
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from lark import Lark
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

def _get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid %s=%s, fallback to %d", name, value, default)
        return default

def main():
    larkClient = Lark(os.getenv("APP_ID"), os.getenv("APP_SECRET"))
    bitable_page_size = _get_int_env("BITABLE_PAGE_SIZE", 500)
    larkClient.list_left_employees_from_bitable(os.getenv("BITABLE_ID"), os.getenv("TABLE_ID"), bitable_page_size)

if __name__ == "__main__":
    main()
