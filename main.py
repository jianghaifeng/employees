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
    start = perf_counter()
    larkClient = Lark(os.getenv("APP_ID"), os.getenv("APP_SECRET"))
    ehr_page_size = _get_int_env("EHR_PAGE_SIZE", 100)
    bitable_page_size = _get_int_env("BITABLE_PAGE_SIZE", 500)

    fetch_start = perf_counter()
    with ThreadPoolExecutor(max_workers=2) as executor:
        employees_future = executor.submit(larkClient.list_employee, ehr_page_size)
        bitable_future = executor.submit(
            larkClient.list_bitable_employee,
            os.getenv("BITABLE_ID"),
            os.getenv("TABLE_ID"),
            bitable_page_size,
        )

        employees = employees_future.result()
        bitableEmployees = bitable_future.result()
    logger.info("Fetch finished in %.2fs", perf_counter() - fetch_start)

    logger.info(f"Found {len(employees)} employees from ehr")
    logger.info(f"Found {len(bitableEmployees)} bitable employees from bitable")

    diff_start = perf_counter()
    update_records = []
    for [employee_id, record_id] in bitableEmployees.items():
        if employee_id not in employees:
            update_records.append(record_id)
            
    if len(update_records) > 0:
        larkClient.update_employee_status(os.getenv("BITABLE_ID"), os.getenv("TABLE_ID"), update_records)

    newEmployees = [employee for employee in employees if employee not in bitableEmployees]

    logger.info("Diff finished in %.2fs, left employees: %d, new employees: %d", perf_counter() - diff_start, len(update_records), len(newEmployees))

    if not newEmployees:
        logger.info("No new employees to sync, total elapsed %.2fs", perf_counter() - start)
        return

    write_start = perf_counter()
    larkClient.batch_add_employees_to_bitable(os.getenv("BITABLE_ID"), os.getenv("TABLE_ID"), newEmployees)
    logger.info("Write finished in %.2fs", perf_counter() - write_start)
    logger.info("Total elapsed %.2fs", perf_counter() - start)

if __name__ == "__main__":
    main()
