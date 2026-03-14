import os
from dotenv import load_dotenv
from lark import Lark
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

def main():
    larkClient = Lark(os.getenv("APP_ID"), os.getenv("APP_SECRET"))
    employees = larkClient.list_employee()
    logger.info(f"Found {len(employees)} employees from ehr")
    bitableEmployees = larkClient.list_bitable_employee(os.getenv("BITABLE_ID"), os.getenv("TABLE_ID"))
    logger.info(f"Found {len(bitableEmployees)} bitable employees from bitable")
    newEmployees = []
    for employee in employees:
        if employee not in bitableEmployees.keys():
            newEmployees.append(employee)
    larkClient.batch_add_employees_to_bitable(os.getenv("BITABLE_ID"), os.getenv("TABLE_ID"), newEmployees)

if __name__ == "__main__":
    main()
