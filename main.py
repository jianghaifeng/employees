import os
from dotenv import load_dotenv
from lark import Lark

load_dotenv()

def main():
    larkClient = Lark(os.getenv("APP_ID"), os.getenv("APP_SECRET"))
    employees = larkClient.list_employee()
    bitableEmployees = larkClient.list_bitable_employee(os.getenv("BITABLE_ID"), os.getenv("TABLE_ID"))
    newEmployees = []
    for employee in employees:
        if employee not in bitableEmployees.keys():
            newEmployees.append(employee)
    larkClient.batch_add_employees_to_bitable(os.getenv("BITABLE_ID"), os.getenv("TABLE_ID"), newEmployees)

if __name__ == "__main__":
    main()
