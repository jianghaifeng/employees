import lark_oapi as lark
from lark_oapi.api.ehr.v1 import *
from lark_oapi.api.bitable.v1 import *
from dotenv import load_dotenv
from typing import Set, Dict, List
import json
import time

class Lark:
    def __init__(self, APP_ID: str, APP_SECRET: str):
        load_dotenv()
        self.client = lark.Client.builder() \
            .app_id(APP_ID) \
            .app_secret(APP_SECRET) \
            .log_level(lark.LogLevel.ERROR) \
            .build()
    
    def list_employee(self, page_size: int = 100) -> Set[str]:
        pageToken, pageSize = '0', page_size
        employees: Set[str] = set() 
        while True:
            request: ListEmployeeRequest = ListEmployeeRequest.builder() \
                .status(2) \
                .user_id_type("open_id") \
                .page_token(pageToken) \
                .page_size(pageSize) \
                .build()
            response: ListEmployeeResponse = self.client.ehr.v1.employee.list(request)
            if not response.success():
                lark.logger.error(
                    f"client.ehr.v1.employee.list failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
                return
            for employee in response.data.items:
                employees.add(employee.user_id)
            if response.data.has_more:
                pageToken = response.data.page_token
            else:
                break
        return employees

    def update_employee_status(self, BITABLE_ID: str, TABLE_ID: str, to_update: List[str]):
        left_employees = []
        timestamp = int(time.time() * 1000)
        for record_id in to_update:
            left_employees.append(AppTableRecord.builder()
                .fields({"离职": timestamp})
                .record_id(record_id)
                .build())
        request: BatchUpdateAppTableRecordRequest = BatchUpdateAppTableRecordRequest.builder() \
            .app_token(BITABLE_ID) \
            .table_id(TABLE_ID) \
            .request_body(BatchUpdateAppTableRecordRequestBody.builder()
                .records(left_employees)
                .build()) \
            .build()
        response: BatchUpdateAppTableRecordResponse = self.client.bitable.v1.app_table_record.batch_update(request)

        if not response.success():
            lark.logger.error(
                f"client.bitable.v1.app_table_record.batch_update failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
            return

    def list_bitable_employee(self, BITABLE_ID: str, TABLE_ID: str, page_size: int = 100) -> Dict[str, str]:
        employees: Dict[str, str] = {}
        pageToken, pageSize = '', page_size
        while True:
            request: SearchAppTableRecordRequest = SearchAppTableRecordRequest.builder() \
            .app_token(BITABLE_ID) \
            .table_id(TABLE_ID) \
            .user_id_type("open_id") \
            .page_token(pageToken) \
            .page_size(pageSize) \
            .request_body(SearchAppTableRecordRequestBody.builder()
                .field_names(["姓名"])
                .filter(FilterInfo.builder()
                    .conjunction("and")
                    .conditions([Condition.builder()
                        .field_name("离职")
                        .operator("isEmpty")
                        .value([])
                        .build()]) \
                    .build()) \
                .build()) \
            .build()

            response: SearchAppTableRecordResponse = self.client.bitable.v1.app_table_record.search(request)
            if not response.success():
                lark.logger.error(
                    f"client.bitable.v1.app_table_record.search failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
                return
            for record in response.data.items:
                employees[record.fields["姓名"][0]["id"]] = record.record_id
            if response.data.has_more:
                pageToken = response.data.page_token
            else:
                break
        return employees

    def batch_add_employees_to_bitable(self, BITABLE_ID: str, TABLE_ID: str, employees: List[str]):
        batchSize = 500
        for i in range(0, len(employees), batchSize):
            records: List[AppTableRecord] = []
            batchEmployees = employees[i:i+batchSize]
            for employee in batchEmployees:
                records.append(AppTableRecord.builder()
                    .fields({"姓名":[{"id":employee}]})
                    .build())
            request: BatchCreateAppTableRecordRequest = BatchCreateAppTableRecordRequest.builder() \
            .app_token(BITABLE_ID) \
            .table_id(TABLE_ID) \
            .user_id_type("open_id") \
            .request_body(BatchCreateAppTableRecordRequestBody.builder()
                .records(records)
                .build()) \
            .build()

            response: BatchCreateAppTableRecordResponse = self.client.bitable.v1.app_table_record.batch_create(request)
            if not response.success():
                lark.logger.error(
                    f"client.bitable.v1.app_table_record.batch_create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
                return False

        return True

    def list_left_employees_from_bitable(self, BITABLE_ID: str, TABLE_ID: str, page_size: int = 100) -> Dict[str, str]:
        employees: Dict[str, str] = {}
        pageToken, pageSize = '', page_size
        while True:
            request: SearchAppTableRecordRequest = SearchAppTableRecordRequest.builder() \
            .app_token(BITABLE_ID) \
            .table_id(TABLE_ID) \
            .user_id_type("open_id") \
            .page_token(pageToken) \
            .page_size(pageSize) \
            .request_body(SearchAppTableRecordRequestBody.builder()
                .filter(FilterInfo.builder()
                    .conjunction("and")
                    .conditions([Condition.builder()
                        .field_name("离职")
                        .operator("isNotEmpty")
                        .value([])
                        .build()]) \
                    .build()) \
                .sort([Sort.builder()
                    .field_name("离职")
                    .desc(True)
                    .build()
                    ]) \
                .build()) \
            .build()

            response: SearchAppTableRecordResponse = self.client.bitable.v1.app_table_record.search(request)
            if not response.success():
                lark.logger.error(
                    f"client.bitable.v1.app_table_record.search failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
                return
            for record in response.data.items:
                print("姓名：", record.fields["姓名"][0]["name"], "工号：", record.fields["姓名.工号"][0]["text"], "部门：", record.fields["姓名.部门"][0])

            if response.data.has_more:
                pageToken = response.data.page_token
            else:
                break