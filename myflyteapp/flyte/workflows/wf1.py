import random
import time
from typing import List

from celery import Celery
from celery import app
from flytekit import task, workflow, conditional

app = Celery('airflow', broker='redis://localhost:6379/', backend='redis://localhost:6379/')
app.autodiscover_tasks(['core', 'dags'])


@task
def _set_file_metadata() -> str:
    print("Inside _set_file_metadata")
    return "_set_file_metadata"


@task
def _convert_file(ip: str) -> str:
    print("Inside _set_file_metadata")
    return "PNG"


@task
def _file_logic_1() -> str:
    print("Inside _file_logic_1")
    return "_file_logic_1"


@task
def _file_logic_2() -> str:
    print("Inside _file_logic_2")
    return "_file_logic_2"


@task
def _split_file_1(flag: str) -> str:
    print("Inside _split_file_1")
    return "_split_file_1"


@task
def _split_file_2(flag: str) -> str:
    print("Inside _split_file_2")
    return "_split_file_2"


@task
def _split_file_3(flag: str) -> str:
    print("Inside _split_file_3")
    return "_split_file_3"


@task
def _merge(files: List[str]) -> str:
    print("Inside _merge")
    return "_merge"


@task
def _pending_header(ip: str) -> str:
    print("Inside _pending_header")
    return "_pending_header"


@task
def _ml_lineitem_parse(ip: str) -> str:
    print("Inside _ml_lineitem_parse")
    return "_ml_lineitem_parse"


@task
def _new_de_tool(ip: str) -> str:
    print("Inside _new_de_tool")
    return "_new_de_tool"


@task
def _validate(ip: str) -> str:
    print("Inside _validate")
    return "_validate"


@task
def _verified(ip: str):
    print("Inside _verified")
    return "_verified"


@workflow
def process_invoice():
    file_metadata = _set_file_metadata()
    file_extension = _convert_file(ip=file_metadata)

    is_success = conditional("file_extension_logic").if_(file_extension == "PNG").then(_file_logic_1()).else_().then(
        _file_logic_2())

    op_1 = _split_file_1(flag=is_success)
    op_2 = _split_file_2(flag=is_success)
    op_3 = _split_file_3(flag=is_success)

    merge_op = _merge(files=[op_1, op_2, op_3])
    header_op = _pending_header(ip=merge_op)
    ml_op = _ml_lineitem_parse(ip=header_op)

    is_validated = _validate(ip=ml_op)

    de_op = _new_de_tool(ip=is_validated)

    is_validated = _validate(ip=de_op)

    _verified(ip=is_validated)


if __name__ == "__main__":
    print(f"Running my_wf() { process_invoice() }")