from typing import List
from celery import Celery
from flytekit import task, workflow, dynamic
import random

app = Celery('airflow', broker='redis://localhost:6379/', backend='redis://localhost:6379/')
app.autodiscover_tasks(['core', 'dags'])


@task
def _set_file_metadata(context: dict) -> dict:
    print("Inside _set_file_metadata")
    return {}


@task
def _convert_file(context: dict) -> str:
    print("Inside _set_file_metadata")
    return "PNG"


@task
def _split_file_1(context: dict) -> str:
    print("Inside _split_file_1")
    return "_split_file_1"


@task
def _split_file_2(context: dict) -> str:
    print("Inside _split_file_2")
    return "_split_file_2"


@task
def _split_file_3(context: dict) -> str:
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
def _new_de_tool(context: dict) -> str:
    print("Inside _new_de_tool")
    return "_new_de_tool"


def validation_loop(is_validated: bool) -> bool:
    while not is_validated:
        de_op = _new_de_tool(context={})
        is_validated = _validate(ip=de_op)

    return True


@task
def _validate(ip: str) -> bool:
    print("Inside _validate")
    sample = random.randint(0, 2)
    if sample == 1:
        return True
    return False


@task
def file_extension_logic(file_type: str) -> dict:
    def _file_logic_1() -> str:
        print("Inside _file_logic_1")
        return "_file_logic_1"

    def _file_logic_2() -> str:
        print("Inside _file_logic_2")
        return "_file_logic_2"

    if file_type == "PNG":
        _file_logic_1()
    else:
        _file_logic_2()

    return {}


@task
def _verified(flag: bool) -> str:
    print("Inside _verified")
    return "_verified"


@dynamic
def dynamic_process_invoice_wf() -> str:
    context = {}
    context = _set_file_metadata(context=context)
    file_extension = _convert_file(context=context)
    context = file_extension_logic(file_type=file_extension)

    op_1 = _split_file_1(context=context)
    op_2 = _split_file_2(context=context)
    op_3 = _split_file_3(context=context)

    merge_op = _merge(files=[op_1, op_2, op_3])
    header_op = _pending_header(ip=merge_op)
    ml_op = _ml_lineitem_parse(ip=header_op)
    is_validated = _validate(ip=ml_op)
    context = validation_loop(is_validated=is_validated)
    return _verified(flag=context)


@workflow
def process_invoice():
    print(f"Running my_wf() {dynamic_process_invoice_wf()}")


if __name__ == "__main__":
    print(f"Running my_wf() {process_invoice()}")
