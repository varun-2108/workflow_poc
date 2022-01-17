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
    link = None
    result = app.send_task(
        "core1.tasks.set_file_metadata",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )
    return result.get()


@task
def _convert_file(ip: str) -> str:
    link = None
    result = app.send_task(
        "core1.tasks.convert_file",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    result = result.get()
    if result == "JPG":
        return "file_logic_1"
    else:
        return "file_logic_2"


@task
def _file_logic_1() -> str:
    link = None
    result = app.send_task(
        "core1.tasks.file_logic_1",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return True


@task
def _file_logic_2() -> str:
    link = None
    result = app.send_task(
        "core1.tasks.file_logic_2",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return True


@task
def _split_file_1(flag: str) -> str:
    link = None
    result = app.send_task(
        "core1.tasks.split_file_1",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return result.get()


@task
def _split_file_2(flag: str) -> str:
    link = None
    result = app.send_task(
        "core1.tasks.split_file_2",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return result.get()


@task
def _split_file_3(flag: str) -> str:
    link = None
    result = app.send_task(
        "core1.tasks.split_file_3",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return result.get()


@task
def _merge(files: List[str]) -> str:
    link = None
    result = app.send_task(
        "core1.tasks.merge",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return result.get()


@task
def _pending_header(ip: str) -> str:
    sample_value = random.randint(0, 2)
    time.sleep(5)
    print("_pending_header", sample_value)
    if sample_value == 2:
        return True
    return False


@task
def _ml_lineitem_parse(ip: str) -> str:
    link = None
    result = app.send_task(
        "core1.tasks.ml_lineitem_parse",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return result.get()


@task
def _new_de_tool(ip: str) -> str:
    sample_value = random.randint(0, 2)
    time.sleep(5)
    print("_new_de_tool", sample_value)
    if sample_value == 2:
        return True
    return False


@task
def _validate(ip: str) -> str:
    link = None
    result = app.send_task(
        "core1.tasks.validate",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return result.get()


@task
def _verified(ip: str):
    link = None
    result = app.send_task(
        "core1.tasks.verified",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return result.get()


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

