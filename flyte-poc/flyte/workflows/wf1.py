import random
import time
from flytekit.core.node_creation import create_node
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
def _split_file_1(flag) -> str:
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
def _split_file_2(flag) -> str:
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
def _split_file_3(flag) -> str:
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
def _merge():
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
def _pending_header():
    sample_value = random.randint(0, 2)
    time.sleep(5)
    print("_pending_header", sample_value)
    if sample_value == 2:
        return True
    return False


@task
def _ml_lineitem_parse() -> str:
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
def _new_de_tool() -> str:
    sample_value = random.randint(0, 2)
    time.sleep(5)
    print("_new_de_tool", sample_value)
    if sample_value == 2:
        return True
    return False


@task
def _validate(ip: str) -> bool:
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
def _verified():
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
    _split_file_2(flag=is_success)
    _split_file_3(flag=is_success)

    merge_node = create_node(_merge)
    header_node = create_node(_pending_header)
    ml_op = _ml_lineitem_parse()
    is_validated = _validate(ip=ml_op)

    # cond_op = conditional("validation_step").if_(is_validated.is_true()).then(_verified()).else_().then(
    #     _new_de_tool())

    de_op = _new_de_tool()
    _validate(ip=de_op)

    merge_node >> header_node
