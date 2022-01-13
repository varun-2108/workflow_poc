from datetime import datetime
import time
import random
from airflow.sensors.python import PythonSensor

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from celery import app

from celery import Celery

app = Celery('airflow', broker='redis://localhost:6379/', backend='redis://localhost:6379/')
app.autodiscover_tasks(['core', 'dags'])


def _set_file_metadata():
    link = None
    result = app.send_task(
        "core1.tasks.set_file_metadata",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )
    return result.get()


def _convert_file():
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


def _file_logic_1():
    link = None
    result = app.send_task(
        "core1.tasks.file_logic_1",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return ["split_file_1", "split_file_2", "split_file_3"]


def _file_logic_2():
    link = None
    result = app.send_task(
        "core1.tasks.file_logic_2",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return ["split_file_1", "split_file_2", "split_file_3"]


def _split_file_1():
    link = None
    result = app.send_task(
        "core1.tasks.split_file_1",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return result.get()


def _split_file_2():
    link = None
    result = app.send_task(
        "core1.tasks.split_file_2",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return result.get()


def _split_file_3():
    link = None
    result = app.send_task(
        "core1.tasks.split_file_3",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return result.get()


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


def _pending_header():
    sample_value = random.randint(0, 2)
    time.sleep(5)
    print("_pending_header", sample_value)
    if sample_value == 2:
        return True
    return False


def _ml_lineitem_parse():
    link = None
    result = app.send_task(
        "core1.tasks.ml_lineitem_parse",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return result.get()


def _new_de_tool():
    sample_value = random.randint(0, 2)
    time.sleep(5)
    print("_new_de_tool", sample_value)
    if sample_value == 2:
        return True
    return False


def _validate():
    link = None
    result = app.send_task(
        "core1.tasks.validate",
        args=[],
        kwargs={"invoice_id": 1},
        link=link,
        queue="queue-1",
    )

    return result.get()


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


with DAG(
        'sample-dag-1',
        description='A simple tutorial DAG',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    set_file_metadata = PythonOperator(task_id="set_file_metadata", python_callable=_set_file_metadata)

    convert_file = BranchPythonOperator(
        task_id='convert_file',
        python_callable=_convert_file,
        do_xcom_push=False
    )

    file_logic_1 = BranchPythonOperator(task_id="file_logic_1", python_callable=_file_logic_1,
                                        do_xcom_push=False)

    file_logic_2 = BranchPythonOperator(task_id="file_logic_2", python_callable=_file_logic_2,
                                        do_xcom_push=False)

    split_file_1 = PythonOperator(task_id="split_file_1", python_callable=_split_file_1,
                                  trigger_rule='none_failed_or_skipped')

    split_file_2 = PythonOperator(task_id="split_file_2", python_callable=_split_file_2,
                                  trigger_rule='none_failed_or_skipped')

    split_file_3 = PythonOperator(task_id="split_file_3", python_callable=_split_file_3,
                                  trigger_rule='none_failed_or_skipped')

    merge = PythonOperator(task_id="merge", python_callable=_merge)

    pending_header = PythonSensor(task_id="pending_header", python_callable=_pending_header, poke_interval=1,
                                  mode='reschedule')

    ml_lineitem_parse = PythonOperator(task_id="ml_lineitem_parse", python_callable=_ml_lineitem_parse)

    validate = PythonOperator(task_id="validate", python_callable=_validate)

    new_de_tool = PythonSensor(task_id="new_de_tool", python_callable=_pending_header, poke_interval=1,
                               mode='reschedule')

    verified = PythonOperator(task_id="verified", python_callable=_verified)

    set_file_metadata >> convert_file
    convert_file >> [file_logic_1, file_logic_2]
    file_logic_1 >> [split_file_1, split_file_2, split_file_3]
    file_logic_2 >> [split_file_1, split_file_2, split_file_3]
    split_file_1 >> merge
    split_file_2 >> merge
    split_file_3 >> merge
    merge >> pending_header
    pending_header >> ml_lineitem_parse
    ml_lineitem_parse >> new_de_tool
    new_de_tool >> validate
    validate >> verified
