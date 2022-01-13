from celery import Celery
import time
import random

app = Celery('service_1', broker='redis://localhost:6379/', backend='redis://localhost:6379/')
app.autodiscover_tasks(['core1'])


@app.task(queue="queue-1")
def service_1_task_1(invoice_id):
    print("service_1_task_1", invoice_id)
    time.sleep(5)
    return 1


@app.task(queue="queue-1")
def service_1_task_2(invoice_id):
    print("service_1_task_2", invoice_id)
    time.sleep(5)
    return 2


@app.task(queue="queue-1")
def set_file_metadata(invoice_id):
    print("set_file_metadata", invoice_id)
    time.sleep(5)
    return 2


@app.task(queue="queue-1")
def convert_file(invoice_id):
    print("set_file_metadata", invoice_id)
    time.sleep(5)
    sample_int = random.randint(0, 1)
    if sample_int == 0:
        return "JPG"
    else:
        return "PNG"


@app.task(queue="queue-1")
def file_logic_1(invoice_id):
    print("file_logic_1", invoice_id)
    time.sleep(5)
    return 2


@app.task(queue="queue-1")
def file_logic_2(invoice_id):
    print("file_logic_2", invoice_id)
    time.sleep(5)
    return 2


@app.task(queue="queue-1")
def split_file_1(invoice_id):
    print("split_file_1", invoice_id)
    time.sleep(5)
    return 2


@app.task(queue="queue-1")
def split_file_2(invoice_id):
    print("split_file_2", invoice_id)
    time.sleep(5)
    return 2


@app.task(queue="queue-1")
def split_file_3(invoice_id):
    print("split_file_3", invoice_id)
    time.sleep(5)
    return 2


@app.task(queue="queue-1")
def merge(invoice_id):
    print("merge", invoice_id)
    time.sleep(5)
    return 2


@app.task(queue="queue-1")
def pending_header(invoice_id):
    print("pending_header", invoice_id)
    time.sleep(5)
    return 2


@app.task(queue="queue-1")
def ml_lineitem_parse(invoice_id):
    print("ml_lineitem_parse", invoice_id)
    time.sleep(5)
    return 2


@app.task(queue="queue-1")
def validate(invoice_id):
    print("validate", invoice_id)
    time.sleep(5)
    return 2


@app.task(queue="queue-1")
def new_de_tool(invoice_id):
    print("new_de_tool", invoice_id)
    time.sleep(5)
    return 2


@app.task(queue="queue-1")
def verified(invoice_id):
    print("verified", invoice_id)
    time.sleep(5)
    return 2
