from celery import Celery

app = Celery('service_2', broker='redis://localhost:6379/')
app.autodiscover_tasks(['core2'])


@app.task(queue="queue-2")
def service_2_task_1(invoice_id):
    print("service_2_task_1", invoice_id)
    return 1


@app.task(queue="queue-2")
def service_2_task_2(invoice_id):
    print("service_2_task_2", invoice_id)
    return 1
