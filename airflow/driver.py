import airflow_client.client
import random
from airflow_client.client.api import config_api
from airflow_client.client.model.dag_run import DAGRun

configuration = airflow_client.client.Configuration(
    host="http://0.0.0.0:8080/api/v1",
    username='admin',
    password='c2Dcudnw7XYa79Rs'
)


# Enter a context with an instance of the API client
with airflow_client.client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = config_api.ConfigApi(api_client)
    dag_id = "sample-dag"
    dag_run_api_instance = api_instance.DAGRunApi(api_client)

    dag_run = DAGRun(
        dag_run_id=f"{dag_id}-{random.randint(1, 1000000000)}",
        dag_id=dag_id,
        external_trigger=True,
    )
    api_response = dag_run_api_instance.post_dag_run(dag_id, dag_run)
    print(api_response)