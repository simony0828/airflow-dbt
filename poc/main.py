from airflow.models import Variable
from lib.airflowdag import AirflowDAG

file_path = Variable.get("dags_file_path")

d = AirflowDAG(
    dag_id='testme',
    yaml_file='{path}/poc/jobs/test.yaml'.format(path=file_path),
)

dag = d.run_dag()