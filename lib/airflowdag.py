import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.db import create_session
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import ExternalTaskSensor

import os
import yaml
from yamlinclude import YamlIncludeConstructor
from datetime import timedelta
from datetime import datetime
from jinja2 import Template

#from lib.watcher import Watcher
#from lib.detector import Detector
from lib.dbt import Dbt

class AirflowDAG():
    def __init__(self, dag_id, yaml_file):
        # Default setting
        self.default_email = 'simonyung@upwork.com'
        self.file_path = os.path.dirname(os.path.abspath(yaml_file))

        # YAML data
        self.config_data = {}

        # DAG setting
        self.dag_id = dag_id
        self.dag_description = None
        self.dag_owner = 'admin'
        self.airflow_data = {}
        self.schedule_interval = '@daily'
        self.start_date = days_ago(1)
        self.default_args = {
            'owner': self.dag_owner,
            'start_date': airflow.utils.dates.days_ago(1),
            'depends_on_past': False,
            'email': [self.default_email],
            'email_on_failure': True,
            'email_on_retry': True,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }

        # Watcher setting
        self.watcher_data = None
        self.watcher = None

        # DQ setting
        self.dq_data = None
        self.detector = None

        # DBT setting
        self.dbt = None

        # Steps setting
        self.steps = {}

        self.__setup_config(yaml_file)
        self.variables = self.__get_all_variables()
        self.__export_all_variables()

    def __read_config(self, config_file):
        ''' For reading and converting YAML file '''
        YamlIncludeConstructor.add_to_loader_class(loader_class=yaml.FullLoader,
            base_dir=self.file_path)
        with open(config_file) as f:
            return yaml.load(f, Loader=yaml.FullLoader)

    def __setup_config(self, config_file):
        ''' Parse the YAML file to global variables '''
        self.config_data = self.__read_config(config_file)
        #print(self.config_data)

        if 'config' not in self.config_data:
            raise Exception("Missing config")
        else:
            if 'owner' not in self.config_data['config']:
                raise Exception("Missing DAG owner")
            if 'description' not in self.config_data['config']:
                raise Exception("Missing DAG description")
            self.dag_owner = self.config_data['config']['owner']
            self.dag_description = self.config_data['config']['description']
            if 'schedule_interval' in self.config_data['config']:
                self.schedule_interval = self.config_data['config']['schedule_interval']
            for c in self.config_data['config']:
                if c in self.default_args:
                    self.default_args[c] = self.config_data['config'][c]

        sleep_time = 30 # DEFAULT: 30 mins
        max_retry = 3 # DEFAULT: num retries
        if 'watcher' in self.config_data:
            if 'sleep_time' in self.config_data['watcher']:
                sleep_time = self.config_data['watcher']['sleep_time']
            if 'max_retry' in self.config_data['watcher']:
                max_retry = self.config_data['watcher']['max_retry']

        if 'steps' not in self.config_data:
            raise Exception("Missing steps definition")
        self.steps = self.config_data['steps']
        i = 0
        for s in self.steps:
            self.steps[s]['seq'] = i
            i = i + 1
            if 'enabled' in self.steps[s]:
                # Remove step from dictionary if not enabled
                if self.steps[s]['enabled'] == False:
                    del self.steps[s]
                    i = i - 1

    def __get_all_variables(self):
        ''' To get all variables from Airflow server '''
        with create_session() as session:
            variables = {var.key: var.val for var in session.query(Variable)}
        return variables

    def __export_all_variables(self):
        ''' To export all Airflow variables to environment '''
        for v in self.variables:
            os.environ[v] = self.variables[v]

    def __replace_variables(self, str):
        ''' To replace any string with the variables list '''
        # Replace str for from Airflow variables list
        for v in self.variables:
            var_name = v
            var_value = self.variables[v]
            str = str.replace("{{ var.value.{vn} }}".format(vn=var_name), var_value).replace('{', '').replace('}', '')
        return str

    def __create_dag(self):
        ''' Create inital DAG to run in Airflow '''
        return DAG(
            self.dag_id,
            description=self.dag_description,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
            start_date=self.start_date,
        )

    def __gen_tasks(self, dag):
        ''' Generate tasks to run in Airflow '''
        list_of_tasks = []

        for s in self.steps:
            if self.steps[s]['operator'] == 'bash' and 'bash_file' in self.steps[s]:
                bash_file = self.__replace_variables(self.steps[s]['bash_file'])
                bash_commmand = open(bash_file, 'r').read()
                list_of_tasks.append(BashOperator(
                    task_id=s,
                    bash_command=bash_commmand,
                    dag=dag,
                ))

            elif self.steps[s]['operator'] == 'bash' and 'bash_script' in self.steps[s]:
                bash_commmand = self.steps[s]['bash_script']
                list_of_tasks.append(BashOperator(
                    task_id=s,
                    bash_command=bash_commmand,
                    dag=dag,
                ))

            elif self.steps[s]['operator'] == 'dbt' and 'dbt_job_id' in self.steps[s]:
                job_id = self.steps[s]['dbt_job_id']
                self.dbt = Dbt()
                list_of_tasks.append(PythonOperator(
                    task_id=s,
                    python_callable=self.__run_dbt,
                    op_kwargs={'job': job_id},
                    dag=dag,
                ))

            elif self.steps[s]['operator'] == 'dbt' and 'dbt_job_name' in self.steps[s]:
                job_name = self.steps[s]['dbt_job_name']
                self.dbt = Dbt()
                list_of_tasks.append(PythonOperator(
                    task_id=s,
                    python_callable=self.__run_dbt,
                    op_kwargs={'job': job_name},
                    dag=dag,
                ))

            elif self.steps[s]['operator'] == 'wait_for_dag' and 'dag_name' in self.steps[s]:
                dag_name = self.steps[s]['dag_name']
                list_of_tasks.append(ExternalTaskSensor(
                    task_id=s,
                    external_dag_id=dag_name,
                    external_task_id=None,  # wait for whole DAG to complete
                    check_existence=True,
                    timeout=120,
                ))

#            elif self.steps[s]['operator'] == 'watcher' and 'watcher_file' in self.steps[s]:
#                watcher_file = self.__replace_variables(self.steps[s]['watcher_file'])
#                self.watcher = Watcher(self.__read_config(watcher_file))
#                list_of_tasks.append(PythonOperator(
#                    task_id=s,
#                    python_callable=self.__run_watcher,
#                    op_kwargs={},
#                    dag=dag,
#                ))
#
#            elif self.steps[s]['operator'] == 'detector' and 'dq_file' in self.steps[s]:
#                dq_file = self.__replace_variables(self.steps[s]['dq_file'])
#                self.detector = Detector(self.__read_config(dq_file))
#                list_of_tasks.append(PythonOperator(
#                    task_id=s,
#                    python_callable=self.__run_detector,
#                    op_kwargs={},
#                    dag=dag,
#                ))

        return list_of_tasks

    def __run_watcher(self, **kwargs):
        ''' Run watcher '''
        self.watcher.run()

    def __run_detector(self, **kwargs):
        ''' Run detector '''
        self.detector.run()

    def __run_dbt(self, **kwargs):
        ''' Run dbt '''
        self.dbt.run(kwargs['job'])

    def run_dag(self):
        ''' Public method to be used to run the DAG '''
        dag = self.__create_dag()
        list_of_tasks = self.__gen_tasks(dag)
        # Set dependency (run in serial according to YAML configuration)
        #for i, t in enumerate(list_of_tasks):
        #    if i < len(list_of_tasks)-1:
        #        list_of_tasks[i] >> list_of_tasks[i+1]
        # Set dependency based on YAML definition
        for i, s in enumerate(self.steps):
            if 'dependency' in self.steps[s]:
                dep_list = self.steps[s]['dependency'].split(',')
                for d in dep_list:
                    j = self.steps[d]['seq']
                    list_of_tasks[j] >> list_of_tasks[i]

        return dag