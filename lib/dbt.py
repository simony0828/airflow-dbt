from dbtc import dbtCloudClient

import os
import time

class Dbt:
    def __init__(self, DBT_CLOUD_ACCOUNT_ID='DBT_CLOUD_ACCOUNT_ID', DBT_CLOUD_API_KEY='DBT_CLOUD_API_KEY', DBT_CLOUD_HOST='DBT_CLOUD_HOST'):
        if DBT_CLOUD_ACCOUNT_ID not in os.environ:
            raise Exception("Missing {v} as environment variable".format(v=DBT_CLOUD_ACCOUNT_ID))
        if DBT_CLOUD_API_KEY not in os.environ:
            raise Exception("Missing {v} as environment variable".format(v=DBT_CLOUD_API_KEY))
        if DBT_CLOUD_HOST in os.environ:
            host = os.environ[DBT_CLOUD_HOST]
        else:
            host = 'cloud.upwork.getdbt.com' # default host

        self.account_id = os.environ[DBT_CLOUD_ACCOUNT_ID]
        api_token = os.environ[DBT_CLOUD_API_KEY]

        self.client = dbtCloudClient(
            api_key=api_token,
            host=host,
            )

    def list_jobs(self):
        ''' Fetch information about a specific run '''
        response = self.client.cloud.list_jobs(
            account_id=self.account_id,
            #state=1, # active jobs only
            )
        #print(response)
        list_of_jobs = []
        for resp in response['data']:
            # Only check for active jobs
            if resp['state'] == 1:
                list_of_jobs.append(str(resp['id']) + '-' + resp['name'])
        return list_of_jobs

    def run(self, job):
        ''' Run job in DBT by id / name '''
        if isinstance(job, int):
            self.__run_by_id(job)
        else:
            self.__run_by_name(job)

    def __run_by_id(self, job_id : int):
        ''' Run job in DBT by id '''
        payload = {'cause': 'Trigger job by Python dbtCloudClient'}
        response = self.client.cloud.trigger_job(
            self.account_id,
            job_id,
            payload,
            #should_poll=True,
            #poll_interval=10, # check every 10 secs
            )

        run_id = response['data']['id']
        status_code = None
        status_message = None

        # Loop to check if job is done or failed
        while(True):
          status = self.get_run_status(run_id)
          status_code = int(status.split('-')[0])
          status_message = status.split('-')[1]
          print("DBT: {m}".format(m=status_message))
          if status_code >= 1 and status_code <= 3:
              time.sleep(10)
              continue
          else:
              break

        if status_code > 10:
            raise Exception(str(status_code) + '-' + status_message)

#        if int(response['data']['status']) > 10:
#            raise Exception(str(response['data']['status']) + '-' + str(response['data']['status_message'] or '')

    def __run_by_name(self, job_name : str):
        ''' Run job in DBT by name '''
        jobs = self.list_jobs()
        job_id = None
        for j in jobs:
            id = j.split('-')[0]
            name = j.split('-')[1]
            if job_name == name:
                job_id = id
                break
        if job_id is not None:
            self.__run_by_id(job_id)
        else:
            raise Exception("Unable to find job name: {jn}".format(jn=job_name))      

    def get_run_status(self, run_id):
        ''' Fetch information about a specific run '''
        response = self.client.cloud.get_run(
            self.account_id,
            run_id,
            )
        #print(response)

        # 1: Queued
        # 2: Starting
        # 3: Running
        # 10: Success
        # 20: Error
        # 30: Cancelled
        return str(response['data']['status']) + '-' + str(response['data']['status_message'] or response['data']['status_humanized'])
