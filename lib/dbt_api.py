import os
import time
import requests
import json
import socket

class Dbt:
    def __init__(self, DBT_CLOUD_ACCOUNT_ID='DBT_CLOUD_ACCOUNT_ID', DBT_CLOUD_API_KEY='DBT_CLOUD_API_KEY', DBT_CLOUD_HOST='DBT_CLOUD_HOST'):
        if DBT_CLOUD_ACCOUNT_ID not in os.environ:
            raise Exception("Missing {v} as environment variable".format(v=DBT_CLOUD_ACCOUNT_ID))
        if DBT_CLOUD_API_KEY not in os.environ:
            raise Exception("Missing {v} as environment variable".format(v=DBT_CLOUD_API_KEY))
        if DBT_CLOUD_HOST not in os.environ:
            host = 'cloud.upwork.getdbt.com'

        account_id = os.environ[DBT_CLOUD_ACCOUNT_ID]
        api_token = os.environ[DBT_CLOUD_API_KEY]

        self.url = 'https://cloud.getdbt.com/api/v2/accounts/{account}.'format(account=account_id)

        self.headers = {
          'Content-Type': 'application/json',
          'Authorization': 'Token {api_token}'
        }.format(api_token=api_token)

    def __api_call(self, url, payload=None):
        ''' Internal function to make curl request/post call '''
        try:
            if payload is None:
                r = requests.get(url, headers=self.headerss, verify=False)
            else:
                r = requests.post(url, json=payload, headers=self.headerss, verify=False)
        except Exception as e:
            raise e
        return json.loads(r.text)

    def run(self, job):
        ''' Run job in DBT by id / name '''
        if isinstance(job, int):
            self.__run_by_id(job)
        else:
            self.__run_by_name(job)

    def __run_by_id(self, job_id : int):
        ''' Run job in DBT '''
        url = "{url}/jobs/{id}/run/".format(
            url=self.url,
            id=job_id,
            )
        response = self.__api_call(url)

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

        return

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
        url = "{url}/runs/{id}/".format(
            url=self.url,
            id=run_id,
            )
        response = self.__api_call(url)
        #print(response)

        # 1: Queued
        # 2: Starting
        # 3: Running
        # 10: Success
        # 20: Error
        # 30: Cancelled
        return str(response['data']['status']) + '-' + str(response['data']['status_message'] or response['data']['status_humanized'])
