import json
import os
import pickle

import pymongo
import time
import shutil

import boto3
import csv

from threading import Thread
from credentials import credentials as mongo_credentials


def execute(job, boto_client):
    boto_client.invoke(
        # ClientContext='MyApp',
        FunctionName='QuantFinBT',
        InvocationType='Event',
        LogType='Tail',
        Payload=json.dumps(job).encode(),
    )


def get_boto_client(client_type='lambda'):
    with open('credentials.csv', 'r') as csvFile:
        reader = csv.reader(csvFile)
        lines = []
        for row in reader:
            lines.append(row)
        credentials = dict(zip(lines[0], lines[1]))

    boto_client = boto3.client(
        client_type,
        aws_access_key_id=credentials['Access key ID'],
        aws_secret_access_key=credentials['Secret access key'],
        region_name='eu-west-3'
    )
    return boto_client


def update_function():
    print('Creating archive...')
    shutil.copy(os.path.abspath(os.path.join(os.path.dirname(__file__), 'Strategy.py')),
                os.path.abspath(os.path.join(os.path.dirname(__file__), 'lambda_distrib')))
    shutil.copy(os.path.abspath(os.path.join(os.path.dirname(__file__), 'lambda_function.py')),
                os.path.abspath(os.path.join(os.path.dirname(__file__), 'lambda_distrib')))

    shutil.make_archive('lambda_distrib', 'zip', 'lambda_distrib/')

    with open('lambda_distrib.zip', 'rb') as f:
        zip_bytes = f.read()

    print('Pushing...')
    response = get_boto_client().update_function_code(
        FunctionName='QuantFinBT',
        ZipFile=zip_bytes,
        Publish=True,
    )


def execute_jobs(jobs, sim_id, max_concurrency=1000, job_timeout=30):
    print(f'Starting distributing for sim {sim_id}...')
    global_start_time = time.time()
    n_jobs = len(jobs)
    finished_jobs = []
    running_jobs = {}
    failed_jobs = []
    progress_timer = time.time()

    mongo_client = pymongo.MongoClient(
        host=mongo_credentials['mongo']['host'],
        port=mongo_credentials['mongo']['port'],
        username=mongo_credentials['mongo']['username'],
        password=mongo_credentials['mongo']['password'],
    )

    while len(finished_jobs) + len(failed_jobs) < n_jobs:

        # Sending jobs
        while len(running_jobs) < max_concurrency and len(jobs):
            job = jobs.pop(0)
            job['sim_id'] = sim_id
            running_jobs[job['id']] = {'job': job, 'start': time.time()}
            Thread(target=execute, args=(job, get_boto_client())).start()

        # Fetching results
        results = mongo_client.qfbt[sim_id].find({})
        for result in results:
            if result['id'] in running_jobs.keys():
                start_time = running_jobs[result['id']]['start']
                if time.time() - start_time > job_timeout:
                    # Fail
                    failed_jobs.append(result)
                    del running_jobs[result['id']]
                else:
                    # Success
                    finished_jobs.append(result)
                    del running_jobs[result['id']]

        if time.time() - progress_timer > 0.5:
            progress_timer = time.time()
            progress = round(len(finished_jobs * 100) / n_jobs)
            print(f'Running: {len(running_jobs)}\nFinished: {len(finished_jobs)}\nFailed: {len(failed_jobs)}\nProgress: {progress}%\n\n')
    progress = round(len(finished_jobs * 100) / n_jobs)
    print(f'Running: {len(running_jobs)}\nFinished: {len(finished_jobs)}\nFailed: {len(failed_jobs)}\nProgress: {progress}%\n\n')
    print(f'Finished in {round(time.time() - global_start_time)} seconds.')


if __name__ == '__main__':
    update_function()

    with open('jobs', 'r') as f:
        jobs = json.load(f)
    sim_id = str(int(time.time()))

    execute_jobs(jobs, sim_id)
