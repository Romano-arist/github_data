"""
This DAG uploads git data to S3.
NOT TESTED, MORE LIKE A TEMPLATE.
"""
import json
from datetime import datetime as dt, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from main import main, init_repositories_upload_by_stars
from db import get_s3_client # change for production


DAG_NAME = "github_data_to_s3"
S3_CONN_ID = "s3_minio"
REPOSITORIES_WITH_TOKENS: dict[str: [str]] = json.loads(Variable.get("github_tokens")) # Store as {repository_parquet_name: [tokens]}
SCHEDULE_INTERVAL = "15 * * * *"
DAG_TIMEOUT = timedelta(minutes=60)
WORKERS_COUNT = 10
REPOSITORIES_PER_FILE_THRESHOLD = 1000 # count of repositories per file

DAG_PARAMS = {
    "labels": {
        "env": "prod",
        "priority": "high", # for alerts needs
    }
}

def notifier_callback(**kwargs):
    print("sending alert")

args = {
    "owner": "owner",
    "description": "Copying git data to S3",
    "on_failure_callback": notifier_callback,
    "depends_on_past": False,
    "retries": 3, # depends on the number of tokens
    "start_date": dt(2025, 5, 20),
}


def get_s3_credentials() -> dict[str, str]:
    s3_conn = BaseHook.get_connection(S3_CONN_ID)
    s3_credentials = {
        "host": s3_conn.host,
        "port": str(s3_conn.port),
    }
    return s3_credentials

def submit_github_repositories_upload(**kwargs):
    """
    Upload repositories to track to S3.
    """
    s3_credentials = get_s3_credentials()
    s3_client = get_s3_client(
        host=s3_credentials["host"],
        port=int(s3_credentials["port"]),
    )
    init_repositories_upload_by_stars(
        github_api_tokens=tokens,
        snapshot_datetime=kwargs["execution_date"],
        s3_client=s3_client,
        start_ts=kwargs["execution_date"] - timedelta(days=90),
        stop_ts=kwargs["execution_date"],
        ts_delta=timedelta(days=3),
        stars_count=100,
        repositories_per_file_threshold=1000
    )

def submit_github_commits_upload(**kwargs):
    """
    Upload repositories commits to S3.
    """
    s3_credentials = get_s3_credentials()
    s3_client = get_s3_client(
        host=s3_credentials["host"],
        port=int(s3_credentials["port"]),
    )
    main(
        github_api_tokens=kwargs.get("tokens"),
        snapshot_datetime=kwargs["execution_date"] - timedelta(days=90), # keep track for the repositories updates for 90 days
        s3_client=s3_client,
        workers_count=WORKERS_COUNT
    )


def verify_data(**kwargs):
    """Check data after upload"""
    raise NotImplementedError("Method not implemented yet")


with DAG(
        DAG_NAME,
        params=DAG_PARAMS,
        schedule_interval=SCHEDULE_INTERVAL,
        dagrun_timeout=DAG_TIMEOUT,
        default_args=args,
        catchup=True,
        max_active_runs=1,
        max_active_tasks=4,
        doc_md=__doc__,
) as dag:
    start_operator = EmptyOperator(task_id="start", on_failure_callback=notifier_callback, )
    finish_operator = EmptyOperator(task_id="finish", on_failure_callback=notifier_callback, )

    for repo, tokens in REPOSITORIES_WITH_TOKENS.items():
        with TaskGroup(f"group_{repo}") as table_group:
            # upload_repositories not necessary if repositories are already uploaded
            upload_repositories = PythonOperator(task_id=f"uploading_repositories_{repo}",
                                                 python_callable=submit_github_repositories_upload,
                                                 on_failure_callback=notifier_callback,
                                                 op_kwargs={
                                                    "tokens": {token: repo for token in tokens},
                                                },
                                                 )
            upload_data_operator = PythonOperator(task_id=f"uploading_commits_{repo}",
                                                  python_callable=submit_github_commits_upload,
                                                  on_failure_callback=notifier_callback,
                                                  op_kwargs={
                                                    "tokens": {token: repo for token in tokens},
                                                },
                                                  )

            verify_data_operator = PythonOperator(
                task_id=f"verify_data_{repo}",
                python_callable=verify_data,
                op_kwargs={
                    "tokens": {token: repo for token in tokens},
                },
                on_failure_callback=notifier_callback,

            )
            join = EmptyOperator(task_id="join", on_failure_callback=notifier_callback, )

            [upload_repositories, upload_data_operator] >> verify_data_operator >> join

        start_operator >> table_group >> finish_operator
