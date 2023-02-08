# Homework for week 2 tasks

### Question 1: January 2020 data

dataset has 447770 rows.

code:


```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)
    path = Path(path).as_posix()
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "green"
    year = 2020
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()

```

### Question 2: Scheduling with Cron

cron for the schedule is 0 5 1 * *

execution:

`prefect deployment build flows/02_gcp/etl_web_to_gcs.py:etl_web_to_gcs -n "task2" --cron "0 5 1 * * " -a`

`prefect agent start -q 'default'`

### Question 3: Loading data to BigQuery

After saving parquet to Bucket using `etl_web_to_gcs.py `


`prefect deployment build etl_gcs_to_bq_task_2_3.py:etl_parent_flow -n "task_3"`

`prefect deployment apply etl_parent_flow-deployment.yaml`

run agent to deploy:

`prefect agent start -q 'default'`

processed lines:
14,851,920


corresponding code

```
from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"{gcs_path}")

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example (very minimal)"""
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DF to BigQuery
    We are using pandas BigQuery functions"""
    
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomprefect.rides",
        project_id="sonorous-house-375411",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append",
    )

@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int, color: str):
    """main etl flow to load data into BigQuery"""

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    print(f"Length of df: {str(len(df))} rows.")


@flow()
def etl_parent_flow(
    months: list[int] = [2, 3], year: int = 2019, color: str = "yellow"
):
    for month in months:
        etl_gcs_to_bq(year, month, color)

if __name__ == '__main__':
    color = "yellow"
    months = [2, 3]
    year = 2021
    etl_parent_flow(months, year, color)


```

### Question 4: Github Storage Block

1. Pushed `github_deploy_week_2.py` to github
2. Created github block `gh-block`
run: 
`prefect deployment build github_deploy_week_2.py:etl_web_to_gcs --name task4 --apply -sb github/gh-block`
`prefect agent start -q 'default'`

88605 rows processed


### Question 5: Email or Slack notifications

I've used Prefect Cloud to setup up notification e-mail

514392 rows processed

Received the following email:

```
Flow run etl-web-to-gcs/rapid-bonobo entered state `Completed` at 2023-02-08T23:03:13.054290+00:00.
Flow ID: 5edb8a71-34e9-46a7-a505-540ecc84ac55
Flow run ID: fec81607-0188-40ab-a9ce-a8245a1f08a6
Flow run URL: https://app.prefect.cloud/account/cecdbbf0-5b0f-40a3-9b04-ab02393bf8b2/workspace/590c3c49-4407-4232-864e-e82faaa953b6/flow-runs/flow-run/2de069e1-71d5-4dcf-b037-4d3b0ee205c5
State message: All states completed.
```


### Question 6: Secrets

8 * were shown

```
from prefect.blocks.system import Secret

secret_block = Secret.load("secret-homework")

secret_block.get()

```
