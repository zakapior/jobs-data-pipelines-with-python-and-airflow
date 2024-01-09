### Portfolio project
# Job application system with Python, data pipelines and Apache Airflow

This is a project for Turing College Data Engineering course Module 2 Sprint 3.

It's point is to gather data from several job boards, transform them and upload to the database using Apache Aiflow. Once they are in the database, I need to present some metrics about the data. As a goal I also need to fine a place to put a custom Hook, Operator and Sensor somewhere in the project.

## Requirements
In order to replicate the project, you would need to have an operating system with capabilies to use Docker with Docker Compose, PostgreSQL and optionally PgAdmin. I have installed the following versions:
- Ubuntu GNU/Linux 22
- Python 3.10.12 (consult the requirements.txt for list of packages are needed)
- PostgreSQL 14.9 (from the package manager)
- PgAdmin 7.7 (from the DockerHub repository)
- Docker 24.05

## Environment
1. You need to have an Airflow environment with additional `feedparser` package. I have installed mine with Docker with this guide: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html. You need to provide `feedparser` as an additional package to be installed for worker container. Alter the following variable and run `docker-compose up -d`:
`_PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- feedparser}`

2. You need to have Connections for Jobicy, Remotive, WeWorkRemotely as public HTTP connections as well as your PostgreSQL.

3. You need to put the `dags` folder contents in the `dags` folder of your Airflow environment.

## Usage
There are three job boards that I decided to use - Remotive, WeWorkRemotely and Jobicy.

I have created three `*_get_jobs` DAGs, that are able to fetch the job postings from either API or RSS feeds and put them into separate JSON files in `/tmp`. I have created custom Hooks and Operators to fetch and transform the data. The intention is to run the DAGs before backfilling to have postings to work with, and then once per day to get new jobs for next jobs.

Then I decided to create a custom sensor (only for Remotive) to detect, if there are any new jobs at Remotive from the JSON file.

Then I created `*_parse_and_upload` DAGs, that looks at the respective JSON file for new jobs using custom operator `Json2SqlOperator`. I use it to get jobs for the current data interval and put the details into the SQL file, ready to be uploaded to the database. Then I use `PythonOperator` with `PostgresHook` to get the file and upload to the database.

1. You need to run `*_get_jobs` to have the initial data.
2. You need to run `*_parse_and_upload` to have the data uploaded to the database. By default the backfilling will happen from 1st of January. There will be more than 700 jobs scheduled and your computer will probably sweat a little.
3. You need to make sure, that next runs for all DAGs are scheduled properly.

You can review the `data_engineer_jobs_daily` view to find how many job offerings per day is posted for Data Engineers.

## Discussion
1. All the boards I have used are posting remote-only jobs, so the metrics for remote jobs is not relevant. I have skipped it.
2. The salary is available only for several 36 postings out of 1696. It is not in any particular form, sometimes it is per hour, sometimes per year, sometimes per month. Sometimes it is in USD, sometimes in EUR and sometimes there is no currency provided. I honestly can't tell myself, what the job will be paid. There fore I have decided to skip the salary metrics, since that is not feasible. I have kept the column for future reference though.