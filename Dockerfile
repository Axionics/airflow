FROM apache/airflow:3.0.6

RUN echo 'remove cache 07/09/25'
ADD requirements.txt .

RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
