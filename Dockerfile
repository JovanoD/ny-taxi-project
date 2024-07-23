FROM python:3.12.4

RUN pip install pandas==2.2.2 pyarrow==17.0.0 SQLAlchemy==2.0.31 psycopg==3.2.1 psycopg-binary==3.2.1 psycopg2-binary==2.9.9
RUN apt-get install wget

WORKDIR /app

COPY ingest_ny_taxi_data.py ingest_ny_taxi_data.py

ENTRYPOINT ["python", "ingest_ny_taxi_data.py"]