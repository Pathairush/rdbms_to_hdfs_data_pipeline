FROM puckel/docker-airflow:1.10.9
COPY requirements.txt requirements.txt
USER root
RUN pip install -r requirements.txt
RUN curl -sSL https://get.docker.com/ | sh