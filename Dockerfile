# INSTALL PYTHON IMAGE
FROM apache/airflow:2.10.4


USER root 
# INSTALL TOOLS
RUN mkdir /opt/data

ADD ./requirements.txt /opt/data


WORKDIR /opt/data


# debug 

# INSTALL INSTANTCLIENT AND DEPENDENCIES

USER airflow 

RUN pip install -r requirements.txt


WORKDIR /

EXPOSE 5000
