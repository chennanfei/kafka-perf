FROM index.alauda.cn/library/ubuntu:14.04


RUN apt-get update && \
    apt-get install -y libpq-dev python-pip python-dev

RUN apt-get install -y python2.7-dev software-properties-common wget \
    && wget -qO - http://packages.confluent.io/deb/3.0/archive.key | sudo apt-key add - \
    && add-apt-repository "deb [arch=amd64] http://packages.confluent.io/deb/3.0 stable main" \
    && apt-get update \
    && apt-get install -y librdkafka-dev \
    && pip install confluent-kafka

ADD kafka_perf.py /kafka_perf.py

CMD ["python", "/kafka_perf.py"]