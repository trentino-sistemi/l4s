FROM debian:buster

ENV DEBIAN_FRONTEND="noninteractive"
ENV PYTHONUNBUFFERED=1 PYTHONIOENCODING="UTF-8"
ENV APP_DIR="/srv/app"

WORKDIR ${APP_DIR}

RUN apt-get update \
    && apt-get -y install --no-install-recommends \
        build-essential git libblas3 libc++-7-dev postgresql-server-dev-all \
        python python-dev python-pip python-setuptools python-wheel \
        unixodbc-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN apt-get update \
    && apt-get -y install --no-install-recommends \
        openssh-client postgresql-client \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY requirements*.txt ${APP_DIR}/
RUN python -m pip install -r requirements.txt
RUN python -m pip install -r requirements-dev.txt

COPY manage.py ${APP_DIR}/
COPY conf ${APP_DIR}/conf
COPY l4s ${APP_DIR}/l4s
COPY web ${APP_DIR}/web

EXPOSE 8000
CMD ["./manage.py", "runserver", "0.0.0.0:8000"]
