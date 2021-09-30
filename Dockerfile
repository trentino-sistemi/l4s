FROM debian:buster

ENV DEBIAN_FRONTEND="noninteractive"
ENV PYTHONUNBUFFERED=1 PYTHONIOENCODING="UTF-8"
ENV APP_DIR="/srv/app"

WORKDIR ${APP_DIR}

RUN apt-get update \
    && apt-get -y install --no-install-recommends \
        build-essential curl gettext git gnupg libblas3 libc++-7-dev openssl \
        postgresql-client postgresql-server-dev-all python3-dev python3-pip \
        python3-setuptools python3-wheel unixodbc-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN apt-get update \
    && apt-get -y install --no-install-recommends \
        openssh-client \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# install microsoft odbc driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get -y install --no-install-recommends \
        msodbcsql17 \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN python3 -m pip install -U pip

COPY requirements*.txt ${APP_DIR}/
RUN python3 -m pip install -r requirements.txt
RUN python3 -m pip install -r requirements-dev.txt

# enable TLSv1.0 to support sql server 2008 on windows server 2008
RUN sed -i 's/^\(MinProtocol = \).*/\1TLSv1.0/' /etc/ssl/openssl.cnf

COPY manage.py ${APP_DIR}/
COPY conf ${APP_DIR}/conf
COPY l4s ${APP_DIR}/l4s
COPY web ${APP_DIR}/web

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 8000
CMD ["/usr/local/bin/entrypoint.sh"]
