FROM jfloff/alpine-python:2.7-slim

RUN apk update && \
    apk upgrade && \
    apk add --no-cache gcc musl-dev python2-dev curl python-dev && \
    apk add krb5-libs && \
    apk add krb5-dev && \
    apk add krb5

RUN apk update && \
    apk upgrade && \
    pip install update && \
    pip install boto3 && \
    pip install Autologging && \
    pip install requests && \
    pip install kafka-python && \
    pip install redis && \
    pip install gssapi


RUN mkdir -p /etc/security/certs/
EXPOSE 8080
WORKDIR /app/

COPY simple-kafka-monitor/ /etc/simple-kafka-monitor
COPY kafka-backpressure-monitor/ /etc/kafka-backpressure-monitor

#ARG env
#RUN echo krb5_$env.conf
#COPY krb5_$env.conf /etc/krb5.conf
#COPY krb5.conf /etc/krb5.conf
COPY krb5_sandbox.conf /etc/
COPY krb5_latest.conf /etc/
COPY krb5_stage.conf /etc/
COPY krb5_prod.conf /etc/

COPY kafka.headless.keytab /etc/security/keytabs/

COPY docker-config/entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# Below are added for testing purpose
#COPY security/ /etc/security
#COPY start_security_setup.sh /etc/
#COPY start_kafka_monitor.sh /etc/
#COPY start_bpmon.sh /etc/

#RUN chmod 755 /etc/start_security_setup.sh
#RUN chmod 755 /etc/start_kafka_monitor.sh
#RUN chmod 755 /etc/start_bpmon.sh

#CMD /etc/start_security_setup.sh && \
#/etc/start_kafka_monitor.sh && \
#/etc/start_bpmon.sh