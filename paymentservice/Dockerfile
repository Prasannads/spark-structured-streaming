FROM openjdk:8
RUN apt-get update
RUN apt-get install -y netcat
RUN apt-get install -y jq
RUN apt-get install -y telnet
COPY target/scala-2.12/paymentservice.jar /usr/lib/paymentservice.jar
COPY start.sh /usr/bin/paymentservice-run.sh
CMD ["/usr/bin/paymentservice-run.sh"]
