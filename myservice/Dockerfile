FROM openjdk:8
RUN apt-get update
RUN apt-get install -y netcat
RUN apt-get install -y jq
RUN apt-get install -y telnet
COPY target/scala-2.12/myservice.jar /usr/lib/myservice.jar
COPY start.sh /usr/bin/myservice-run.sh
CMD ["/usr/bin/myservice-run.sh"]
