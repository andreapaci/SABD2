FROM ubuntu
USER root
ARG DEBIAN_FRONTEND=noninteractive

# install dev tools
RUN apt-get update
RUN apt-get install -y curl tar sudo rsync net-tools wget
RUN apt install -y maven openjdk-8-jdk openjdk-8-jre

# # java
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64/jre
ENV PATH $PATH:$JAVA_HOME/bin


ADD ./Producer /usr/local/Producer

RUN cd /usr/local/Producer && mvn clean compile assembly:single
RUN cd /usr/local && mv Producer/target/kafkaclient-1-jar-with-dependencies.jar ./Producer/app.jar



CMD cd /usr/local/Producer && java -jar app.jar


