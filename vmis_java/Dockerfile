FROM adoptopenjdk/openjdk14:jdk-14.0.2_12

WORKDIR /root

RUN apt-get update -qqy && apt-get install -qqy curl maven python


# Get Google cloud SDK
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/root/google-cloud-sdk/bin

ENV M2_HOME /usr/share/maven

COPY ./src src
COPY ./pom.xml .

COPY ./start.sh .
RUN chmod +x ./start.sh

RUN mvn clean package
RUN rm -fr .m2/ src/ pom.xml

# default max_map_count of 64k is not enough for JVM on 52GB machines
RUN echo "vm.max_map_count=262144" >> /etc/sysctl.conf

ENTRYPOINT ["./start.sh"]

