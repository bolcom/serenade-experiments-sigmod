FROM rust:1.51 AS builder

WORKDIR /usr/src/serenade

RUN apt-get update
RUN apt-get install -y lsb-release wget software-properties-common

RUN wget https://apt.llvm.org/llvm.sh
RUN chmod +x llvm.sh
RUN ./llvm.sh 12

COPY ./src src
COPY ./benches benches
COPY ./Cargo.* ./

RUN cargo build --release

FROM rust:1.51 as releaser

COPY --from=redboxoss/scuttle:latest /scuttle /bin/scuttle

WORKDIR /app
RUN groupadd -r app && useradd -r -g app -u 1002 app --home /app

# Get Google cloud SDK
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz
# Installing the package
RUN tar -C . -xvf /tmp/google-cloud-sdk.tar.gz \
  && google-cloud-sdk/install.sh

# Adding the package path to local
ENV PATH $PATH:/app/google-cloud-sdk/bin
RUN mkdir /.config && chown -R app:app /.config

COPY --chown=app:app --from=builder /usr/src/serenade/target/release/serving /app

COPY ./start_webservice.sh .

RUN chown -R app:app /app
USER app

ENTRYPOINT ["/bin/scuttle", "./start_webservice.sh"]
EXPOSE 8080/tcp
