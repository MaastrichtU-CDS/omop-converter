FROM ubuntu:20.04

WORKDIR /usr/src/app

RUN cat /etc/os-release

# Install the necessary dependencies
RUN apt update
RUN apt-get -y install python3-pip postgresql-client-12 libpq-dev curl zip
ENV PATH="${PATH}:/usr/lib/postgresql/12/bin"

COPY ./cdm_parser ./cdm_parser
COPY requirements.txt vocabulary.zip init.sh ./

# Install the requirements for the CDM parser
RUN pip3 install --no-cache-dir -r requirements.txt

CMD [ "./init.sh" ]
