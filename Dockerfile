FROM ubuntu:20.04

WORKDIR /usr/src/app

RUN cat /etc/os-release

# Install the necessary dependencies
RUN apt update
RUN apt-get -y install python3-pip postgresql-client-12 libpq-dev curl zip
ENV PATH="${PATH}:/usr/lib/postgresql/12/bin"

# Copying the scripts to transform the dataset
COPY ./cdm_parser ./cdm_parser
# Copy additonal files - the vocabularies that will be used (can be downloaded
# from Athena) and the postgres files with the SQL statements to create the
# OMOP databse.
COPY requirements.txt init.sh vocabularies.zip postgresql.zip ./
# For the light version, remove the vocabularies.zip above and copy the following:
# COPY vocabularies-light.zip ./vocabularies.zip

# Install the requirements for the CDM parser
RUN pip3 install --no-cache-dir -r requirements.txt

CMD [ "./init.sh" ]
