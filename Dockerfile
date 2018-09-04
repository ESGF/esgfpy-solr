# Docker image containing Python package
# to interact with an ESGF Solr server

FROM python:3.6

# install ESGF harvesting software
COPY . /usr/local/esgfpy-solr
RUN cd /usr/local/esgfpy-solr && \
    pip install --no-cache-dir -r requirements.txt
    
ENV PYTHONPATH=/usr/local/esgfpy-solr

WORKDIR /usr/local/esgfpy-solr

