FROM corpusops/python:3
USER root
WORKDIR /app
ADD apt.txt ./
RUN : \
    && apt-get update -yqq \
    && apt-get install -yqq python3-pip \
    && apt-get install -yqq $(grep -vE "^\s*#" apt.txt  | tr "\n" " ")
ADD install.sh \
    LICENSE \
    MANIFEST.in \
    CHANGES.md \
    README.md \
    requirements.txt \
    setup.py \
    test-requirements.txt \
    tests \
    tox.ini \
    ./
ADD terra_bonobo_nodes terra_bonobo_nodes/
ADD tests tests
RUN ./install.sh
