FROM python:3.7-slim-buster as builder
# COPY requirements.txt requirements.txt
# use the binary in docker to avoid building the universe
# RUN sed -i 's/psycopg2/psycopg2-binary/' requirements.txt
RUN apt-get update
RUN apt-get install -y libpq-dev python3-dev libsystemd-dev build-essential pkg-config
COPY ./ ./
RUN python -m pip wheel -w wheelhouse .

FROM python:3.7-slim-buster
RUN apt-get update
RUN apt-get -y install libpq5
COPY --from=builder wheelhouse wheelhouse
RUN python -m pip --no-cache-dir install csvbase -f wheelhouse
ENV FLASK_APP=csvbase.web:init_app()
ENV FLASK_ENV=development
EXPOSE 6001
CMD ["flask", "run", "-p", "6001", "-h", "0.0.0.0"]
