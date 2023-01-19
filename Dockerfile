# syntax=docker/dockerfile:1.4
FROM python:3.7-slim-buster as builder
RUN apt-get update && apt-get install -y \
        libpq-dev python3-dev libsystemd-dev build-essential pkg-config
COPY ./ ./
RUN python -m pip wheel -w wheelhouse .

FROM python:3.7-slim-buster as base

ENV PYTHONUNBUFFERED=1
ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV TINI_VERSION="v0.19.0"
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN apt-get update && apt-get -y install libpq5
COPY --from=builder wheelhouse wheelhouse
RUN python -m pip --no-cache-dir install csvbase --no-index -f wheelhouse

FROM base as migrations
COPY alembic.ini .
COPY migrations migrations
ENTRYPOINT ["/tini", "--"]
CMD ["alembic", "upgrade", "head"]

FROM base
COPY --from=migrations alembic.ini .
COPY --from=migrations migrations migrations
ENV FLASK_APP=csvbase.web:init_app()
ENV FLASK_DEBUG=0
EXPOSE 6001
ENTRYPOINT ["/tini", "--"]
CMD ["bash", "-c", "alembic upgrade head && gunicorn \"csvbase.web:init_app()\" -b \":6001\""]

