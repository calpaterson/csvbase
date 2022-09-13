FROM python:3.7-slim-buster as builder
RUN apt-get update
RUN apt-get install -y libpq-dev python3-dev libsystemd-dev build-essential pkg-config
COPY ./ ./
RUN python -m pip wheel -w wheelhouse .

FROM python:3.7-slim-buster
RUN apt-get update
RUN apt-get -y install libpq5
COPY --from=builder wheelhouse wheelhouse
RUN python -m pip --no-cache-dir install csvbase --no-index -f wheelhouse
ENV FLASK_APP=csvbase.web:init_app()
ENV FLASK_ENV=production
EXPOSE 6001
CMD ["gunicorn", "csvbase.web:init_app()", "-b", ":6001"]
