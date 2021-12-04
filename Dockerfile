FROM python:3.7-slim-buster
COPY requirements.txt requirements.txt
# use the binary in docker to avoid building the universe
RUN sed -i 's/psycopg2/psycopg2-binary/' requirements.txt
RUN python -m pip install -r requirements.txt
COPY ./ ./
ENV FLASK_APP=csvbase.app
ENV FLASK_ENV=development
EXPOSE 6001
CMD ["flask", "run", "-p", "6001", "-h", "0.0.0.0"]
