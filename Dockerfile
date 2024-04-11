FROM python:3.12

WORKDIR /opt/app

COPY postgres_to_es/requirements.txt requirements.txt

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt --no-cache-dir

COPY postgres_to_es .

CMD ["python", "app.py"]