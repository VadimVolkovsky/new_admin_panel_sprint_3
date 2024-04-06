FROM python:3.12

WORKDIR /opt/app

COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt --no-cache-dir

COPY . .

#CMD ["bash", "deploy.sh"]
CMD ["python", "data_loader.py"]