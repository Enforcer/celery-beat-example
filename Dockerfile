FROM python:3.6-alpine
ADD . /code
WORKDIR /code
RUN pip install -r requirements.txt
CMD ["celery", "-A", "tasks", "worker", "-B"]
