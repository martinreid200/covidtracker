FROM python:3.8.5-slim-buster
WORKDIR /code
COPY requirements.txt /code/
RUN pip install -r requirements.txt
#ENV FLASK_APP ./app.py
#CMD flask run --host=0.0.0.0
CMD gunicorn --bind 0.0.0.0:5001 --access-logfile - app:server