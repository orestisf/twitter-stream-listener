FROM python:3
COPY requirements.txt stream_listener.py /app/
WORKDIR /app
RUN pip install -r requirements.txt
CMD [ "python", "stream_listener.py" ]
