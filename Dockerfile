FROM python:3.7.4

WORKDIR /app

ADD requirements.txt .

RUN pip install -r requirements.txt
ADD . .

ENTRYPOINT [ "python3", "server.py" ]
CMD [ "--help" ]
