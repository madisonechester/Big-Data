FROM python:3.12-slim

ADD MadisonChester_Assignment1.py .

RUN mkdir -p /home/app
WORKDIR /home/app
COPY . /home/app

RUN pip install requests pandas pymongo

CMD ["python", "./MadisonChester_Assignment1.py"]