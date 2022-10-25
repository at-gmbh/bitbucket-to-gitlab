FROM python:3.8

# ENV foo /Bar  ===  foo=/Bar

WORKDIR /code
COPY requirements.txt .
COPY main.py .

RUN pip install -r requirements.txt

CMD [ "python", "./main.py" ] 
