FROM python:3.8-slim

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . app
WORKDIR app

ENTRYPOINT ["python", "-m", "nhldata.app", "--start_date", "20200804", "--end_date", "20200805"]
