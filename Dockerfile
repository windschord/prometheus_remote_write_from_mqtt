FROM python:3-slim

RUN apt update && \
    apt upgrade -y &&\
    apt install -y python3-snappy &&\
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/*

COPY src/ /app/
COPY requirements.txt /app/

WORKDIR /app
RUN pip install -r requirements.txt
CMD ["python", "write.py"]
