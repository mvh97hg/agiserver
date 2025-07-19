FROM python:3.13-alpine

WORKDIR /app
COPY requirements.txt .

RUN apk add --no-cache curl\
    && pip install --no-cache-dir \
    -r requirements.txt \
    && chown 1000:1000 /app \
    && rm -rf /root/.cache /var/cache/apk/*

COPY main.py .

EXPOSE 4753 8080
USER 1000
CMD ["python", "main.py"]