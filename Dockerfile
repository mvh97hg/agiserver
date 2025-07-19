FROM python:3.13-alpine

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir \
    -r requirements.txt \
    && chown 1000:1000 /app \
    && rm -rf /root/.cache

COPY main.py .

EXPOSE 4753
USER 1000
CMD ["python", "main.py"]