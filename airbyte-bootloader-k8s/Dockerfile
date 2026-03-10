FROM python:3.10-alpine
ENV PYTHONUNBUFFERED=1
RUN apk add --no-cache su-exec && pip install --no-cache-dir pg8000
WORKDIR /app
COPY src/ src/
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
EXPOSE 8080
ENTRYPOINT ["/app/entrypoint.sh"]
