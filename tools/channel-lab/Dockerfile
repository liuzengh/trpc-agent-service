FROM python:3.12-slim
WORKDIR /app
COPY app.py ./
COPY web/ ./web/
RUN useradd --uid 10001 --create-home lab && mkdir /data && chown lab:lab /data
USER lab
EXPOSE 8080
ENTRYPOINT ["python", "-u", "app.py", "--host", "0.0.0.0", "--database", "/data/channel-lab.sqlite"]
