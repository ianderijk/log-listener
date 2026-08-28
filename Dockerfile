FROM python:3.12-slim

WORKDIR /log_listener

RUN pip install uv

COPY pyproject.toml

RUN uv add pilake --index-url 'http://192.168.0.29:8080/simple

COPY src/log_listener ./log_listener

EXPOSE 9020

CMD ["uv", "run", "-m", "listener"]
