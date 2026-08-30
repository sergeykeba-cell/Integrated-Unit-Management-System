FROM python:3.12-slim

# fonts-liberation is REQUIRED at the OS level (not pip) for pdf_generator.py —
# without it, PDF generation fails with:
# ValueError: Can't map determine family/bold/italic for timesukr
RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# server.py's default BIND_HOST is 127.0.0.1, which is NOT reachable from
# outside the container. We override it via env var at runtime — verify
# server.py actually reads BIND_HOST from the environment (or add support
# for it) before relying on this in production.
ENV BIND_HOST=0.0.0.0
ENV PORT=8080

EXPOSE 8080

CMD ["python", "server.py"]
