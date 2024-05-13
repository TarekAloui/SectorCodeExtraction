FROM downloads.unstructured.io/unstructured-io/unstructured:latest

WORKDIR /app

COPY src /app/src

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip

RUN pip install -r requirements.txt

ENV PYTHONPATH=/app

# Add an entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Use the entrypoint script to handle arguments
ENTRYPOINT ["/app/entrypoint.sh"]

