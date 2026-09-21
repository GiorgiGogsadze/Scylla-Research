FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tablet_monitor.py .
COPY load_data.py .
COPY run_workload.py .
COPY capture_streaming_counters.py .

CMD ["python", "tablet_monitor.py"]