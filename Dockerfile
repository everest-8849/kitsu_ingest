FROM python:3.9-slim

# Create non-root user
RUN groupadd -r kitsu && useradd -r -g kitsu kitsu

# Install ffmpeg with minimal dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only requirements first to leverage Docker cache
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY kitsu_ingest/ /app/kitsu_ingest/
COPY pyproject.toml /app/
COPY .env /app/

# Install package
RUN pip install --no-cache-dir .

# Create processed directory with proper permissions
RUN mkdir -p /app/kitsu_ingest/processed && chmod 777 /app/kitsu_ingest/processed

# Make the Python module available as a command
RUN echo '#!/bin/bash\npython -m kitsu_ingest "$@"' > /usr/local/bin/kitsu-ingest && \
    chmod +x /usr/local/bin/kitsu-ingest

# Switch to non-root user
USER kitsu

# Define volume for processed data - use the correct path
VOLUME ["/app/kitsu_ingest/processed"]

# Use shell as entrypoint
CMD ["/bin/bash"]