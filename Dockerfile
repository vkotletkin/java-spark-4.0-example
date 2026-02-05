FROM postgis/postgis:17-3.5

RUN apt-get update && \
    apt-get install -y postgis && \
    rm -rf /var/lib/apt/lists/*