FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv/ bin/


WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./

RUN uv sync --locked


COPY ingest_data.py .


ENTRYPOINT ["python", "ingest_data.py"]


# docker run -it --rm \
#     -e POSTGRES_USER="root" \
#     -e POSTGRES_PASSWORD="root" \
#     -e POSTGRES_DB="ny_taxi" \
#     -v ny_taxi_postgres_data:/var/lib/postgresql \
#     -p 5432:5432 \
#     --network=pg-network \
#     --name pgdatabase \
#     postgres:18
#   
# 
# 
# uv run python ingest_data.py \
# 
#   --pg-user=root \
#   --pg-pass=root \
#   --pg-host=pgdatabase \
#   --pg-port=5432 \
#   --pg-db=ny_taxi \
#   --target-table=yellow_taxi_trips_2021_1 \
#   --year=2021 \
#   --month=1 \
#   --chunksize=100000

# docker run -it --rm \
#   --network=pipeline_default \
#   taxi_ingest:v001 \
#     --pg-user=root \
#     --pg-pass=root \
#     --pg-host=pgdatabase \
#     --pg-port=5432 \
#     --pg-db=ny_taxi \
#     --target-table=yellow_taxi_trips_2021_1 \
#     --chunk-size=100000
