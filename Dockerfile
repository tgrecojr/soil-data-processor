FROM python:3.11.6-slim-bookworm AS build-base
RUN python3 -m venv /opt/.venv
# ensure that virtualenv will be active
ENV PATH="/opt/.venv/bin:$PATH"

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get -y install --no-install-recommends libpq-dev build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies:
COPY requirements.txt .
RUN pip install -r requirements.txt

FROM gcr.io/distroless/python3-debian12 AS release
VOLUME ["/soildata"]
WORKDIR /code/
ENV PATH="/opt/.venv/bin:$PATH"
# Copy only virtualenv with all packages
COPY --from=build-base /opt/.venv /opt/.venv
# Run the application:
COPY fieldmappings.py fieldmappings.py
COPY processsoildata.py processsoildata.py
CMD ["python", "processsoildata.py"]
