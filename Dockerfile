FROM python:3.9.6-slim-buster AS build-base
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

FROM python:3.9.6-slim-buster AS release
VOLUME ["/soildata"]
WORKDIR /code/
ENV PATH="/opt/.venv/bin:$PATH"
# Copy only virtualenv with all packages
COPY --from=build-base /opt/.venv /opt/.venv
# Run the application:
COPY fieldmappings.py fieldmappings.py
COPY processsoildata.py processsoildata.py
#CMD ["python", "processsoildata.py"]