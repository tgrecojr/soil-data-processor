# 🌱 Soil Data Processor

[![Docker Image CI](https://github.com/tgrecojr/soil-data-processor/actions/workflows/docker-image.yml/badge.svg)](https://github.com/tgrecojr/soil-data-processor/actions/workflows/docker-image.yml)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A robust, containerized Python ETL pipeline for processing weather station soil temperature and environmental data into a TimescaleDB database for time-series analysis.

## 📖 Overview

This application processes fixed-width format soil monitoring data from weather stations, performing data transformations and storing the results in a PostgreSQL/TimescaleDB hypertable optimized for time-series queries. The system handles temperature unit conversions, data validation, and duplicate record management.

### Key Features

- 🚀 **Containerized deployment** with multi-stage Docker builds
- 📊 **TimescaleDB integration** for optimized time-series data storage  
- 🌡️ **Temperature conversion** from Celsius to Fahrenheit
- 🔄 **Data validation** and cleansing (removes invalid sensor readings)
- 🛡️ **Error handling** for duplicate records and connection failures
- 📈 **Multi-depth soil monitoring** (5cm, 10cm, 20cm, 50cm, 100cm)
- 🧪 **Comprehensive test suite** with 80%+ code coverage
- 🔒 **Security hardened** with non-root container execution

## 🏗️ Architecture

```
Fixed-width Files → Pandas DataFrame → Data Cleansing → 
Temperature Conversion → TimescaleDB Hypertable
```

The application follows a traditional ETL pattern:
- **Extract**: Parses fixed-width weather station data files
- **Transform**: Converts temperatures, removes invalid values, formats timestamps  
- **Load**: Inserts processed data into TimescaleDB with conflict resolution

## 📋 Prerequisites

- **Python 3.8+** (if running locally)
- **Docker** (recommended for deployment)
- **PostgreSQL 12+** with TimescaleDB extension
- **Environment variables** for database connectivity

## 🚀 Quick Start

### Option 1: Docker (Recommended)

```bash
# 1. Clone the repository
git clone https://github.com/tgrecojr/soil-data-processor.git
cd soil-data-processor

# 2. Build the Docker image
docker build -t soil-data-processor .

# 3. Run with environment variables
docker run --env-file .env soil-data-processor
```

### Option 2: Local Development

```bash
# 1. Clone and setup
git clone https://github.com/tgrecojr/soil-data-processor.git
cd soil-data-processor

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set environment variables
export SOIL_DATA_LOCATION="/path/to/data/*.txt"
export SOIL_DATABASE="your_database"
export SOIL_DATABASE_USER="your_user"  
export SOIL_DATABASE_PASSWORD="your_password"
export SOIL_DATABASE_HOST="localhost"

# 4. Run the processor
python processsoildata.py
```

## ⚙️ Configuration

### Required Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `SOIL_DATA_LOCATION` | Glob pattern for input data files | `/data/soil/*.txt` |
| `SOIL_DATABASE` | PostgreSQL database name | `soildb` |
| `SOIL_DATABASE_USER` | Database username | `soiluser` |
| `SOIL_DATABASE_PASSWORD` | Database password | `secure_password` |
| `SOIL_DATABASE_HOST` | Database hostname | `localhost` |

### Sample .env file

```bash
SOIL_DATA_LOCATION=/data/weather_station/*.txt
SOIL_DATABASE=soil_monitoring
SOIL_DATABASE_USER=soil_processor
SOIL_DATABASE_PASSWORD=your_secure_password
SOIL_DATABASE_HOST=timescaledb.example.com
```

## 📁 Data Format

The application expects fixed-width format files from weather stations with the following key fields:

```
- Station ID (WBANNO)
- UTC Date/Time and Local Date/Time
- Soil temperatures at 5 depths (5cm, 10cm, 20cm, 50cm, 100cm) 
- Air temperature and humidity measurements
- Precipitation data
- Solar radiation readings
```

Invalid sensor readings (-99.000, -9999.0, -9999) are automatically filtered out during processing.

## 🗄️ Database Schema

The processor creates a TimescaleDB hypertable with the following structure:

```sql
CREATE TABLE soil_data (
    time TIMESTAMPTZ NOT NULL,          -- Primary partitioning key
    SOIL_TEMP_5 INTEGER,                -- 5cm depth temperature (°F)
    SOIL_TEMP_10 float8,                -- 10cm depth temperature (°F)
    SOIL_TEMP_20 float8,                -- 20cm depth temperature (°F)
    SOIL_TEMP_50 float8,                -- 50cm depth temperature (°F)
    SOIL_TEMP_100 float8,               -- 100cm depth temperature (°F)
    T_CALC float8,                      -- Air temperature (°F)
    T_HR_AVG DECIMAL,                   -- Hourly average temperature (°F)
    P_CALC float8,                      -- Precipitation
    RH_HR_AVG DECIMAL,                  -- Hourly average relative humidity
    PRIMARY KEY(time)
);
```

## 🧪 Testing

The project includes a comprehensive test suite with unit, integration, and database tests.

```bash
# Install test dependencies
pip install -r requirements.txt

# Run all tests with coverage
python run_tests.py

# Run specific test categories
python run_tests.py --unit          # Unit tests only
python run_tests.py --integration   # Integration tests only  
python run_tests.py --database      # Database tests only

# View detailed coverage report
open htmlcov/index.html            # After running tests
```

### Test Coverage

- **Unit Tests**: Temperature conversion, data validation, datetime parsing
- **Integration Tests**: End-to-end ETL pipeline processing
- **Database Tests**: Connection handling, table creation, data insertion
- **Field Mapping Tests**: Schema validation and data type consistency

## 🚢 Deployment

### Production Docker Deployment

```bash
# Multi-architecture build
docker buildx build --platform linux/amd64,linux/arm64 -t soil-data-processor .

# Run with production settings
docker run -d \
  --name soil-processor \
  --restart unless-stopped \
  --env-file production.env \
  soil-data-processor
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: soil-data-processor
spec:
  replicas: 1
  selector:
    matchLabels:
      app: soil-data-processor
  template:
    metadata:
      labels:
        app: soil-data-processor
    spec:
      containers:
      - name: processor
        image: ghcr.io/tgrecojr/soil-data-processor:latest
        envFrom:
        - secretRef:
            name: soil-processor-secrets
```

### CI/CD Pipeline

The project includes GitHub Actions with a comprehensive quality gate:

**Test Job** (runs first):
- **Automated testing** with full test suite execution
- **Code quality checks** with Black formatting and Flake8 linting
- **Coverage reporting** with 80% minimum threshold
- **TimescaleDB integration testing** using containerized database
- **Failure prevention** - Docker build only proceeds if all tests pass

**Build Job** (runs after tests pass):
- **Multi-architecture Docker builds** (linux/amd64, linux/arm64)
- **Container security scanning** with SBOM generation
- **Container registry publishing** (GitHub Container Registry)
- **Automated tagging** with semantic versioning

## 📊 Monitoring

For production deployments, consider implementing:

- **Application metrics**: Processing time, record counts, error rates
- **Database monitoring**: Connection health, query performance
- **Resource monitoring**: CPU, memory, disk usage
- **Log aggregation**: Centralized logging with alerts
- **Health checks**: Container readiness and liveness probes

## 🔧 Development

### Project Structure

```
.
├── processsoildata.py           # Main ETL processing script
├── fieldmappings.py             # Data schema and column definitions  
├── Dockerfile                   # Multi-stage container build
├── requirements.txt             # Python dependencies
├── run_tests.py                 # Test runner with coverage
├── pytest.ini                   # Test configuration
├── .coveragerc                  # Coverage settings
├── tests/                       # Comprehensive test suite
│   ├── test_processsoildata.py # Unit tests
│   ├── test_integration.py     # Integration tests
│   ├── test_database.py        # Database tests
│   ├── test_fieldmappings.py   # Schema validation tests
│   └── fixtures/               # Test data
└── .github/workflows/          # CI/CD automation
```

### Code Quality

```bash
# Code formatting
black *.py

# Linting
flake8 *.py

# Type checking  
mypy *.py

# Security scanning
bandit -r .
```

## 🐛 Troubleshooting

### Common Issues

**Database Connection Errors**
```bash
# Check environment variables
echo $SOIL_DATABASE_HOST

# Test database connectivity
psql -h $SOIL_DATABASE_HOST -U $SOIL_DATABASE_USER -d $SOIL_DATABASE
```

**File Processing Issues** 
```bash
# Verify file permissions and location
ls -la $SOIL_DATA_LOCATION

# Check file format matches expected fixed-width structure
head -1 /path/to/your/data.txt
```

**TimescaleDB Extension**
```sql
-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Verify hypertable creation
SELECT * FROM timescaledb_information.hypertables;
```

### Performance Optimization

- **Batch Processing**: Process multiple files concurrently
- **Database Connection Pooling**: Use connection pools for high-throughput scenarios  
- **Memory Management**: Monitor pandas DataFrame memory usage for large files
- **Index Optimization**: Add indexes on frequently queried columns

## 📈 Sample Queries

Once data is loaded, you can perform time-series analysis:

```sql
-- Average soil temperature by depth over the last 30 days
SELECT 
    DATE_TRUNC('day', time) as day,
    AVG(SOIL_TEMP_5) as avg_temp_5cm,
    AVG(SOIL_TEMP_10) as avg_temp_10cm,
    AVG(SOIL_TEMP_20) as avg_temp_20cm
FROM soil_data 
WHERE time >= NOW() - INTERVAL '30 days'
GROUP BY day
ORDER BY day;

-- Temperature trends by depth
SELECT 
    DATE_TRUNC('hour', time) as hour,
    AVG(SOIL_TEMP_5) as temp_5cm,
    AVG(SOIL_TEMP_50) as temp_50cm,
    AVG(SOIL_TEMP_100) as temp_100cm
FROM soil_data
WHERE time >= NOW() - INTERVAL '7 days'
GROUP BY hour
ORDER BY hour;
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Run tests (`python run_tests.py`) 
4. Commit your changes (`git commit -m 'Add amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

### Development Workflow

- All code changes require tests
- Maintain 80%+ code coverage
- Follow PEP 8 style guidelines
- Update documentation for new features

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙋‍♂️ Support

For questions, issues, or contributions:

- **Issues**: [GitHub Issues](https://github.com/tgrecojr/soil-data-processor/issues)
- **Discussions**: [GitHub Discussions](https://github.com/tgrecojr/soil-data-processor/discussions)
- **Documentation**: See [CLAUDE.md](CLAUDE.md) for detailed technical documentation

---

Built with ❤️ for environmental monitoring and data analysis