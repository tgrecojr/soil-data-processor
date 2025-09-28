# Soil Data Processor

A Python-based ETL pipeline for processing and storing soil temperature and environmental data into a PostgreSQL/TimescaleDB database.

## Architecture

This is a containerized Python application that:
- Processes fixed-width format soil data files
- Transforms temperature data from Celsius to Fahrenheit
- Cleanses data by removing invalid/missing values
- Stores processed data in a TimescaleDB hypertable for time-series analysis

## Project Structure

```
.
├── Dockerfile                    # Multi-stage Docker build
├── requirements.txt              # Python dependencies (includes test deps)
├── processsoildata.py           # Main ETL processing script
├── fieldmappings.py             # Data schema and column definitions
├── run_tests.py                 # Test runner script
├── pytest.ini                   # Test configuration
├── .coveragerc                  # Coverage reporting configuration
├── tests/                       # Test suite
│   ├── __init__.py
│   ├── conftest.py             # Shared test fixtures
│   ├── test_processsoildata.py # Unit tests for main processing
│   ├── test_integration.py     # End-to-end integration tests
│   ├── test_database.py        # Database operation tests
│   ├── test_fieldmappings.py   # Field mapping validation tests
│   └── fixtures/               # Test data files
│       ├── __init__.py
│       └── sample_data.txt     # Sample fixed-width soil data
├── .github/workflows/           # CI/CD workflows
│   └── docker-image.yml        # Multi-arch Docker builds
└── .python-version             # Python version specification
```

## Key Components

### processsoildata.py:136
Main entry point with ETL pipeline:
- **Environment Configuration**: Loads database credentials and file paths
- **Database Setup**: Creates TimescaleDB hypertable if not exists
- **Data Processing**: Batch processes files with pandas transformations
- **Data Loading**: Inserts processed records with conflict handling

### fieldmappings.py:3-121
Data schema definition:
- Fixed-width column specifications for parsing
- Field name mappings for soil temperature sensors (5cm, 10cm, 20cm, 50cm, 100cm)
- NumPy data type definitions for performance

### Dockerfile:5-24
Multi-stage containerized deployment:
- **Build Stage**: Creates Python virtual environment with compiled dependencies
- **Runtime Stage**: Uses Google's distroless image for security
- **Security**: Runs as non-root user with minimal attack surface

## Environment Variables

Required environment variables for operation:

```bash
SOIL_DATA_LOCATION      # Glob pattern for input data files
SOIL_DATABASE           # Database name
SOIL_DATABASE_USER      # Database username
SOIL_DATABASE_PASSWORD  # Database password
SOIL_DATABASE_HOST      # Database hostname
```

## Database Schema

TimescaleDB hypertable `soil_data`:
```sql
CREATE TABLE soil_data (
    time TIMESTAMPTZ NOT NULL,          -- Primary partitioning key
    SOIL_TEMP_5 INTEGER,                -- 5cm depth temperature (°F)
    SOIL_TEMP_10 float8,                -- 10cm depth temperature (°F)
    SOIL_TEMP_20 float8,                -- 20cm depth temperature (°F)
    SOIL_TEMP_50 float8,                -- 50cm depth temperature (°F)
    SOIL_TEMP_100 float8,               -- 100cm depth temperature (°F)
    T_CALC float8,                      -- Calculated air temperature (°F)
    T_HR_AVG DECIMAL,                   -- Hourly average temperature (°F)
    P_CALC float8,                      -- Calculated precipitation
    RH_HR_AVG DECIMAL,                  -- Hourly average relative humidity
    PRIMARY KEY(time)
);
```

## Development Commands

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run processing
python processsoildata.py
```

### Docker Operations
```bash
# Build image
docker build -t soil-data-processor .

# Run container with environment
docker run --env-file .env soil-data-processor

# Multi-arch build (as done in CI)
docker buildx build --platform linux/amd64,linux/arm64 -t soil-data-processor .
```

### Testing & Quality Assurance
```bash
# Install test dependencies
pip install -r requirements.txt

# Run all tests with coverage
python run_tests.py

# Run specific test categories
python run_tests.py --unit          # Unit tests only
python run_tests.py --integration   # Integration tests only  
python run_tests.py --database      # Database tests only

# Run tests without coverage
python run_tests.py --no-cov

# Run with pytest directly
pytest tests/ -v --cov=. --cov-report=html

# Code formatting (recommended)
black *.py

# Linting (recommended)
flake8 *.py

# Type checking (recommended)
mypy *.py
```

## CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/docker-image.yml`) with comprehensive quality gates:

**Test Job**:
- **Python setup**: Python 3.8 with pip caching
- **TimescaleDB service**: Containerized database for integration tests
- **Dependency installation**: All test and linting dependencies
- **Code quality**: Black formatting checks and Flake8 linting
- **Test execution**: Full test suite with coverage reporting
- **Coverage upload**: Codecov integration with failure prevention

**Build Job** (only runs if tests pass):
- **Multi-architecture builds**: linux/amd64, linux/arm64
- **Container registry**: GitHub Container Registry (ghcr.io)
- **Security features**: SBOM generation, provenance attestation
- **Caching**: GitHub Actions cache for faster builds
- **Triggers**: Push to main branch and pull requests

**Quality Gates**:
- All tests must pass (80% coverage minimum)
- Code must pass formatting checks
- Linting must pass with critical error detection
- Docker build only proceeds after successful testing

## Test Suite

### Test Categories

**Unit Tests** (`test_processsoildata.py`):
- Temperature conversion functions (Celsius to Fahrenheit)
- Invalid value removal (-99.000, -9999.0, -9999)  
- DateTime parsing and formatting
- Environment variable loading and validation

**Integration Tests** (`test_integration.py`):
- End-to-end data processing pipeline
- File parsing with realistic fixed-width data
- Complete ETL transformations
- Error handling for duplicate records and missing variables

**Database Tests** (`test_database.py`):
- Database connection and error handling
- Table creation and TimescaleDB hypertable setup
- Data insertion with proper type validation
- UniqueViolation and connection failure scenarios

**Field Mapping Tests** (`test_fieldmappings.py`):
- Schema validation and consistency checks
- Column specification format validation
- Required field presence verification
- Data type alignment testing

### Test Infrastructure

- **Fixtures**: Realistic sample data in `tests/fixtures/`
- **Configuration**: `pytest.ini` with coverage settings and test markers
- **Coverage**: HTML and terminal reports with 80% target
- **Mocking**: Database connections and file operations
- **CI Integration**: Ready for GitHub Actions integration

### Test Data

Sample fixed-width format data with:
- Valid soil temperature readings at multiple depths (5cm, 10cm, 20cm, 50cm, 100cm)
- Environmental measurements (air temperature, precipitation, humidity)
- Invalid values for testing data cleansing (-99.000, -9999.0)
- Realistic weather station data format

## Best Practices Implementation

### Security
- ✅ **Non-root container execution**
- ✅ **Distroless base image** (minimal attack surface)
- ✅ **No hardcoded secrets** (environment variables)
- ✅ **Multi-stage builds** (reduced image size)

### Performance
- ✅ **Pandas vectorized operations** for data transformations
- ✅ **Memory mapping** for large file processing
- ✅ **Batch database operations** with connection pooling
- ✅ **TimescaleDB hypertables** for time-series optimization

### Reliability
- ✅ **Duplicate handling** (UPSERT with UniqueViolation catch)
- ✅ **Error handling** for malformed data (skip_blank_lines, on_bad_lines='skip')
- ✅ **Connection management** with context managers
- ✅ **Data validation** (remove sentinel values like -99.000, -9999.0)

### Testing & Quality
- ✅ **Comprehensive test suite** (unit, integration, database tests)
- ✅ **Code coverage reporting** (80% target with HTML reports)
- ✅ **Test fixtures and mocking** for reliable, isolated tests
- ✅ **Automated test runner** with category-specific execution

### DevOps
- ✅ **Infrastructure as Code** (Dockerfile, GitHub Actions)
- ✅ **Multi-arch support** for diverse deployment environments  
- ✅ **Container scanning** with SBOM and provenance
- ✅ **Automated CI/CD** pipeline

## Deployment

The application is designed for containerized deployment:

1. **Local/Development**: Direct Python execution with local PostgreSQL
2. **Production**: Container orchestration (Kubernetes, Docker Compose)
3. **Cloud**: Compatible with managed container services (AWS ECS, GCP Cloud Run)

## Quality Assurance

### Test Execution
```bash
# Run full test suite with coverage
python run_tests.py

# Run specific test categories
python run_tests.py --unit          # Unit tests only
python run_tests.py --integration   # Integration tests only
python run_tests.py --database      # Database tests only

# View coverage report
open htmlcov/index.html             # After running tests
```

### Test Results
- **80% coverage target** with detailed HTML reports
- **Realistic test data** matching production format
- **Mocked dependencies** for reliable, isolated testing
- **Category-based execution** for focused testing

## Monitoring Recommendations

For production deployment, consider adding:
- Application metrics (processing time, record counts)
- Database connection monitoring
- Log aggregation and alerting
- Health check endpoints
- Resource utilization tracking
- Test automation in CI/CD pipeline

## Data Flow

```
Fixed-width Files → Pandas DataFrame → Data Cleansing → 
Temperature Conversion → TimescaleDB Hypertable
```

The pipeline processes soil monitoring data from weather stations, transforming raw measurements into a queryable time-series database optimized for analytical workloads.