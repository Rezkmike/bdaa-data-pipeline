
# BDAA Data Pipeline

This repository contains the **BDAA Data Pipeline**, a modular and scalable pipeline designed for ingesting, processing, and managing data streams effectively.

## Features

- **Data Ingestion**: Handles ingestion from various sources using Pub/Sub.
- **Data Processing**: Supports transformation and processing of raw data.
- **Modularity**: Organized into versions (v1, v2) for iterative improvements and backward compatibility.
- **Scalable Infrastructure**: Configurations for integration with Pub/Sub and Dataflow.

---

## Repository Structure

```
bdaa-data-pipeline/
├── README.md                # Project documentation
├── code/                    # Contains the pipeline code
│   ├── v1/                  # First version of the pipeline
│   │   ├── step-1-data-ingestion.py   # Ingest data
│   │   ├── step-2-data-from-pubsub.py # Read from Pub/Sub
│   │   ├── step-3-process-data.py     # Process and transform data
│   │   └── requirements.txt           # Dependencies for v1
│   ├── v2/                  # Second version of the pipeline
│       ├── step-1-data-ingestion.py   # Updated ingestion logic
│       ├── step-2-dataflow-sink.yaml  # Dataflow sink configurations
│       └── requirements.txt           # Dependencies for v2
```

---

## Prerequisites

- Python 3.8 or higher
- Pub/Sub configured in your Google Cloud Platform (GCP) project
- [Apache Beam SDK](https://beam.apache.org/) for Dataflow integration (if using v2)

---

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/bdaa-data-pipeline.git
   cd bdaa-data-pipeline
   ```

2. Navigate to the desired pipeline version:
   ```bash
   cd code/v1  # or code/v2
   ```

3. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

## Usage

### Running the Pipeline

#### Version 1
```bash
python step-1-data-ingestion.py
python step-2-data-from-pubsub.py
python step-3-process-data.py
```

#### Version 2
```bash
python step-1-data-ingestion.py
# For Dataflow:
gcloud dataflow jobs run --config-file=step-2-dataflow-sink.yaml
```

### Configurations
Modify the configurations in each script or YAML file to align with your GCP project and dataset requirements.

---

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a feature branch: `git checkout -b feature-name`.
3. Commit changes: `git commit -m "Add feature-name"`.
4. Push to the branch: `git push origin feature-name`.
5. Open a pull request.

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

---
