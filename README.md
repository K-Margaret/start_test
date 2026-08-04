# Production ETL & Data Ingestion Automation

## 📖 Overview
This repository contains a collection of Python-based ETL scripts developed and deployed in a production B2B e-commerce environment. The codebase handles the continuous, automated ingestion, validation, and synchronization of multi-source data (APIs, databases, and google sheets) to support various internal business operations.

Rather than a monolithic application, this is a pragmatic, modular collection of operational workflows designed for reliability, easy maintenance, and clear separation of concerns.

## ⚙️ Key Engineering Features
- **Robust Error Handling & Fail-safes**: Scripts include retry logic and exception handling to manage API rate limits, network timeouts, and unexpected data formats without breaking the daily pipeline.
- **Structured Logging**: Implemented comprehensive logging across workflows to ensure traceability, simplify debugging, and provide audit trails for data ingestion.
- **Separation of Concerns**: Core operational logic is isolated in `src/main/`, while shared helper functions (e.g., database connectors, formatting utilities) are centralized in `src/utils/` to prevent code duplication.
- **Secure Credential Management**: Sensitive configurations and API keys are externalized and managed securely, separate from the core logic.

## 📂 Project Structure
```text
├── creds/                 # Secure credential management and configuration
├── data/                  # Local data handling and temporary storage
├── src/
│   ├── main/              # Core operational scripts (e.g., automated pipelines, DB syncs)
│   └── utils/             # Shared helper functions, connectors, and data validation logic
└── README.md
