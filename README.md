# Production ETL & Data Ingestion Automation

## 📖 Overview
I designed and built this collection of Python-based ETL scripts during my role as a Data Engineer at a B2B e-commerce company developing its own warehouse management and logistics software. 

The codebase handles the continuous, automated ingestion, validation, and synchronization of multi-source data (APIs, databases, and Google Sheets) to keep internal business operations running smoothly. Rather than a heavy, monolithic application, this is a pragmatic, modular set of operational workflows built for reliability, easy maintenance, and clear separation of concerns.

## ⚙️ Key Engineering Features
- **Robust Error Handling & Fail-safes**: Scripts include retry logic and exception handling to manage API rate limits, network timeouts, and unexpected data formats without breaking the daily pipeline.
- **Structured Logging**: Implemented comprehensive logging across workflows to ensure traceability, simplify debugging, and provide clear audit trails for data ingestion.
- **Separation of Concerns**: Core operational logic lives in `src/main/`, while shared helper functions (like database connectors and formatting utilities) are centralized in `src/utils/` to prevent code duplication.
- **Secure Credential Management**: Sensitive configurations and API keys are externalized and managed securely, separate from the core logic.

## 📂 Project Structure
```text
├── creds/                 # Secure credential management and configuration
├── data/                  # Local data handling and temporary storage
├── src/
│   ├── main/              # Core operational scripts (e.g., automated pipelines, DB syncs)
│   └── utils/             # Shared helper functions, connectors, and data validation logic
└── README.md