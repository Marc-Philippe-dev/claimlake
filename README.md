# ClaimsLake — Lakehouse sinistres automobile

Projet de portfolio data engineering, de bout en bout, sur un cas d'assurance IARD :
ingestion multi-sources, architecture Médaillon, SCD de type 2, qualité des données,
streaming, orchestration, CI/CD et restitution Power BI, du poste local à Azure Databricks.

## Installation

Prérequis : Python 3.10+, Java 17+, Git. Sous Windows : WSL2 (Debian).

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements-dev.txt
cp .env.example .env    # then adapt .env to your machine
```

## Tests et qualité du code

```bash
pytest          # unit tests
ruff check src  # lint
```