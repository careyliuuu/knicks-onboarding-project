# 🏀 Knicks Enterprise AI Pipeline

> **An Event-Driven, Human-in-the-Loop GenAI Pipeline built on Apache Airflow & Astronomer.**

This project is an enterprise-grade data pipeline that predicts the outcome of New York Knicks games using **Llama 3 (8B)** via the **Groq API**. It demonstrates modern data engineering practices including Event-Driven Architecture (Airflow Assets), Human-in-the-Loop validation, and automated CI/CD.

---

## 🏗️ Architecture Overview

The pipeline consists of two decoupled DAGs connected by an **Airflow Asset** (`redshift://knicks_predictions`).

### 1. Producer DAG: `knicks_enterprise_pipeline`
* **Ingest:** Loads raw game data from CSV (Kaggle dataset).
* **Transform:** Processes team stats using Python (Pandas).
* **AI Inference:** Sends game context (Opponent, Date, Recent Stats) to **Groq/Llama 3** to generate a prediction.
* **Human-in-the-Loop:** Pauses execution for manual approval via the Airflow UI (`approve_analysis` task).
* **Load:** Writes the approved prediction to **Amazon Redshift**.
* **Trigger:** Updates the `redshift://knicks_predictions` asset.

### 2. Consumer DAG: `knicks_analytics_dashboard`
* **Trigger:** Automatically starts when the upstream asset is updated.
* **Action:** Simulates refreshing a downstream analytics dashboard or sending notifications based on the new data.

---

## 🚀 Key Features

* **GenAI Integration:** Uses the [Airflow AI SDK](https://github.com/astronomer/airflow-ai-sdk) to interface with Groq's Llama 3 model for high-speed inference.
* **Event-Driven Scheduling:** Decouples the prediction logic from the dashboard logic using Airflow Datasets/Assets.
* **Human Validation:** Implements a "Stop & Review" step to prevent AI hallucinations from reaching production tables.
* **Data Warehouse:** Persists structured predictions into **Amazon Redshift** with idempotent storage logic (`CASCADE` drops).
* **Observability:** Full lineage tracking via **Astro Observe** (OpenLineage) with a defined Data Product.
* **CI/CD:** Automated deployments to Astronomer via **GitHub Actions** on push to `main`.

---

## 📂 Project Structure

* **`dags/`**: Contains Python files for Airflow DAGs.
    * `knicks_prediction_flow.py`: Main Pipeline (Producer)
    * `downstream_analytics.py`: Dashboard Trigger (Consumer)
    * `assets.py`: Shared Asset Definitions
* **`include/`**: Raw data sources (e.g., `knicks_data.csv`).
* **`.github/workflows/`**: CI/CD Configuration (`astro-deploy.yaml`).
* **`Dockerfile`**: Astro Runtime Environment.
* **`requirements.txt`**: Python Dependencies.

---

## 🛠️ Setup & Deployment

### Local Development
1. **Install Astro CLI:** `brew install astro`
2. **Start Airflow:** `astro dev start`
3. **Access UI:** Open `localhost:8080` (User: `admin`, Pass: `admin`)

### Cloud Deployment (CI/CD)
This project is configured with **GitHub Actions**.
1. Push changes to the `main` branch.
2. The workflow automatically builds and deploys to the **Astronomer Cloud**.
3. Monitor the deployment in the **Astro Dashboard**.

---

## 📊 Data Lineage

This project uses **OpenLineage** to track data movement.

* **Producer:** `knicks_enterprise_pipeline`
* **Asset:** `redshift://knicks_predictions`
* **Consumer:** `knicks_analytics_dashboard`

View the full lineage graph in the **Astro Observe** tab.

---

## 📞 Contact

* **Maintainer:** Carey Liu
* **Platform:** Astronomer / Apache Airflow
