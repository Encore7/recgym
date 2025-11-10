# RecGym — Local Production-Grade Recommender System (2026-ready)

![GitHub last commit](https://img.shields.io/github/last-commit/<your-username>/recgym)
![GitHub repo size](https://img.shields.io/github/repo-size/<your-username>/recgym)
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)
![Docker Compose](https://img.shields.io/badge/Docker-Compose-blue)
![MLflow](https://img.shields.io/badge/MLflow-2.14-success)

---

## Overview
RecGym is a **self-contained, production-style recommender system** demonstrating modern **MLOps, observability, and real-time data engineering** — all running locally using Docker.

- **Stack:** FastAPI, Kafka, Flink, Feast, MLflow, PyTorch, LightGBM, Redis, Postgres  
- **Infra:** Prometheus, Grafana, Tempo, Loki, OpenTelemetry  
- **Dataset:** RetailRocket (session-based e-commerce dataset)  
- **Goal:** Full-fledged project showcasing real industry skills for Data Scientist / ML Engineer roles.

---

## Quick Start

```bash
git clone https://github.com/<your-username>/recgym.git
cd recgym
cp .env.example .env
make up
```

## Check running services:

```bash
make ps
```

## Stop and remove:

```bash
make down
```

---

**Services**

| Service | URL | Purpose |
|----------|-----|----------|
| Postgres |  http://localhost:5432 | warehouse & offline features |
| Redis |  http://localhost:6379 | online features & rate limit |
| Kafka UI | http://localhost:8080 | topic monitor |
| MLflow | http://localhost:5000 | experiment tracking |
| MinIO | http://localhost:9001 | artifact storage |
| Grafana | http://localhost:3000 | dashboards |
| Prometheus | http://localhost:9090 | metrics |
| Tempo | http://localhost:3200 | traces |
| Loki | http://localhost:3100 | logs |

---

**Roadmap**

| Phase | Focus | Status |
|--------|--------|---------|
| 0 | Infrastructure + Observability | ✅ Completed |
| 1 | Kafka + Schema Registry + Synthetic Event Generator | ✅ Completed |
| 2 | Feature Store (Feast) + Streaming Flink Jobs | 🔜 |
| 3 | Modeling (Two-Tower + LightGBM Re-ranker) | ⏳ |
| 4 | FastAPI Serving + Auth + Monitoring | ⏳ |
| 5 | Drift Detection + Bandits + Canary Deploys | ⏳ |

---

**Developer Notes**

- Python ≥ 3.11  
- Uses `ruff`, `black`, `mypy` for code quality  
- `Makefile` handles all infra operations  
- All services run locally; no cloud credits required  

---

**License**

MIT — freely usable for learning and portfolio use.
