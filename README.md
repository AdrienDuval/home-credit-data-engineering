# Home Credit Data Engineering – Credit Risk Lakehouse

Projet d’**analyse du risque de crédit** (dataset Home Credit Default Risk) sur une **architecture Lakehouse Medaillon (Bronze / Silver / Gold)**.

![Architecture Medaillon – Pipeline Banking Credit Risk](docs/medallion-architecture.png)

## Architecture

**Sources** (PostgreSQL + fichiers CSV) → **Bronze** (données brutes HDFS) → **Silver** (validation, jointures, agrégations) → **Gold** (KPIs risque, portefeuille) → **Datamarts PostgreSQL** → **API (JWT) + Dashboard Next.js**.

- **Script de présentation (français) :** [PRESENTATION_PROJET.md](PRESENTATION_PROJET.md) — objectifs, Docker, architecture medaillon, étapes, revue des résultats.
- **Commandes d’exécution :** [run.md](run.md) — ingestion Bronze, Silver, Gold, datamarts.
- **Checklist datamarts complète :** [DATAMART_SETUP.md](DATAMART_SETUP.md).

## Démarrage rapide

```bash
docker compose up -d
```

Puis suivre [run.md](run.md) pour l’ingestion Bronze, Silver, Gold et le chargement des datamarts.
