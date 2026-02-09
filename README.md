# Home Credit Data Engineering – Credit Risk Lakehouse

Projet d’**analyse du risque de crédit** (dataset Home Credit Default Risk) sur une **architecture Lakehouse Medaillon (Bronze / Silver / Gold)**.

![Architecture Medaillon – Pipeline Banking Credit Risk](docs/medallion-architecture.png)

## Architecture

**Sources** (PostgreSQL + fichiers CSV) → **Bronze** (données brutes HDFS) → **Silver** (validation, jointures, agrégations) → **Gold** (KPIs risque, portefeuille) → **Datamarts PostgreSQL** → **API (JWT) + Dashboard Next.js**.

- **Commandes d’exécution :** [run.md](run.md) — ingestion Bronze, Silver, Gold, datamarts (Standalone ou YARN).

## Vérification des tables Gold (PostgreSQL)

Après exécution du Gold processor et chargement des datamarts, les tables du schéma `datamart` dans PostgreSQL contiennent les données agrégées (risque client, portefeuille, bureau, anciennes demandes). Exemple de vérification des effectifs :

![Tables Gold PostgreSQL — Gold processor et comptages datamart](docs/screenshot%20os%20postgress%20gold%20tables.png)

## Démarrage rapide

```bash
docker compose up -d
```

Puis suivre [run.md](run.md) pour l’ingestion Bronze, Silver, Gold et le chargement des datamarts.
