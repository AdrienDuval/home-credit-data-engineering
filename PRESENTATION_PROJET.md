# Script de présentation — Home Credit Data Engineering

Document de présentation du projet : objectifs, architecture, étapes et résultats. À utiliser comme fil conducteur pour la démo (Docker, Spark UI, NameNode, datamarts, API, dashboard).

---

## 1. Objectifs et buts du projet

**Contexte métier**  
Construire une plateforme de **analyse du risque de crédit** à partir de données bancaires (dataset Home Credit Default Risk), depuis l’ingestion des données brutes jusqu’à la restitution pour la décision (tableaux de bord, API, reporting).

**Objectifs principaux :**

- **Ingérer** des données issues de deux sources : une base PostgreSQL (système interne) et des fichiers CSV (historique bureau, paiements, anciennes demandes).
- **Nettoyer, valider et unifier** les données dans une couche Silver (règles de qualité, jointures, agrégations).
- **Produire des indicateurs métier** (risque client, exposition portefeuille) dans une couche Gold et les exposer via des **datamarts** PostgreSQL.
- **Exposer les données** via une **API sécurisée (JWT)** avec pagination et un **dashboard** (graphiques, liste clients, fiche client détaillée avec historique bureau et anciennes demandes).

**Livrables :**

- Pipeline **Bronze → Silver → Gold** (architecture medaillon / lakehouse).
- Datamarts : risque client, résumé portefeuille, crédits bureau par client, anciennes demandes par client.
- API REST (FastAPI) + frontend (Next.js) pour visualisation et exploration.

---

## 2. Configuration Docker et environnement

**À montrer en premier** : l’écosystème tourne entièrement dans Docker.

### 2.1 Services (docker-compose)

- **postgres** (`home_credit_postgres`) — Source A : base opérationnelle (tables `raw.application_train`, `raw.application_test`). Port 5432.
- **namenode** (`home_credit_namenode`) — NameNode HDFS. Ports 9870 (UI web), 8020 (HDFS).
- **datanode** (`home_credit_datanode`) — DataNode HDFS (stockage distribué).
- **spark-master** (`home_credit_spark_master`) — Master Spark. Ports 8080 (UI Spark Master), 7077 (connexion des workers).
- **spark-worker** (`home_credit_spark_worker`) — Worker Spark (exécution des jobs).

**Démarrage :**

```bash
docker compose up -d
```

### 2.2 À montrer pendant la présentation

1. **NameNode HDFS**  
   - URL : http://localhost:9870  
   - Montrer : **Utilities → Browse the file system**  
   - Arborescence : `/raw` (Bronze), `/silver`, `/gold` — partitions par date (ingest_date ou year/month/day).

2. **Spark Master UI**  
   - URL : http://localhost:8080  
   - Montrer : **Workers** (1 worker, cœurs, mémoire), **Completed Applications** (liste des jobs : bronze_csv_*, silver_processor, gold_processor, gold_datamart_extended).

3. **PostgreSQL**  
   - Schéma `raw` (sources), schéma `datamart` (tables de restitution).  
   - Commandes utiles dans `run.md` (section 5.2) et `DATAMART_SETUP.md`.

---

## 3. Architecture medaillon (Bronze, Silver, Gold)

Flux global : **Sources (PostgreSQL + CSV) → Bronze → Silver → Gold → Datamarts PostgreSQL → API + Dashboard**.

### 3.1 Bronze — Données brutes, traçables

**But :** Stocker les données telles qu’elles arrivent, avec un marquage de date d’ingestion et de source. Aucune règle métier ; rejouable.

**Étapes et objectifs :**

| Étape | Source | Objectif | Où ça s’exécute |
|--------|--------|----------|------------------|
| Bronze Postgres | PostgreSQL | Lire `application_train` et `application_test`, écrire en Parquet sur HDFS (`/raw/postgres/...`) | `feeder_postgres.py` |
| Bronze CSV | Fichiers CSV | Lire chaque jeu (bureau, bureau_balance, previous_application, installments_payments, etc.), ajouter `ingest_date` et `source_system`, écrire en Parquet (`/raw/csv/<dataset>/ingest_date=YYYY-MM-DD`) | `feeder_csv.py` |

**À montrer :** Dans l’UI Spark, les applications **bronze_csv_bureau**, **bronze_csv_previous_application**, **bronze_postgres_application_train**, etc. Dans HDFS : contenu de `/raw/postgres/` et `/raw/csv/`.

### 3.2 Silver — Données validées et unifiées

**But :** Qualité (règles de validation), enrichissement (jointures, fenêtrage), une vue par thème et par client.

**Étapes et objectifs :**

| Étape | Entrées | Objectif | Sortie Silver |
|--------|--------|----------|----------------|
| silver_client_application | Bronze Postgres | Valider les lignes (règles métier), supprimer les invalides | 1 ligne par demande valide |
| silver_bureau_summary | Bronze bureau + bureau_balance | Dernier état par crédit bureau (window), agrégation par client (dette, retard, etc.) | 1 ligne par client |
| silver_payment_behavior | Bronze installments_payments | Retards de paiement, fenêtre par client, agrégats (retard moyen, nombre de retards) | 1 ligne par client |
| silver_previous_applications | Bronze previous_application | Agrégation par client (taux de rejet, etc.) | 1 ligne par client |

**À montrer :** Application **silver_processor** dans Spark UI. Dans HDFS : `/silver/silver_client_application/`, `/silver/silver_bureau_summary/`, etc. Logs dans `logs/silver/processor.txt`.

### 3.3 Gold — Indicateurs métier et risque

**But :** Répondre aux questions « Qui est risqué ? » et « Comment se porte le portefeuille par segment de risque ? ».

**Étapes et objectifs :**

| Table Gold | Entrées | Objectif |
|------------|--------|----------|
| gold_client_risk_profile | Silver (application, bureau_summary, payment_behavior, previous_applications) | 1 ligne par client : revenu, exposition crédit, ratio dette bureau, score retard, taux rejet, segment de risque (HIGH / MEDIUM / LOW) |
| gold_portfolio_risk | gold_client_risk_profile | 1 ligne par segment : nombre de clients, exposition totale, taux de défaut moyen |

**À montrer :** Application **gold_processor** dans Spark UI. HDFS : `/gold/gold_client_risk_profile/`, `/gold/gold_portfolio_risk/`. Logs : `logs/gold/processor.txt`.

### 3.4 Datamarts PostgreSQL — Couche de restitution

**But :** Servir l’API et le dashboard sans refaire les calculs Spark à chaque requête.

| Datamart | Contenu | Rôle |
|----------|--------|------|
| datamart_client_risk | Copie de gold_client_risk_profile | Liste paginée, détail client, filtres |
| datamart_portfolio_summary | Copie de gold_portfolio_risk | 3 graphiques (répartition risque, taux défaut, exposition) |
| datamart_client_bureau | Chargé depuis Bronze bureau | Crédits bureau par client (fiche détail) |
| datamart_client_previous_apps | Chargé depuis Bronze previous_application | Anciennes demandes par client (fiche détail) |

**Chargement :**  
- Principal : `gold/processor.py --write-datamart` (client_risk + portfolio_summary).  
- Étendu : `gold/datamart_extended.py` (bureau + previous_applications), avec la **même** date d’ingestion que le Bronze CSV.

**À montrer :** Applications **gold_datamart_extended** et jobs Gold avec `--write-datamart` dans Spark UI. Dans PostgreSQL : `\dt datamart.*` et quelques `SELECT COUNT(*)` (voir `DATAMART_SETUP.md` ou `run.md` section 5.2 / 5.3).

---

## 4. Revue des résultats

### 4.1 Pipeline et données

- **Bronze :** Fichiers Parquet partitionnés sur HDFS ; rejouable avec une date d’ingestion.
- **Silver :** Quatre tables Silver avec métriques de qualité dans les logs.
- **Gold :** Tables Gold sur HDFS + chargement dans les datamarts.
- **Vérifications :** Comptages dans PostgreSQL (datamart_client_risk, datamart_portfolio_summary, datamart_client_bureau, datamart_client_previous_apps) comme dans `DATAMART_SETUP.md` ou run.md.

### 4.2 API (FastAPI)

- **Authentification :** `POST /token` (JWT).
- **Endpoints :**  
  - `GET /clients/risk` (paginated, filtres : segment, revenu, exposition),  
  - `GET /clients/risk/{id}` (fiche client),  
  - `GET /clients/risk/{id}/bureau`,  
  - `GET /clients/risk/{id}/previous-applications`,  
  - `GET /portfolio/summary` (pour les graphiques).

### 4.3 Dashboard (Next.js)

- Connexion (JWT), dashboard avec **3 graphiques** (répartition risque, taux de défaut par segment, exposition par segment).
- **Liste clients** paginée avec filtres et recherche par ID.
- **Fiche client** : profil de risque, graphiques (facteurs de risque, revenu vs crédit/dette), historique **bureau** et **anciennes demandes** lorsque les datamarts étendus sont chargés.

---

## 5. Récapitulatif pour la démo

1. **Docker** — Montrer `docker compose up -d`, les conteneurs (postgres, namenode, datanode, spark-master, spark-worker).
2. **NameNode** — http://localhost:9870, navigation dans `/raw`, `/silver`, `/gold`.
3. **Spark Master** — http://localhost:8080, workers et applications terminées (bronze, silver, gold, datamart_extended).
4. **Architecture** — Rappeler le schéma : Sources → Bronze → Silver → Gold → Datamarts → API + Dashboard.
5. **Résultats** — PostgreSQL (datamart.*), API (Swagger ou curl), Dashboard (login, graphiques, liste clients, fiche client avec bureau et anciennes demandes).

**Référence des commandes détaillées :** `run.md` (exécution pas à pas) et `DATAMART_SETUP.md` (checklist complète datamarts).
