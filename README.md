# Pipeline ETL pour les Offres d'Emploi

Ce projet implémente un pipeline ETL (Extract, Transform, Load) pour collecter, traiter et stocker des offres d'emploi provenant de l'API France Travail (anciennement Pôle Emploi) et du site Welcome to the Jungle.

## Architecture

Le pipeline est structuré en trois étapes principales :

1. **Extraction** : Récupération des données depuis les sources (API France Travail, scraping Welcome to the Jungle)
2. **Transformation** : Nettoyage, structuration et enrichissement des données brutes
3. **Chargement** : Stockage des données transformées dans une base de données PostgreSQL sur AWS RDS

## Fonctionnalités

- Extraction des offres d'emploi par date ou par mots-clés
- Nettoyage et normalisation des données textuelles
- Extraction structurée des informations (salaire, type de contrat, localisation, etc.)
- Analyse des technologies mentionnées dans les descriptions
- Chargement optimisé dans une base de données PostgreSQL
- Gestion des erreurs et logging détaillé

## Prérequis

- Python 3.8+
- Bibliothèques Python : pandas, sqlalchemy, psycopg2, requests, beautifulsoup4
- Accès à AWS (RDS PostgreSQL, S3)

## Installation

1. Cloner le dépôt
2. Installer les dépendances :
   ```
   pip install -r requirements.txt
   ```
3. Configurer les variables d'environnement (ou utiliser le script `configure_env.py`)

## Utilisation

### Exécution du pipeline complet

```bash
python run_etl.py --start-date 20250613 --end-date 20250614 --output-csv
```

### Options disponibles

- `--mode` : Mode d'exécution (`full`, `extract`, `transform`, `load`)
- `--start-date` : Date de début au format YYYYMMDD
- `--end-date` : Date de fin au format YYYYMMDD
- `--skip-db` : Ne pas charger les données dans la base de données
- `--output-csv` : Générer un fichier CSV avec les données transformées
- `--verbose` : Afficher les messages de debug détaillés

### Exemples d'utilisation

Extraction seule :
```bash
python run_etl.py --mode extract --start-date 20250613 --output-csv
```

Transformation seule :
```bash
python run_etl.py --mode transform --output-csv
```

Chargement seul :
```bash
python run_etl.py --mode load
```

## Structure du projet

```
├── data/
│   ├── raw/             # Données brutes extraites
│   ├── intermediate/    # Données intermédiaires
│   └── processed/       # Données transformées prêtes pour le chargement
├── docs/                # Documentation
├── logs/                # Fichiers de logs
├── src/
│   └── etl/
│       ├── api/         # Modules pour l'API France Travail
│       │   ├── extraction.py
│       │   ├── transformation.py
│       │   └── loading.py
│       └── scraper/     # Modules pour le scraping Welcome to the Jungle
├── tests/               # Tests unitaires et d'intégration
├── configure_env.py     # Script de configuration des variables d'environnement
├── run_etl.py           # Script principal d'exécution du pipeline
└── requirements.txt     # Dépendances Python
```

## Documentation

Pour plus de détails sur chaque module, consultez les fichiers de documentation dans le dossier `docs/` :

- [Documentation de la transformation](docs/transformation_documentation.md)
- [Documentation de l'extraction](docs/extraction_documentation.md)
- [Documentation du chargement](docs/loading_documentation.md)

## Base de données

La base de données PostgreSQL sur AWS RDS contient les tables suivantes :

- `france_travail_jobs` : Offres d'emploi de France Travail
- `welcome_jungle_jobs` : Offres d'emploi de Welcome to the Jungle
- `job_skills` : Table de liaison pour les compétences associées aux offres

## Auteur

Projet développé dans le cadre du BLOC 2 - Architecture AI
