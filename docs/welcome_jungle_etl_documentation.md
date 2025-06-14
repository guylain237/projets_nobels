# Documentation ETL Welcome to the Jungle

## Vue d'ensemble

Ce document décrit le pipeline ETL (Extract, Transform, Load) pour les données de Welcome to the Jungle. Le pipeline extrait les offres d'emploi depuis des fichiers JSON stockés dans un bucket AWS S3, transforme les données pour les normaliser et les enrichir, puis les charge dans une base de données PostgreSQL pour analyse ultérieure.

## Architecture du pipeline

Le pipeline ETL est composé de trois étapes principales :

1. **Extraction** : Récupération des données brutes depuis AWS S3
2. **Transformation** : Nettoyage et enrichissement des données
3. **Chargement** : Insertion des données transformées dans PostgreSQL

## Structure du projet

```
src/etl/
├── api/
│   └── dotenv_utils.py      # Utilitaires pour les variables d'environnement
├── scrapers/
│   ├── extraction.py        # Module d'extraction des données
│   ├── transformation.py    # Module de transformation des données
│   ├── loading.py           # Module de chargement des données
│   ├── init.py              # Point d'entrée du pipeline ETL
│   └── view_data.py         # Utilitaire pour visualiser les données
└── db_config.py             # Configuration de la base de données
```

## Configuration requise

- Python 3.8+
- Accès à AWS S3
- Base de données PostgreSQL
- Variables d'environnement configurées dans un fichier `.env`

### Variables d'environnement

Le fichier `.env` doit contenir les variables suivantes :

```
# AWS Credentials
AWS_ACCESS_KEY_ID=votre_access_key
AWS_SECRET_ACCESS_KEY=votre_secret_key
AWS_REGION=votre_region

# S3 Configuration
S3_BUCKET_NAME=data-lake-brut

# PostgreSQL Configuration
DB_HOST=votre_host
DB_PORT=5432
DB_NAME=votre_db
DB_USER=votre_user
DB_PASSWORD=votre_password
```

## Étape d'extraction

L'extraction récupère les fichiers JSON depuis AWS S3 et les convertit en DataFrame pandas.

### Fonctionnalités principales

- Connexion au bucket S3 avec authentification AWS
- Téléchargement des fichiers JSON
- Mise en cache local des fichiers pour éviter des téléchargements répétés
- Support pour l'extraction d'un fichier spécifique ou de tous les fichiers disponibles

### Utilisation

```python
from src.etl.scrapers.extraction import extract_welcome_jungle_data

# Extraire toutes les données
df, local_file = extract_welcome_jungle_data(all_files=True)

# Extraire un fichier spécifique
df, local_file = extract_welcome_jungle_data(specific_file="welcome_jungle_all_jobs_all_locations_20250611_022755.json")

# Forcer le téléchargement même si le fichier existe localement
df, local_file = extract_welcome_jungle_data(all_files=True, force_download=True)
```

## Étape de transformation

La transformation nettoie et enrichit les données extraites pour les préparer au chargement.

### Fonctionnalités principales

- Normalisation des types de contrat (CDI, CDD, STAGE, ALTERNANCE, FREELANCE, OTHER)
- Extraction des informations de salaire (min, max, devise, période)
- Normalisation des lieux de travail
- Extraction des informations d'expérience
- Détection des technologies mentionnées dans les descriptions
- Standardisation des noms de colonnes et des formats de données

### Algorithmes clés

#### Normalisation des types de contrat

La fonction `normalize_contract_type` utilise un dictionnaire enrichi de mots-clés pour classifier les types de contrat. Elle combine le type de contrat et le titre du poste pour une meilleure détection et inclut des règles spécifiques pour les cas particuliers comme les temps partiels, les postes commerciaux, les postes étudiants, etc.

#### Extraction des salaires

La fonction `extract_salary_info` analyse les chaînes de caractères de salaire pour extraire :
- Salaire minimum et maximum
- Devise (EUR, USD, GBP)
- Période (annuel, mensuel, hebdomadaire, journalier, horaire)

### Utilisation

```python
from src.etl.scrapers.transformation import transform_welcome_jungle_data

# Transformer les données
transformed_df = transform_welcome_jungle_data(df)

# Sauvegarder les données transformées
from src.etl.scrapers.transformation import save_transformed_data
output_file = save_transformed_data(transformed_df)
```

## Étape de chargement

Le chargement insère les données transformées dans une base de données PostgreSQL.

### Fonctionnalités principales

- Création automatique de la table si elle n'existe pas
- Connexion sécurisée à PostgreSQL avec gestion des erreurs
- Support pour l'ajout de nouvelles données ou le remplacement des données existantes

### Structure de la table PostgreSQL

La table `welcome_jungle_jobs` contient les colonnes suivantes :

| Colonne | Type | Description |
|---------|------|-------------|
| id | SERIAL | Clé primaire auto-incrémentée |
| job_id | VARCHAR(255) | Identifiant unique de l'offre |
| title | VARCHAR(255) | Titre de l'offre d'emploi |
| company_name | VARCHAR(255) | Nom de l'entreprise |
| lieu_travail | TEXT | Lieu de travail normalisé |
| contract_type_std | VARCHAR(50) | Type de contrat normalisé |
| min_salary | FLOAT | Salaire minimum |
| max_salary | FLOAT | Salaire maximum |
| salary_currency | VARCHAR(10) | Devise du salaire |
| salary_period | VARCHAR(20) | Période du salaire |
| min_experience | FLOAT | Expérience minimale requise (en années) |
| max_experience | FLOAT | Expérience maximale requise (en années) |
| experience_level | VARCHAR(50) | Niveau d'expérience normalisé |
| url_source | TEXT | URL de l'offre d'emploi |
| publication_date | DATE | Date de publication |
| source | VARCHAR(50) | Source des données |
| processing_date | DATE | Date de traitement |
| has_python | BOOLEAN | Indique si Python est mentionné |
| has_java | BOOLEAN | Indique si Java est mentionné |
| has_javascript | BOOLEAN | Indique si JavaScript est mentionné |
| ... | ... | ... |

### Utilisation

```python
from src.etl.scrapers.loading import get_db_connection, load_welcome_jungle_data

# Obtenir une connexion à la base de données
engine = get_db_connection()

# Charger les données
success = load_welcome_jungle_data(transformed_df, engine)
```

## Exécution du pipeline complet

Le pipeline complet peut être exécuté via le module `init.py`.

### Options de ligne de commande

- `--extract` : Exécuter uniquement l'étape d'extraction
- `--transform` : Exécuter uniquement l'étape de transformation
- `--load` : Exécuter uniquement l'étape de chargement
- `--all` : Exécuter toutes les étapes du pipeline
- `--force` : Forcer le chargement des données même si elles existent déjà
- `--force-download` : Forcer le téléchargement des fichiers même s'ils existent déjà localement
- `--file` : Spécifier un fichier particulier à traiter

### Exemples d'utilisation

```bash
# Exécuter tout le pipeline
python -m src.etl.scrapers.init

# Exécuter uniquement l'étape d'extraction
python -m src.etl.scrapers.init --extract

# Exécuter uniquement l'étape de transformation
python -m src.etl.scrapers.init --transform

# Exécuter uniquement l'étape de chargement
python -m src.etl.scrapers.init --load

# Exécuter tout le pipeline avec un fichier spécifique
python -m src.etl.scrapers.init --all --file welcome_jungle_all_jobs_all_locations_20250611_022755.json

# Forcer le téléchargement et le chargement
python -m src.etl.scrapers.init --all --force --force-download
```

## Visualisation des données

Le module `view_data.py` permet de visualiser les données à différentes étapes du pipeline.

### Options de ligne de commande

- `--step extract` : Afficher les données après l'étape d'extraction
- `--step transform` : Afficher les données après l'étape de transformation
- `--step all` : Afficher les données après chaque étape

### Exemples d'utilisation

```bash
# Afficher les données extraites
python -m src.etl.scrapers.view_data --step extract

# Afficher les données transformées
python -m src.etl.scrapers.view_data --step transform

# Afficher les données à chaque étape
python -m src.etl.scrapers.view_data --step all
```

## Améliorations apportées

### Normalisation des types de contrat

- Réduction significative des contrats classés comme "OTHER" (de 26.90% à 11.55%)
- Amélioration de la détection des CDI (de 0.95% à 8.83%)
- Amélioration de la détection des CDD (de 0% à 5.43%)
- Légère amélioration de la détection des alternances (de 40.76% à 42.80%)

### Extraction des salaires

- Support pour divers formats de salaire (plages, valeurs uniques, négociables)
- Détection des devises (EUR, USD, GBP)
- Détection des périodes de salaire (annuel, mensuel, hebdomadaire, journalier, horaire)
- Gestion des cas spéciaux comme "37/40" heures par semaine

## Prochaines étapes

1. **Analyse des données** : Utiliser les données chargées pour des analyses statistiques et des visualisations
2. **Optimisation des performances** : Améliorer les performances du pipeline pour traiter de plus grands volumes de données
3. **Tests unitaires** : Ajouter des tests unitaires pour les fonctions clés comme `extract_salary_info` et `normalize_contract_type`
4. **Automatisation** : Mettre en place une exécution automatique périodique du pipeline
5. **Monitoring** : Ajouter des métriques et des alertes pour surveiller la santé du pipeline

## Conclusion

Le pipeline ETL Welcome to the Jungle est maintenant opérationnel et prêt à être utilisé pour l'analyse des données d'offres d'emploi. Les données sont extraites, transformées et chargées de manière fiable, avec une attention particulière portée à la normalisation des types de contrat et à l'extraction des informations de salaire.
