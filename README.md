/projet_nobels
│
├── etl/
│   ├── __init__.py
│   ├── extraction.py       # Récupération des données de l'API Nobel
│   ├── scraping.py         # Scraping des pages Wikipedia
│   ├── transformation.py   # Traitement et nettoyage des données
│   └── loading.py          # Insertion des données dans la base SQLite
│
├── database/
│   ├── Nobels.db           # Fichier de base SQLite
│   └── schema.sql          # Script SQL de création des tables
│
├── cli/
│   └── main.py             # Interface en ligne de commande orchestrant le ETL
│
├── utils/
│   ├── config.py           # Paramètres de configuration globaux
│   └── logger.py           # Gestion des logs
│
├── README.md               # Documentation du projet
└── requirements.txt        # Dépendances Python (requests, beautifulsoup4, etc.)
