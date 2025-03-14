# cli/main.py

import os
import sys
import argparse

# Ajouter le répertoire racine du projet dans le PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importer la fonction de création de la base de données
from databases.createdb import create_db

def main(args):
    # Création de la base de données à partir du script schema.sql
    create_db()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Création de la base de données pour le projet Nobels"
    )
    parser.add_argument("--verbose", action="store_true", help="Mode verbeux")
    args = parser.parse_args()
    
    main(args)
