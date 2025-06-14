#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script pour vérifier la structure de la table france_travail_jobs.
"""

import os
import sys
import pandas as pd
import sqlalchemy
from sqlalchemy import text

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importer l'utilitaire de chargement des variables d'environnement
from src.etl.api.dotenv_utils import load_dotenv

def main():
    """
    Fonction principale pour vérifier la structure de la table.
    """
    # Charger les variables d'environnement
    load_dotenv()
    
    # Récupérer les informations de connexion depuis les variables d'environnement
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    # Construire l'URL de connexion
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        # Créer le moteur de connexion
        engine = sqlalchemy.create_engine(db_url)
        
        # Tester la connexion et récupérer la structure de la table
        with engine.connect() as conn:
            # Vérifier si la table existe
            result = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'france_travail_jobs')"))
            table_exists = result.scalar()
            
            if not table_exists:
                print("La table 'france_travail_jobs' n'existe pas dans la base de données.")
                return
            
            # Récupérer un échantillon de données pour voir les colonnes
            result = conn.execute(text("SELECT * FROM france_travail_jobs LIMIT 1"))
            
            # Afficher les noms de colonnes
            columns = result.keys()
            print("\nColonnes disponibles dans la table france_travail_jobs:")
            for i, col in enumerate(columns):
                print(f"{i+1}. {col}")
            
            # Récupérer quelques statistiques
            result = conn.execute(text("SELECT COUNT(*) FROM france_travail_jobs"))
            count = result.scalar()
            print(f"\nNombre total d'enregistrements: {count}")
            
            # Afficher un échantillon de données pour les premières colonnes
            df = pd.read_sql("SELECT * FROM france_travail_jobs LIMIT 5", engine)
            print("\nAperçu des données (5 premiers enregistrements):")
            print(df.head().to_string())
            
            # Afficher les types de données
            print("\nTypes de données:")
            for col in df.columns:
                print(f"{col}: {df[col].dtype}")
    
    except Exception as e:
        print(f"Erreur lors de la vérification de la structure de la table: {e}")

if __name__ == "__main__":
    main()
