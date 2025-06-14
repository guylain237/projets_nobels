#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de test pour le chargement des données d'offres d'emploi France Travail.
Utilise un fichier CSV transformé existant pour tester le chargement dans PostgreSQL.
"""

import os
import pandas as pd
import glob
from datetime import datetime
import logging
from src.etl.api.loading import get_db_engine, load_job_data_to_db, prepare_job_data_for_loading

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def find_latest_processed_csv():
    """
    Trouve le fichier CSV transformé le plus récent dans le dossier data/processed.
    
    Returns:
        str: Chemin vers le dernier fichier CSV transformé
    """
    pattern = os.path.join("data", "processed", "france_travail_processed_*.csv")
    csv_files = glob.glob(pattern)
    
    if not csv_files:
        logger.error("Aucun fichier CSV transformé trouvé.")
        return None
    
    # Trouver le fichier le plus récent
    latest_file = max(csv_files, key=os.path.getctime)
    return latest_file

def test_loading():
    """
    Teste le processus de chargement sur un fichier CSV transformé existant.
    """
    print("=== Test du module de chargement ===\n")
    
    # 1. Trouver et charger le dernier fichier CSV transformé
    latest_csv = find_latest_processed_csv()
    if not latest_csv:
        print("Impossible de procéder sans fichier transformé.")
        return
    
    print(f"1. Chargement des données depuis {latest_csv}...")
    try:
        transformed_df = pd.read_csv(latest_csv, low_memory=False)
        print(f"   - {len(transformed_df)} offres d'emploi chargées")
        print(f"   - Colonnes disponibles: {transformed_df.columns.tolist()[:10]}...")
    except Exception as e:
        print(f"Erreur lors du chargement du fichier CSV: {e}")
        return
    
    # 2. Préparer les données pour le chargement
    print("\n2. Préparation des données pour le chargement...")
    try:
        prepared_df = prepare_job_data_for_loading(transformed_df)
        if prepared_df is None:
            print("Échec de la préparation des données.")
            return
        print(f"   - {len(prepared_df)} offres préparées pour le chargement")
        print(f"   - Colonnes alignées avec le schéma de la base de données: {prepared_df.columns.tolist()[:10]}...")
    except Exception as e:
        print(f"Erreur lors de la préparation des données: {e}")
        return
    
    # 3. Établir la connexion à la base de données
    print("\n3. Connexion à la base de données PostgreSQL...")
    try:
        engine = get_db_engine()
        if engine is None:
            print("Échec de la connexion à la base de données.")
            return
        print("   - Connexion établie avec succès")
    except Exception as e:
        print(f"Erreur lors de la connexion à la base de données: {e}")
        return
    
    # 4. Charger les données dans la base de données
    print("\n4. Chargement des données dans la base de données...")
    try:
        # Limiter à 10 enregistrements pour le test
        test_df = prepared_df.head(10).copy() if len(prepared_df) > 10 else prepared_df.copy()
        
        # Ajouter un préfixe TEST aux IDs pour éviter les conflits
        test_df['id'] = 'TEST_' + test_df['id'].astype(str)
        
        result = load_job_data_to_db(test_df, table_name='france_travail_jobs', test_mode=True)
        if result:
            print(f"   - {result} offres chargées avec succès dans la table france_travail_jobs")
        else:
            print("   - Aucune offre chargée dans la base de données")
    except Exception as e:
        print(f"Erreur lors du chargement des données: {e}")
        return
    
    print("\n=== Test du module de chargement terminé ===")

if __name__ == "__main__":
    test_loading()
