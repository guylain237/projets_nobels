#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script pour extraire toutes les données France Travail sans filtrage par date.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importer les fonctions nécessaires
from src.etl.api.extraction import list_s3_files, download_s3_file, extract_jobs_to_dataframe

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_all_data():
    """
    Extrait toutes les données France Travail disponibles sans filtrage par date.
    """
    print("=== Extraction de toutes les données France Travail ===\n")
    
    # 1. Lister tous les fichiers dans S3
    print("1. Liste des fichiers dans S3...")
    s3_files = list_s3_files()
    if not s3_files:
        print("Aucun fichier trouvé dans S3.")
        return None
    
    print(f"Nombre total de fichiers trouvés: {len(s3_files)}")
    
    # 2. Télécharger tous les fichiers
    print("\n2. Téléchargement des fichiers...")
    temp_dir = "data/temp/s3_downloads"
    os.makedirs(temp_dir, exist_ok=True)
    
    downloaded_files = []
    for i, s3_key in enumerate(s3_files):
        print(f"Téléchargement {i+1}/{len(s3_files)}: {os.path.basename(s3_key)}")
        downloaded_file = download_s3_file('data-lake-brut', s3_key, temp_dir)
        if downloaded_file:
            downloaded_files.append(downloaded_file)
    
    print(f"\nTéléchargement terminé: {len(downloaded_files)}/{len(s3_files)} fichiers téléchargés")
    
    # 3. Extraire les données
    print("\n3. Extraction des données...")
    if not downloaded_files:
        print("Aucun fichier téléchargé pour l'extraction.")
        return None
    
    df = extract_jobs_to_dataframe(downloaded_files)
    
    if df is None or df.empty:
        print("Aucune donnée extraite.")
        return None
    
    print(f"\nExtraction réussie: {len(df)} offres d'emploi trouvées")
    
    # 4. Sauvegarder les données
    print("\n4. Sauvegarde des données...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "data/intermediate"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/france_travail_data_all_{timestamp}.csv"
    
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Données sauvegardées dans: {output_path}")
    
    # 5. Afficher quelques statistiques
    print("\n5. Statistiques sur les données extraites:")
    print(f"- Nombre total d'offres: {len(df)}")
    print(f"- Colonnes disponibles: {', '.join(df.columns[:10])}...")
    
    if 'dateCreation' in df.columns:
        # Convertir les dates en datetime pour analyse
        df['dateCreation'] = pd.to_datetime(df['dateCreation'], errors='coerce')
        min_date = df['dateCreation'].min()
        max_date = df['dateCreation'].max()
        print(f"- Période couverte: du {min_date.strftime('%d/%m/%Y')} au {max_date.strftime('%d/%m/%Y')}")
    
    print("\n=== Extraction terminée ===")
    return output_path

if __name__ == "__main__":
    extract_all_data()
