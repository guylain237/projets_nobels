#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script pour lister les dates disponibles dans les fichiers de données France Travail.
"""

import os
import sys
import logging
from datetime import datetime

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importer les fonctions nécessaires
from src.etl.api.extraction import list_s3_files, get_s3_client

def list_available_dates():
    """
    Liste les dates disponibles dans les fichiers de données France Travail.
    """
    print("=== Liste des dates disponibles dans les fichiers France Travail ===\n")
    
    # 1. Vérifier la connexion S3
    print("1. Connexion à S3...")
    s3_client = get_s3_client()
    if not s3_client:
        print("Échec de la connexion à S3.")
        return
    
    # 2. Lister les fichiers dans S3
    print("2. Liste des fichiers dans S3...")
    s3_files = list_s3_files()
    if not s3_files:
        print("Aucun fichier trouvé dans S3.")
        return
    
    print(f"Nombre total de fichiers trouvés: {len(s3_files)}")
    
    # 3. Extraire les dates des noms de fichiers
    print("\n3. Extraction des dates disponibles...")
    available_dates = set()
    
    for s3_key in s3_files:
        file_name = os.path.basename(s3_key)
        
        # Extraire la date du nom de fichier
        date_parts = file_name.split('_')
        for part in date_parts:
            if len(part) == 8 and part.isdigit():
                # Convertir en format lisible
                try:
                    date_obj = datetime.strptime(part, "%Y%m%d")
                    formatted_date = date_obj.strftime("%d/%m/%Y")
                    available_dates.add((part, formatted_date))
                    break
                except ValueError:
                    continue
    
    # Trier les dates
    sorted_dates = sorted(list(available_dates), key=lambda x: x[0])
    
    # Afficher les dates disponibles
    print("\nDates disponibles (format: YYYYMMDD - JJ/MM/AAAA):")
    for raw_date, formatted_date in sorted_dates:
        print(f"- {raw_date} - {formatted_date}")
    
    # Afficher la plage complète
    if sorted_dates:
        first_date = sorted_dates[0][1]
        last_date = sorted_dates[-1][1]
        print(f"\nPlage de dates disponibles: du {first_date} au {last_date}")
        print(f"Pour l'extraction, utilisez: --start-date {sorted_dates[0][0]} --end-date {sorted_dates[-1][0]}")
    
    print("\n=== Fin de la liste des dates disponibles ===")

if __name__ == "__main__":
    list_available_dates()
