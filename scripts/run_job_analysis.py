#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script pour exécuter l'analyse des offres d'emploi.
"""

import os
import sys
import argparse
import logging
from datetime import datetime

# Ajouter le répertoire parent au chemin Python pour importer les modules du projet
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/job_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(description='Analyse des offres d\'emploi')
    parser.add_argument('--source', choices=['rds', 's3', 'local'], default='local',
                        help='Source des données à analyser (rds, s3, local)')
    parser.add_argument('--output-dir', type=str, default='reports',
                        help='Répertoire de sortie pour les rapports et visualisations')
    return parser.parse_args()

def main():
    """Fonction principale du script."""
    # Créer les dossiers nécessaires
    os.makedirs('logs', exist_ok=True)
    os.makedirs('reports/figures', exist_ok=True)
    
    # Parser les arguments
    args = parse_arguments()
    
    # Importer les fonctions d'analyse
    from src.analysis.job_analysis import (
        generate_report, 
        load_jobs_from_rds, 
        load_jobs_from_s3, 
        load_jobs_from_local
    )
    
    logger.info(f"Analyse des offres d'emploi depuis {args.source}")
    
    # Charger les données selon la source spécifiée
    if args.source == 'rds':
        logger.info("Chargement des données depuis RDS PostgreSQL")
        try:
            df = load_jobs_from_rds()
        except Exception as e:
            logger.error(f"Erreur lors du chargement depuis RDS: {e}")
            logger.info("Tentative de chargement depuis les fichiers locaux...")
            df = load_jobs_from_local()
    elif args.source == 's3':
        logger.info("Chargement des données depuis S3")
        try:
            df = load_jobs_from_s3()
        except Exception as e:
            logger.error(f"Erreur lors du chargement depuis S3: {e}")
            logger.info("Tentative de chargement depuis les fichiers locaux...")
            df = load_jobs_from_local()
    else:  # local
        logger.info("Chargement des données depuis les fichiers locaux")
        df = load_jobs_from_local()
    
    # Vérifier si des données ont été chargées
    if df is None or df.empty:
        logger.error("Aucune donnée n'a pu être chargée. Impossible de générer le rapport.")
        return
    
    logger.info(f"Données chargées avec succès: {len(df)} offres d'emploi")
    
    # Générer le rapport
    output_dir = args.output_dir
    logger.info(f"Génération du rapport dans {output_dir}")
    generate_report(df, output_dir=output_dir)
    
    logger.info("Analyse terminée avec succès !")

if __name__ == "__main__":
    main()
