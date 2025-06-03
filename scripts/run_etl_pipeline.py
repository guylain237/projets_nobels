#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script pour exécuter le pipeline ETL complet.
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
        logging.FileHandler(f"logs/etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(description='Exécution du pipeline ETL')
    parser.add_argument('--source', choices=['welcome_jungle', 'pole_emploi', 'all'], default='all',
                        help='Source des données (welcome_jungle, pole_emploi, all)')
    parser.add_argument('--no-local', action='store_true', help='Ne pas sauvegarder en local')
    parser.add_argument('--no-s3', action='store_true', help='Ne pas uploader vers S3')
    parser.add_argument('--no-rds', action='store_true', help='Ne pas charger dans RDS')
    return parser.parse_args()

def main():
    """Fonction principale du script."""
    # Créer les dossiers nécessaires
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data/processed/welcome_jungle', exist_ok=True)
    os.makedirs('data/processed/pole_emploi', exist_ok=True)
    
    # Parser les arguments
    args = parse_arguments()
    
    # Importer les fonctions du pipeline ETL
    from src.etl.etl_pipeline import run_welcome_jungle_pipeline, run_pole_emploi_pipeline
    
    logger.info(f"Exécution du pipeline ETL pour {args.source}")
    
    if args.source in ['welcome_jungle', 'all']:
        logger.info("Exécution du pipeline ETL pour Welcome to the Jungle")
        run_welcome_jungle_pipeline(
            save_local=not args.no_local,
            upload_s3=not args.no_s3,
            load_rds=not args.no_rds
        )
    
    if args.source in ['pole_emploi', 'all']:
        logger.info("Exécution du pipeline ETL pour Pôle Emploi")
        run_pole_emploi_pipeline(
            save_local=not args.no_local,
            upload_s3=not args.no_s3,
            load_rds=not args.no_rds
        )
    
    logger.info("Pipeline ETL terminé avec succès !")

if __name__ == "__main__":
    main()
