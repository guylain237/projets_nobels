#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal pour l'exécution du pipeline ETL complet.
Ce script sert de point d'entrée unique pour lancer les différentes étapes du pipeline.
"""

import os
import sys
import argparse
import logging
from datetime import datetime

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuration du logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """
    Parse les arguments de la ligne de commande.
    """
    parser = argparse.ArgumentParser(description="Pipeline ETL pour les données d'offres d'emploi")
    
    # Options générales
    parser.add_argument('--source', choices=['france_travail', 'welcome_jungle', 'all'], 
                        default='france_travail', help="Source de données à traiter (défaut: france_travail)")
    
    # Options de pipeline
    parser.add_argument('--mode', choices=['full', 'extract', 'transform', 'load'], 
                        default='full', help="Mode d'exécution (défaut: full)")
    
    # Options spécifiques à France Travail
    parser.add_argument('--all-data', action='store_true',
                        help="Extraire toutes les données disponibles sans filtrage par date (France Travail)")
    parser.add_argument('--start-date', type=str,
                        help="Date de début au format YYYYMMDD (France Travail)")
    parser.add_argument('--end-date', type=str,
                        help="Date de fin au format YYYYMMDD (France Travail)")
    
    # Options générales
    parser.add_argument('--skip-db', action='store_true',
                        help="Ne pas charger les données dans la base de données")
    parser.add_argument('--output-csv', action='store_true',
                        help="Générer un fichier CSV avec les données transformées")
    parser.add_argument('--verbose', action='store_true',
                        help="Afficher les messages de debug détaillés")
    parser.add_argument('--input-file', type=str,
                        help="Chemin vers un fichier CSV spécifique à utiliser comme source de données")
    
    return parser.parse_args()

def setup_environment():
    """
    Configure les variables d'environnement nécessaires pour le pipeline ETL.
    """
    # Définir les variables d'environnement pour AWS et la base de données
    os.environ['DB_HOST'] = 'datawarehouses.c32ygg4oyapa.eu-north-1.rds.amazonaws.com'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_NAME'] = 'datawarehouses'
    os.environ['DB_USER'] = 'admin'
    # Utiliser un mot de passe sans caractères spéciaux pour éviter les problèmes d'encodage
    os.environ['DB_PASSWORD'] = 'mwgzgsPDd7x'  # Version simplifiée du mot de passe
    os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAS2VS4EK2UIF56F5O'
    os.environ['AWS_SECRET_ACCESS_KEY'] = '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'
    os.environ['S3_BUCKET'] = 'data-lake-brut'
    
    # Vérifier que les répertoires nécessaires existent
    dirs_to_check = [
        'data/raw/france_travail', 
        'data/raw/welcome_jungle',
        'data/processed/france_travail', 
        'data/processed/welcome_jungle',
        'logs'
    ]
    
    for dir_path in dirs_to_check:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Répertoire créé: {dir_path}")
    
    logger.info("Environnement configuré avec succès")
    return True

def run_france_travail_pipeline(args):
    """
    Exécute le pipeline ETL pour France Travail.
    
    Args:
        args: Arguments de ligne de commande
    
    Returns:
        int: Code de retour (0 pour succès, 1 pour échec)
    """
    logger.info("=== Démarrage du pipeline France Travail ===")
    
    # Construire la commande pour run_etl.py
    cmd_args = [sys.executable, "scripts/run_etl.py", f"--mode={args.mode}"]
    
    if args.all_data:
        cmd_args.append("--all-data")
    else:
        if args.start_date:
            cmd_args.append(f"--start-date={args.start_date}")
        if args.end_date:
            cmd_args.append(f"--end-date={args.end_date}")
    
    if args.skip_db:
        cmd_args.append("--skip-db")
    if args.output_csv:
        cmd_args.append("--output-csv")
    if args.verbose:
        cmd_args.append("--verbose")
    if args.input_file:
        cmd_args.append(f"--input-file={args.input_file}")
    
    # Exécuter la commande
    import subprocess
    logger.info(f"Exécution de la commande: {' '.join(cmd_args)}")
    
    try:
        result = subprocess.run(cmd_args, check=True)
        logger.info("Pipeline France Travail terminé avec succès")
        return result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"Erreur lors de l'exécution du pipeline France Travail: {e}")
        return e.returncode

def run_welcome_jungle_pipeline(args):
    """
    Exécute le pipeline ETL pour Welcome to the Jungle.
    
    Args:
        args: Arguments de ligne de commande
    
    Returns:
        int: Code de retour (0 pour succès, 1 pour échec)
    """
    logger.info("=== Démarrage du pipeline Welcome to the Jungle ===")
    logger.warning("Le pipeline Welcome to the Jungle n'est pas encore implémenté")
    return 0

def main():
    """
    Fonction principale qui orchestre l'exécution du pipeline ETL.
    """
    # Configurer l'environnement
    if not setup_environment():
        logger.error("Échec de la configuration de l'environnement")
        return 1
    
    # Récupérer les arguments
    args = parse_arguments()
    
    # Configurer le niveau de log
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    start_time = datetime.now()
    
    try:
        # Exécuter le pipeline selon la source sélectionnée
        if args.source == 'france_travail' or args.source == 'all':
            result_ft = run_france_travail_pipeline(args)
            if result_ft != 0 and args.source == 'france_travail':
                return result_ft
        
        if args.source == 'welcome_jungle' or args.source == 'all':
            result_wj = run_welcome_jungle_pipeline(args)
            if result_wj != 0 and args.source == 'welcome_jungle':
                return result_wj
        
        # Fin du pipeline
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"=== Pipeline ETL complet terminé avec succès ===")
        logger.info(f"Durée totale: {duration}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Erreur lors de l'exécution du pipeline: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
