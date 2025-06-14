#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de lancement du pipeline ETL complet pour les offres d'emploi.
Ce script simplifie l'exécution du pipeline ETL en configurant l'environnement
et en lançant le processus d'extraction, transformation et chargement.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
import argparse

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/run_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_environment():
    """
    Configure les variables d'environnement nécessaires pour le pipeline ETL.
    """
    # Définir les variables d'environnement pour AWS et la base de données
    os.environ['DB_HOST'] = 'datawarehouses.c32ygg4oyapa.eu-north-1.rds.amazonaws.com'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_NAME'] = 'datawarehouses'
    os.environ['DB_USER'] = 'admin'
    os.environ['DB_PASSWORD'] = 'm!wgz#$gsPD}d7x'
    os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAS2VS4EK2UIF56F5O'
    os.environ['AWS_SECRET_ACCESS_KEY'] = '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'
    os.environ['S3_BUCKET'] = 'data-lake-brut'
    
    # Vérifier que les répertoires nécessaires existent
    dirs_to_check = ['data/raw/france_travail', 'data/intermediate', 'data/processed', 'logs']
    for dir_path in dirs_to_check:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Répertoire créé: {dir_path}")
    
    logger.info("Environnement configuré avec succès")
    return True

def parse_arguments():
    """
    Parse les arguments de la ligne de commande.
    """
    parser = argparse.ArgumentParser(description="Pipeline ETL pour les offres d'emploi")
    
    # Options générales
    parser.add_argument('--mode', choices=['full', 'extract', 'transform', 'load'], 
                        default='full', help="Mode d'exécution (défaut: full)")
    
    # Options de date
    today = datetime.now().strftime("%Y%m%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    
    parser.add_argument('--start-date', type=str, default=yesterday,
                        help=f"Date de début au format YYYYMMDD (défaut: {yesterday})")
    parser.add_argument('--end-date', type=str, default=today,
                        help=f"Date de fin au format YYYYMMDD (défaut: {today})")
    
    # Options avancées
    parser.add_argument('--skip-db', action='store_true',
                        help="Ne pas charger les données dans la base de données")
    parser.add_argument('--output-csv', action='store_true',
                        help="Générer un fichier CSV avec les données transformées")
    parser.add_argument('--verbose', action='store_true',
                        help="Afficher les messages de debug détaillés")
    parser.add_argument('--input-file', type=str,
                        help="Chemin vers un fichier CSV spécifique à utiliser comme source de données")
    
    return parser.parse_args()

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
    
    # Ajouter le chemin du répertoire src pour les imports
    sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))
    
    # Afficher le mode d'exécution
    logger.info(f"=== Démarrage du pipeline ETL en mode '{args.mode}' ===")
    logger.info(f"Période: {args.start_date} à {args.end_date}")
    
    start_time = datetime.now()
    
    try:
        if args.mode == 'full' or args.mode == 'extract':
            # Extraction
            from src.etl.api.extraction import extract_by_date_range
            logger.info("Étape 1: EXTRACTION - Récupération des offres d'emploi")
            raw_df = extract_by_date_range(args.start_date, args.end_date)
            
            if raw_df is None or len(raw_df) == 0:
                logger.warning("Aucune donnée extraite pour la période spécifiée")
                return 0
                
            logger.info(f"Extraction terminée: {len(raw_df)} offres d'emploi récupérées")
            
            # Sauvegarder les données brutes en CSV
            if args.output_csv:
                raw_csv_path = f"data/raw/france_travail/france_travail_raw_{args.start_date}_{args.end_date}.csv"
                raw_df.to_csv(raw_csv_path, index=False, encoding='utf-8')
                logger.info(f"Données brutes sauvegardées dans {raw_csv_path}")
        
        if args.mode == 'full' or args.mode == 'transform':
            # Transformation
            from src.etl.api.transformation import transform_job_dataframe, apply_keyword_analysis
            
            # Si on ne fait que la transformation, charger les données brutes
            if args.mode == 'transform':
                import pandas as pd
                import glob
                
                # Si un fichier d'entrée spécifique est fourni, l'utiliser
                if args.input_file and os.path.exists(args.input_file):
                    logger.info(f"Utilisation du fichier spécifié: {args.input_file}")
                    raw_df = pd.read_csv(args.input_file, encoding='utf-8')
                else:
                    # Chercher d'abord dans le dossier intermediate
                    intermediate_files = glob.glob(f"data/intermediate/france_travail_data_*.csv")
                    if intermediate_files:
                        latest_file = max(intermediate_files, key=os.path.getctime)
                        logger.info(f"Chargement des données intermédiaires depuis {latest_file}")
                        raw_df = pd.read_csv(latest_file, encoding='utf-8')
                    else:
                        # Sinon chercher dans le dossier raw
                        raw_files = glob.glob(f"data/raw/france_travail/france_travail_raw_*.csv")
                        if not raw_files:
                            logger.error("Aucun fichier de données brutes trouvé pour la transformation")
                            return 1
                        
                        latest_file = max(raw_files, key=os.path.getctime)
                        logger.info(f"Chargement des données brutes depuis {latest_file}")
                        raw_df = pd.read_csv(latest_file, encoding='utf-8')
            
            logger.info("Étape 2: TRANSFORMATION - Nettoyage et enrichissement des données")
            transformed_df = transform_job_dataframe(raw_df)
            
            if transformed_df is not None:
                # Ajouter l'analyse des mots-clés
                logger.info("Application de l'analyse par mots-clés")
                transformed_df = apply_keyword_analysis(transformed_df)
                logger.info(f"Transformation terminée: {len(transformed_df)} offres d'emploi transformées")
            else:
                logger.error("Échec de la transformation des données")
                return 1
            
            # Sauvegarder les données transformées en CSV
            if args.output_csv or args.mode == 'transform':
                # Créer le dossier pole_emploi s'il n'existe pas
                os.makedirs("data/processed/pole_emploi", exist_ok=True)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                transformed_csv_path = f"data/processed/pole_emploi/france_travail_processed_{timestamp}.csv"
                transformed_df.to_csv(transformed_csv_path, index=False, encoding='utf-8')
                logger.info(f"Données transformées sauvegardées dans {transformed_csv_path}")
        
        if (args.mode == 'full' or args.mode == 'load') and not args.skip_db:
            # Chargement
            from src.etl.api.loading import prepare_job_data_for_loading, get_db_connection
            from src.etl.api.loading import create_jobs_table, load_jobs_to_database
            
            # Si on ne fait que le chargement, charger les données transformées
            if args.mode == 'load':
                import pandas as pd
                import glob
                
                # Trouver le fichier le plus récent dans le dossier pole_emploi
                processed_files = glob.glob(f"data/processed/pole_emploi/france_travail_processed_*.csv")
                if not processed_files:
                    # Si aucun fichier n'est trouvé dans pole_emploi, chercher dans le dossier processed
                    processed_files = glob.glob(f"data/processed/france_travail_processed_*.csv")
                    if not processed_files:
                        logger.error("Aucun fichier de données transformées trouvé pour le chargement")
                        return 1
                    
                latest_file = max(processed_files, key=os.path.getctime)
                logger.info(f"Chargement des données transformées depuis {latest_file}")
                transformed_df = pd.read_csv(latest_file, encoding='utf-8')
            
            logger.info("Étape 3: CHARGEMENT - Préparation des données pour la base de données")
            load_ready_df = prepare_job_data_for_loading(transformed_df)
            
            if load_ready_df is None:
                logger.error("Échec de la préparation des données pour le chargement")
                return 1
            
            logger.info("Connexion à la base de données RDS...")
            engine = get_db_connection()
            
            if engine is None:
                logger.error("Impossible de se connecter à la base de données")
                logger.warning("Veuillez vérifier que l'instance RDS est accessible depuis votre réseau")
                logger.warning("Les données transformées ont été sauvegardées en CSV si --output-csv a été utilisé")
                return 1
            
            # Créer la table si nécessaire
            if create_jobs_table(engine):
                # Charger les données
                records_loaded = load_jobs_to_database(load_ready_df, engine)
                logger.info(f"Chargement terminé: {records_loaded} offres d'emploi insérées dans la base de données")
            else:
                logger.error("Échec de la création de la table dans la base de données")
                return 1
        elif args.skip_db:
            logger.info("Chargement dans la base de données ignoré (--skip-db)")
        
        # Fin du pipeline
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"=== Pipeline ETL terminé avec succès ===")
        logger.info(f"Durée totale: {duration}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Erreur lors de l'exécution du pipeline: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
