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
import glob

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuration du logging
os.makedirs("logs", exist_ok=True)
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
    # Utiliser un mot de passe sans caractères spéciaux pour éviter les problèmes d'encodage
    os.environ['DB_PASSWORD'] = 'mwgzgsPDd7x'  # Version simplifiée du mot de passe
    os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAS2VS4EK2UIF56F5O'
    os.environ['AWS_SECRET_ACCESS_KEY'] = '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'
    os.environ['S3_BUCKET'] = 'data-lake-brut'
    
    # Vérifier que les répertoires nécessaires existent
    dirs_to_check = ['data/raw/france_travail', 'data/intermediate', 'data/processed/pole_emploi', 'logs']
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
    parser.add_argument('--all-data', action='store_true',
                        help="Extraire toutes les données disponibles sans filtrage par date")
    
    return parser.parse_args()

def check_data_already_loaded():
    """
    Vérifie si les données sont déjà chargées dans la base de données.
    
    Returns:
        tuple: (bool, int) - (True si les données sont déjà chargées, nombre d'enregistrements)
    """
    try:
        from src.etl.api.loading import get_db_connection
        from sqlalchemy import text
        
        # Connexion à la base de données
        engine = get_db_connection()
        if engine is None:
            logger.warning("Impossible de vérifier si les données sont déjà chargées: échec de connexion à la base")
            return False, 0
        
        # Vérifier si la table existe et contient des données
        with engine.connect() as connection:
            # Vérifier si la table existe
            check_table_query = text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'france_travail_jobs')")
            table_exists = connection.execute(check_table_query).fetchone()[0]
            
            if not table_exists:
                return False, 0
            
            # Compter le nombre d'enregistrements
            count_query = text("SELECT COUNT(*) FROM france_travail_jobs")
            record_count = connection.execute(count_query).fetchone()[0]
            
            # Si la table contient des données, considérer que l'ETL a déjà été exécuté
            if record_count > 0:
                return True, record_count
            
        return False, 0
    except Exception as e:
        logger.warning(f"Erreur lors de la vérification des données déjà chargées: {e}")
        return False, 0

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
    
    # Afficher le mode d'exécution
    logger.info(f"=== Démarrage du pipeline ETL en mode '{args.mode}' ===")
    if not args.all_data:
        logger.info(f"Période: {args.start_date} à {args.end_date}")
    else:
        logger.info("Extraction de toutes les données disponibles")
    
    start_time = datetime.now()
    
    try:
        # Extraction
        if args.mode == 'full' or args.mode == 'extract':
            from src.etl.api.extraction import extract_data
            
            logger.info("Étape 1: EXTRACTION - Récupération des données brutes")
            
            # Déterminer si on utilise un fichier d'entrée spécifique ou si on fait une extraction
            if args.input_file:
                logger.info(f"Utilisation du fichier d'entrée spécifié: {args.input_file}")
                import pandas as pd
                raw_df = pd.read_csv(args.input_file, low_memory=False)
            else:
                # Toujours extraire toutes les données disponibles sans filtrage par date
                logger.info("Extraction de toutes les données disponibles sans filtrage par date")
                raw_df = extract_data(output_format="dataframe", all_data=True)
                
                if raw_df is None or raw_df.empty:
                    logger.error("Aucune donnée extraite.")
                    return 1
                
                logger.info(f"Extraction terminée: {len(raw_df)} offres d'emploi extraites")
        
        # Transformation
        if args.mode == 'full' or args.mode == 'transform':
            from src.etl.api.transformation import transform_job_dataframe, apply_keyword_analysis
            
            # Si on ne fait que la transformation, charger les données intermédiaires
            if args.mode == 'transform' and not args.input_file:
                import pandas as pd
                import glob
                
                # Trouver le fichier le plus récent
                if args.input_file:
                    intermediate_file = args.input_file
                else:
                    intermediate_files = glob.glob(f"data/intermediate/france_travail_data_*.csv")
                    if not intermediate_files:
                        logger.error("Aucun fichier intermédiaire trouvé pour la transformation")
                        return 1
                        
                    intermediate_file = max(intermediate_files, key=os.path.getctime)
                
                logger.info(f"Chargement des données intermédiaires depuis {intermediate_file}")
                raw_df = pd.read_csv(intermediate_file, low_memory=False)
            
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
