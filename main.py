#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal pour exécuter le pipeline de collecte et d'analyse d'offres d'emploi.
"""

import os
import sys
import logging
import argparse
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/pipeline_execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(description='Pipeline de collecte et analyse d\'offres d\'emploi')
    parser.add_argument('--action', choices=['scrape', 'etl', 'analyze', 'all'], default='all',
                        help='Action à exécuter (scrape, etl, analyze, all)')
    parser.add_argument('--source', choices=['welcome_jungle', 'pole_emploi', 'all'], default='welcome_jungle',
                        help='Source des données (welcome_jungle, pole_emploi, all)')
    parser.add_argument('--terms', type=str, help='Termes de recherche séparés par des virgules')
    parser.add_argument('--pages', type=int, default=3, help='Nombre maximum de pages à scraper par terme')
    parser.add_argument('--no-s3', action='store_true', help='Ne pas utiliser S3')
    parser.add_argument('--no-rds', action='store_true', help='Ne pas utiliser RDS')
    return parser.parse_args()

def main():
    """Fonction principale du pipeline."""
    # Créer les dossiers nécessaires
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data/raw/welcome_jungle', exist_ok=True)
    os.makedirs('data/raw/pole_emploi', exist_ok=True)
    os.makedirs('data/processed/welcome_jungle', exist_ok=True)
    os.makedirs('data/processed/pole_emploi', exist_ok=True)
    os.makedirs('reports/figures', exist_ok=True)
    
    # Parser les arguments
    args = parse_arguments()
    
    # Afficher les informations de configuration
    logger.info("Démarrage du pipeline de collecte et analyse d'offres d'emploi")
    logger.info(f"Action: {args.action}")
    logger.info(f"Source: {args.source}")
    
    # Définir les termes de recherche
    search_terms = args.terms.split(',') if args.terms else ["data scientist", "data engineer", "data analyst"]
    logger.info(f"Termes de recherche: {search_terms}")
    
    # Exécuter les actions demandées
    if args.action in ['scrape', 'all']:
        logger.info("Étape 1: Collecte des données")
        if args.source in ['welcome_jungle', 'all']:
            from src.data_collection.scrapers.welcome_jungle_improved import WelcomeToTheJungleScraper
            scraper = WelcomeToTheJungleScraper()
            
            # Scraper les offres
            all_jobs = []
            for term in search_terms:
                logger.info(f"Scraping des offres pour le terme: {term}")
                jobs = scraper.scrape_jobs(term, max_pages=args.pages)
                logger.info(f"Nombre d'offres trouvées pour '{term}': {len(jobs)}")
                all_jobs.extend(jobs)
            
            logger.info(f"Nombre total d'offres trouvées: {len(all_jobs)}")
            
            # Récupérer les détails de chaque offre
            job_details_list = []
            for i, job in enumerate(all_jobs):
                logger.info(f"Progression: {i+1}/{len(all_jobs)} offres récupérées")
                job_details = scraper.scrape_job_details(job['url'])
                job_details_list.append(job_details)
            
            # Sauvegarder les résultats
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"data/raw/welcome_jungle/welcome_jungle_{timestamp}.json"
            scraper.save_jobs_to_json(job_details_list, output_file)
            
            # Uploader vers S3 si demandé
            if not args.no_s3:
                logger.info("Upload vers S3...")
                scraper.upload_to_s3(output_file)
        
        if args.source in ['pole_emploi', 'all']:
            logger.info("Collecte des données depuis Pôle Emploi non implémentée")
    
    if args.action in ['etl', 'all']:
        logger.info("Étape 2: Traitement des données (ETL)")
        if args.source in ['welcome_jungle', 'all']:
            from src.etl.extraction import extract_welcome_jungle_data
            from src.etl.transformation import transform_to_dataframe
            from src.etl.loading import save_to_local, load_to_s3, load_jobs_to_rds
            
            # Extraire les données
            jobs_data = extract_welcome_jungle_data()
            
            # Transformer les données
            jobs_df = transform_to_dataframe(jobs_data)
            
            # Sauvegarder en local
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            processed_file = f"data/processed/welcome_jungle/processed_jobs_{timestamp}.json"
            save_to_local(jobs_df, processed_file)
            
            # Charger vers S3 si demandé
            if not args.no_s3:
                bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
                s3_path = f"processed/welcome_jungle/processed_jobs_{timestamp}.json"
                load_to_s3(jobs_df, bucket_name, s3_path)
            
            # Charger dans RDS si demandé
            if not args.no_rds:
                try:
                    load_jobs_to_rds(jobs_df)
                except Exception as e:
                    logger.error(f"Erreur lors du chargement dans RDS: {e}")
        
        if args.source in ['pole_emploi', 'all']:
            logger.info("Traitement des données de Pôle Emploi non implémenté")
    
    if args.action in ['analyze', 'all']:
        logger.info("Étape 3: Analyse des données")
        from src.analysis.job_analysis import generate_report
        
        # Générer le rapport d'analyse
        generate_report()
    
    logger.info("Pipeline terminé avec succès !")

if __name__ == "__main__":
    main()
