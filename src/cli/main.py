#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Interface en ligne de commande pour le projet de collecte et analyse d'offres d'emploi.
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
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """
    Parse les arguments de ligne de commande.
    """
    parser = argparse.ArgumentParser(description='Pipeline de collecte et analyse d\'offres d\'emploi')
    subparsers = parser.add_subparsers(dest='command', help='Commandes disponibles')
    
    # Commande de scraping
    scrape_parser = subparsers.add_parser('scrape', help='Scraper des offres d\'emploi')
    scrape_parser.add_argument('--source', choices=['welcome_jungle', 'pole_emploi', 'all'], default='welcome_jungle',
                        help='Source des données (welcome_jungle, pole_emploi, all)')
    scrape_parser.add_argument('--terms', type=str, help='Termes de recherche séparés par des virgules')
    scrape_parser.add_argument('--pages', type=int, default=3, help='Nombre maximum de pages à scraper par terme')
    scrape_parser.add_argument('--no-s3', action='store_true', help='Ne pas uploader vers S3')
    
    # Commande ETL
    etl_parser = subparsers.add_parser('etl', help='Exécuter le pipeline ETL')
    etl_parser.add_argument('--source', choices=['welcome_jungle', 'pole_emploi', 'all'], default='all',
                        help='Source des données (welcome_jungle, pole_emploi, all)')
    etl_parser.add_argument('--no-local', action='store_true', help='Ne pas sauvegarder en local')
    etl_parser.add_argument('--no-s3', action='store_true', help='Ne pas uploader vers S3')
    etl_parser.add_argument('--no-rds', action='store_true', help='Ne pas charger dans RDS')
    
    # Commande d'analyse
    analyze_parser = subparsers.add_parser('analyze', help='Analyser les offres d\'emploi')
    analyze_parser.add_argument('--source', choices=['rds', 's3', 'local'], default='local',
                        help='Source des données à analyser (rds, s3, local)')
    
    # Commande de déploiement Lambda
    lambda_parser = subparsers.add_parser('lambda', help='Déployer des fonctions Lambda')
    lambda_parser.add_argument('--function', choices=['welcome_jungle', 'pole_emploi', 'etl'], required=True,
                        help='Fonction à déployer')
    
    # Commande de test d'infrastructure
    test_parser = subparsers.add_parser('test', help='Tester les composants d\'infrastructure')
    test_parser.add_argument('--component', choices=['s3', 'rds', 'lambda'], required=True,
                        help='Composant à tester')
    
    return parser.parse_args()

def main():
    """
    Fonction principale de l'interface en ligne de commande.
    """
    # Créer les dossiers nécessaires
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data/raw/welcome_jungle', exist_ok=True)
    os.makedirs('data/raw/pole_emploi', exist_ok=True)
    os.makedirs('data/processed/welcome_jungle', exist_ok=True)
    os.makedirs('data/processed/pole_emploi', exist_ok=True)
    
    # Parser les arguments
    args = parse_arguments()
    
    # Si aucune commande n'est spécifiée, afficher l'aide
    if not args.command:
        logger.error("Aucune commande spécifiée. Utilisez --help pour voir les options disponibles.")
        return
    
    # Exécuter la commande demandée
    if args.command == 'scrape':
        from src.data_collection.scrapers.welcome_jungle_improved import WelcomeToTheJungleScraper
        
        logger.info(f"Scraping des offres depuis {args.source}")
        
        if args.source in ['welcome_jungle', 'all']:
            # Définir les termes de recherche
            search_terms = args.terms.split(',') if args.terms else ["data scientist", "data engineer", "data analyst"]
            
            # Créer le scraper
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
    
    elif args.command == 'etl':
        from src.etl.etl_pipeline import run_welcome_jungle_pipeline, run_pole_emploi_pipeline
        
        logger.info(f"Exécution du pipeline ETL pour {args.source}")
        
        if args.source in ['welcome_jungle', 'all']:
            run_welcome_jungle_pipeline(
                save_local=not args.no_local,
                upload_s3=not args.no_s3,
                load_rds=not args.no_rds
            )
        
        if args.source in ['pole_emploi', 'all']:
            run_pole_emploi_pipeline(
                save_local=not args.no_local,
                upload_s3=not args.no_s3,
                load_rds=not args.no_rds
            )
    
    elif args.command == 'analyze':
        from src.analysis.job_analysis import generate_report, load_jobs_from_rds, load_jobs_from_s3, load_jobs_from_local
        
        logger.info(f"Analyse des offres d'emploi depuis {args.source}")
        
        # Charger les données selon la source spécifiée
        if args.source == 'rds':
            df = load_jobs_from_rds()
        elif args.source == 's3':
            df = load_jobs_from_s3()
        else:  # local
            df = load_jobs_from_local()
        
        # Générer le rapport
        generate_report(df)
    
    elif args.command == 'lambda':
        from src.infrastructure.lambda_setup import deploy_lambda_function
        
        logger.info(f"Déploiement de la fonction Lambda {args.function}")
        
        # Déployer la fonction Lambda spécifiée
        if args.function == 'welcome_jungle':
            deploy_lambda_function(
                function_name="welcome_jungle_scraper",
                handler="src.data_collection.scrapers.welcome_jungle_improved.lambda_handler",
                description="Fonction Lambda pour le scraping de Welcome to the Jungle"
            )
        elif args.function == 'pole_emploi':
            logger.info("Déploiement de la fonction Lambda pour Pôle Emploi non implémenté")
        elif args.function == 'etl':
            deploy_lambda_function(
                function_name="etl_pipeline",
                handler="src.etl.etl_pipeline.lambda_handler",
                description="Fonction Lambda pour le pipeline ETL"
            )
    
    elif args.command == 'test':
        logger.info(f"Test du composant {args.component}")
        
        if args.component == 's3':
            from src.infrastructure.s3_setup import test_s3_connection
            test_s3_connection()
        elif args.component == 'rds':
            from src.infrastructure.rds_setup import test_rds_connection
            test_rds_connection()
        elif args.component == 'lambda':
            from src.infrastructure.lambda_setup import test_lambda_function
            test_lambda_function()

if __name__ == '__main__':
    main()
