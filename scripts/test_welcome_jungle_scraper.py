#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de test pour le scraper Welcome to the Jungle amélioré.
Permet de tester le scraper avec des termes de recherche personnalisés
ou une URL d'offre spécifique.
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime
from collections import Counter

# Ajouter le répertoire parent au chemin Python pour importer les modules du projet
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importer le scraper
from src.data_collection.scrapers.welcome_jungle_improved import WelcomeToTheJungleScraper

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(description='Test du scraper Welcome to the Jungle')
    parser.add_argument('--terms', type=str, help='Termes de recherche séparés par des virgules')
    parser.add_argument('--pages', type=int, default=1, help='Nombre maximum de pages à scraper par terme')
    parser.add_argument('--url', type=str, help='URL spécifique d\'une offre à scraper')
    parser.add_argument('--upload-s3', action='store_true', help='Uploader les résultats vers S3')
    return parser.parse_args()

def main():
    """Fonction principale du script de test."""
    args = parse_arguments()
    
    # Créer le scraper
    scraper = WelcomeToTheJungleScraper()
    
    # Définir les termes de recherche par défaut si non spécifiés
    search_terms = args.terms.split(',') if args.terms else ["data scientist", "data engineer", "data analyst"]
    max_pages = args.pages
    
    # Créer le dossier de sauvegarde s'il n'existe pas
    os.makedirs('raw/welcome_jungle', exist_ok=True)
    
    # Timestamp pour le nom de fichier
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Si une URL spécifique est fournie, scraper uniquement cette offre
    if args.url:
        logger.info(f"Scraping de l'URL spécifique: {args.url}")
        job_details = scraper.scrape_job_details(args.url)
        
        # Sauvegarder les détails de l'offre
        output_file = f"raw/welcome_jungle/specific_job_{timestamp}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(job_details, f, ensure_ascii=False, indent=4)
        
        logger.info(f"Détails de l'offre sauvegardés dans {output_file}")
        
        # Afficher les détails de l'offre
        for key, value in job_details.items():
            if key != 'description':
                logger.info(f"{key}: {value}")
            else:
                logger.info(f"description: {value[:100]}...")
        
        # Uploader vers S3 si demandé
        if args.upload_s3:
            logger.info("Upload vers S3...")
            scraper.upload_to_s3(output_file)
            logger.info("Upload terminé.")
        
        return
    
    # Scraper les offres d'emploi
    all_jobs = []
    for term in search_terms:
        jobs = scraper.scrape_jobs(term, max_pages=max_pages)
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
    output_file = f"raw/welcome_jungle/test_welcome_jungle_{timestamp}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(job_details_list, f, ensure_ascii=False, indent=4)
    
    logger.info(f"Résultats sauvegardés dans {output_file}")
    
    # Statistiques sur les champs remplis
    stats = {
        'title': 0,
        'company': 0,
        'location': 0,
        'contract_type': 0,
        'description': 0
    }
    
    for job in job_details_list:
        for field in stats:
            if job.get(field):
                stats[field] += 1
    
    logger.info("Statistiques de remplissage des champs:")
    for field, count in stats.items():
        percentage = (count / len(job_details_list)) * 100 if job_details_list else 0
        logger.info(f"  - {field}: {count}/{len(job_details_list)} ({percentage:.2f}%)")
    
    # Uploader vers S3 si demandé
    if args.upload_s3:
        logger.info("Upload vers S3...")
        scraper.upload_to_s3(output_file)
        logger.info("Upload terminé.")

if __name__ == "__main__":
    main()
