#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script d'analyse de la structure HTML d'une page d'offre d'emploi Welcome to the Jungle.
Permet d'identifier les sélecteurs CSS pertinents pour le scraper.
"""

import os
import sys
import argparse
import requests
from bs4 import BeautifulSoup
import logging
from urllib.parse import urlparse

# Ajouter le répertoire parent au chemin Python pour importer les modules du projet
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(description='Analyse de la structure HTML d\'une page d\'offre Welcome to the Jungle')
    parser.add_argument('--url', type=str, required=True, help='URL de l\'offre à analyser')
    return parser.parse_args()

def analyze_page(url):
    """Analyse la structure HTML d'une page d'offre."""
    logger.info(f"Analyse de la page: {url}")
    
    # Récupérer le contenu de la page
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        logger.error(f"Erreur lors de la récupération de la page: {response.status_code}")
        return
    
    # Parser le HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Analyser l'URL pour extraire des informations
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.strip('/').split('/')
    
    # Extraire des informations de l'URL
    logger.info("Informations extraites de l'URL:")
    if len(path_parts) >= 3:
        logger.info(f"  - Entreprise (depuis URL): {path_parts[-3]}")
        logger.info(f"  - Titre (depuis URL): {path_parts[-1]}")
        logger.info(f"  - Localisation (depuis URL): {path_parts[-2]}")
    
    # Rechercher le titre
    logger.info("\nRecherche du titre:")
    title_selectors = [
        'h1.ais-Highlight',
        'h1.sc-bqWxrE',
        'h1.sc-fmSAUk',
        'h1',
        'meta[property="og:title"]',
        'title'
    ]
    
    for selector in title_selectors:
        elements = soup.select(selector)
        if elements:
            for i, el in enumerate(elements):
                content = el.get('content') if el.name == 'meta' else el.text.strip()
                logger.info(f"  - {selector} [{i}]: {content}")
    
    # Rechercher l'entreprise
    logger.info("\nRecherche de l'entreprise:")
    company_selectors = [
        'div.sc-bXCLTC',
        'div.sc-hmdomO',
        'div.sc-gFGZVQ',
        'a[data-testid="company-link"]',
        'meta[property="og:site_name"]'
    ]
    
    for selector in company_selectors:
        elements = soup.select(selector)
        if elements:
            for i, el in enumerate(elements):
                content = el.get('content') if el.name == 'meta' else el.text.strip()
                logger.info(f"  - {selector} [{i}]: {content}")
    
    # Rechercher la localisation
    logger.info("\nRecherche de la localisation:")
    location_selectors = [
        'div.sc-gFGZVQ',
        'div.sc-hmdomO',
        'div[data-testid="job-location"]',
        'span.sc-jIZahH'
    ]
    
    for selector in location_selectors:
        elements = soup.select(selector)
        if elements:
            for i, el in enumerate(elements):
                logger.info(f"  - {selector} [{i}]: {el.text.strip()}")
    
    # Rechercher le type de contrat
    logger.info("\nRecherche du type de contrat:")
    contract_selectors = [
        'div.sc-gFGZVQ',
        'div.sc-hmdomO',
        'div[data-testid="job-contract"]',
        'span.sc-jIZahH'
    ]
    
    for selector in contract_selectors:
        elements = soup.select(selector)
        if elements:
            for i, el in enumerate(elements):
                logger.info(f"  - {selector} [{i}]: {el.text.strip()}")
    
    # Rechercher la description
    logger.info("\nRecherche de la description:")
    description_selectors = [
        'div.sc-kFuwaP',
        'div.sc-iXzfSG',
        'div[data-testid="job-description"]',
        'main',
        'article'
    ]
    
    for selector in description_selectors:
        elements = soup.select(selector)
        if elements:
            for i, el in enumerate(elements):
                text = el.text.strip()
                preview = text[:100] + "..." if len(text) > 100 else text
                logger.info(f"  - {selector} [{i}]: {preview}")
    
    # Sauvegarder l'analyse dans un fichier HTML
    with open('job_page_analysis.html', 'w', encoding='utf-8') as f:
        f.write(f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Analyse de la page {url}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #333; }}
                h2 {{ color: #666; }}
                pre {{ background-color: #f5f5f5; padding: 10px; border-radius: 5px; overflow-x: auto; }}
                .section {{ margin-bottom: 20px; }}
            </style>
        </head>
        <body>
            <h1>Analyse de la page {url}</h1>
            
            <div class="section">
                <h2>Structure HTML</h2>
                <pre>{soup.prettify()}</pre>
            </div>
        </body>
        </html>
        """)
    
    logger.info(f"\nAnalyse complète sauvegardée dans job_page_analysis.html")

def main():
    """Fonction principale du script."""
    args = parse_arguments()
    analyze_page(args.url)

if __name__ == "__main__":
    main()
