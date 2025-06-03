#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Scraper amélioré pour Welcome to the Jungle.
Permet de récupérer les offres d'emploi et leurs détails.
"""

import os
import re
import json
import time
import logging
import requests
import boto3
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

class WelcomeToTheJungleScraper:
    """Scraper pour le site Welcome to the Jungle."""
    
    BASE_URL = "https://www.welcometothejungle.com"
    SEARCH_URL = f"{BASE_URL}/fr/jobs"
    
    def __init__(self):
        """Initialisation du scraper."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def _get_page(self, url):
        """Récupère le contenu d'une page."""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Erreur lors de la récupération de la page {url}: {e}")
            return None
    
    def scrape_jobs(self, search_term, max_pages=3):
        """
        Scrape les offres d'emploi pour un terme de recherche donné.
        
        Args:
            search_term (str): Terme de recherche
            max_pages (int): Nombre maximum de pages à scraper
            
        Returns:
            list: Liste des offres d'emploi trouvées
        """
        jobs = []
        
        for page in range(1, max_pages + 1):
            logger.info(f"Scraping de la page {page}/{max_pages} pour le terme '{search_term}'...")
            
            # Construire l'URL de recherche
            search_url = f"{self.SEARCH_URL}?query={search_term.replace(' ', '%20')}&page={page}"
            
            # Récupérer le contenu de la page
            html_content = self._get_page(search_url)
            if not html_content:
                logger.warning(f"Impossible de récupérer la page {page} pour '{search_term}'")
                break
            
            # Parser le HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Trouver les cartes d'offres d'emploi
            job_cards = soup.select('div[data-testid="job-card"]')
            
            if not job_cards:
                # Essayer un autre sélecteur si le premier ne fonctionne pas
                job_cards = soup.select('div.sc-bXCLTC')
            
            if not job_cards:
                # Essayer encore un autre sélecteur
                job_cards = soup.select('a[data-testid="job-link"]')
            
            if not job_cards:
                logger.warning(f"Aucune offre trouvée sur la page {page} pour '{search_term}'")
                break
            
            logger.info(f"Nombre d'offres trouvées sur la page {page}: {len(job_cards)}")
            
            # Extraire les informations de chaque carte
            for card in job_cards:
                # Trouver le lien vers l'offre
                job_link = card.select_one('a[href^="/fr/companies"]')
                
                if not job_link:
                    # Essayer un autre sélecteur
                    job_link = card.select_one('a[data-testid="job-link"]')
                
                if not job_link:
                    # Si c'est déjà un lien
                    job_link = card if card.name == 'a' else None
                
                if not job_link:
                    continue
                
                # Récupérer l'URL de l'offre
                job_url = job_link.get('href')
                if job_url and not job_url.startswith('http'):
                    job_url = f"{self.BASE_URL}{job_url}"
                
                # Extraire le titre de l'offre
                job_title = None
                title_element = card.select_one('h3') or card.select_one('h4') or card.select_one('span.sc-jIZahH')
                if title_element:
                    job_title = title_element.text.strip()
                
                # Extraire l'entreprise
                company = None
                company_element = card.select_one('div.sc-gFGZVQ') or card.select_one('div.sc-hmdomO')
                if company_element:
                    company = company_element.text.strip()
                
                # Ajouter l'offre à la liste
                if job_url:
                    jobs.append({
                        'title': job_title,
                        'company': company,
                        'url': job_url
                    })
            
            # Pause pour éviter d'être bloqué
            if page < max_pages:
                time.sleep(2)
        
        return jobs
    
    def scrape_job_details(self, job_url):
        """
        Scrape les détails d'une offre d'emploi.
        
        Args:
            job_url (str): URL de l'offre d'emploi
            
        Returns:
            dict: Détails de l'offre d'emploi
        """
        logger.info(f"Scraping des détails de l'offre: {job_url}")
        
        # Récupérer le contenu de la page
        html_content = self._get_page(job_url)
        if not html_content:
            logger.warning(f"Impossible de récupérer les détails de l'offre: {job_url}")
            return {}
        
        # Parser le HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Initialiser les détails de l'offre
        job_details = {
            'url': job_url,
            'scraped_at': datetime.now().isoformat()
        }
        
        # Extraire le titre de l'offre avec plusieurs méthodes
        title = None
        
        # Méthode 1: Rechercher dans le HTML avec différents sélecteurs
        title_selectors = [
            'h1.ais-Highlight',
            'h1.sc-bqWxrE',
            'h1.sc-fmSAUk',
            'h1',
            'div.sc-kFuwaP h1',
            'div.sc-iXzfSG h1'
        ]
        
        for selector in title_selectors:
            title_element = soup.select_one(selector)
            if title_element and title_element.text.strip() and 'offre d\'emploi' not in title_element.text.lower():
                title = title_element.text.strip()
                break
        
        # Méthode 2: Extraire depuis l'URL si le HTML ne fournit pas un titre valide
        if not title or title.lower() == 'offre d\'emploi':
            parsed_url = urlparse(job_url)
            path_parts = parsed_url.path.strip('/').split('/')
            if len(path_parts) >= 1:
                # Le dernier segment de l'URL contient souvent le titre
                url_title = path_parts[-1].replace('-', ' ').title()
                # Vérifier si c'est un titre valide (pas un identifiant)
                if len(url_title) > 5 and not url_title.isdigit() and not re.match(r'^[a-f0-9]{8,}$', url_title):
                    title = url_title
        
        # Méthode 3: Rechercher dans les balises meta
        if not title or title.lower() == 'offre d\'emploi':
            meta_title = soup.select_one('meta[property="og:title"]')
            if meta_title and meta_title.get('content') and 'offre d\'emploi' not in meta_title.get('content').lower():
                title = meta_title.get('content')
        
        # Méthode 4: Utiliser le titre de la page
        if not title or title.lower() == 'offre d\'emploi':
            page_title = soup.select_one('title')
            if page_title and page_title.text.strip() and 'offre d\'emploi' not in page_title.text.lower():
                title = page_title.text.strip()
                # Nettoyer le titre (enlever le nom du site)
                title = title.split('|')[0].strip() if '|' in title else title
        
        # Méthode 5: Générer un titre basé sur des mots-clés et l'entreprise
        if not title or title.lower() == 'offre d\'emploi':
            # Essayer de trouver des mots-clés de métier dans l'URL ou le contenu
            job_keywords = ['data scientist', 'data engineer', 'développeur', 'ingénieur', 'analyste', 
                           'chef de projet', 'manager', 'consultant', 'technicien', 'responsable']
            
            # Rechercher dans l'URL
            url_lower = job_url.lower()
            found_keywords = [kw for kw in job_keywords if kw in url_lower]
            
            # Rechercher dans le contenu de la page
            if not found_keywords:
                page_text = soup.get_text().lower()
                found_keywords = [kw for kw in job_keywords if kw in page_text]
            
            # Construire un titre avec le mot-clé et l'entreprise si possible
            if found_keywords:
                company_name = self._extract_company_name(job_url, soup)
                if company_name:
                    title = f"{found_keywords[0].title()} - {company_name}"
                else:
                    title = found_keywords[0].title()
        
        # Si toutes les méthodes échouent, utiliser un titre générique
        if not title:
            title = "Offre d'emploi"
        
        job_details['title'] = title
        
        # Extraire le nom de l'entreprise
        company = self._extract_company_name(job_url, soup)
        job_details['company'] = company
        
        # Extraire la localisation
        location = self._extract_location(job_url, soup)
        job_details['location'] = location
        
        # Extraire le type de contrat
        contract_type = self._extract_contract_type(job_url, soup, title)
        job_details['contract_type'] = contract_type
        
        # Extraire la description
        description = self._extract_description(soup)
        job_details['description'] = description
        
        return job_details
    
    def _extract_company_name(self, job_url, soup):
        """Extrait le nom de l'entreprise depuis l'URL ou le HTML."""
        # Méthode 1: Extraire depuis l'URL
        parsed_url = urlparse(job_url)
        path_parts = parsed_url.path.strip('/').split('/')
        if len(path_parts) >= 3:
            company_from_url = path_parts[-3].replace('-', ' ').title()
            # Vérifier si c'est un nom d'entreprise valide (pas un identifiant)
            if len(company_from_url) > 2 and not company_from_url.isdigit() and not re.match(r'^[a-f0-9]{8,}$', company_from_url):
                return company_from_url
        
        # Méthode 2: Rechercher dans le HTML
        company_selectors = [
            'div.sc-bXCLTC',
            'div.sc-hmdomO',
            'div.sc-gFGZVQ',
            'a[data-testid="company-link"]',
            'meta[property="og:site_name"]'
        ]
        
        for selector in company_selectors:
            company_element = soup.select_one(selector)
            if company_element:
                if company_element.name == 'meta':
                    company = company_element.get('content')
                else:
                    company = company_element.text.strip()
                
                if company and company != "Welcome to the Jungle":
                    return company
        
        return "Entreprise non spécifiée"
    
    def _extract_location(self, job_url, soup):
        """Extrait la localisation depuis l'URL ou le HTML."""
        # Méthode 1: Extraire depuis l'URL
        parsed_url = urlparse(job_url)
        path_parts = parsed_url.path.strip('/').split('/')
        if len(path_parts) >= 2:
            location_from_url = path_parts[-2].replace('-', ' ').title()
            # Vérifier si c'est une localisation valide (pas un identifiant)
            if len(location_from_url) > 2 and not location_from_url.isdigit() and not re.match(r'^[a-f0-9]{8,}$', location_from_url):
                return location_from_url
        
        # Méthode 2: Rechercher des villes communes dans l'URL
        common_cities = ['paris', 'lyon', 'marseille', 'bordeaux', 'lille', 'toulouse', 'nantes', 'strasbourg', 'montpellier', 'nice']
        url_lower = job_url.lower()
        for city in common_cities:
            if city in url_lower:
                return city.title()
        
        # Méthode 3: Rechercher dans le HTML
        location_selectors = [
            'div.sc-gFGZVQ',
            'div.sc-hmdomO',
            'div[data-testid="job-location"]',
            'span.sc-jIZahH'
        ]
        
        for selector in location_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.text.strip()
                # Vérifier si le texte ressemble à une localisation
                if any(city in text.lower() for city in common_cities) or re.search(r'\b\d{5}\b', text):  # Code postal français
                    return text
        
        return "Localisation non spécifiée"
    
    def _extract_contract_type(self, job_url, soup, title):
        """Extrait le type de contrat depuis le titre, l'URL ou le HTML."""
        contract_types = {
            'cdi': 'CDI',
            'cdd': 'CDD',
            'stage': 'Stage',
            'alternance': 'Alternance',
            'freelance': 'Freelance',
            'interim': 'Intérim',
            'temps partiel': 'Temps partiel',
            'temps plein': 'Temps plein'
        }
        
        # Méthode 1: Rechercher dans le titre et l'URL
        text_to_search = (title + ' ' + job_url).lower()
        for key, value in contract_types.items():
            if key in text_to_search:
                return value
        
        # Méthode 2: Rechercher dans le HTML
        contract_selectors = [
            'div.sc-gFGZVQ',
            'div.sc-hmdomO',
            'div[data-testid="job-contract"]',
            'span.sc-jIZahH'
        ]
        
        for selector in contract_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.text.strip().lower()
                for key, value in contract_types.items():
                    if key in text:
                        return value
        
        # Par défaut, supposer que c'est un CDI (le plus courant)
        return "CDI"
    
    def _extract_description(self, soup):
        """Extrait la description de l'offre depuis le HTML."""
        # Essayer plusieurs sélecteurs pour trouver la description
        description_selectors = [
            'div.sc-kFuwaP',
            'div.sc-iXzfSG',
            'div[data-testid="job-description"]',
            'section[data-testid="job-section"]'
        ]
        
        for selector in description_selectors:
            description_element = soup.select_one(selector)
            if description_element:
                return description_element.get_text(separator='\n').strip()
        
        # Si aucun sélecteur spécifique ne fonctionne, essayer de récupérer le contenu principal
        main_element = soup.select_one('main')
        if main_element:
            return main_element.get_text(separator='\n').strip()
        
        # En dernier recours, récupérer tout le texte de la page
        return soup.get_text(separator='\n').strip()
    
    def save_to_json(self, data, filename=None):
        """
        Sauvegarde les données dans un fichier JSON.
        
        Args:
            data: Données à sauvegarder
            filename (str, optional): Nom du fichier. Si None, un nom par défaut est généré.
            
        Returns:
            str: Chemin du fichier sauvegardé
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"raw/welcome_jungle/welcome_jungle_{timestamp}.json"
        
        # Créer le dossier s'il n'existe pas
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        logger.info(f"Données sauvegardées dans {filename}")
        return filename
    
    def upload_to_s3(self, file_path):
        """
        Upload un fichier vers S3.
        
        Args:
            file_path (str): Chemin du fichier à uploader
            
        Returns:
            bool: True si l'upload a réussi, False sinon
        """
        try:
            # Charger explicitement les identifiants AWS depuis les variables d'environnement
            aws_access_key = os.getenv('KEY_ACCESS')
            aws_secret_key = os.getenv('KEY_SECRET')
            bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
            
            if not aws_access_key or not aws_secret_key:
                logger.error("Identifiants AWS manquants. Vérifiez les variables d'environnement KEY_ACCESS et KEY_SECRET.")
                return False
            
            # Créer le client S3 avec les identifiants explicites
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
            
            # Définir le chemin dans S3
            s3_path = f"raw/welcome_jungle/{os.path.basename(file_path)}"
            
            logger.info(f"Upload du fichier {file_path} vers s3://{bucket_name}/{s3_path}")
            
            # Uploader le fichier
            s3_client.upload_file(file_path, bucket_name, s3_path)
            
            logger.info(f"Upload réussi vers s3://{bucket_name}/{s3_path}")
            return True
        
        except Exception as e:
            logger.error(f"Erreur lors de l'upload vers S3: {e}")
            return False

def lambda_handler(event, context):
    """
    Handler pour AWS Lambda.
    
    Args:
        event: Événement Lambda
        context: Contexte Lambda
        
    Returns:
        dict: Résultat de l'exécution
    """
    try:
        # Initialiser le scraper
        scraper = WelcomeToTheJungleScraper()
        
        # Récupérer les paramètres de l'événement
        search_terms = event.get('search_terms', ["data scientist", "data engineer", "data analyst"])
        max_pages = event.get('max_pages', 3)
        
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
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/tmp/welcome_jungle_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(job_details_list, f, ensure_ascii=False, indent=4)
        
        # Uploader vers S3
        s3_path = f"raw/welcome_jungle/welcome_jungle_{timestamp}.json"
        bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
        
        # Charger explicitement les identifiants AWS depuis les variables d'environnement
        aws_access_key = os.getenv('KEY_ACCESS')
        aws_secret_key = os.getenv('KEY_SECRET')
        
        # Créer le client S3 avec les identifiants explicites
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        
        # Uploader le fichier
        s3_client.upload_file(filename, bucket_name, s3_path)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Scraping terminé avec succès',
                'jobs_count': len(job_details_list),
                's3_path': f"s3://{bucket_name}/{s3_path}"
            })
        }
    
    except Exception as e:
        logger.error(f"Erreur dans le handler Lambda: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f"Erreur: {str(e)}"
            })
        }

if __name__ == "__main__":
    # Test local
    scraper = WelcomeToTheJungleScraper()
    jobs = scraper.scrape_jobs("data scientist", max_pages=1)
    
    if jobs:
        logger.info(f"Nombre d'offres trouvées: {len(jobs)}")
        
        # Récupérer les détails de la première offre
        job_details = scraper.scrape_job_details(jobs[0]['url'])
        
        # Sauvegarder les détails
        filename = scraper.save_to_json(job_details)
        
        # Uploader vers S3
        scraper.upload_to_s3(filename)
