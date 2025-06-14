#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal d'exécution du pipeline ETL.
Ce script orchestre l'extraction, la transformation et le chargement
des données provenant de toutes les sources (France Travail et Welcome to the Jungle).
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta

# Configurer le logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importer les modules ETL
from etl.api.dotenv_utils import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration des variables environnement AWS et PostgreSQL
def setup_environment_variables():
    """
    Configure les variables d'environnement nécessaires au pipeline ETL
    """
    # Variables AWS - utiliser les variables d'environnement du fichier .env
    dotenv.load_dotenv()
    
    # Variables AWS
    if not os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('KEY_ACCESS'):
        os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('KEY_ACCESS')
    if not os.getenv('AWS_SECRET_ACCESS_KEY') and os.getenv('KEY_SECRET'):
        os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('KEY_SECRET')
        
    # Variables base de données
    if not os.getenv('DB_HOST'):
        os.environ['DB_HOST'] = os.getenv('RDS_HOST', 'datas.c32ygg4oyapa.eu-north-1.rds.amazonaws.com')
    if not os.getenv('DB_PORT'):
        os.environ['DB_PORT'] = os.getenv('RDS_PORT', '5432')
    if not os.getenv('DB_NAME'):
        os.environ['DB_NAME'] = os.getenv('RDS_DATABASE', 'postgres')
    if not os.getenv('DB_USER'):
        os.environ['DB_USER'] = os.getenv('RDS_USER', 'postgres')
    if not os.getenv('DB_PASSWORD'):
        os.environ['DB_PASSWORD'] = os.getenv('RDS_PASSWORD', '')
        
    # Vérifier si les variables critiques sont configurées
    missing_vars = []
    for var in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'DB_HOST', 'DB_USER', 'DB_PASSWORD']:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.warning(f"Variables d'environnement manquantes: {', '.join(missing_vars)}")
        logger.warning("Certaines fonctionnalités pourraient ne pas fonctionner correctement.")
        logger.info("Exécutez 'python configure_env.py' pour configurer l'environnement.")

# Configurer l'environnement
setup_environment_variables()

def run_france_travail_pipeline(start_date=None, end_date=None, extract_skills=True):
    """
    Exécute le pipeline ETL pour les données France Travail.
    
    Args:
        start_date (str): Date de début au format YYYYMMDD
        end_date (str): Date de fin au format YYYYMMDD
        extract_skills (bool): Si True, extrait aussi les compétences des offres
        
    Returns:
        dict: Résultats du pipeline contenant le nombre d'enregistrements et de compétences traités
    """
    from etl.api.france_travail_etl import run_pipeline
    
    logger.info(f"Démarrage du pipeline France Travail ({start_date} - {end_date})")
    
    # Préparer les arguments pour le pipeline France Travail
    import sys
    import argparse
    from argparse import Namespace
    
    # Créer un objet Namespace simulé avec les arguments requis
    args = Namespace(
        start_date=start_date,
        end_date=end_date,
        include_s3=True,
        output_dir="data/intermediate",
        skip_load=False,
        no_skills=not extract_skills
    )
    
    # Exécuter le pipeline France Travail
    results = run_pipeline(args)
    
    logger.info(f"Pipeline France Travail terminé: {results['jobs_loaded']} offres traitées, "
               f"{results['skills_loaded']} compétences, {results['job_skills_loaded']} relations job_skills")
    
    # Formater les résultats pour correspondre au format attendu
    return {
        'jobs_loaded': results['jobs_loaded'],
        'skills_loaded': results['skills_loaded'],
        'job_skills_loaded': results['job_skills_loaded'],
        'extracted': results['extracted'],
        'transformed': results['transformed'],
        'success': results['success'],
        'duration': results['duration_seconds'],
    }

def run_welcome_jungle_pipeline():
    """
    Exécute le pipeline ETL pour les données Welcome to the Jungle.
    
    Returns:
        int: Nombre d'enregistrements traités
    """
    from etl.scrapers.etl_pipeline import execute_etl_pipeline
    
    logger.info("Démarrage du pipeline Welcome to the Jungle")
    records = execute_etl_pipeline()
    logger.info(f"Pipeline Welcome to the Jungle terminé: {records} enregistrements traités")
    
    return records

def test_database_connection():
    """
    Teste la connexion à la base de données et affiche les informations de diagnostic.
    
    Returns:
        bool: True si la connexion est établie, False sinon
    """
    from etl.api.loading import get_db_connection
    
    logger.info("Test de connexion à la base de données...")
    engine = get_db_connection()
    
    if engine is not None:
        logger.info("Connexion à la base de données établie avec succès")
        return True
    else:
        logger.error("Échec de connexion à la base de données")
        
        # Afficher des informations de diagnostic
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        logger.info(f"Informations de connexion: Host={db_host}, Port={db_port}, DB={db_name}")
        
        # Suggestions pour résoudre le problème
        logger.info("Suggestions de dépannage:")
        logger.info("1. Vérifiez que l'instance RDS est en cours d'exécution")
        logger.info("2. Assurez-vous que le groupe de sécurité autorise les connexions depuis votre adresse IP")
        logger.info("3. Vérifiez que la base de données est configurée pour autoriser l'accès public")
        logger.info("4. Confirmez que les identifiants sont corrects")
        
        return False

def run_full_etl_pipeline(sources, start_date=None, end_date=None, extract_skills=True):
    """
    Exécute le pipeline ETL complet pour toutes les sources spécifiées.
    
    Args:
        sources (list): Liste des sources à traiter ('all', 'france_travail', 'welcome_jungle')
        start_date (str): Date de début au format YYYYMMDD
        end_date (str): Date de fin au format YYYYMMDD
    
    Returns:
        dict: Résultats par source
    """
    # Si aucune date n'est spécifiée, utiliser la date d'aujourd'hui
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    
    # Si seule la date de fin est spécifiée, utiliser une semaine avant comme date de début
    if not start_date:
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
        
    # Vérifier la période de temps
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    days_diff = (end_dt - start_dt).days
    
    if days_diff > 30:
        logger.warning(f"Période de temps importante: {days_diff} jours. Le traitement peut prendre du temps.")
    
    logger.info(f"Démarrage du pipeline ETL complet pour la période {start_date} - {end_date}")
    
    # Tester la connexion à la base de données avant de commencer
    if not test_database_connection():
        logger.warning("Le pipeline continuera mais le chargement risque d'échouer")
    
    results = {}
    
    # Traiter France Travail
    if 'all' in sources or 'france_travail' in sources:
        try:
            results['france_travail'] = run_france_travail_pipeline(start_date, end_date, extract_skills)
        except Exception as e:
            logger.error(f"Erreur dans le pipeline France Travail: {e}")
            results['france_travail'] = {'jobs_loaded': 0, 'skills_loaded': 0, 'job_skills_loaded': 0}
    
    # Traiter Welcome to the Jungle
    if 'all' in sources or 'welcome_jungle' in sources:
        try:
            results['welcome_jungle'] = run_welcome_jungle_pipeline()
        except Exception as e:
            logger.error(f"Erreur dans le pipeline Welcome to the Jungle: {e}")
            results['welcome_jungle'] = 0
    
    # Afficher un résumé des résultats
    logger.info("=== Résumé du pipeline ETL ===")
    for source, result in results.items():
        if isinstance(result, dict):
            logger.info(f"{source}: {result['jobs_loaded']} offres, {result['skills_loaded']} compétences, {result['job_skills_loaded']} relations job_skills")
        else:
            logger.info(f"{source}: {result} enregistrements traités")
    
    return results

if __name__ == "__main__":
    # Définir les arguments de la ligne de commande
    parser = argparse.ArgumentParser(description="Exécution du pipeline ETL pour les offres d'emploi")
    parser.add_argument('--sources', type=str, nargs='+', default=['all'], 
                        choices=['all', 'france_travail', 'welcome_jungle'],
                        help="Sources de données à traiter")
    parser.add_argument('--start-date', type=str, help="Date de début au format YYYYMMDD")
    parser.add_argument('--end-date', type=str, help="Date de fin au format YYYYMMDD")
    parser.add_argument('--test-db', action='store_true', help="Tester uniquement la connexion à la base de données")
    parser.add_argument('--no-skills', action='store_true', help="Ne pas extraire les compétences des offres d'emploi")
    parser.add_argument('--output-dir', type=str, default="data/intermediate", help="Répertoire de sortie pour les fichiers intermédiaires")
    
    args = parser.parse_args()
    
    # Si l'option test-db est spécifiée, tester uniquement la connexion
    if args.test_db:
        test_database_connection()
        sys.exit(0)
    
    # Exécuter le pipeline
    results = run_full_etl_pipeline(args.sources, args.start_date, args.end_date, not args.no_skills)
    
    # Afficher un résumé final
    print("\n=====================================================")
    print("\u2728 RÉSUMÉ FINAL DU PIPELINE ETL \u2728")
    print("=====================================================\n")
    
    if 'france_travail' in results:
        ft_results = results['france_travail']
        print("\n\U0001F4C8 FRANCE TRAVAIL:")
        print(f"  - Offres extraites: {ft_results.get('extracted', 0)}")
        print(f"  - Offres transformées: {ft_results.get('transformed', 0)}")
        print(f"  - Offres chargées: {ft_results.get('jobs_loaded', 0)}")
        print(f"  - Compétences chargées: {ft_results.get('skills_loaded', 0)}")
        print(f"  - Relations job-skills: {ft_results.get('job_skills_loaded', 0)}")
        print(f"  - Durée: {ft_results.get('duration', 0):.2f} secondes")
        success_icon = '\u2705' if ft_results.get('success', False) else '\u274c'
        print(f"  - Statut: {success_icon}")
    
    if 'welcome_jungle' in results and results['welcome_jungle'] > 0:
        print("\n\U0001F4C9 WELCOME TO THE JUNGLE:")
        print(f"  - Offres traitées: {results['welcome_jungle']}")
    
    print("\n=====================================================")
