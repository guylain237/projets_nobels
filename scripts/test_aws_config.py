#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de test pour la configuration AWS (S3, RDS, Lambda).
Permet de vérifier la connectivité et les permissions AWS.
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime

# Ajouter le répertoire parent au chemin Python pour importer les modules du projet
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importer les modules d'infrastructure
from src.infrastructure.s3_setup import test_s3_connection, create_bucket, list_buckets
from src.infrastructure.rds_setup import test_rds_connection
from src.infrastructure.lambda_setup import test_lambda_function

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/aws_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(description='Test de la configuration AWS')
    parser.add_argument('--service', choices=['s3', 'rds', 'lambda', 'all'], default='all',
                        help='Service AWS à tester (s3, rds, lambda, all)')
    parser.add_argument('--create-bucket', action='store_true', help='Créer un bucket S3 si inexistant')
    parser.add_argument('--bucket-name', type=str, default='data-lake-brut',
                        help='Nom du bucket S3 à utiliser ou créer')
    return parser.parse_args()

def test_s3(bucket_name, create_if_not_exists=False):
    """Teste la connexion et les opérations S3."""
    logger.info("Test de la connexion S3...")
    
    # Tester la connexion S3
    if test_s3_connection():
        logger.info("Connexion S3 réussie !")
        
        # Lister les buckets
        buckets = list_buckets()
        logger.info(f"Buckets disponibles: {', '.join([b['Name'] for b in buckets])}")
        
        # Vérifier si le bucket existe
        bucket_exists = any(b['Name'] == bucket_name for b in buckets)
        if bucket_exists:
            logger.info(f"Le bucket '{bucket_name}' existe.")
        else:
            logger.warning(f"Le bucket '{bucket_name}' n'existe pas.")
            
            # Créer le bucket si demandé
            if create_if_not_exists:
                logger.info(f"Tentative de création du bucket '{bucket_name}'...")
                if create_bucket(bucket_name):
                    logger.info(f"Bucket '{bucket_name}' créé avec succès !")
                else:
                    logger.error(f"Échec de la création du bucket '{bucket_name}'.")
    else:
        logger.error("Échec de la connexion S3. Vérifiez vos identifiants AWS.")

def test_rds():
    """Teste la connexion RDS."""
    logger.info("Test de la connexion RDS...")
    
    # Tester la connexion RDS
    if test_rds_connection():
        logger.info("Connexion RDS réussie !")
    else:
        logger.error("Échec de la connexion RDS. Vérifiez vos identifiants et la configuration réseau.")
        logger.info("Note: Si votre base de données n'est pas accessible publiquement, vous devrez peut-être configurer un tunnel SSH ou modifier les règles de sécurité.")

def test_lambda():
    """Teste la configuration Lambda."""
    logger.info("Test de la configuration Lambda...")
    
    # Tester la configuration Lambda
    if test_lambda_function():
        logger.info("Configuration Lambda réussie !")
    else:
        logger.error("Échec de la configuration Lambda. Vérifiez vos identifiants et permissions.")

def main():
    """Fonction principale du script."""
    # Créer le dossier de logs s'il n'existe pas
    os.makedirs('logs', exist_ok=True)
    
    # Parser les arguments
    args = parse_arguments()
    
    # Afficher les informations de configuration
    logger.info("Test de la configuration AWS")
    logger.info(f"Service: {args.service}")
    
    # Charger les variables d'environnement depuis .env
    from dotenv import load_dotenv
    load_dotenv()
    
    # Vérifier les identifiants AWS
    aws_access_key = os.getenv('KEY_ACCESS')
    aws_secret_key = os.getenv('KEY_SECRET')
    
    if not aws_access_key or not aws_secret_key:
        logger.error("Identifiants AWS manquants. Vérifiez les variables d'environnement KEY_ACCESS et KEY_SECRET.")
        return
    
    logger.info(f"Identifiants AWS: {aws_access_key[:5]}...{aws_access_key[-5:]}")
    
    # Tester les services demandés
    if args.service in ['s3', 'all']:
        test_s3(args.bucket_name, args.create_bucket)
    
    if args.service in ['rds', 'all']:
        test_rds()
    
    if args.service in ['lambda', 'all']:
        test_lambda()
    
    logger.info("Tests terminés.")

if __name__ == "__main__":
    main()
