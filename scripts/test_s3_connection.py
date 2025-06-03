#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de test pour la connexion à AWS S3.
Permet de vérifier la connectivité, lister les buckets et tester les opérations de base.
"""

import os
import sys
import json
import logging
import argparse
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

# Ajouter le répertoire parent au chemin Python pour importer les modules du projet
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/s3_connection_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(description='Test de connexion AWS S3')
    parser.add_argument('--bucket-name', type=str, default='data-lake-brut',
                        help='Nom du bucket S3 à utiliser ou créer')
    parser.add_argument('--create-bucket', action='store_true',
                        help='Créer le bucket s\'il n\'existe pas')
    parser.add_argument('--upload-test-file', action='store_true',
                        help='Uploader un fichier de test')
    parser.add_argument('--list-files', action='store_true',
                        help='Lister les fichiers dans le bucket')
    return parser.parse_args()

def get_s3_client():
    """Crée et retourne un client S3 avec les identifiants AWS."""
    # Charger les identifiants AWS
    aws_access_key = os.getenv('KEY_ACCESS')
    aws_secret_key = os.getenv('KEY_SECRET')
    
    if not aws_access_key or not aws_secret_key:
        logger.error("Identifiants AWS manquants. Vérifiez les variables d'environnement KEY_ACCESS et KEY_SECRET.")
        return None
    
    try:
        # Créer le client S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        return s3_client
    except Exception as e:
        logger.error(f"Erreur lors de la création du client S3: {e}")
        return None

def test_s3_connection():
    """Teste la connexion à AWS S3."""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False
        
        # Tester la connexion en listant les buckets
        response = s3_client.list_buckets()
        buckets = response.get('Buckets', [])
        
        logger.info(f"Connexion S3 réussie ! {len(buckets)} buckets trouvés.")
        return True
    
    except ClientError as e:
        logger.error(f"Erreur de connexion S3: {e}")
        return False
    
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        return False

def list_buckets():
    """Liste les buckets S3 disponibles."""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return []
        
        response = s3_client.list_buckets()
        buckets = response.get('Buckets', [])
        
        logger.info(f"Buckets disponibles ({len(buckets)}):")
        for bucket in buckets:
            logger.info(f"  - {bucket['Name']} (créé le {bucket['CreationDate']})")
        
        return buckets
    
    except Exception as e:
        logger.error(f"Erreur lors de la liste des buckets: {e}")
        return []

def bucket_exists(bucket_name):
    """Vérifie si un bucket existe."""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False
        
        response = s3_client.list_buckets()
        buckets = response.get('Buckets', [])
        
        return any(b['Name'] == bucket_name for b in buckets)
    
    except Exception as e:
        logger.error(f"Erreur lors de la vérification du bucket: {e}")
        return False

def create_bucket(bucket_name):
    """Crée un bucket S3."""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False
        
        # Créer le bucket
        s3_client.create_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' créé avec succès !")
        return True
    
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyOwnedByYou':
            logger.info(f"Le bucket '{bucket_name}' existe déjà et vous appartient.")
            return True
        elif error_code == 'BucketAlreadyExists':
            logger.error(f"Le bucket '{bucket_name}' existe déjà mais appartient à un autre compte.")
        else:
            logger.error(f"Erreur lors de la création du bucket: {e}")
        return False
    
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        return False

def upload_test_file(bucket_name):
    """Uploade un fichier de test dans le bucket."""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False
        
        # Créer un fichier de test
        test_file = "test_s3_upload.json"
        test_data = {
            "test": True,
            "timestamp": datetime.now().isoformat(),
            "message": "Test de connexion S3 réussi !"
        }
        
        with open(test_file, 'w', encoding='utf-8') as f:
            json.dump(test_data, f, ensure_ascii=False, indent=4)
        
        # Uploader le fichier
        s3_key = f"tests/test_s3_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        s3_client.upload_file(test_file, bucket_name, s3_key)
        
        # Supprimer le fichier local
        os.remove(test_file)
        
        logger.info(f"Fichier de test uploadé avec succès: s3://{bucket_name}/{s3_key}")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de l'upload du fichier de test: {e}")
        return False

def list_files(bucket_name, prefix=""):
    """Liste les fichiers dans un bucket S3."""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False
        
        # Lister les fichiers
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            logger.info(f"Aucun fichier trouvé dans s3://{bucket_name}/{prefix}")
            return True
        
        files = response['Contents']
        logger.info(f"Fichiers dans s3://{bucket_name}/{prefix} ({len(files)}):")
        
        for file in files:
            size_mb = file['Size'] / (1024 * 1024)
            logger.info(f"  - {file['Key']} ({size_mb:.2f} MB, modifié le {file['LastModified']})")
        
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de la liste des fichiers: {e}")
        return False

def main():
    """Fonction principale du script."""
    # Créer le dossier de logs s'il n'existe pas
    os.makedirs('logs', exist_ok=True)
    
    # Parser les arguments
    args = parse_arguments()
    
    # Charger les variables d'environnement depuis .env
    from dotenv import load_dotenv
    load_dotenv()
    
    # Afficher les informations de configuration
    logger.info("Test de connexion AWS S3")
    logger.info(f"Bucket: {args.bucket_name}")
    
    # Tester la connexion S3
    if test_s3_connection():
        # Lister les buckets
        buckets = list_buckets()
        
        # Vérifier si le bucket existe
        if bucket_exists(args.bucket_name):
            logger.info(f"Le bucket '{args.bucket_name}' existe.")
        else:
            logger.warning(f"Le bucket '{args.bucket_name}' n'existe pas.")
            
            # Créer le bucket si demandé
            if args.create_bucket:
                logger.info(f"Tentative de création du bucket '{args.bucket_name}'...")
                create_bucket(args.bucket_name)
        
        # Uploader un fichier de test si demandé
        if args.upload_test_file and bucket_exists(args.bucket_name):
            logger.info("Upload d'un fichier de test...")
            upload_test_file(args.bucket_name)
        
        # Lister les fichiers si demandé
        if args.list_files and bucket_exists(args.bucket_name):
            logger.info("Liste des fichiers dans le bucket...")
            list_files(args.bucket_name)
            
            # Lister les fichiers dans les dossiers spécifiques
            for prefix in ["raw/welcome_jungle/", "raw/pole_emploi/", "processed/"]:
                list_files(args.bucket_name, prefix)
    else:
        logger.error("Échec de la connexion S3. Vérifiez vos identifiants AWS.")

if __name__ == "__main__":
    main()
