#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de test pour la connexion à la base de données RDS PostgreSQL.
Permet de vérifier la connectivité et les permissions de la base de données.
"""

import os
import sys
import logging
import argparse
import psycopg2
from datetime import datetime

# Ajouter le répertoire parent au chemin Python pour importer les modules du projet
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/rds_connection_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(description='Test de connexion RDS PostgreSQL')
    parser.add_argument('--host', type=str, help='Hôte de la base de données')
    parser.add_argument('--port', type=int, default=5432, help='Port de la base de données')
    parser.add_argument('--dbname', type=str, help='Nom de la base de données')
    parser.add_argument('--user', type=str, help='Utilisateur de la base de données')
    parser.add_argument('--password', type=str,
                        help='Mot de passe de la base de données (optionnel, utilisera la variable d\'environnement si non spécifié)')
    parser.add_argument('--timeout', type=int, default=10,
                        help='Timeout de connexion en secondes')
    parser.add_argument('--create-tables', action='store_true',
                        help='Créer les tables si elles n\'existent pas')
    return parser.parse_args()

def test_connection(host, port, dbname, user, password, timeout=10):
    """Teste la connexion à la base de données PostgreSQL."""
    try:
        logger.info(f"Tentative de connexion à {host}:{port}/{dbname} avec l'utilisateur {user}...")
        
        # Connexion à la base de données
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=timeout
        )
        
        # Vérifier la connexion
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"Connexion réussie ! Version PostgreSQL: {version[0]}")
            
            # Lister les tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = cursor.fetchall()
            
            if tables:
                logger.info("Tables existantes:")
                for table in tables:
                    logger.info(f"  - {table[0]}")
            else:
                logger.info("Aucune table n'existe dans le schéma public.")
        
        conn.close()
        return True
    
    except psycopg2.OperationalError as e:
        logger.error(f"Erreur de connexion: {e}")
        logger.info("Causes possibles:")
        logger.info("1. La base de données n'est pas accessible publiquement (problème de sécurité réseau)")
        logger.info("2. Les identifiants sont incorrects")
        logger.info("3. La base de données n'existe pas ou n'est pas démarrée")
        logger.info("4. Le port est bloqué par un pare-feu")
        return False
    
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        return False

def create_tables(host, port, dbname, user, password, timeout=10):
    """Crée les tables nécessaires si elles n'existent pas."""
    try:
        # Connexion à la base de données
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=timeout
        )
        conn.autocommit = True
        
        # Créer les tables
        with conn.cursor() as cursor:
            # Table des offres d'emploi
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id SERIAL PRIMARY KEY,
                    job_id VARCHAR(255) UNIQUE NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    company VARCHAR(255),
                    location VARCHAR(255),
                    contract_type VARCHAR(100),
                    description TEXT,
                    url VARCHAR(512),
                    source VARCHAR(50),
                    date_posted TIMESTAMP,
                    date_extracted TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Table des compétences
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS skills (
                    id SERIAL PRIMARY KEY,
                    job_id VARCHAR(255) REFERENCES jobs(job_id) ON DELETE CASCADE,
                    skill VARCHAR(100) NOT NULL,
                    UNIQUE(job_id, skill)
                );
            """)
            
            # Index pour améliorer les performances
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_contract_type ON jobs(contract_type);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_skills_skill ON skills(skill);")
            
            logger.info("Tables créées avec succès !")
        
        conn.close()
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de la création des tables: {e}")
        return False

def main():
    """Fonction principale du script."""
    # Créer le dossier de logs s'il n'existe pas
    os.makedirs('logs', exist_ok=True)
    
    # Charger les variables d'environnement depuis .env
    from dotenv import load_dotenv
    load_dotenv()
    
    # Parser les arguments
    args = parse_arguments()
    
    # Utiliser les valeurs des arguments ou des variables d'environnement
    host = args.host or os.getenv('DB_HOST')
    port = args.port or int(os.getenv('DB_PORT', '5432'))
    dbname = args.dbname or os.getenv('DB_NAME')
    user = args.user or os.getenv('DB_USER')
    password = args.password or os.getenv('DB_PASSWORD')
    
    # Vérifier que tous les paramètres sont définis
    if not all([host, dbname, user, password]):
        logger.error("Paramètres de connexion incomplets. Veuillez définir les variables d'environnement ou fournir les arguments.")
        sys.exit(1)
    
    # Tester la connexion
    if test_connection(host, port, dbname, user, password, args.timeout):
        # Créer les tables si demandé
        if args.create_tables:
            logger.info("Création des tables...")
            if create_tables(host, port, dbname, user, password, args.timeout):
                logger.info("Tables créées ou mises à jour avec succès !")
            else:
                logger.error("Échec de la création des tables.")
    else:
        logger.error("Échec de la connexion à la base de données.")
        logger.info("Suggestion: Vérifiez que votre base de données est accessible publiquement ou configurez un tunnel SSH.")

if __name__ == "__main__":
    main()
