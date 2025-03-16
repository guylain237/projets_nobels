# cli/main.py
from src.utils import get_logger
from src.etl import extractionfile

# Importer la fonction de création de la base de données
from src.databases import create_db

def main():
    # Création de la base de données à partir du script schema.sql
    logger = get_logger(__name__)
    logger.info("Creation de la base de données...")
    create_db()
    logger.info("Base de données créée avec succès !")
    
    # Extraction des données du fichier JSON
    logger.info("Extraction des données...")
    extractionfile()
    logger.info("Données extraites avec succès !")
if __name__ == '__main__':
    main()
