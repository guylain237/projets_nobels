#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script d'analyse et de visualisation des données d'offres d'emploi France Travail.
Ce script se connecte à la base de données PostgreSQL, extrait les données,
et génère des visualisations pour explorer les tendances.
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from datetime import datetime
import sqlalchemy
from sqlalchemy import text

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importer les modules d'analyse avancée
from analyze_salary import analyze_salary_distribution, analyze_salary_by_technology
from analyze_correlations import analyze_tech_contract_correlation
from analyze_geography import analyze_geographic_distribution
from analyze_temporal import analyze_temporal_evolution

# Importer l'utilitaire de chargement des variables d'environnement
from src.etl.api.dotenv_utils import load_dotenv

# Configuration du logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_environment():
    """
    Configure les variables d'environnement nécessaires pour l'analyse.
    """
    # Charger les variables d'environnement depuis le fichier .env
    load_dotenv()
    
    # Vérifier que les variables essentielles sont définies
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Variables d'environnement manquantes: {', '.join(missing_vars)}")
        logger.error("Veuillez vérifier votre fichier .env")
        return False
    
    # Créer le dossier pour les visualisations
    os.makedirs("data/analysis/visualizations", exist_ok=True)
    
    logger.info("Environnement configuré avec succès")
    return True

def get_db_connection():
    """
    Établit une connexion à la base de données PostgreSQL.
    
    Returns:
        sqlalchemy.engine.Engine: Objet de connexion à la base de données
    """
    try:
        # Récupérer les informations de connexion depuis les variables d'environnement
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        
        # Construire l'URL de connexion
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        # Créer le moteur de connexion
        engine = sqlalchemy.create_engine(db_url)
        
        # Tester la connexion
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            if result.scalar() == 1:
                logger.info("Connexion à la base de données établie avec succès")
                return engine
            else:
                logger.error("La connexion à la base de données a échoué")
                return None
    
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à la base de données: {e}")
        return None

def load_data_from_db(engine):
    """
    Charge les données depuis la base de données.
    
    Args:
        engine (sqlalchemy.engine.Engine): Objet de connexion à la base de données
        
    Returns:
        pandas.DataFrame: DataFrame contenant les données chargées
    """
    try:
        # Requête SQL pour récupérer les données
        query = """
        SELECT * FROM france_travail_jobs
        """
        
        # Exécuter la requête et charger les résultats dans un DataFrame
        df = pd.read_sql(query, engine)
        
        logger.info(f"Données chargées avec succès: {len(df)} enregistrements")
        return df
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données: {e}")
        return None

def analyze_contract_types(df):
    """
    Analyse la distribution des types de contrat.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
    """
    try:
        # Compter les occurrences de chaque type de contrat
        contract_counts = df['contract_type_std'].value_counts()
        
        # Créer un graphique
        plt.figure(figsize=(12, 6))
        sns.barplot(x=contract_counts.index, y=contract_counts.values)
        plt.title('Distribution des Types de Contrat', fontsize=16)
        plt.xlabel('Type de Contrat', fontsize=12)
        plt.ylabel('Nombre d\'Offres', fontsize=12)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Sauvegarder le graphique
        output_path = "data/analysis/visualizations/contract_types_distribution.png"
        plt.savefig(output_path)
        logger.info(f"Graphique sauvegardé: {output_path}")
        
        # Afficher les statistiques
        logger.info("Distribution des types de contrat:")
        for contract_type, count in contract_counts.items():
            logger.info(f"  {contract_type}: {count} offres ({count/len(df)*100:.1f}%)")
        
        return output_path
    
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des types de contrat: {e}")
        return None

def analyze_cities(df):
    """
    Analyse la distribution des offres par ville.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
    """
    try:
        # Extraire les villes à partir de lieu_travail
        def extract_city(lieu_travail):
            try:
                lieu_dict = eval(lieu_travail) if isinstance(lieu_travail, str) else {}
                if 'libelle' in lieu_dict:
                    libelle = lieu_dict['libelle']
                    # Le format est "XX - NOM_VILLE"
                    if ' - ' in libelle:
                        return libelle.split(' - ')[1]
                    return libelle  # Pour les cas comme "Belgique"
                return None
            except Exception:
                return None
        
        df['ville_extraite'] = df['lieu_travail'].apply(extract_city)
        
        # Compter les occurrences de chaque ville (top 15)
        city_counts = df['ville_extraite'].value_counts().head(15)
        
        # Créer un graphique
        plt.figure(figsize=(14, 8))
        sns.barplot(x=city_counts.values, y=city_counts.index)
        plt.title('Top 15 des Villes avec le Plus d\'Offres d\'Emploi', fontsize=16)
        plt.xlabel('Nombre d\'Offres', fontsize=12)
        plt.ylabel('Ville', fontsize=12)
        plt.tight_layout()
        
        # Sauvegarder le graphique
        output_path = "data/analysis/visualizations/cities_distribution.png"
        plt.savefig(output_path)
        logger.info(f"Graphique sauvegardé: {output_path}")
        
        # Afficher les statistiques
        logger.info("Top 15 des villes avec le plus d'offres:")
        for city, count in city_counts.items():
            logger.info(f"  {city}: {count} offres ({count/len(df)*100:.1f}%)")
        
        return output_path
    
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des villes: {e}")
        return None

def analyze_technologies(df):
    """
    Analyse les technologies mentionnées dans les offres.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
    """
    try:
        # Créer une liste de technologies à rechercher
        technologies = [
            'python', 'java', 'javascript', 'typescript', 'react', 'angular', 'vue', 
            'node', 'php', 'sql', 'nosql', 'mongodb', 'postgresql', 'mysql', 
            'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'devops',
            'machine learning', 'data science', 'ai', 'deep learning'
        ]
        
        # Utiliser les colonnes has_* existantes pour certaines technologies
        tech_counts = {
            'python': df['has_python'].sum(),
            'java': df['has_java'].sum(),
            'javascript': df['has_javascript'].sum(),
            'sql': df['has_sql'].sum(),
            'aws': df['has_aws'].sum(),
            'machine learning': df['has_machine_learning'].sum()
        }
        
        # Pour les autres technologies, rechercher dans le titre et la description
        other_techs = [tech for tech in technologies if tech not in tech_counts]
        for tech in other_techs:
            # Rechercher dans le titre et la description
            mask = (
                df['intitule'].str.lower().str.contains(tech, na=False) | 
                df['description_clean'].str.lower().str.contains(tech, na=False)
            )
            tech_counts[tech] = mask.sum()
        
        # Convertir en Series et trier
        tech_series = pd.Series(tech_counts).sort_values(ascending=False)
        tech_series = tech_series[tech_series > 0]  # Filtrer les technologies avec au moins une occurrence
        
        if len(tech_series) > 0:
            # Créer un graphique
            plt.figure(figsize=(14, 8))
            sns.barplot(x=tech_series.values, y=tech_series.index)
            plt.title('Technologies les Plus Demandées dans les Offres d\'Emploi', fontsize=16)
            plt.xlabel('Nombre d\'Offres', fontsize=12)
            plt.ylabel('Technologie', fontsize=12)
            plt.tight_layout()
            
            # Sauvegarder le graphique
            output_path = "data/analysis/visualizations/technologies_distribution.png"
            plt.savefig(output_path)
            logger.info(f"Graphique sauvegardé: {output_path}")
            
            # Afficher les statistiques
            logger.info("Technologies les plus demandées:")
            for tech, count in tech_series.items():
                logger.info(f"  {tech}: {count} offres ({count/len(df)*100:.1f}%)")
            
            return output_path
        else:
            logger.warning("Aucune technologie trouvée dans les offres")
            return None
    
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des technologies: {e}")
        return None

def analyze_contract_duration(df):
    """
    Analyse la durée des contrats pour les CDD.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
    """
    try:
        # Filtrer les CDD
        cdd_df = df[df['contract_type_std'] == 'CDD'].copy()
        
        if len(cdd_df) > 0 and 'duree_contrat' in cdd_df.columns:
            # Convertir la durée en numérique si possible
            cdd_df['duree_mois'] = pd.to_numeric(cdd_df['duree_contrat'], errors='coerce')
            
            # Filtrer les valeurs valides
            cdd_df = cdd_df.dropna(subset=['duree_mois'])
            
            if len(cdd_df) > 0:
                # Créer un histogramme
                plt.figure(figsize=(12, 6))
                sns.histplot(cdd_df['duree_mois'], bins=20, kde=True)
                plt.title('Distribution de la Durée des Contrats CDD (en mois)', fontsize=16)
                plt.xlabel('Durée (mois)', fontsize=12)
                plt.ylabel('Nombre d\'Offres', fontsize=12)
                plt.tight_layout()
                
                # Sauvegarder le graphique
                output_path = "data/analysis/visualizations/contract_duration_distribution.png"
                plt.savefig(output_path)
                logger.info(f"Graphique sauvegardé: {output_path}")
                
                # Afficher les statistiques
                logger.info("Statistiques sur la durée des CDD:")
                logger.info(f"  Durée moyenne: {cdd_df['duree_mois'].mean():.1f} mois")
                logger.info(f"  Durée médiane: {cdd_df['duree_mois'].median():.1f} mois")
                logger.info(f"  Durée minimale: {cdd_df['duree_mois'].min():.1f} mois")
                logger.info(f"  Durée maximale: {cdd_df['duree_mois'].max():.1f} mois")
                
                return output_path
            else:
                logger.warning("Aucune durée de contrat valide trouvée")
                return None
        else:
            logger.warning("Pas de CDD ou pas de colonne 'duree_contrat' dans les données")
            return None
    
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse de la durée des contrats: {e}")
        return None

def generate_dashboard(visualization_paths):
    """
    Génère un tableau de bord HTML avec toutes les visualisations.
    
    Args:
        visualization_paths (list): Liste des chemins vers les visualisations
        
    Returns:
        str: Chemin vers le fichier HTML généré
    """
    try:
        # Filtrer les chemins None
        visualization_paths = [p for p in visualization_paths if p]
        
        if not visualization_paths:
            logger.warning("Aucune visualisation à inclure dans le tableau de bord")
            return None
        
        # Créer le contenu HTML
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Tableau de Bord des Offres d'Emploi France Travail</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 20px;
                    background-color: #f5f5f5;
                }
                h1 {
                    color: #333;
                    text-align: center;
                }
                .dashboard {
                    display: flex;
                    flex-wrap: wrap;
                    justify-content: center;
                    gap: 20px;
                    margin-top: 20px;
                }
                .visualization {
                    background-color: white;
                    border-radius: 8px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                    padding: 15px;
                    max-width: 100%;
                }
                .visualization img {
                    max-width: 100%;
                    height: auto;
                }
                .visualization h2 {
                    margin-top: 0;
                    color: #555;
                }
                footer {
                    margin-top: 30px;
                    text-align: center;
                    color: #777;
                }
            </style>
        </head>
        <body>
            <h1>Tableau de Bord des Offres d'Emploi France Travail</h1>
            <div class="dashboard">
        """
        
        # Ajouter chaque visualisation
        for i, path in enumerate(visualization_paths):
            title = os.path.basename(path).replace('_', ' ').replace('.png', '').title()
            html_content += f"""
                <div class="visualization">
                    <h2>{title}</h2>
                    <img src="../../../{path}" alt="{title}">
                </div>
            """
        
        # Fermer le HTML
        html_content += """
            </div>
            <footer>
                <p>Généré le """ + datetime.now().strftime("%d/%m/%Y à %H:%M:%S") + """</p>
            </footer>
        </body>
        </html>
        """
        
        # Sauvegarder le fichier HTML
        output_path = "data/analysis/visualizations/dashboard.html"
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Tableau de bord généré: {output_path}")
        return output_path
    
    except Exception as e:
        logger.error(f"Erreur lors de la génération du tableau de bord: {e}")
        return None

def main():
    """
    Fonction principale qui orchestre l'analyse des données.
    """
    start_time = datetime.now()
    logger.info(f"=== Démarrage de l'analyse des offres d'emploi France Travail ===")
    logger.info(f"Date et heure de début: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Configurer l'environnement
        if not setup_environment():
            logger.error("Échec de la configuration de l'environnement")
            return 1
        
        # Se connecter à la base de données
        engine = get_db_connection()
        if not engine:
            logger.error("Impossible de se connecter à la base de données")
            return 1
        
        # Charger les données
        df = load_data_from_db(engine)
        if df is None or df.empty:
            logger.error("Aucune donnée à analyser")
            return 1
        
        logger.info(f"Analyse de {len(df)} offres d'emploi")
        
        # Générer les visualisations
        visualization_paths = []
        
        # Analyse des types de contrat
        contract_viz = analyze_contract_types(df)
        visualization_paths.append(contract_viz)
        
        # Analyse des villes
        cities_viz = analyze_cities(df)
        visualization_paths.append(cities_viz)
        
        # Analyse des technologies
        tech_viz = analyze_technologies(df)
        visualization_paths.append(tech_viz)
        
        # Analyse de la durée des contrats
        duration_viz = analyze_contract_duration(df)
        visualization_paths.append(duration_viz)
        
        # Analyses avancées
        logger.info("Exécution des analyses avancées...")
        
        # 1. Analyse des salaires
        logger.info("Analyse des salaires...")
        salary_viz = analyze_salary_distribution(df)
        if salary_viz:
            visualization_paths.append(salary_viz)
        
        salary_tech_viz = analyze_salary_by_technology(df)
        if salary_tech_viz:
            visualization_paths.append(salary_tech_viz)
        
        # 2. Corrélation entre technologies et types de contrat
        logger.info("Analyse des corrélations entre technologies et types de contrat...")
        tech_contract_viz = analyze_tech_contract_correlation(df)
        if tech_contract_viz:
            visualization_paths.append(tech_contract_viz)
        
        # 3. Analyse géographique avancée
        logger.info("Analyse géographique avancée...")
        geo_viz = analyze_geographic_distribution(df)
        if geo_viz:
            visualization_paths.append(geo_viz)
        
        # 4. Évolution temporelle des offres
        logger.info("Analyse de l'évolution temporelle des offres...")
        temp_viz = analyze_temporal_evolution(df)
        if temp_viz:
            visualization_paths.append(temp_viz)
        
        # Générer le tableau de bord
        dashboard_path = generate_dashboard(visualization_paths)
        
        # Fin de l'analyse
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"=== Analyse terminée ===")
        logger.info(f"Date et heure de fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Durée totale: {duration}")
        
        if dashboard_path:
            logger.info(f"Tableau de bord disponible à: {dashboard_path}")
            logger.info(f"Ouvrez ce fichier dans votre navigateur pour visualiser les résultats")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Erreur lors de l'analyse des données: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
