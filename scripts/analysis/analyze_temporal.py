#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'analyse de l'évolution temporelle des offres d'emploi France Travail.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def analyze_temporal_evolution(df):
    """
    Analyse l'évolution temporelle des offres d'emploi.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
        
    Returns:
        str: Chemin vers la visualisation générée
    """
    try:
        logger.info("Analyse de l'évolution temporelle des offres...")
        
        # Créer une copie du DataFrame pour éviter les modifications en place
        temp_df = df.copy()
        
        # Vérifier si les colonnes de date existent
        date_columns = ['date_creation', 'date_actualisation']
        missing_columns = [col for col in date_columns if col not in temp_df.columns]
        
        if missing_columns:
            logger.warning(f"Colonnes de date manquantes: {missing_columns}")
            # Utiliser uniquement les colonnes disponibles
            date_columns = [col for col in date_columns if col in temp_df.columns]
        
        if not date_columns:
            logger.warning("Aucune colonne de date trouvée")
            return None
        
        # Convertir les colonnes de date en datetime
        for col in date_columns:
            try:
                temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')
            except Exception as e:
                logger.warning(f"Erreur lors de la conversion de la colonne {col} en datetime: {e}")
                date_columns.remove(col)
        
        if not date_columns:
            logger.warning("Aucune colonne de date valide après conversion")
            return None
        
        # Utiliser la première colonne de date disponible pour l'analyse
        date_col = date_columns[0]
        logger.info(f"Utilisation de la colonne {date_col} pour l'analyse temporelle")
        
        # Filtrer les lignes avec des dates valides
        temp_df = temp_df.dropna(subset=[date_col])
        
        if len(temp_df) == 0:
            logger.warning("Aucune donnée avec des dates valides")
            return None
        
        # Créer la figure
        plt.figure(figsize=(16, 12))
        
        # 1. Évolution du nombre d'offres par jour
        plt.subplot(2, 2, 1)
        
        # Grouper par jour
        daily_counts = temp_df.groupby(temp_df[date_col].dt.date).size()
        
        # Trier par date
        daily_counts = daily_counts.sort_index()
        
        # Tracer l'évolution
        sns.lineplot(x=daily_counts.index, y=daily_counts.values)
        plt.title('Évolution du nombre d\'offres par jour')
        plt.xlabel('Date')
        plt.ylabel('Nombre d\'offres')
        plt.xticks(rotation=45)
        
        # 2. Évolution du nombre d'offres par mois
        plt.subplot(2, 2, 2)
        
        # Créer une colonne pour le mois
        temp_df['month'] = temp_df[date_col].dt.to_period('M')
        
        # Grouper par mois
        monthly_counts = temp_df.groupby('month').size()
        
        # Convertir les périodes en dates pour le tracé
        month_dates = [pd.to_datetime(str(period)) for period in monthly_counts.index]
        
        # Tracer l'évolution
        sns.barplot(x=[date.strftime('%Y-%m') for date in month_dates], y=monthly_counts.values)
        plt.title('Évolution du nombre d\'offres par mois')
        plt.xlabel('Mois')
        plt.ylabel('Nombre d\'offres')
        plt.xticks(rotation=45)
        
        # 3. Évolution des types de contrat au fil du temps
        if 'contract_type_std' in temp_df.columns:
            plt.subplot(2, 2, 3)
            
            # Limiter aux types de contrats les plus courants pour la lisibilité
            top_contracts = temp_df['contract_type_std'].value_counts().nlargest(5).index
            
            # Filtrer pour ne garder que les types de contrat significatifs
            filtered_temp_df = temp_df[temp_df['contract_type_std'].isin(top_contracts)]
            
            # Créer une colonne pour le trimestre
            filtered_temp_df['quarter'] = filtered_temp_df[date_col].dt.to_period('Q')
            
            # Grouper par trimestre et type de contrat
            contract_evolution = filtered_temp_df.groupby(['quarter', 'contract_type_std']).size().unstack()
            
            # Convertir les périodes en dates pour le tracé
            quarter_dates = [pd.to_datetime(str(period)) for period in contract_evolution.index]
            
            # Tracer l'évolution
            contract_evolution.plot(kind='line', marker='o', ax=plt.gca())
            plt.title('Évolution des types de contrat par trimestre')
            plt.xlabel('Trimestre')
            plt.ylabel('Nombre d\'offres')
            plt.xticks(rotation=45)
            plt.legend(title='Type de contrat')
        
        # 4. Évolution des technologies au fil du temps
        tech_columns = ['has_python', 'has_java', 'has_javascript', 'has_sql', 'has_aws', 'has_machine_learning']
        available_tech_columns = [col for col in tech_columns if col in temp_df.columns]
        
        if available_tech_columns:
            plt.subplot(2, 2, 4)
            
            # Créer une colonne pour le trimestre si ce n'est pas déjà fait
            if 'quarter' not in temp_df.columns:
                temp_df['quarter'] = temp_df[date_col].dt.to_period('Q')
            
            # Créer un DataFrame pour stocker les résultats
            tech_evolution_data = []
            
            for quarter in sorted(temp_df['quarter'].unique()):
                quarter_df = temp_df[temp_df['quarter'] == quarter]
                total_offers = len(quarter_df)
                
                for tech_col in available_tech_columns:
                    tech_name = tech_col.replace('has_', '').replace('_', ' ').title()
                    tech_count = quarter_df[tech_col].sum()
                    tech_proportion = tech_count / total_offers if total_offers > 0 else 0
                    
                    tech_evolution_data.append({
                        'Quarter': quarter,
                        'Technology': tech_name,
                        'Count': tech_count,
                        'Proportion': tech_proportion
                    })
            
            tech_evolution_df = pd.DataFrame(tech_evolution_data)
            
            # Créer un tableau croisé pour la visualisation
            pivot_df = tech_evolution_df.pivot(index='Quarter', columns='Technology', values='Proportion')
            
            # Convertir les périodes en dates pour le tracé
            quarter_dates = [pd.to_datetime(str(period)) for period in pivot_df.index]
            
            # Tracer l'évolution
            pivot_df.plot(kind='line', marker='o', ax=plt.gca())
            plt.title('Évolution de la proportion des technologies par trimestre')
            plt.xlabel('Trimestre')
            plt.ylabel('Proportion')
            plt.xticks(rotation=45)
            plt.legend(title='Technologie', bbox_to_anchor=(1.05, 1), loc='upper left')
        
        # Ajuster la mise en page
        plt.tight_layout()
        
        # Sauvegarder la figure
        os.makedirs("data/analysis/visualizations", exist_ok=True)
        output_path = "data/analysis/visualizations/temporal_evolution.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        # Générer des statistiques supplémentaires pour le logging
        date_range = temp_df[date_col].max() - temp_df[date_col].min()
        logger.info(f"Période couverte par l'analyse: {date_range.days} jours")
        logger.info(f"Date la plus ancienne: {temp_df[date_col].min()}")
        logger.info(f"Date la plus récente: {temp_df[date_col].max()}")
        
        # Trouver le jour avec le plus d'offres
        if len(daily_counts) > 0:
            max_day = daily_counts.idxmax()
            max_count = daily_counts.max()
            logger.info(f"Jour avec le plus d'offres: {max_day} ({max_count} offres)")
        
        logger.info(f"Analyse temporelle terminée, visualisation sauvegardée: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse temporelle: {e}")
        return None
