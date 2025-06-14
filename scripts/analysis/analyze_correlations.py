#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'analyse des corrélations entre technologies et types de contrat.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import logging

logger = logging.getLogger(__name__)

def analyze_tech_contract_correlation(df):
    """
    Analyse la corrélation entre les technologies demandées et les types de contrat.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
        
    Returns:
        str: Chemin vers la visualisation générée
    """
    try:
        logger.info("Analyse de la corrélation entre technologies et types de contrat...")
        
        # Créer une copie du DataFrame pour éviter les modifications en place
        corr_df = df.copy()
        
        # Vérifier si les colonnes nécessaires existent
        tech_columns = ['has_python', 'has_java', 'has_javascript', 'has_sql', 'has_aws', 'has_machine_learning']
        missing_tech_columns = [col for col in tech_columns if col not in corr_df.columns]
        
        if 'contract_type_std' not in corr_df.columns:
            logger.warning("Colonne contract_type_std manquante")
            return None
        
        if missing_tech_columns:
            logger.warning(f"Colonnes de technologies manquantes: {missing_tech_columns}")
            # Utiliser uniquement les colonnes disponibles
            tech_columns = [col for col in tech_columns if col in corr_df.columns]
        
        if not tech_columns:
            logger.warning("Aucune colonne de technologie trouvée")
            return None
        
        # Filtrer pour ne garder que les types de contrat les plus courants
        top_contracts = corr_df['contract_type_std'].value_counts().nlargest(5).index
        corr_df = corr_df[corr_df['contract_type_std'].isin(top_contracts)]
        
        # Créer un tableau croisé pour chaque technologie et type de contrat
        result_data = []
        
        for tech_col in tech_columns:
            # Extraire le nom de la technologie (sans le préfixe 'has_')
            tech_name = tech_col.replace('has_', '').replace('_', ' ').title()
            
            # Calculer la proportion d'offres avec cette technologie pour chaque type de contrat
            for contract_type in top_contracts:
                # Nombre total d'offres pour ce type de contrat
                total_contract = len(corr_df[corr_df['contract_type_std'] == contract_type])
                
                # Nombre d'offres avec cette technologie pour ce type de contrat
                tech_contract = len(corr_df[(corr_df['contract_type_std'] == contract_type) & (corr_df[tech_col] == True)])
                
                # Calculer la proportion
                proportion = tech_contract / total_contract if total_contract > 0 else 0
                
                # Ajouter à la liste des données
                result_data.append({
                    'Technology': tech_name,
                    'ContractType': contract_type,
                    'Proportion': proportion,
                    'Count': tech_contract,
                    'Total': total_contract
                })
        
        # Convertir en DataFrame
        result_df = pd.DataFrame(result_data)
        
        # Créer un tableau croisé pour la visualisation
        pivot_df = result_df.pivot(index='Technology', columns='ContractType', values='Proportion')
        
        # Créer la figure
        plt.figure(figsize=(12, 10))
        
        # Créer la heatmap
        ax = plt.subplot(2, 1, 1)
        sns.heatmap(pivot_df, annot=True, cmap='YlGnBu', fmt='.1%', cbar_kws={'label': 'Proportion'})
        plt.title('Proportion des technologies par type de contrat')
        plt.ylabel('Technologie')
        plt.xlabel('Type de contrat')
        
        # Créer un graphique à barres empilées pour une autre perspective
        # Préparer les données
        # Utiliser uniquement les colonnes disponibles pour éviter l'erreur de longueur
        tech_columns_available = [col for col in tech_columns if col in corr_df.columns]
        
        tech_contract_counts = pd.crosstab(
            index=corr_df['contract_type_std'],
            columns=[corr_df[col] for col in tech_columns_available],
            rownames=['ContractType'],
            colnames=tech_columns_available
        )
        
        # Renommer les colonnes pour plus de clarté
        tech_contract_counts.columns = [col.replace('has_', '').replace('_', ' ').title() for col in tech_columns_available]
        
        # Normaliser pour obtenir des proportions
        tech_contract_proportions = tech_contract_counts.div(tech_contract_counts.sum(axis=1), axis=0)
        
        # Créer le graphique à barres empilées
        ax = plt.subplot(2, 1, 2)
        tech_contract_proportions.plot(kind='bar', stacked=True, ax=ax, colormap='tab10')
        plt.title('Répartition des technologies par type de contrat')
        plt.xlabel('Type de contrat')
        plt.ylabel('Proportion')
        plt.legend(title='Technologie', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xticks(rotation=45)
        
        # Ajuster la mise en page
        plt.tight_layout()
        
        # Sauvegarder la figure
        os.makedirs("data/analysis/visualizations", exist_ok=True)
        output_path = "data/analysis/visualizations/tech_contract_correlation.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Analyse de corrélation terminée, visualisation sauvegardée: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse de corrélation: {e}")
        return None
