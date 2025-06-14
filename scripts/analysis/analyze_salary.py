#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'analyse des salaires pour les offres d'emploi France Travail.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import logging

logger = logging.getLogger(__name__)

def analyze_salary_distribution(df):
    """
    Analyse la distribution des salaires dans les offres d'emploi.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
        
    Returns:
        str: Chemin vers la visualisation générée
    """
    try:
        logger.info("Analyse de la distribution des salaires...")
        
        # Créer une copie du DataFrame pour éviter les modifications en place
        salary_df = df.copy()
        
        # Vérifier si les colonnes de salaire existent
        if 'min_salary' not in salary_df.columns or 'max_salary' not in salary_df.columns:
            logger.warning("Colonnes de salaire non trouvées dans les données")
            return None
        
        # Filtrer les lignes avec des valeurs de salaire valides
        salary_df = salary_df[(salary_df['min_salary'].notna()) | (salary_df['max_salary'].notna())]
        
        if len(salary_df) == 0:
            logger.warning("Aucune donnée de salaire valide trouvée")
            return None
        
        # Calculer le salaire moyen pour chaque offre
        salary_df['avg_salary'] = salary_df[['min_salary', 'max_salary']].mean(axis=1)
        
        # Créer une colonne pour le type de contrat (pour la coloration)
        if 'contract_type_std' in salary_df.columns:
            # Limiter aux types de contrats les plus courants pour la lisibilité
            top_contracts = salary_df['contract_type_std'].value_counts().nlargest(5).index
            salary_df = salary_df[salary_df['contract_type_std'].isin(top_contracts)]
        
        # Créer la figure
        plt.figure(figsize=(12, 8))
        
        # Créer plusieurs visualisations
        plt.subplot(2, 2, 1)
        sns.histplot(data=salary_df, x='avg_salary', bins=30, kde=True)
        plt.title('Distribution des salaires moyens')
        plt.xlabel('Salaire moyen')
        plt.ylabel('Nombre d\'offres')
        
        # Distribution des salaires par type de contrat
        if 'contract_type_std' in salary_df.columns:
            plt.subplot(2, 2, 2)
            sns.boxplot(data=salary_df, x='contract_type_std', y='avg_salary')
            plt.title('Distribution des salaires par type de contrat')
            plt.xlabel('Type de contrat')
            plt.ylabel('Salaire moyen')
            plt.xticks(rotation=45)
        
        # Salaires min et max
        plt.subplot(2, 2, 3)
        sns.histplot(data=salary_df, x='min_salary', color='blue', alpha=0.5, label='Min', bins=20)
        sns.histplot(data=salary_df, x='max_salary', color='red', alpha=0.5, label='Max', bins=20)
        plt.title('Distribution des salaires minimum et maximum')
        plt.xlabel('Salaire')
        plt.ylabel('Nombre d\'offres')
        plt.legend()
        
        # Écart salarial
        salary_df['salary_gap'] = salary_df['max_salary'] - salary_df['min_salary']
        plt.subplot(2, 2, 4)
        sns.histplot(data=salary_df, x='salary_gap', bins=20, kde=True)
        plt.title('Écart entre salaire minimum et maximum')
        plt.xlabel('Écart salarial')
        plt.ylabel('Nombre d\'offres')
        
        # Ajuster la mise en page
        plt.tight_layout()
        
        # Sauvegarder la figure
        os.makedirs("data/analysis/visualizations", exist_ok=True)
        output_path = "data/analysis/visualizations/salary_analysis.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Analyse des salaires terminée, visualisation sauvegardée: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des salaires: {e}")
        return None

def analyze_salary_by_technology(df):
    """
    Analyse les salaires en fonction des technologies demandées.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
        
    Returns:
        str: Chemin vers la visualisation générée
    """
    try:
        logger.info("Analyse des salaires par technologie...")
        
        # Créer une copie du DataFrame pour éviter les modifications en place
        tech_salary_df = df.copy()
        
        # Vérifier si les colonnes nécessaires existent
        tech_columns = ['has_python', 'has_java', 'has_javascript', 'has_sql', 'has_aws', 'has_machine_learning']
        missing_columns = [col for col in tech_columns if col not in tech_salary_df.columns]
        
        if missing_columns:
            logger.warning(f"Colonnes de technologies manquantes: {missing_columns}")
            # Utiliser uniquement les colonnes disponibles
            tech_columns = [col for col in tech_columns if col in tech_salary_df.columns]
        
        if not tech_columns:
            logger.warning("Aucune colonne de technologie trouvée")
            return None
        
        # Filtrer les lignes avec des valeurs de salaire valides
        tech_salary_df = tech_salary_df[(tech_salary_df['min_salary'].notna()) | (tech_salary_df['max_salary'].notna())]
        
        if len(tech_salary_df) == 0:
            logger.warning("Aucune donnée de salaire valide trouvée")
            return None
        
        # Calculer le salaire moyen pour chaque offre
        tech_salary_df['avg_salary'] = tech_salary_df[['min_salary', 'max_salary']].mean(axis=1)
        
        # Créer la figure
        plt.figure(figsize=(12, 8))
        
        # Préparer les données pour la visualisation
        tech_salary_data = []
        
        for tech_col in tech_columns:
            # Extraire le nom de la technologie (sans le préfixe 'has_')
            tech_name = tech_col.replace('has_', '').replace('_', ' ').title()
            
            # Calculer le salaire moyen pour les offres avec cette technologie
            with_tech = tech_salary_df[tech_salary_df[tech_col] == True]['avg_salary'].mean()
            
            # Calculer le salaire moyen pour les offres sans cette technologie
            without_tech = tech_salary_df[tech_salary_df[tech_col] == False]['avg_salary'].mean()
            
            # Ajouter à la liste des données
            tech_salary_data.append({
                'Technology': tech_name,
                'With': with_tech,
                'Without': without_tech,
                'Difference': with_tech - without_tech,
                'Ratio': with_tech / without_tech if without_tech > 0 else 0
            })
        
        # Convertir en DataFrame
        tech_salary_df = pd.DataFrame(tech_salary_data)
        
        # Trier par différence de salaire
        tech_salary_df = tech_salary_df.sort_values(by='Difference', ascending=False)
        
        # Créer le graphique à barres
        ax = plt.subplot(1, 1, 1)
        
        # Largeur des barres
        bar_width = 0.35
        
        # Positions des barres
        r1 = np.arange(len(tech_salary_df))
        r2 = [x + bar_width for x in r1]
        
        # Créer les barres
        ax.bar(r1, tech_salary_df['With'], color='#5DA5DA', width=bar_width, edgecolor='grey', label='Avec la technologie')
        ax.bar(r2, tech_salary_df['Without'], color='#F15854', width=bar_width, edgecolor='grey', label='Sans la technologie')
        
        # Ajouter les étiquettes, le titre et la légende
        ax.set_xlabel('Technologie')
        ax.set_ylabel('Salaire moyen')
        ax.set_title('Comparaison des salaires moyens selon les technologies requises')
        ax.set_xticks([r + bar_width/2 for r in range(len(tech_salary_df))])
        ax.set_xticklabels(tech_salary_df['Technology'])
        plt.xticks(rotation=45)
        ax.legend()
        
        # Ajouter les valeurs sur les barres
        for i, (with_val, without_val) in enumerate(zip(tech_salary_df['With'], tech_salary_df['Without'])):
            plt.text(i, with_val + 500, f'{with_val:.0f}', ha='center', va='bottom', fontsize=9)
            plt.text(i + bar_width, without_val + 500, f'{without_val:.0f}', ha='center', va='bottom', fontsize=9)
        
        # Ajuster la mise en page
        plt.tight_layout()
        
        # Sauvegarder la figure
        os.makedirs("data/analysis/visualizations", exist_ok=True)
        output_path = "data/analysis/visualizations/salary_by_technology.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Analyse des salaires par technologie terminée, visualisation sauvegardée: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des salaires par technologie: {e}")
        return None
