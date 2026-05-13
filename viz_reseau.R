################################################################################
# viz_reseau.R
# RÔLE : Cartes du réseau routier, coûts, pentes, démographie, RWI.
#
# PEUT TOURNER SANS RELANCER LES SCRIPTS PRÉCÉDENTS si les .rds sont à jour.
#
# RELANCER 01_reseau.R avant ce script si :
#   → le fichier PBF a changé (nouveau téléchargement OSM)
#   → de nouvelles zones d'entrepôt ont été ajoutées / modifiées
#   → les données WorldPop, NISR ou RPHC5 ont été mises à jour
#   → les paramètres BUFFER_DEMO_M, BUFFER_RWI_M ou BUFFER_ENTREPOT_M ont changé
#   → les paramètres K_RWI_TAILLE, ALPHA_LOG_POP ou POIDS_PROFIL_EMPLOI_RPHC5 ont changé
#
# RELANCER 02_couts.R avant ce script si :
#   → les paramètres de flotte (params_flotte, vitesses_flotte, facteurs_pente)
#     ont changé
#   → les valeurs VEHICULE_REFERENCE ou TONNES_PAR_musd ont changé
#   → le DEM (pentes) a été recalculé
#
# FICHIERS LUS : persist_geodata.rds, persist_reseau_base.rds (pour carte III),
#                persist_reseau_couts.rds, persist_entreposages.rds
################################################################################