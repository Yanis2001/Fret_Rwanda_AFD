################################################################################
# viz_vulnerabilite.R
# RÔLE : Cartes de vulnérabilité (réseau dégradé, criticité, détours,
#        report modal) et graphiques de distribution des surcoûts.
#
# PEUT TOURNER SANS RELANCER LES SCRIPTS PRÉCÉDENTS si les .rds sont à jour.
#
# RELANCER 04_vulnerabilite.R avant ce script si :
#   → le scénario a changé (NOM_SCENARIO, OSM_IDS_PERTURBES_MANUEL,
#     CENTRE_PERTURBATION_*, RAYON_PERTURBATION_M, SEUIL_RISQUE_RASTER)
#   → DUREE_JOURS ou TYPE_EVENEMENT ont changé
#   → N_TOP_ARETES_CRITIQUES ou SEUIL_PAIRES_CRITICITE ont changé
#   → les flux de fret (persist_flux_fret.rds) ont changé
#     → dans ce cas relancer aussi 03_transport.R avant 04_vulnerabilite.R
#
# RELANCER 02_couts.R + 03_transport.R + 04_vulnerabilite.R si :
#   → le réseau routier lui-même a changé (nouveau PBF, nouvelles corrections)
#
# FICHIERS LUS : persist_geodata.rds, persist_entreposages.rds,
#                persist_reseau_fret.rds, persist_flux_fret.rds,
#                persist_vulnerabilite.rds
################################################################################