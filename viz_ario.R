################################################################################
# viz_ario.R
# RÔLE : Graphiques ARIO-inventory (trajectoires de production par secteur et
#        province, pertes indirectes, heatmap de la matrice Z).
#
# PEUT TOURNER SANS RELANCER LES SCRIPTS PRÉCÉDENTS si les .rds sont à jour.
#
# RELANCER 05_ario.R avant ce script si :
#   → les paramètres ARIO ont changé (ARIO_PSI, ARIO_ALPHA_MAX,
#     ARIO_TAU_ALPHA, ARIO_INV_DUREE_JOURS, ARIO_HORIZON_JOURS)
#   → le scénario de perturbation a changé (relancer 04_vulnerabilite.R d'abord)
#   → les flux gravitaires ont changé (relancer 03_transport.R d'abord)
#
# RELANCER 01 → 02 → 03 → 04 → 05 en séquence complète si :
#   → le réseau physique a changé (nouveau PBF)
#
# FICHIERS LUS : persist_geodata.rds, persist_entreposages.rds,
#                persist_vulnerabilite.rds, persist_ario.rds
################################################################################