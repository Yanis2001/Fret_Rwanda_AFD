################################################################################
# viz_fret.R
# RÔLE : Cartes et graphiques des flux de fret (trafic, répartition modale,
#        composition sectorielle, heatmaps, Sankey, secteur dominant).
#
# PEUT TOURNER SANS RELANCER LES SCRIPTS PRÉCÉDENTS si les .rds sont à jour.
#
# RELANCER 01_reseau.R + 02_couts.R + 03_transport.R avant ce script si :
#   → les paramètres BETA_SECTEUR ont changé (sensibilité gravitaire)
#   → la matrice A ou production_totale ont changé (table IO)
#   → PART_ECHANGEABLE ou PART_DEMANDE_FINALE ont changé
#   → SEUIL_FLUX_TONNES a changé (filtre affectation)
#   → de nouvelles zones d'entrepôt ont été ajoutées ou retirées
#   → les profils PROFILS_OFFRE ou PROFILS_DEMANDE ont été modifiés
#   → POIDS_PROFIL_EMPLOI_RPHC5 ou K_RWI_OFFRE ont changé
#
# RELANCER uniquement 03_transport.R (sans 01 ni 02) si :
#   → seuls les paramètres gravitaires (BETA, PART_*) ont changé
#     et que le réseau physique est inchangé
#
# FICHIERS LUS : persist_geodata.rds, persist_entreposages.rds,
#                persist_flux_fret.rds, persist_reseau_fret.rds
################################################################################