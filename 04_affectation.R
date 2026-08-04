################################################################################
# 04_affectation.R
# RÔLE : Affectation All-or-Nothing du fret sur le réseau multi-modal
#        (équilibre BPR/MSA si congestion activée), calcul des émissions et de
#        la saturation par arête, puis exports et sauvegardes des résultats.
#        Ce module reprend l'ancienne PARTIE VIII de 03_transport.R, désormais
#        isolée pour clarifier le pipeline (03 = modèle éco + gravitaire,
#        04 = affectation réseau + résultats).
# ENTRÉES  : persist_reseau_couts.rds, persist_graphe_mm.rds,
#            persist_mapping_mm.rds, persist_entreposages.rds,
#            persist_flux_fret.rds (+ affectation_cache.rds)
# SORTIES  : persist_reseau_fret.rds + exports CSV/GeoPackage
# DÉPEND DE : 00_parametres.R, 01_reseau.R, 02_couts.R, 03_transport.R
################################################################################

source("00_parametres.R")

# Affectation du fret à l'équilibre (BPR/MSA) : SOURCE UNIQUE partagée avec
# 05_vulnerabilite.R (affecter_equilibre_msa / preparer_congestion).
source("outils_affectation_equilibre.R")

cat("=== Chargement des objets amont (réseau, graphe, flux gravitaires) ===\n")

# Mêmes entrées que 03_transport.R (réseau de coûts, graphe multimodal, mappings,
# entreposages), auxquelles s'ajoute persist_flux_fret.rds qui contient les flux
# gravitaires sectoriels et la matrice OD en tonnes produits par 03.
.ent  <- readRDS(PERSIST_ENTREPOSAGES)
.res  <- readRDS(PERSIST_RESEAU_COUTS)
.mm   <- readRDS(PERSIST_GRAPHE_MM)
.map  <- readRDS(PERSIST_MAPPING_MM)
.flux <- readRDS(PERSIST_FLUX_FRET)

# noeuds_entreposage, n_warehouses, warehouse_nodes_base, etc.
list2env(.ent, envir = .GlobalEnv)
reseau             <- .res$reseau
n_noeuds           <- .mm$n_noeuds
n_vehicules        <- .mm$n_vehicules
n_aretes_physiques <- length(.map$lookup_type[.map$lookup_type == "route"]) / .mm$n_vehicules

# Recharger le graphe multimodal dans l'environnement dédié aux gros objets
stocker_lourd("graphe_multimodal", .mm$graphe_multimodal)

# Vecteurs de correspondance arête multimodale ↔ arête physique / véhicule
mapping_aretes_mm <- .map$mapping_aretes_mm
lookup_type       <- .map$lookup_type
lookup_physique   <- .map$lookup_physique
lookup_vehicule   <- .map$lookup_vehicule
max_idx_mm        <- .map$max_idx_mm
poids_mm          <- .map$poids_mm

# Flux issus du modèle gravitaire (03) : flux_gravitaire[[s]] (tonnes par secteur,
# n_warehouses × n_warehouses) et flux_tonnes_total (OD agrégée et projetée RoW).
# list2env charge aussi les matrices éco/gravitaires (flux_total, A, recap_io,
# recap_zones…) réutilisées par les exports de la PARTIE VIII ci-dessous.
list2env(.flux, envir = .GlobalEnv)

rm(.ent, .res, .mm, .map, .flux)

# Redéfinition locale de node_multi avec n_noeuds chargé
node_multi <- function(v_idx, n_id) as.integer((v_idx - 1L) * n_noeuds + n_id)

cat("✓ Objets chargés\n\n")

################################################################################
# PARTIE VIII — AFFECTATION DU FRET ET RÉSULTATS
# Affecte chaque flux OD (déjà en tonnes) au chemin optimal du graphe
# multi-modal (All-or-Nothing), puis produit les visualisations et exports.
################################################################################

# ==============================================================================
# VIII.1 : Conversion et affectation All-or-Nothing
# VERSION OPTIMISÉE MÉMOIRE pour Onyxia SSP Cloud
#
#   1. Nettoyage explicite des objets intermédiaires devenus inutiles
#   2. gc() plus agressif (à chaque itération au lieu de toutes les 10)
#   3. Remplacement de shortest_paths(output="epath") par une reconstruction
#      plus légère via distances() + vecteur prédécesseurs
#   4. Vectorisation de l'affectation des volumes (suppression boucle interne)
#   5. Monitoring de la RAM pour détecter les problèmes en amont
# ==============================================================================

# ── ÉTAPE 0 : Nettoyage agressif de la mémoire avant de commencer ─────────────
# On supprime tous les objets intermédiaires qui ne serviront plus.

cat("── Nettoyage mémoire avant Partie VIII.1 ──────────────────────────────\n")

objets_a_supprimer <- c(
  # Réseaux intermédiaires de la Partie III
  "reseau_lisse", "reseau_subdivise", "graphe_base", "graphe_igraph",
  "aretes_lisse", "aretes_perdues", "aretes_avec_geom", "aretes_diag",
  "aretes_check", "aretes_centroides", "noeuds_lisse", "noeuds_hors_geante",
  "noeuds_sf", "composantes_finales", "comp_lisse",
  
  # Données brutes OSM
  "routes_raw", "routes_attrs_raw", "routes_clean",
  "attrs_df", "attrs_clean", "landuse_test", "place_test", "villes_raw",
  
  # Couches géographiques lourdes
  "dem", "zones_urbaines_union",
  "zones_urbaines", "zones_industrielles", "zones_retail",
  "centroides_indus", "centroides_indus_sf", "centroides_retail",
  "centroides_retail_sf", "tous_existants", "tous_existants2",
  "bbox_poly", "emprise_sf", "emprise_points",
  
  # Grandes tables intermédiaires
  "edges_intra", "edges_transb", "all_edges_mm", "mapping_aretes_mm",
  "vertices_mm", "aretes_base_tbl", "aretes_df", "aretes_ref",
  "couts_wide", "couts_veh", "ratio_df", "ratio_moyen_df",
  
  # Objets tmap (peuvent être très lourds)
  "carte_verif_routes", "carte_aretes_perdues", "carte_ratio",
  "carte_ratio_moyen", "carte_pentes", "cartes_vehicules",
  "reseau_tmp", "reseau_ratio", "reseau_ratio_moyen",
  
  # Caches déjà intégrés
  "pentes_df", "cache", "cache_od", "cache_lu",
  
  # Diagnostics
  "distrib_road_type", "distrib_surface", "distrib_taille",
  "distrib_province", "centroides_perdues", "provinces_join",
  "verif", "zero_duckdb", "na_duckdb", "arete_ids_duckdb"
)

# On supprime uniquement les objets qui existent (évite les warnings)
objets_existants <- objets_a_supprimer[
  sapply(objets_a_supprimer, exists, envir = .GlobalEnv)
]
if (length(objets_existants) > 0) {
  rm(list = objets_existants, envir = .GlobalEnv)
  cat("  Supprimés :", length(objets_existants), "objets\n")
}

# Fonction utilitaire pour afficher la RAM utilisée
afficher_ram <- function(etape = "") {
  ram_mb <- round(sum(gc()[, 2]), 1)
  cat("  [RAM ", etape, "] ", ram_mb, " MB utilisés\n", sep = "")
}

# Double gc() pour forcer la libération complète 
invisible(gc(full = TRUE))
invisible(gc(full = TRUE))
afficher_ram("après nettoyage")

# ── ÉTAPE 1 : Pré-filtrage des paires OD à traiter ───────────────────────────
# flux_tonnes_total (n_warehouses × n_warehouses) a déjà été construit et projeté
# en VII.5 (flux RoW injectés sur les postes frontières optimaux).
# On filtre directement les paires actives à partir de cette matrice.
# Plutôt que de boucler sur n_warehouses² paires puis de filtrer par seuil,
# on construit d'abord la liste des paires pertinentes. Ça permet aussi
# d'avoir une barre de progression exacte et de mieux répartir les gc().

paires_actives <- which(flux_tonnes_total > SEUIL_FLUX_TONNES, arr.ind = TRUE)
paires_actives <- paires_actives[paires_actives[, 1] != paires_actives[, 2], ]

# Cache isolé par scénario (cf. DIR_CACHE_SCENARIO dans 00_parametres.R) :
# l'affectation dépend des coûts et de la matrice OD, donc des paramètres testés.
CACHE_AFFECTATION      <- file.path(DIR_CACHE_SCENARIO, "affectation_cache.rds")
cache_affectation_valide <- FALSE

# Empreinte (hash) des entrées du calcul. Si l'une d'entre elles change,
# le hash change, et le cache est automatiquement rejeté.
# digest::digest() produit une empreinte stable de n'importe quel objet R :
# deux objets identiques donnent le même hash, deux objets différents donnent
# des hashs différents. On combine plusieurs entrées dans une liste.
if (!requireNamespace("digest", quietly = TRUE)) {
  install.packages("digest")
}

empreinte_entrees <- digest::digest(
  list(
    flux_tonnes_total = flux_tonnes_total,    # dépend de BETA, TONNES, DEMANDE_FINALE_SAM, RoW
    # Les matrices SECTORIELLES sont ce qui détermine réellement les volumes
    # affectés : flux_tonnes_total ne sert qu'à sélectionner les paires actives.
    # Sans elles dans l'empreinte, une modification de la ventilation sectorielle
    # (ou de la projection RoW) laisserait le cache d'affectation valide à tort.
    flux_gravitaire   = flux_gravitaire,
    seuil             = SEUIL_FLUX_TONNES,
    n_aretes          = n_aretes_physiques,
    n_warehouses      = n_warehouses,
    n_vehicules       = n_vehicules,
    n_aretes_mm       = igraph::ecount(recuperer_lourd("graphe_multimodal")),
    # Paramètres de congestion : tout changement invalide le cache d'affectation
    # (capacités, BPR, MSA, conversion PCU, équivalents PCU, interrupteur global).
    congestion        = CONGESTION,
    capacites_route   = capacites_route_df,
    bpr               = c(BPR_ALPHA, BPR_BETA),
    msa               = c(MSA_MAX_ITER, MSA_TOL),
    conversion_pcu    = c(TAUX_CHARGEMENT, JOURS_TRAFIC_AN),
    facteur_pcu       = params_flotte_df$facteur_pcu,
    # Paramètres EOQ : taille d'envoi q* de Wilson + ventilation du coût logistique
    # (comptabilité). Tout changement invalide le cache d'affectation.
    eoq               = list(
      EOQ_REMPLISSAGE_MIN, HEURES_PAR_AN,
      TAUX_DETENTION_STOCK, VALEUR_RWF_PAR_TONNE,
      params_flotte_df$cout_chargement_rwf,
      params_flotte_df$cout_dechargement_rwf,
      params_flotte_df$capacite_tonnes,
      params_flotte_df$facteur_pcu
    )
  ),
  algo = "xxhash64"
)

if (file.exists(CACHE_AFFECTATION)) {
  
  cat("── Tentative de chargement du cache d'affectation ─────────────────────\n")
  cache_aff <- readRDS(CACHE_AFFECTATION)
  
  # Le cache est valide SEULEMENT si :
  #   1. l'empreinte correspond (entrées inchangées)
  #   2. le tableau 3D est présent 
  # Cette deuxième condition évite de charger un cache produit par une version
  # antérieure du script qui ne sauvegardait pas la dimension sectorielle.
  if (!is.null(cache_aff$empreinte) &&
      cache_aff$empreinte == empreinte_entrees &&
      !is.null(cache_aff$volume_trafic_mm_s)) {
    
    volume_trafic_mm_s       <- cache_aff$volume_trafic_mm_s
    paires_traitees          <- cache_aff$paires_traitees
    paires_non_connectees    <- cache_aff$paires_non_connectees
    # Comptabilité EOQ (composantes de coût par secteur) ; NULL si cache produit
    # par une version antérieure.
    compta_eoq               <- cache_aff$compta_eoq
    cache_affectation_valide <- TRUE
    
    cat("  ✓ Cache d'affectation valide\n")
    cat("    Paires traitées      :", paires_traitees, "\n")
    cat("    Paires non connectées:", paires_non_connectees, "\n")
    cat("    → Affectation ignorée (~2-5 min gagnées)\n\n")
    
  } else {
    cat("  ⚠ Cache d'affectation invalide ou obsolète — recalcul\n\n")
  }
}

# ── Diagnostic du filtre par seuil ────────────────────────────────────────────
# On calcule le nombre de paires exclues par le seuil SEUIL_FLUX_TONNES
# pour vérifier que le filtre n'élimine pas trop de flux économiquement
# significatifs. Une paire exclue = flux trop faible pour être affecté
# au réseau routier, mais qui contribue quand même au tonnage total.

# Toutes les paires hors diagonale (i ≠ j), sans filtre de seuil
toutes_paires <- which(flux_tonnes_total > 0, arr.ind = TRUE)
toutes_paires <- toutes_paires[toutes_paires[, 1] != toutes_paires[, 2], ]

# Paires sous le seuil = toutes les paires MOINS celles retenues
n_paires_sous_seuil <- nrow(toutes_paires) - nrow(paires_actives)

# Tonnage total des paires exclues (pour juger de leur poids économique)
tonnage_exclu <- sum(flux_tonnes_total[toutes_paires]) -
  sum(flux_tonnes_total[paires_actives])
tonnage_total_avant_filtre <- sum(flux_tonnes_total[toutes_paires])

cat("── Diagnostic du filtre seuil (", SEUIL_FLUX_TONNES, "tonnes) ──────────\n")
flush.console()
cat("  Paires totales (flux > 0)  :", nrow(toutes_paires), "\n")
flush.console()
cat("  Paires retenues (> seuil)  :", nrow(paires_actives), "\n")
flush.console()
cat("  Paires exclues (< seuil)   :", n_paires_sous_seuil,
    "(", round(n_paires_sous_seuil / nrow(toutes_paires) * 100, 1), "%)\n")
flush.console()
cat("  Tonnage exclu              :", format(round(tonnage_exclu), big.mark = " "),
    "tonnes (", round(tonnage_exclu / tonnage_total_avant_filtre * 100, 2),
    "% du total)\n\n")
flush.console()
cat("  Paires OD à traiter :", format(nrow(paires_actives), big.mark = " "),
    "(sur", format(n_warehouses^2 - n_warehouses, big.mark = " "),
    "possibles)\n\n")
flush.console()

# ══════════════════════════════════════════════════════════════════════════════
# PRÉPARATION DE LA CONGESTION (capacité par type de route + conversion PCU)
# Ces objets servent à la fois dans la boucle d'équilibre (en cas de recalcul)
# ET au calcul du taux de saturation final (dans tous les cas, même au
# chargement depuis le cache). On les construit donc ici, hors du bloc
# conditionnel d'affectation.
# ══════════════════════════════════════════════════════════════════════════════

# road_type de chaque arête PHYSIQUE, dans l'ordre des indices physiques
# (1..n_aretes_physiques = ordre des arêtes de `reseau`, identique au tableau 3D).
# Objets invariants de la congestion (capacités PCU par arête, conversion PCU par
# véhicule) — construits par preparer_congestion() (fonction partagée avec 05).
# Calculés DANS TOUS LES CAS (même au chargement du cache) car ils servent au
# taux de saturation final calculé plus bas.
.prep_cong <- preparer_congestion()
C_phys <- .prep_cong$C_phys   # capacité PCU/jour par arête physique
conv_v <- .prep_cong$conv_v   # conversion tonnes/an → PCU/jour, par véhicule

# Diagnostic de calibration : un road_type présent dans le réseau mais absent de
# capacites_route_df reçoit la capacité minimale (déjà appliquée dans
# preparer_congestion) ; on le signale explicitement pour que ce trou soit visible.
types_sans_capacite <- setdiff(unique(.prep_cong$road_type_phys), capacites_route_df$road_type)
if (length(types_sans_capacite) > 0) {
  warning(sprintf(
    "Types de route sans capacite definie (capacite min appliquee par defaut) : %s",
    paste(types_sans_capacite, collapse = ", ")
  ))
}

# Nombre d'itérations d'équilibre : 1 seule passe si la congestion est désactivée
# (on tombe alors sur l'affectation All-or-Nothing).
n_iter_msa <- if (isTRUE(CONGESTION)) MSA_MAX_ITER else 1L
cat("── Congestion :", if (isTRUE(CONGESTION)) "ACTIVÉE" else "désactivée",
    "— itérations d'équilibre max :", n_iter_msa, "──────\n\n")

# ══════════════════════════════════════════════════════════════════════════════
# BLOC CONDITIONNEL : l'affectation ne s'exécute que si pas de cache valide
# ══════════════════════════════════════════════════════════════════════════════
if (!cache_affectation_valide) {

  # ── Affectation à l'équilibre (fonction partagée avec 05_vulnerabilite.R) ─────
  # Rejoue la boucle MSA/BPR (affectation AON sectorielle + EOQ, moyennage 1/n)
  # sur le réseau INTACT (aucune arête bloquée). Voir outils_affectation_equilibre.R :
  # SOURCE UNIQUE de la méthode d'équilibre, également utilisée par le 05.
  res_aff <- affecter_equilibre_msa(integer(0))

  volume_trafic_mm_s    <- res_aff$volume_trafic_mm_s    # charge d'équilibre 3D [arête,véhicule,secteur]
  compta_eoq            <- res_aff$compta_eoq            # ventilation des coûts logistiques par secteur
  paires_traitees       <- res_aff$paires_traitees
  paires_non_connectees <- res_aff$paires_non_connectees

  # ── SAUVEGARDE DU CACHE ───────────────────────────────────────────────────────
  cat("=== Sauvegarde du cache d'affectation ===\n")
  
  saveRDS(
    list(
      volume_trafic_mm_s    = volume_trafic_mm_s,
      paires_traitees       = paires_traitees,
      paires_non_connectees = paires_non_connectees,
      compta_eoq            = compta_eoq,   # ventilation des coûts logistiques
      empreinte             = empreinte_entrees,
      date_creation         = Sys.time()
    ),
    CACHE_AFFECTATION
  )
  
  cat("  ✓ Cache sauvegardé :", CACHE_AFFECTATION, "\n")
  cat("  → Au prochain lancement (sans changement), l'affectation sera ignorée\n\n")
  
}  # fin du if (!cache_affectation_valide)

# ══════════════════════════════════════════════════════════════════════════════
# Reconstruction des matrices agrégées (exécutée DANS TOUS LES CAS)
# ══════════════════════════════════════════════════════════════════════════════
# Que l'on vienne du cache ou d'un recalcul complet, on dispose maintenant
# de volume_trafic_mm_s (le tableau 3D complet [arête, véhicule, secteur]).
# On en dérive les agrégations nécessaires pour la suite du script :
#   - volume_trafic_mm   : total par (arête, véhicule), sommé sur secteurs
#   - volume_trafic      : total par arête, sommé sur véhicules ET secteurs
#   - volume_par_secteur : total par (arête, secteur), sommé sur véhicules

# apply(X, MARGIN, FUN) : applique une fonction sur certaines dimensions d'un tableau.
# MARGIN = c(1, 2) signifie "pour chaque combinaison des dimensions 1 et 2,
# applique FUN sur la dimension restante (ici la 3e = secteurs)".
# Résultat : une matrice 2D [arête, véhicule] avec la somme sur tous les secteurs.
volume_trafic_mm  <- apply(volume_trafic_mm_s, c(1, 2), sum)

# rowSums() : somme sur les colonnes, pour chaque ligne.
# Appliqué à volume_trafic_mm (matrice [arête, véhicule]), ça donne le
# total par arête, tous véhicules confondus.
volume_trafic     <- rowSums(volume_trafic_mm)

# Cette fois on somme sur la dimension 2 (véhicules) en gardant les
# dimensions 1 (arêtes) et 3 (secteurs) → matrice [arête, secteur].
volume_par_secteur <- apply(volume_trafic_mm_s, c(1, 3), sum)

# Conversion en data.frame pour l'export CSV en Partie VIII.3.
# Les noms de colonnes sont préfixés par "vol_t_" pour indiquer "volume en tonnes".
volume_par_secteur_df <- as.data.frame(volume_par_secteur)
colnames(volume_par_secteur_df) <- paste0("vol_t_", SECTEURS)

# ── Charge physique (PCU/jour) et taux de saturation par arête ────────────────
# Calculé dans TOUS les cas (recalcul OU chargement du cache). Remplissage FIXE :
# conv_v convertit le tonnage par arête×véhicule (volume_trafic_mm) en PCU/jour, le
# produit matriciel sommant sur les véhicules.
# saturation_phys = V/C : >1 signale un tronçon surchargé.
charge_pcu_jour <- as.vector(volume_trafic_mm %*% conv_v)
saturation_phys <- charge_pcu_jour / C_phys

# ── Calcul des émissions totales affectées sur le réseau ──────────────────────
# On calcule les émissions absolues (CO2, NOx, PM2.5) générées par l'ensemble
# des flux de fret modélisés, arête par arête.
#
# Principe : pour chaque arête, on multiplie son intensité d'émission
# par tonne-km par le volume de trafic affecté et par la longueur de l'arête.
#   Émissions_arête = intensité_par_tkm × volume_tonnes × length_km
#
# co2_kg_par_tkm, nox_g_par_tkm et pm25_g_par_tkm sont les intensités
# unitaires calculées en Partie V.1 et intégrées dans reseau.
# volume_trafic est le vecteur de tonnes affectées par arête (calculé juste
# au-dessus via rowSums()).
# length_km est la longueur de chaque arête en kilomètres.

# Récupération des attributs d'émissions et de longueur pour toutes les arêtes.
# On extrait ces trois colonnes depuis reseau en un seul appel pour
# éviter de réactiver le réseau plusieurs fois.
aretes_emissions_base <- reseau %>%
  activate("edges") %>%
  as_tibble() %>%
  select(length_km, co2_kg_par_tkm, nox_g_par_tkm, pm25_g_par_tkm)

# Calcul vectorisé des émissions totales par arête.
# replace_na(..., 0) : les arêtes sans données d'émissions (ex : arêtes
# topologiques créées lors de la subdivision, arêtes de longueur nulle)
# contribuent 0 au total au lieu de propager des NA dans toute la colonne.
# Le produit est vectorisé : R multiplie élément par élément les trois
# vecteurs de même longueur (n_aretes lignes chacun).
emissions_co2_aretes  <- replace_na(aretes_emissions_base$co2_kg_par_tkm,  0) *
  volume_trafic *
  replace_na(aretes_emissions_base$length_km, 0)

emissions_nox_aretes  <- replace_na(aretes_emissions_base$nox_g_par_tkm,   0) *
  volume_trafic *
  replace_na(aretes_emissions_base$length_km, 0)

emissions_pm25_aretes <- replace_na(aretes_emissions_base$pm25_g_par_tkm,  0) *
  volume_trafic *
  replace_na(aretes_emissions_base$length_km, 0)

# Intégration dans reseau comme attributs des arêtes.
# Les unités sont converties pour rester lisibles dans les exports :
#   CO2  : kg → tonnes  (÷ 1 000) — ordre de grandeur typique : quelques t/arête
#   NOx  : g  → kg      (÷ 1 000) — ordre de grandeur typique : quelques kg/arête
#   PM2.5: g  → kg      (÷ 1 000) — ordre de grandeur typique : < 1 kg/arête
#     (les PM2.5 sont émises en quantités bien inférieures au NOx,
#      d'où l'importance de garder la colonne en kg et non en tonnes
#      pour ne pas afficher des valeurs trop proches de zéro)
reseau <- reseau %>%
  activate("edges") %>%
  mutate(
    emissions_co2_t    = emissions_co2_aretes  / 1000,
    emissions_nox_kg   = emissions_nox_aretes  / 1000,
    emissions_pm25_kg  = emissions_pm25_aretes / 1000
  )

# Rapport global d'émissions (pour le log console).
# Ces totaux agrègent toutes les arêtes du réseau et donc tous les flux OD
# modélisés. Ils constituent un ordre de grandeur de l'empreinte carbone
# et polluante du fret routier dans le modèle.
co2_total_reseau_t   <- sum(emissions_co2_aretes,  na.rm = TRUE) / 1000
nox_total_reseau_kg  <- sum(emissions_nox_aretes,  na.rm = TRUE) / 1000
pm25_total_reseau_kg <- sum(emissions_pm25_aretes, na.rm = TRUE) / 1000

cat("── Émissions totales du fret modélisé ──────────────────────────────\n")
cat("  CO2   total :", format(round(co2_total_reseau_t),   big.mark = " "), "tonnes\n")
cat("  NOx   total :", format(round(nox_total_reseau_kg),  big.mark = " "), "kg\n")
cat("  PM2.5 total :", format(round(pm25_total_reseau_kg), big.mark = " "), "kg\n\n")

# ── Sanity check : le tonnage affecté doit être cohérent ──────────────────────
# Note : on somme sur toutes les dimensions de volume_trafic_mm_s (tableau 3D).
# Le tonnage affecté sera plusieurs fois supérieur au tonnage attendu, car
# chaque flux OD est compté sur TOUTES les arêtes de son chemin (un flux
# de 100t qui emprunte 20 arêtes contribue 100×20 = 2000 tonnes-arêtes).
# Ce qui compte, c'est que le ratio soit stable et qu'il n'y ait pas de NA.
tonnage_affecte <- sum(volume_par_secteur)
tonnage_attendu <- sum(flux_tonnes_total[paires_actives])

cat("  Tonnage affecté au réseau (cumulé sur toutes les arêtes) :",
    format(round(tonnage_affecte), big.mark = " "), "tonnes-arêtes\n")
cat("  Tonnage OD attendu (paires > seuil) :",
    format(round(tonnage_attendu), big.mark = " "), "tonnes\n")
cat("  Ratio moyen (≈ longueur moyenne de chemin en arêtes) :",
    round(tonnage_affecte / tonnage_attendu, 1), "\n")

# ══════════════════════════════════════════════════════════════════════════════
# SUITE : calculs rapides toujours exécutés (qu'on ait un cache ou non)
# ══════════════════════════════════════════════════════════════════════════════

# ── Statistiques de répartition modale ────────────────────────────────────────
cat("Répartition modale du trafic (tonnes × km) :\n")

longueurs_km <- reseau %>%
  activate("edges") %>%
  as_tibble() %>%
  pull(length_km)

tkm_total <- sum(volume_trafic * longueurs_km, na.rm = TRUE)

for (v in seq_len(n_vehicules)) {
  veh_nom <- VEHICULES_IDS$nom[v]
  tkm_veh <- sum(volume_trafic_mm[, v] * longueurs_km, na.rm = TRUE)
  pct     <- if (tkm_total > 0) round(tkm_veh / tkm_total * 100, 1) else 0
  cat("  ", veh_nom, ":", format(round(tkm_veh), big.mark = " "),
      "t×km (", pct, "%)\n")
}
cat("\n")

# ── Étape 6 : Intégration des volumes au réseau ───────────────────────────────
reseau <- reseau %>%
  activate("edges") %>%
  mutate(
    volume_tonnes       = volume_trafic,
    volume_camionnette  = volume_trafic_mm[, "camionnette"],
    volume_camion_moyen = volume_trafic_mm[, "camion_moyen"],
    volume_camion_lourd = volume_trafic_mm[, "camion_lourd"],
    part_camion_lourd   = if_else(
      volume_tonnes > 0,
      round(volume_camion_lourd / volume_tonnes * 100, 1),
      0
    ),
    classe_trafic = case_when(
      volume_tonnes == 0     ~ "Aucun",
      volume_tonnes < 500    ~ "Très faible",
      volume_tonnes < 5000   ~ "Faible",
      volume_tonnes < 25000  ~ "Moyen",
      volume_tonnes < 100000 ~ "Élevé",
      TRUE                   ~ "Très élevé"
    ),
    classe_trafic = factor(classe_trafic,
                           levels = c("Aucun", "Très faible", "Faible",
                                      "Moyen", "Élevé", "Très élevé")),
    # ── Attributs de congestion ──────────────────────────────────────────────
    # charge_pcu_jour = trafic converti en équivalents voiture/jour (PCU/jour) ;
    # capacite_pcu_jour = capacité du tronçon selon son type de route ;
    # taux_saturation = charge/capacité (>1 = tronçon surchargé) ;
    # classe_saturation = catégorie lisible pour la cartographie des goulots.
    charge_pcu_jour   = charge_pcu_jour,
    capacite_pcu_jour = C_phys,
    taux_saturation   = round(saturation_phys, 3),
    classe_saturation = case_when(
      is.na(taux_saturation) ~ "Inconnu",
      taux_saturation < 0.5  ~ "Fluide",
      taux_saturation < 0.8  ~ "Dense",
      taux_saturation < 1.0  ~ "Proche saturation",
      TRUE                   ~ "Saturé"
    ),
    classe_saturation = factor(classe_saturation,
                               levels = c("Fluide", "Dense", "Proche saturation",
                                          "Saturé", "Inconnu"))
  )

# ── Bilan de saturation du réseau (log console) ───────────────────────────────
# Donne un aperçu rapide des goulots d'étranglement après affectation.
cat("── Saturation du réseau (V/C) ──────────────────────────────────────\n")
cat("  Saturation max          :", round(max(saturation_phys, na.rm = TRUE), 2), "\n")
cat("  Arêtes saturées (V/C>1) :", sum(saturation_phys > 1, na.rm = TRUE),
    "/", length(saturation_phys), "\n")
cat("  Arêtes denses (V/C>0.8) :", sum(saturation_phys > 0.8, na.rm = TRUE), "\n\n")

# Nettoyage final
invisible(gc(full = TRUE))
cat("✓ Partie VIII.1 terminée\n\n")

# Identification des arêtes les plus empruntées
reseau %>%
  activate("edges") %>%
  st_as_sf() %>%
  mutate(arete_idx = row_number()) %>%
  st_drop_geometry() %>%
  select(osm_id, name, road_type, surface, longueur_m, volume_tonnes) %>%
  filter(!is.na(volume_tonnes), volume_tonnes > 0) %>%
  arrange(desc(volume_tonnes)) %>%
  slice_head(n = 20) %>%
  mutate(
    longueur_km   = round(longueur_m / 1000, 2),
    volume_tonnes = round(volume_tonnes)
  ) %>%
  select(osm_id, name, road_type, surface, longueur_km, volume_tonnes) %>%
  print(n = 20)

# ==============================================================================
# TRANSITION VIII.1 → VIII.2
# Recrée les objets de statistiques (volumes_par_zone, stats_trafic) qui
# avaient été supprimés du patch de VIII.1 et qui sont requis en VIII.2.
# ==============================================================================

# ── Statistiques de trafic sur le réseau ──────────────────────────────────────
stats_trafic <- reseau %>%
  activate("edges") %>%
  as_tibble() %>%
  filter(volume_tonnes > 0) %>%
  summarise(
    n_aretes_actives = n(),
    volume_max_t     = max(volume_tonnes),
    volume_moyen_t   = mean(volume_tonnes),
    volume_median_t  = median(volume_tonnes)
  )

cat("Statistiques du trafic fret sur le réseau:\n")
cat("  Arêtes avec trafic  :",
    format(stats_trafic$n_aretes_actives, big.mark = " "), "\n")
cat("  Volume max (arête)  :",
    format(round(stats_trafic$volume_max_t), big.mark = " "), "tonnes\n")
cat("  Volume moyen (actif):",
    format(round(stats_trafic$volume_moyen_t), big.mark = " "), "tonnes\n\n")

# ── Zones les plus actives (origines + destinations cumulées) ─────────────────
# volumes_par_zone est nécessaire en VIII.2 pour dimensionner les points
# des zones sur les cartes (taille_point ∝ log10(offre + demande)).
cat("Activité fret par zone (origines + destinations):\n")

volumes_par_zone <- tibble(
  Zone       = noeuds_entreposage$warehouse_name,
  Type       = noeuds_entreposage$warehouse_type,
  Offre_kt   = round(rowSums(flux_tonnes_total) / 1000, 1),
  Demande_kt = round(colSums(flux_tonnes_total) / 1000, 1)
) %>%
  mutate(Total_kt = Offre_kt + Demande_kt) %>%
  arrange(desc(Total_kt))

print(head(volumes_par_zone, 15))
cat("\n")

cat("✓ Transition VIII.1 → VIII.2 terminée\n\n")


# ==============================================================================
# VIII.3 : Exports finaux
# Exporte toutes les matrices (flux tonnes, offre/demande en mrd RWF, IO)
# en CSV et le réseau complet avec volumes fret en GeoPackage.
# ==============================================================================

cat("Export des données du modèle de fret...\n")

write.csv(as.data.frame(A),
          file.path(DIR_EXPORTS,"table_io_coefficients.csv"),
          row.names = TRUE)
write.csv(recap_io,
          file.path(DIR_EXPORTS,"table_io_recap.csv"),
          row.names = FALSE)
write.csv(as.data.frame(flux_total) %>% rownames_to_column("Zone"),
          file.path(DIR_EXPORTS,"matrice_flux_gravitaire_tonnes.csv"),
          row.names = FALSE)
write.csv(as.data.frame(flux_tonnes_total) %>% rownames_to_column("Zone"),
          file.path(DIR_EXPORTS,"matrice_flux_fret_tonnes.csv"),
          row.names = FALSE)
write.csv(recap_zones,
          file.path(DIR_EXPORTS,"offre_demande_zones.csv"),
          row.names = FALSE)

# ── Export complémentaire : réseau avec volumes fret ──────────────────────────
aretes_fret_export <- reseau %>%
  activate("edges") %>%
  st_as_sf() %>%
  select(osm_id, name, road_type, surface,
         length_km, speed_kmh, cost_per_tkm,
         volume_tonnes, classe_trafic,
         volume_camionnette, volume_camion_moyen, volume_camion_lourd,
         part_camion_lourd)

st_write(aretes_fret_export,
         file.path(DIR_EXPORTS, "reseau_avec_fret.gpkg"),
         delete_dsn = TRUE, quiet = TRUE)
cat("✓ GeoPackage avec volumes fret exporté\n")

# Export du détail sectoriel par arête
# Ce fichier permet de répondre à des questions comme :
# "Quelle part du trafic sur la RN1 est de l'Agriculture ?"
aretes_fret_sectoriel <- aretes_fret_export %>%
  st_drop_geometry() %>%
  bind_cols(volume_par_secteur_df)

write.csv(aretes_fret_sectoriel,
          file.path(DIR_EXPORTS, "volumes_fret_par_secteur.csv"),
          row.names = FALSE)
cat("✓ Export sectoriel par arête sauvegardé\n")

# ── Export de la comptabilité des coûts logistiques (EOQ) ─────────────────────
# Produit DANS TOUS LES CAS : on exporte la ventilation du coût logistique annuel
# par secteur — les 4 composantes (commande, transport, stock cyclique, stock en
# transit), leur total, et la taille d'envoi q* moyenne par jambe (pondérée par le
# flux). (Le test !is.null couvre seulement un ancien cache sans cette table.)
if (!is.null(compta_eoq)) {
  compta_eoq_df <- as.data.frame(compta_eoq) %>%
    rownames_to_column("secteur") %>%
    mutate(
      cout_total_rwf = cout_commande + cout_transport +
                       cout_stock_cyclique + cout_stock_transit,
      # q* moyen par jambe (tonnes) = Σ_jambes(Q·q*) / Σ_jambes(Q) ; NA si aucune
      # jambe comptabilisée. flux_tonnes/flux_x_qopt sont des cumuls PAR JAMBE
      # servant uniquement à cette moyenne — retirés de la sortie finale.
      q_opt_moyen_t  = ifelse(flux_tonnes > 0, flux_x_qopt / flux_tonnes, NA_real_)
    ) %>%
    select(-flux_x_qopt, -flux_tonnes)   # cumuls internes, retirés de la sortie

  write.csv(compta_eoq_df,
            file.path(DIR_EXPORTS, "comptabilite_couts_eoq.csv"),
            row.names = FALSE)
  cat("✓ Comptabilité des coûts logistiques (EOQ) exportée\n")
}

# ==============================================================================
# SAUVEGARDE INTER-SCRIPTS
# ==============================================================================

cat("=== Sauvegarde des objets persistants (04_affectation) ===\n")

# Les flux et matrices gravitaires (persist_flux_fret.rds) sont désormais
# sauvegardés par 03_transport.R, en amont. Ce module ne produit que le réseau
# enrichi des volumes de fret (persist_reseau_fret.rds).
#
# Libération préalable des matrices gravitaires rechargées depuis
# persist_flux_fret.rds (elles ne servent plus après l'affectation) : sans ce
# rm(), elles resteraient en mémoire et augmenteraient le pic RAM du saveRDS
# du réseau qui suit. intersect() évite tout warning si l'une est absente.
rm(list = intersect(
  c("flux_gravitaire", "flux_total", "flux_tonnes_total",
    "offre_zones", "demande_zones", "prod_zones", "dem_zones",
    "e_zones", "m_zones", "offre_total", "demande_total",
    "flux_par_secteur_df", "recap_zones", "A", "recap_io"),
  ls(envir = .GlobalEnv)
), envir = .GlobalEnv)
invisible(gc(full = TRUE))
invisible(gc(full = TRUE))
afficher_ram("avant la sauvegarde du réseau")

# ── Sauvegarde : réseau enrichi avec volumes fret ─────────────────────────────
# On ne sauvegarde que les objets effectivement lus par les scripts en aval
# (viz_fret.R, viz_vulnerabilite.R, 05_vulnerabilite.R).
# volume_trafic_mm_s / volume_trafic / volume_trafic_mm sont intentionnellement
# exclus : aucun script en aval ne les lit depuis ce fichier (ils sont déjà
# intégrés dans les arêtes de reseau ou disponibles via affectation_cache).
saveRDS(
  list(
    reseau         = reseau,
    volume_par_secteur    = volume_par_secteur,
    volume_par_secteur_df = volume_par_secteur_df,
    volumes_par_zone      = volumes_par_zone,
    date_creation         = Sys.time()
  ),
  PERSIST_RESEAU_FRET
)
cat("✓ persist_reseau_fret.rds\n\n")

# ── Nettoyage final explicite ─────────────────────────────────────────────────
# Quand un script R se termine, la session tente de libérer automatiquement
# tous les objets en mémoire. Les gros objets (sfnetworks, igraph, tableaux 3D)
# peuvent provoquer un crash lors de ce nettoyage automatique si la RAM est
# saturée. On les détruit explicitement ici pour prévenir ce crash.
cat("── Nettoyage final ─────────────────────────────────────────────────────\n")

objets_fin <- c(
  "reseau", "volume_trafic_mm_s", "volume_trafic",
  "volume_trafic_mm", "volume_par_secteur", "volume_par_secteur_df",
  "volumes_par_zone", "paires_actives", "aretes_fret_export",
  "aretes_fret_sectoriel", "aretes_emissions_base", "longueurs_km",
  "emissions_co2_aretes", "emissions_nox_aretes", "emissions_pm25_aretes",
  "noms_zones_uniques", "flux_total_long", "od_long"
)
rm(list = intersect(objets_fin, ls(envir = .GlobalEnv)), envir = .GlobalEnv)

# Vider env_lourds : contient le graphe multi-modal (plusieurs centaines de MB)
rm(list = ls(envir = env_lourds), envir = env_lourds)

invisible(gc(full = TRUE))
invisible(gc(full = TRUE))
afficher_ram("fin de script")

cat("✓ Nettoyage terminé — session stable\n")
