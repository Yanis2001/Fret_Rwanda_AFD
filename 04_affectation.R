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

# Double gc() pour forcer la libération complète (premier passage marque,
# deuxième passage collecte réellement)
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

CACHE_AFFECTATION      <- file.path(DIR_CACHE, "affectation_cache.rds")
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
    # Paramètres du choix de véhicule par EOQ (version A)
    eoq               = list(
      CHOIX_VEHICULE_EOQ, HEURES_PAR_AN,
      TAUX_DETENTION_STOCK, VALEUR_RWF_PAR_TONNE,
      params_flotte_df$cout_chargement_rwf,
      params_flotte_df$cout_dechargement_rwf,
      params_flotte_df$capacite_tonnes
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
# Ce diagnostic est affiché même quand le cache d'affectation est valide.

# Toutes les paires hors diagonale (i ≠ j), sans filtre de seuil
toutes_paires <- which(flux_tonnes_total > 0, arr.ind = TRUE)
toutes_paires <- toutes_paires[toutes_paires[, 1] != toutes_paires[, 2], ]

# Paires sous le seuil = toutes les paires actives MOINS celles retenues
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
road_type_phys <- reseau %>%
  activate("edges") %>%
  as_tibble() %>%
  pull(road_type)

# Capacité (PCU/jour) de chaque arête physique, déduite de son type de route.
# Les types absents de la table de capacité (ou NA) reçoivent la capacité la
# plus faible, pour éviter une capacité manquante (division par NA).
C_phys <- capacites_route_df$capacite_pcu_jour[
  match(road_type_phys, capacites_route_df$road_type)
]

# Diagnostic de calibration : un NA dans C_phys signale un road_type présent dans
# le réseau mais absent de capacites_route_df (table de capacité incomplète) — ou
# un road_type manquant en amont. On l'affiche explicitement avant de le combler,
# pour que ce trou de calibration soit visible plutôt que masqué silencieusement.
types_sans_capacite <- setdiff(unique(road_type_phys), capacites_route_df$road_type)
if (length(types_sans_capacite) > 0) {
  warning(sprintf(
    "Types de route sans capacite definie (capacite min appliquee par defaut) : %s",
    paste(types_sans_capacite, collapse = ", ")
  ))
}

C_phys[is.na(C_phys)] <- min(capacites_route_df$capacite_pcu_jour)

# Vecteur de conversion tonnes/an → PCU/jour : un coefficient par véhicule,
# dans l'ordre des colonnes du tableau de trafic (VEHICULES_IDS$vehicule_id).
#   PCU/jour_v = tonnes_an_v × conv_v
#   conv_v = facteur_pcu / (capacite_tonnes × TAUX_CHARGEMENT × JOURS_TRAFIC_AN)
.veh_order <- VEHICULES_IDS$vehicule_id
.m_veh     <- match(.veh_order, params_flotte_df$vehicule_id)
conv_v <- params_flotte_df$facteur_pcu[.m_veh] /
  (params_flotte_df$capacite_tonnes[.m_veh] * TAUX_CHARGEMENT * JOURS_TRAFIC_AN)

# Paramètres EOQ par véhicule, dans le MÊME ordre que les colonnes du tableau de
# trafic et que les lignes de dists_all (VEHICULES_IDS$vehicule_id) :
#   K_vec   = coût fixe par trajet (chargement + déchargement), RWF
#   cap_vec = capacité de chargement, tonnes
K_vec   <- params_flotte_df$cout_chargement_rwf[.m_veh] +
           params_flotte_df$cout_dechargement_rwf[.m_veh]
cap_vec <- params_flotte_df$capacite_tonnes[.m_veh]
rm(.veh_order, .m_veh)

# Nombre d'itérations d'équilibre : 1 seule passe si la congestion est désactivée
# (on retombe alors exactement sur l'affectation All-or-Nothing).
n_iter_msa <- if (isTRUE(CONGESTION)) MSA_MAX_ITER else 1L
cat("── Congestion :", if (isTRUE(CONGESTION)) "ACTIVÉE" else "désactivée",
    "— itérations d'équilibre max :", n_iter_msa, "──────\n\n")

# ══════════════════════════════════════════════════════════════════════════════
# BLOC CONDITIONNEL : l'affectation ne s'exécute que si pas de cache valide
# ══════════════════════════════════════════════════════════════════════════════
if (!cache_affectation_valide) {

  # ── Préparation invariante de la boucle d'équilibre (calculée une seule fois) ─
  # poids_mm_libre = coûts de Dijkstra à charge nulle (référence). À chaque
  # itération MSA on repart de ces coûts puis on applique le facteur BPR.
  poids_mm_libre <- igraph::E(recuperer_lourd("graphe_multimodal"))$weight

  # Temps de trajet par arête multimodale (heures), pour le coût de stock en
  # transit. Transbordements = 0 ; NA éventuels mis à 0.
  temps_mm <- igraph::E(recuperer_lourd("graphe_multimodal"))$travel_time_h
  temps_mm[is.na(temps_mm)] <- 0

  # Décomposition du poids d'arête (RWF/tonne) en part « TEMPS » et part « HORS
  # TEMPS » (carburant + usure), pour n'appliquer la congestion (BPR) qu'au temps.
  # La part temps (temps × valeur_temps / capacité, selon le véhicule de l'arête)
  # est calculé en 02.
  poids_temps_mm <- igraph::E(recuperer_lourd("graphe_multimodal"))$weight_temps
  if (is.null(poids_temps_mm))
    stop("Attribut d'arête 'weight_temps' absent du graphe multimodal : ",
         "relancer 02_couts.R pour régénérer persist_graphe_mm.rds.")
  poids_temps_mm[is.na(poids_temps_mm)] <- 0
  poids_horstemps_mm <- pmax(poids_mm_libre - poids_temps_mm, 0)

  # volume_eq_s = charge d'ÉQUILIBRE accumulée (tonnes, [arête, véhicule, secteur]).
  # V_phys = charge physique correspondante en PCU/jour, par arête physique.
  # Initialisées à 0 → la 1ère itération est une affectation AON à coûts libres.
  volume_eq_s <- array(
    0,
    dim      = c(n_aretes_physiques, n_vehicules, N_SECTEURS),
    dimnames = list(NULL, VEHICULES_IDS$vehicule_id, SECTEURS)
  )
  V_phys <- rep(0, n_aretes_physiques)

  # ══════════════════════════════════════════════════════════════════════════
  # BOUCLE D'ÉQUILIBRE (MSA) : chaque itération relance une affectation AON
  # complète avec des coûts congestionnés, puis moyenne la charge obtenue avec
  # celle des itérations précédentes (pas 1/n) jusqu'à stabilisation (gap<tol).
  # Si CONGESTION = FALSE, n_iter_msa = 1 → une seule passe AON à coûts libres.
  # ══════════════════════════════════════════════════════════════════════════
  for (iter_msa in seq_len(n_iter_msa)) {

  # ── Coûts congestionnés de l'itération : coût_libre × facteur BPR ────────────
  # Le facteur BPR dépend de la saturation V/C de chaque arête PHYSIQUE ; on
  # l'applique ensuite aux copies multimodales (une par véhicule) de l'arête,
  # repérées via lookup_physique. À l'itération 1, V_phys = 0 → facteur = 1,
  # donc on retrouve exactement les coûts libres (et l'AON).
  f_bpr_phys <- 1 + BPR_ALPHA * (V_phys / C_phys)^BPR_BETA

  # Facteur de congestion PAR ARÊTE multimodale : f_bpr de l'arête physique sur
  # les arêtes « route », 1 ailleurs (transbordements jamais congestionnés).
  .est_route <- lookup_type == "route"
  f_edge <- rep(1, length(poids_mm_libre))
  f_edge[.est_route] <- f_bpr_phys[lookup_physique[.est_route]]

  # BPR appliqué au TEMPS (fonction volume-délai) : seule la composante « temps »
  # du coût généralisé enfle (carburant/usure inchangés) ; le temps de trajet
  # congestionné (temps_mm_c) alimente aussi τ_v de l'EOQ → un bien de valeur fuit
  # les routes saturées (lien congestion → choix modal).
  poids_mm   <- poids_horstemps_mm + poids_temps_mm * f_edge
  temps_mm_c <- temps_mm * f_edge

  # ── ÉTAPE 4 : Préparation des matrices de résultats ───────────────────────────
  # Tableau 3D (arêtes × véhicules × secteurs) pour conserver l'information sectorielle.
  # Chaque "tranche" du tableau correspond à un secteur économique.
  # Exemple de lecture : volume_trafic_mm_s[500, "camion_moyen", "Agriculture"]
  # = tonnes d'Agriculture transportées par camion moyen sur l'arête n°500
  volume_trafic_mm_s <- array(
    0,
    dim      = c(n_aretes_physiques, n_vehicules, N_SECTEURS),
    dimnames = list(NULL, VEHICULES_IDS$vehicule_id, SECTEURS)
  )
  
  paires_traitees       <- 0
  paires_non_connectees <- 0

  # ── ÉTAPE 5 : Boucle principale par zone origine ──────────────────────────────
  # On parcourt les zones origine une par une. Pour chaque origine i, on calcule
  # en UNE SEULE fois les distances vers toutes les destinations (bien plus
  # efficace qu'une requête par paire).
  
  cat("Affectation du fret au réseau (All-or-Nothing multi-modal)...\n")
  
  # targets_all_global est un vecteur contenant les indices des nœuds-entrepôts
  # dans le graphe multi-modal, pour chaque couche véhicule. Par exemple, si
  # l'entrepôt A est au nœud 5 du réseau physique, il apparaît 3 fois :
  # une fois par couche véhicule : targets_all_global = (5,..., 1005,..., 2005,...).
  targets_all_global <- as.vector(sapply(
    seq_len(n_vehicules),
    function(v) node_multi(v, warehouse_nodes_base)
  ))
  
  # On regroupe les paires actives par origine pour traiter toute une origine
  # en une passe Dijkstra.
  # split(x, f) découpe le vecteur x en morceaux, un par valeur unique de f
  # paires_par_origine = list(
  #  "1" = c(2, 3, 5),    ← depuis l'origine 1, les destinations sont 2, 3 et 5
  #  "2" = c(1, 4),       ← depuis l'origine 2, les destinations sont 1 et 4
  #  "3" = c(1),          ← depuis l'origine 3, la destination est 1
  #  ...)
  paires_par_origine <- split(
    paires_actives[, 2],       # destinations
    paires_actives[, 1]        # origines (clé du split)
  )
  # Vecteur de l'indice des noeuds qui ont une destination après filtrage (si tous 
  # ont au moins une destination, origines_a_traiter = (1,2,...,n_warehouse))
  origines_a_traiter <- as.integer(names(paires_par_origine))
  
  n_origines <- length(origines_a_traiter)
  
  # Barre de progression
  pb_aff <- progress_bar$new(
    format = paste0("  Itér. ", iter_msa, "/", n_iter_msa,
                    " [:bar] :percent | ETA: :eta | :current/:total"),
    total  = n_origines,
    clear  = FALSE,
    width  = 70
  )
  
  for (idx_i in seq_along(origines_a_traiter)) {
    
    i <- origines_a_traiter[idx_i]
    destinations_i <- paires_par_origine[[as.character(i)]]
    
    # Indices globaux des sources pour cette origine (3 couches véhicule)
    sources_i <- as.integer(sapply(
      seq_len(n_vehicules),
      function(v) node_multi(v, warehouse_nodes_base[i])
    ))
    
    # Dijkstra en une passe : depuis les n_vehicules sources vers toutes les destinations.
    # Résultat : matrice n_vehicules × (n_warehouses × n_vehicules)
    dists_all <- igraph::distances(
      recuperer_lourd("graphe_multimodal"),
      v       = sources_i,
      to      = targets_all_global,
      weights = poids_mm
    )
    
    # Pour chaque destination j active avec i, on identifie la meilleure
    # combinaison (couche de départ, couche d'arrivée) puis on reconstruit
    # le chemin et on affecte le volume.
    
    for (j in destinations_i) {
      
      # ── Dijkstra : calcul du chemin optimal (une seule fois pour la paire i,j) ──
      # On cherche le chemin de moindre coût entre i et j, indépendamment
      # de la nature de la marchandise. Ce chemin sera ensuite utilisé pour
      # ventiler les volumes de TOUS les secteurs :
      # le routage physique ne dépend pas du type de marchandise, seulement
      # des coûts de transport.
      # Cette hypothèse est cohérente avec la structure du modèle : les
      # différences sectorielles interviennent dans la GÉNÉRATION des flux
      # mais pas dans le ROUTAGE.
      
      cols_j   <- j + (seq_len(n_vehicules) - 1) * n_warehouses
      min_cout <- min(dists_all[, cols_j], na.rm = TRUE)
      
      if (is.infinite(min_cout)) {
        paires_non_connectees <- paires_non_connectees + 1
        next
      }
      
      # Identification de la meilleure combinaison de couches véhicule
      best_idx_mat <- which(dists_all[, cols_j] == min_cout, arr.ind = TRUE)
      if (!is.matrix(best_idx_mat)) best_idx_mat <- matrix(best_idx_mat, nrow = 1)
      best_from <- sources_i[best_idx_mat[1, 1]]
      best_to   <- targets_all_global[cols_j[best_idx_mat[1, 2]]]
      
      # Reconstruction du chemin optimal (liste des arêtes empruntées)
      path_obj <- igraph::shortest_paths(
        recuperer_lourd("graphe_multimodal"),
        from    = best_from,
        to      = best_to,
        weights = poids_mm,
        output  = "epath"
      )
      edges_path_mm <- as.integer(path_obj$epath[[1]])
      rm(path_obj)
      
      if (length(edges_path_mm) == 0) {
        paires_non_connectees <- paires_non_connectees + 1
        next
      }
      
      # ── Identification des arêtes physiques valides sur ce chemin ───────────
      # On filtre les arêtes "route" (pas les transbordements entre véhicules)
      # et on récupère leur indice physique et leur véhicule associé.
      edges_valides <- edges_path_mm[edges_path_mm <= max_idx_mm]
      types_e       <- lookup_type[edges_valides]
      edges_routes  <- edges_valides[types_e == "route"]
      
      if (length(edges_routes) == 0) {
        paires_traitees <- paires_traitees + 1
        next
      }
      
      idx_phys_vec <- lookup_physique[edges_routes]
      veh_id_vec   <- lookup_vehicule[edges_routes]
      
      # On ne garde que les arêtes avec un indice physique et un véhicule valides
      valides <- idx_phys_vec >= 1 &
        idx_phys_vec <= n_aretes_physiques &
        veh_id_vec != ""
      
      if (!any(valides)) {
        paires_traitees <- paires_traitees + 1
        next
      }
      
      idx_phys_vec <- idx_phys_vec[valides]
      veh_id_vec   <- veh_id_vec[valides]
      col_veh_vec  <- match(veh_id_vec, VEHICULES_IDS$vehicule_id)

      # ── Coût de transport mono-véhicule de l'OD (RWF/tonne), par véhicule ───
      # dists_all[v, cols_j[v]] = coût d'aller de i à j ENTIÈREMENT dans la couche
      # du véhicule v (sans transbordement). C'est le c_v dont l'EOQ a besoin pour
      # arbitrer le choix de véhicule. Inf = véhicule injoignable en mono-couche.
      cout_transp_veh <- dists_all[cbind(seq_len(n_vehicules), cols_j)]
      eoq_possible    <- isTRUE(CHOIX_VEHICULE_EOQ) && any(is.finite(cout_transp_veh))

      # ── Temps de trajet par véhicule LE LONG du chemin (τ_v) ────────────────
      # Pour chaque véhicule v, on somme son temps de parcours sur les arêtes
      # physiques du chemin retenu. Une route saturée est allongée de τ_v). 
      # τ_v est ensuite converti en fraction d'année.
      if (eoq_possible) {
        .mm_ids <- outer(idx_phys_vec,
                         (seq_len(n_vehicules) - 1L) * n_aretes_physiques, `+`)
        .t      <- temps_mm_c[.mm_ids]
        .t[is.na(.t)] <- 0
        tau_v_an <- colSums(matrix(.t, nrow = length(idx_phys_vec))) / HEURES_PAR_AN
        rm(.mm_ids, .t)
      } else {
        tau_v_an <- rep(0, n_vehicules)
      }

      # ── Ventilation sectorielle sur le chemin trouvé ────────────────────────
      # Le ROUTAGE (arêtes physiques) reste commun à tous les secteurs. En revanche,
      # le VÉHICULE est choisi PAR SECTEUR par lot économique (EOQ) :
      # selon le flux Q et la valeur V_s de la marchandise, le secteur emprunte le
      # véhicule qui minimise son coût logistique total. Si l'EOQ est désactivé
      # (ou aucun véhicule mono-couche joignable), on garde le véhicule du chemin
      # de moindre coût.
      for (s in SECTEURS) {
        
        # ── Indice numérique du secteur dans la 3e dimension du tableau ───────
        # Le tableau volume_trafic_mm_s a pour dimensions :
        #   [arête physique, véhicule, secteur]
        # Pour l'indexer efficacement, on a besoin de l'indice ENTIER du secteur
        # (1 pour "Agriculture", 2 pour "Cultures_export", etc.) et pas de son nom texte.
        # match(s, SECTEURS) retourne la position de s dans le vecteur SECTEURS.
        # Exemple : match("Mines", SECTEURS) → 3
        idx_s <- match(s, SECTEURS)
        
        # Volume en tonnes pour ce secteur entre i et j
        # flux_gravitaire[[s]] est directement en tonnes (converti avant Furness)
        flux_ij_s <- flux_gravitaire[[s]][i, j]
        
        # Si le flux sectoriel est négligeable, on passe au secteur suivant
        # pour ne pas alourdir inutilement les calculs
        if (is.na(flux_ij_s) || flux_ij_s < 1) next

        # ── Choix du véhicule par lot économique (EOQ) ────────────────────────
        # On compare les véhicules par leur coût logistique total annuel :
        #   CLT(q,v) = (Q/q)·K_v + Q·c_v + (q/2)·V_s·r + Q·τ_v·V_s·r
        # (commande + transport + stock cyclique + stock en transit), avec la taille
        # d'envoi optimale q* = √(2·Q·K_v/(V_s·r)) plafonnée à la capacité. Le terme
        # en transit (Q·τ_v·V_s·r) rend le choix sensible au temps de trajet τ_v. Le
        # secteur emprunte le véhicule de CLT minimal, affecté à TOUTES les arêtes du
        # chemin (col_v constant). Sans EOQ, on garde le véhicule du chemin.
        if (eoq_possible) {
          Vs    <- VALEUR_RWF_PAR_TONNE[s]
          q_opt <- pmin(sqrt(2 * flux_ij_s * K_vec / (Vs * TAUX_DETENTION_STOCK)),
                        cap_vec)
          clt   <- (flux_ij_s / q_opt) * K_vec +
                   flux_ij_s * cout_transp_veh +
                   (q_opt / 2) * Vs * TAUX_DETENTION_STOCK +
                   flux_ij_s * tau_v_an * Vs * TAUX_DETENTION_STOCK  # stock en transit
          clt[!is.finite(cout_transp_veh)] <- Inf   # véhicule injoignable exclu
          col_v <- rep.int(which.min(clt), length(idx_phys_vec))
        } else {
          col_v <- col_veh_vec                       # véhicule du chemin 
        }

        # ── Affectation vectorisée sur un tableau 3D ──────────────────────────
        # On veut ajouter flux_ij_s à TOUTES les cellules (a, v, s) où :
        #   - a parcourt les arêtes physiques du chemin (idx_phys_vec)
        #   - v parcourt les véhicules correspondants (col_veh_vec)
        #   - s est fixé au secteur courant (idx_s)
        #
        # Pour indexer un tableau à N dimensions, on passe une matrice à N colonnes :
        # chaque LIGNE de cette matrice = un triplet (arête, véhicule, secteur)
        # qui désigne UNE cellule unique du tableau 3D.
        #
        # cbind(idx_phys_vec, col_v, idx_s) construit cette matrice :
        #   - idx_phys_vec et col_v sont des vecteurs de même longueur (autant que
        #     d'arêtes du chemin ; col_v = colonne du véhicule EOQ choisi)
        #   - idx_s est un scalaire : R le RECYCLE automatiquement pour qu'il
        #     apparaisse sur chaque ligne
        # Résultat : une matrice à 3 colonnes avec une ligne par arête du chemin.
        indices_3d <- cbind(idx_phys_vec, col_v, idx_s)
        
        # volume_trafic_mm_s[indices_3d] : quand on passe une matrice à N colonnes
        # à un tableau à N dimensions, R interprète CHAQUE LIGNE comme un jeu
        # d'indices. C'est l'équivalent vectorisé de faire :
        #   volume_trafic_mm_s[arete1, veh1, s] += flux_ij_s
        #   volume_trafic_mm_s[arete2, veh2, s] += flux_ij_s
        #   ...
        # mais sans boucle R explicite, donc beaucoup plus rapide.
        volume_trafic_mm_s[indices_3d] <-
          volume_trafic_mm_s[indices_3d] + flux_ij_s
      }
      
      paires_traitees <- paires_traitees + 1
    }
    
    # Nettoyage explicite de dists_all avant l'itération suivante
    rm(dists_all)
    
    # gc() nettoie la RAM à chaque itération pour éviter les pics de RAM qui font crasher R.
    if (idx_i %% 5 == 0) {
      invisible(gc(verbose = FALSE))
    }
    
    pb_aff$tick()
  }
  # ── fin de la boucle sur les origines (une affectation AON complète) ──────────

  # ── MISE À JOUR D'ÉQUILIBRE (MSA) ─────────────────────────────────────────────
  # volume_trafic_mm_s contient la charge AON de CETTE itération (charge
  # auxiliaire Y). On la moyenne avec la charge d'équilibre courante au pas 1/n :
  #   V^{n} = V^{n-1} + (1/n) · (Y − V^{n-1})
  # Le moyennage amortit les oscillations et fait converger vers l'équilibre de
  # Wardrop (tous les itinéraires utilisés d'une OD finissent au même coût).
  volume_eq_s <- volume_eq_s + (1 / iter_msa) * (volume_trafic_mm_s - volume_eq_s)

  # Recalcul de la charge physique (PCU/jour) à partir de la charge d'équilibre :
  # somme sur les secteurs → [arête, véhicule], puis conversion tonnes/an→PCU/jour.
  .vol_eq_mm <- apply(volume_eq_s, c(1, 2), sum)
  V_new      <- as.vector(.vol_eq_mm %*% conv_v)

  # Critère de convergence : variation relative L1 de la charge entre 2 itérations.
  gap_msa <- sum(abs(V_new - V_phys)) / max(sum(V_new), 1)
  V_phys  <- V_new

  # Diagnostic d'itération : saturation max et nombre d'arêtes surchargées (V/C>1).
  .sat_iter <- V_phys / C_phys
  cat(sprintf(
    "  → Itér. %d/%d : gap = %.4f | saturation max = %.2f | arêtes V/C>1 : %d\n",
    iter_msa, n_iter_msa, gap_msa, max(.sat_iter, na.rm = TRUE),
    sum(.sat_iter > 1, na.rm = TRUE)
  ))
  rm(.vol_eq_mm, .sat_iter)
  invisible(gc(verbose = FALSE))

  # Arrêt anticipé si la charge ne bouge presque plus (équilibre atteint).
  # On ne teste pas à l'itération 1 (la charge passe de 0 à sa 1ère valeur).
  if (iter_msa > 1 && gap_msa < MSA_TOL) {
    cat("  ✓ Convergence atteinte (gap <", MSA_TOL, ") à l'itération",
        iter_msa, "\n\n")
    break
  }

  }  # ── fin de la boucle d'équilibre MSA ──────────────────────────────────────

  # La charge retenue est la charge d'ÉQUILIBRE (et non la dernière AON brute).
  # On la réinjecte dans volume_trafic_mm_s pour que TOUT l'aval (agrégations,
  # émissions, exports, viz) reste inchangé.
  volume_trafic_mm_s <- volume_eq_s
  rm(volume_eq_s)
  invisible(gc(verbose = FALSE))


  # ── SAUVEGARDE DU CACHE ───────────────────────────────────────────────────────
  cat("=== Sauvegarde du cache d'affectation ===\n")
  
  saveRDS(
    list(
      volume_trafic_mm_s    = volume_trafic_mm_s,
      paires_traitees       = paires_traitees,
      paires_non_connectees = paires_non_connectees,
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
# Calculé dans TOUS les cas (recalcul OU chargement du cache) à partir de la
# charge finale par arête×véhicule (volume_trafic_mm, en tonnes/an) et des
# capacités par type de route (C_phys, en PCU/jour). conv_v convertit chaque
# colonne véhicule de tonnes/an en PCU/jour ; le produit matriciel somme sur
# les véhicules. saturation_phys = V/C : >1 signale un tronçon surchargé.
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
