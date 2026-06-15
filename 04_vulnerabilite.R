################################################################################
# 04_vulnerabilite.R
# RÔLE : Simulation d'un scénario de perturbation (inondation, glissement...),
#        recalcul de la matrice OD dégradée, surcoûts, arêtes critiques.
# ENTRÉES  : persist_reseau_fret.rds, persist_graphe_mm.rds,
#            persist_mapping_mm.rds, persist_entreposages.rds + od_cache.rds
# SORTIES  : persist_vulnerabilite.rds + cartes PNG + CSV
# DÉPEND DE : 00_parametres.R, 01_reseau.R, 02_couts.R, 03_transport.R
# NOTE : Modifier NOM_SCENARIO dans 00_parametres.R avant de relancer.
################################################################################

source("00_parametres.R")

cat("=== Chargement des objets ===\n")

.geo  <- readRDS(PERSIST_GEODATA)
.ent  <- readRDS(PERSIST_ENTREPOSAGES)
.mm   <- readRDS(PERSIST_GRAPHE_MM)
.map  <- readRDS(PERSIST_MAPPING_MM)
.fret <- readRDS(PERSIST_RESEAU_FRET)

list2env(.geo,  envir = .GlobalEnv)
list2env(.ent,  envir = .GlobalEnv)
reseau         <- .fret$reseau
flux_tonnes_total     <- readRDS(PERSIST_FLUX_FRET)$flux_tonnes_total
n_noeuds              <- .mm$n_noeuds
n_vehicules           <- .mm$n_vehicules
stocker_lourd("graphe_multimodal", .mm$graphe_multimodal)
lookup_type           <- .map$lookup_type
lookup_physique       <- .map$lookup_physique
lookup_vehicule       <- .map$lookup_vehicule
max_idx_mm            <- .map$max_idx_mm
n_aretes_physiques    <- length(.map$lookup_type[.map$lookup_type == "route"]) / .mm$n_vehicules
node_multi <- function(v_idx, n_id) as.integer((v_idx - 1L) * n_noeuds + n_id)

# Chargement de la matrice OD de référence (cache existant)
.od_cache <- readRDS(file.path(DIR_CACHE, "od_cache.rds"))
od_long   <- .od_cache$od_long
rm(.geo, .ent, .mm, .map, .fret, .od_cache)

cat("✓ Objets chargés\n\n")

################################################################################
# PARTIE IX — ANALYSE DE VULNÉRABILITÉ ET DE CONTOURNEMENT
#
# OBJECTIF : Simuler la suppression d'une ou plusieurs arêtes du réseau
#            (routes inondées, glissements de terrain, etc.) et mesurer
#            l'impact sur les coûts de transport entre toutes les paires OD.
#
# STRUCTURE :
#   IX.1 — Identification des arêtes perturbées
#   IX.2 — Recalcul de la matrice OD sur le réseau dégradé
#   IX.3 — Calcul des surcoûts et classification des impacts
#   IX.4 — Identification des arêtes critiques (analyse de sensibilité)
#   IX.5 — Cartes et exports
#
# DESIGN POUR L'AVENIR :
#   Ce script est conçu pour accepter, à terme, des cartes raster de zones
#   inondables (ex : sorties de modèles hydrologiques HEC-RAS, HAND, JRC)
#   ou de glissements de terrain (ex : données NASA LSAF, susceptibilité USGS).
#   Pour l'instant, trois modes de définition des perturbations sont disponibles :
#     Mode A — Manuel       : liste d'osm_id ou de coordonnées GPS fournie à la main
#     Mode B — Buffer zone  : toutes les routes dans un rayon autour d'un point
#     Mode C — Raster risque: (PRÊT À BRANCHER) intersection avec un raster externe
#
# DÉPENDANCES :
#   Ce bloc dépend des objets construits dans les Parties I à VIII :
#     reseau      — réseau sfnetworks avec coûts et volumes
#     graphe_multimodal  — graphe igraph multi-modal (3 couches véhicules)
#     od_long            — matrice OD de référence (avant perturbation)
#     noeuds_entreposage — liste et indices des zones d'entrepôt
#     warehouse_nodes_base — indices des entrepôts dans le graphe de base
#     VEHICULES_IDS      — tableau des types de véhicules
#     node_multi()       — fonction de remappage multi-modal
#     fond_carte()       — fonction de fond de carte réutilisable
#     DIR_OUTPUT         — dossier de sortie
################################################################################


################################################################################
# PARTIE IX.1 — IDENTIFICATION DES ARÊTES PERTURBÉES
#
# Les trois modes sont fusionnés en un seul ensemble d'arêtes.
################################################################################

cat("── Identification des arêtes perturbées ──────────────────────────────\n\n")

# On commence avec un ensemble vide d'osm_id perturbés.
# character(0) est un vecteur de chaînes de caractères vide en R.
osm_ids_perturbes <- character(0)

# ── Mode A : identifiants manuels ─────────────────────────────────────────────
if (length(OSM_IDS_PERTURBES_MANUEL) > 0) {
  # as.character() : s'assure que les identifiants sont des chaînes de texte
  # (les osm_id peuvent parfois être chargés comme des entiers numériques et arrondir)
  osm_ids_perturbes <- union(osm_ids_perturbes,
                             as.character(OSM_IDS_PERTURBES_MANUEL))
  cat("  Mode A (manuel) :", length(OSM_IDS_PERTURBES_MANUEL),
      "osm_id fournis\n")
}

# ── Mode B : buffer géographique ──────────────────────────────────────────────
if (UTILISER_MODE_BUFFER) {
  
  # Création du point central de la perturbation en WGS84 (GPS)
  # puis reprojection en UTM 35S (même CRS que le réseau) pour que
  # le buffer soit exprimé en mètres et non en degrés.
  point_perturbation <- st_sfc(
    st_point(c(CENTRE_PERTURBATION_LON, CENTRE_PERTURBATION_LAT)),
    crs = 4326    # WGS84 = système GPS standard
  ) %>%
    st_transform(crs = 32735)    # UTM Zone 35S = système métrique (Afrique de l'Est)
  
  # st_buffer() crée un cercle de rayon RAYON_PERTURBATION_M autour du point.
  # Ce cercle représente la zone géographique affectée par l'événement.
  zone_perturbation_buffer <- st_buffer(point_perturbation,
                                        dist = RAYON_PERTURBATION_M)
  
  # Récupération de toutes les arêtes du réseau sous forme sf
  aretes_sf_mode_b <- reseau %>%
    activate("edges") %>%
    st_as_sf()
  
  # Filtrage optionnel par type de route
  if (!is.null(TYPES_ROUTES_PERTURBES)) {
    aretes_sf_mode_b <- aretes_sf_mode_b %>%
      filter(road_type %in% TYPES_ROUTES_PERTURBES)
  }
  
  # st_intersects() : teste si chaque arête croise la zone de perturbation.
  # lengths() > 0 : TRUE si l'arête intersecte au moins partiellement la zone.
  # On utilise l'intersection (et non l'inclusion totale) car une route peut
  # n'être que partiellement dans la zone inondée mais quand même bloquée.
  # Création d'un vecteur logique (valeur TRUE ou FALSE) d'une taille N_totale_arêtes
  dans_buffer <- lengths(st_intersects(aretes_sf_mode_b,
                                       zone_perturbation_buffer)) > 0
  
  # Récupération des osm_id des arêtes dans le buffer
  # as.character() : conversion en texte pour la cohérence avec les autres modes
  ids_mode_b <- as.character(aretes_sf_mode_b$osm_id[dans_buffer])
  ids_mode_b <- ids_mode_b[!is.na(ids_mode_b)]  # Supprimer les NA éventuels
  
  # Tirage aléatoire : seule une proportion PROP_ROUTES_INONDEES des routes
  # dans le buffer est effectivement considérée comme inondée.
  if (PROP_ROUTES_INONDEES_BUFFER < 1.0 && length(ids_mode_b) > 0) {
    set.seed(SEED_INONDATION)
    n_inondees <- max(1, round(length(ids_mode_b) * PROP_ROUTES_INONDEES_BUFFER))
    ids_mode_b <- sample(ids_mode_b, size = n_inondees, replace = FALSE)
    cat("  → Après tirage aléatoire (seed =", SEED_INONDATION,
        ", prop =", PROP_ROUTES_INONDEES_BUFFER, ") :",
        length(ids_mode_b), "routes effectivement inondées\n")
  }
  
  # union() fusionne deux vecteurs sans doublons
  osm_ids_perturbes <- union(osm_ids_perturbes, ids_mode_b)
  
  cat("  Mode B (buffer", RAYON_PERTURBATION_M / 1000, "km) :",
      length(ids_mode_b), "arêtes inondées\n")
}

# ── Mode C : raster de risque ─────────────────────────────────────────────────
if (UTILISER_MODE_RASTER) {
  
  if (!file.exists(CHEMIN_RASTER_RISQUE)) {
    # warning() affiche un message d'avertissement sans arrêter le script
    # (contrairement à stop() qui arrêterait l'exécution)
    warning("  ⚠ Mode C activé mais fichier raster introuvable : ",
            CHEMIN_RASTER_RISQUE, "\n  Mode C ignoré.\n")
  } else {
    
    cat("  Mode C (raster) : chargement de", CHEMIN_RASTER_RISQUE, "...\n")
    
    # Chargement du raster de risque avec terra
    raster_risque <- rast(CHEMIN_RASTER_RISQUE)
    
    # Reprojection en UTM 35S pour cohérence avec le réseau routier
    raster_risque <- project(raster_risque, "EPSG:32735", method = "bilinear")
    
    # Récupération des arêtes du réseau
    aretes_sf_mode_c <- reseau %>%
      activate("edges") %>%
      st_as_sf() %>%
      mutate(arete_idx_c = row_number())
    
    # Pour chaque arête, on calcule la proportion de sa longueur
    # qui se trouve dans une zone à risque supérieur au seuil.
    #
    # Approche :
    #   1. Échantillonner des points le long de chaque arête (tous les 100m)
    #   2. Extraire la valeur du raster à chaque point
    #   3. Calculer la proportion de points avec valeur > SEUIL_RISQUE_RASTER
    #   4. Si proportion > PROPORTION_MIN_EXPOSEE → arête perturbée
    
    cat("    Échantillonnage des points le long des arêtes...\n")
    
    # calculer_exposition_raster() : fonction qui calcule pour une arête donnée
    # la proportion de sa longueur exposée au risque selon le raster.
    calculer_exposition_raster <- function(ligne_geom, raster, seuil, espacement = 100) {
      
      longueur <- as.numeric(st_length(ligne_geom))
      
      # Au moins 2 points ; plus de points pour les longues arêtes
      n_pts <- max(2, floor(longueur / espacement))
      
      pts <- st_line_sample(ligne_geom, n = n_pts, type = "regular") %>%
        st_cast("POINT")
      
      # terra::extract() : lit la valeur du raster à chaque point
      valeurs <- terra::extract(raster, vect(pts))[, 2]
      
      # proportion de points dépassant le seuil de risque
      if (all(is.na(valeurs))) return(0)
      mean(valeurs > seuil, na.rm = TRUE)
    }
    
    # Application à toutes les arêtes avec barre de progression
    n_aretes_c <- nrow(aretes_sf_mode_c)
    proportions_exposees <- numeric(n_aretes_c)
    
    pb_raster <- progress_bar$new(
      format = "  Raster exposition [:bar] :percent | ETA: :eta",
      total  = n_aretes_c, clear = FALSE, width = 60
    )
    
    for (i in seq_len(n_aretes_c)) {
      proportions_exposees[i] <- calculer_exposition_raster(
        aretes_sf_mode_c$geometry[i],
        raster_risque,
        SEUIL_RISQUE_RASTER
      )
      pb_raster$tick()
    }
    
    # Sélection des arêtes suffisamment exposées
    ids_mode_c <- as.character(
      aretes_sf_mode_c$osm_id[proportions_exposees >= PROPORTION_MIN_EXPOSEE]
    )
    ids_mode_c <- ids_mode_c[!is.na(ids_mode_c)]
    
    # Tirage aléatoire : seule une proportion PROP_ROUTES_INONDEES des routes
    # exposées est effectivement considérée comme inondée.
    if (PROP_ROUTES_INONDEES_RASTER < 1.0 && length(ids_mode_c) > 0) {
      set.seed(SEED_INONDATION)
      n_inondees <- max(1, round(length(ids_mode_c) * PROP_ROUTES_INONDEES_RASTER))
      ids_mode_c <- sample(ids_mode_c, size = n_inondees, replace = FALSE)
      cat("  → Après tirage aléatoire (seed =", SEED_INONDATION,
          ", prop =", PROP_ROUTES_INONDEES_RASTER, ") :",
          length(ids_mode_c), "routes effectivement inondées\n")
    }
    
    osm_ids_perturbes <- union(osm_ids_perturbes, ids_mode_c)
    
    cat("  Mode C (raster, seuil", SEUIL_RISQUE_RASTER, ") :",
        length(ids_mode_c), "arêtes inondées\n")
  }
}

# ── Bilan : arêtes effectivement perturbées ───────────────────────────────────
# On traduit maintenant les osm_id en indices d'arêtes dans le graphe igraph.
# Ce sont ces indices qui seront utilisés pour supprimer les arêtes.
aretes_reseau_sf <- reseau %>%
  activate("edges") %>%
  st_as_sf() %>%
  mutate(arete_idx = row_number())

# match() : pour chaque osm_id perturbé, trouve son indice de ligne dans le réseau
# !is.na() : supprime les osm_id non trouvés (hors réseau, déjà supprimés, etc.)
indices_aretes_perturbees <- aretes_reseau_sf$arete_idx[
  aretes_reseau_sf$osm_id %in% osm_ids_perturbes
]
indices_aretes_perturbees <- indices_aretes_perturbees[
  !is.na(indices_aretes_perturbees)
]

n_perturb <- length(indices_aretes_perturbees)

if (n_perturb == 0) {
  # Si aucune arête n'est trouvée, on arrête avec un message explicatif
  stop("⚠ Aucune arête perturbée identifiée. Vérifiez les paramètres du scénario.\n",
       "  → Mode Buffer : les coordonnées GPS sont-elles dans le pays étudié ?\n",
       "  → Mode Manuel : les osm_id existent-ils dans le réseau ?\n",
       "  → Mode Raster : le seuil est-il trop élevé ?\n")
}

cat("\n✓ Arêtes perturbées identifiées :", n_perturb, "\n")

# Objet sf des arêtes perturbées — utilisé dans :
#   - viz_vulnerabilite.R (cartes A, B, C, D : surlignage des routes coupées)
# On le construit ici une seule fois et on le sauvegarde dans PERSIST_VULNERAB
aretes_perturbees_sf <- aretes_reseau_sf %>%
  filter(arete_idx %in% indices_aretes_perturbees)

# Synthèse attributaire des arêtes perturbées (pour le rapport)
resume_perturb <- aretes_reseau_sf %>%
  filter(arete_idx %in% indices_aretes_perturbees) %>%
  st_drop_geometry() %>%
  summarise(
    n_aretes         = n(),
    longueur_km      = round(sum(longueur_m, na.rm = TRUE) / 1000, 1),
    pct_du_reseau    = round(n() / nrow(aretes_reseau_sf) * 100, 2),
    road_types       = paste(sort(unique(road_type)), collapse = ", "),
    surfaces         = paste(sort(unique(surface)),   collapse = ", ")
  )

cat("  Longueur totale   :", resume_perturb$longueur_km, "km\n")
cat("  Part du réseau    :", resume_perturb$pct_du_reseau, "%\n")
cat("  Types de routes   :", resume_perturb$road_types, "\n")
cat("  Surfaces          :", resume_perturb$surfaces, "\n\n")


################################################################################
# PARTIE IX.2 — RECALCUL DE LA MATRICE OD SUR LE RÉSEAU DÉGRADÉ
#
# On reconstruit le graphe multi-modal en retirant les arêtes perturbées,
# puis on recalcule les distances OD optimales sur ce réseau dégradé.
# La comparaison avant/après donne les surcoûts de transport.
#
# NOTE SUR LA STRATÉGIE DE SUPPRESSION :
#   On ne supprime pas physiquement les arêtes du réseau sfnetworks
#   (ce serait difficile à annuler proprement). À la place, on leur donne
#   un poids infini dans igraph : Dijkstra ne les empruntera jamais,
#   ce qui revient à les supprimer logiquement du réseau.
#   Pour restaurer le réseau, il suffit de remettre les poids d'origine.
################################################################################

cat("── Reconstruction du graphe dégradé ──────────────────────────────────\n\n")

# ── Initialisation des vecteurs d'accumulation pour les itinéraires de contournement ──
# On initialise ici les trois vecteurs qui mesurent l'usage des arêtes de détour. 
# Trois métriques par arête physique :
#   surcout_pondere_arete  : Σ(surcoût_relatif_% × volume_tonnes) pour tous les flux
#                            reroutés passant par cette arête (indicateur d'exposition)
#   volume_detourne_arete  : Σ(volume_tonnes) rerouté passant par cette arête (tonnes)
surcout_pondere_arete  <- numeric(n_aretes_physiques)
volume_detourne_arete  <- numeric(n_aretes_physiques)

# ── Table de lookup pour les coûts OD de référence ────────────────────────────
# Plutôt que de faire filter(od_long, id_origine == i, id_destination == j)
# à chaque itération de la boucle interne (O(n) par appel), on construit
# un vecteur nommé qui permet un accès direct en O(1) via la clé "i_j".
# paste0(i, "_", j) est la clé unique pour chaque paire OD.
od_ref_map <- setNames(
  od_long$cout_rwf,
  paste0(od_long$id_origine, "_", od_long$id_destination)
)
cat("  Table de référence OD pré-chargée (", length(od_ref_map), "paires)\n\n")

# ── Récupération des poids originaux du graphe multi-modal ────────────────────
# igraph::E() : accède aux arêtes (edges) du graphe.
# $weight : attribut "weight" de chaque arête (coût de transport en RWF).
poids_originaux <- igraph::E(recuperer_lourd("graphe_multimodal"))$weight

# On travaille sur une COPIE du graphe multi-modal pour ne pas altérer l'original.
# Le graphe original (graphe_multimodal) reste intact et servira de référence.
graphe_degrade <- recuperer_lourd("graphe_multimodal")

# ── Mise à l'infini des arêtes perturbées dans TOUTES les couches véhicule ────
# Dans le graphe multi-modal, chaque arête physique existe en N_vehicules
# exemplaires (une par couche). On doit bloquer l'arête dans TOUTES les couches.
#
# La correspondance entre arête physique (indice 1..n_aretes) et arêtes
# multi-modales (une par couche) est donnée par le vecteur lookup construit en V.2.

# Toutes les arêtes multi-modales de type "route" (pas de transbordement)
# dont l'indice physique est dans la liste des arêtes perturbées
indices_mm_perturbes <- which(
  lookup_type     == "route" &
    lookup_physique %in% indices_aretes_perturbees
)

cat("  Arêtes multi-modales à bloquer :", length(indices_mm_perturbes),
    "(", n_perturb, "arêtes physiques ×", n_vehicules, "couches véhicules)\n")

# Attribution d'un poids infini aux arêtes perturbées.
# Inf en R est la valeur "infini" — Dijkstra ne traversera jamais une arête
# de poids infini car il existerait toujours un chemin de moindre coût.
# C'est mathématiquement équivalent à supprimer les arêtes du graphe.
igraph::E(graphe_degrade)$weight[indices_mm_perturbes] <- Inf

cat("  ✓ Graphe dégradé construit (arêtes bloquées avec poids = Inf)\n\n")

# ── Recalcul de la matrice OD sur le réseau dégradé ───────────────────────────
cat("  Recalcul des distances OD sur le réseau dégradé...\n")

# Cache de la table aretes_couts_tous en mémoire vive.
aretes_ems_cache <- duck_query(
  "SELECT arete_id, vehicule_id, co2_kg, nox_g, pm25_g FROM aretes_couts_tous"
)
aretes_ems_idx <- setNames(
  seq_len(nrow(aretes_ems_cache)),
  paste0(aretes_ems_cache$arete_id, "_", aretes_ems_cache$vehicule_id)
)
cat("  Cache émissions chargé :", nrow(aretes_ems_cache), "arêtes × véhicules\n\n")

# On stocke les résultats dans une liste, puis on l'assemble en data.frame.
# La structure est identique à od_long (Partie VI) pour faciliter la comparaison.
od_rows_degrade <- list()
idx_deg         <- 0

# ── Chargement du checkpoint si disponible ────────────────────────────────────   
CHECKPOINT_OD_DEG <- file.path(DIR_EXPORTS, "od_degrade_checkpoint.rds")     
origines_deja_traitees <- c()                                                
if (file.exists(CHECKPOINT_OD_DEG)) {                                        
  checkpoint <- readRDS(CHECKPOINT_OD_DEG)                                   
  od_rows_degrade        <- checkpoint$od_rows_degrade                       
  idx_deg                <- checkpoint$idx_deg                               
  origines_deja_traitees <- checkpoint$origines_deja_traitees                
  cat("  ✓ Checkpoint chargé — reprise depuis l'origine",                    
      max(origines_deja_traitees), "\n")                                     
}   

for (i in seq_along(warehouse_nodes_base)) {
  
  if (i %in% origines_deja_traitees) next
  
  sources_i <- sapply(seq_len(n_vehicules),
                      function(v) node_multi(v, warehouse_nodes_base[i]))
  
  targets_all <- as.vector(sapply(
    seq_len(n_vehicules),
    function(v) node_multi(v, warehouse_nodes_base)
  ))
  
  # Calcul des distances depuis l'entrepôt i vers tous les autres entrepôts
  # dans le graphe DÉGRADÉ (routes bloquées = poids infini).
  # La syntaxe est identique au Dijkstra de la Partie VI, seul le graphe change.
  dists_deg <- igraph::distances(
    graphe_degrade,
    v       = sources_i,
    to      = targets_all,
    weights = igraph::E(graphe_degrade)$weight
  )

  # Reconstruction des chemins depuis chaque couche véhicule de l'origine i
  # vers TOUTES les cibles en une seule passe par couche (n_vehicules appels)
  chemins_par_vehicule <- lapply(seq_len(n_vehicules), function(v) {
    igraph::shortest_paths(
      graphe_degrade,
      from    = sources_i[v],
      to      = targets_all,
      weights = igraph::E(graphe_degrade)$weight,
      output  = "epath"
    )$epath
  })

  # Attributs des arêtes extraits une seule fois par origine (évite un appel par j).
  edge_attrs_deg <- igraph::edge_attr(graphe_degrade)

  for (j in seq_along(warehouse_nodes_base)) {
    if (i == j) next
    
    cols_j      <- j + (seq_len(n_vehicules) - 1) * length(warehouse_nodes_base)
    min_cout_deg <- min(dists_deg[, cols_j], na.rm = TRUE)
    
    # ── Reconstruction du chemin dégradé pour mesurer la distance réelle ──────
    distance_km_degrade <- NA_real_
    co2_kg_degrade      <- NA_real_
    nox_g_degrade       <- NA_real_
    pm25_g_degrade      <- NA_real_

    if (!is.infinite(min_cout_deg)) {

      # Lookup du chemin dans chemins_par_vehicule (pré-calculé par origine).
      # v_source    = couche véhicule optimale (ligne dans dists_deg).
      # t_cible_idx = index dans targets_all correspondant à la destination j.
      best_idx_mat <- which(dists_deg[, cols_j] == min_cout_deg, arr.ind = TRUE)
      if (!is.matrix(best_idx_mat)) best_idx_mat <- matrix(best_idx_mat, nrow = 1)
      v_source    <- best_idx_mat[1, 1]
      t_cible_idx <- cols_j[best_idx_mat[1, 2]]
      edges_path_deg <- as.integer(chemins_par_vehicule[[v_source]][[t_cible_idx]])

      if (length(edges_path_deg) > 0) {

        edges_routes_deg <- edges_path_deg[lookup_type[edges_path_deg] == "route"]

        if (length(edges_routes_deg) > 0) {
          idx_phys_deg <- lookup_physique[edges_routes_deg]
          veh_id_deg   <- lookup_vehicule[edges_routes_deg]
          cles_ems     <- paste0(idx_phys_deg, "_", veh_id_deg)
          idx_ems      <- aretes_ems_idx[cles_ems]
          valides_ems  <- !is.na(idx_ems)
          co2_kg_degrade <- sum(aretes_ems_cache$co2_kg[idx_ems[valides_ems]], na.rm = TRUE)
          nox_g_degrade  <- sum(aretes_ems_cache$nox_g[idx_ems[valides_ems]],  na.rm = TRUE)
          pm25_g_degrade <- sum(aretes_ems_cache$pm25_g[idx_ems[valides_ems]], na.rm = TRUE)
        }

        # Accumulation pour les itinéraires de contournement
        cout_ref_ij <- od_ref_map[paste0(i, "_", j)]
        if (!is.na(cout_ref_ij) && min_cout_deg > cout_ref_ij && cout_ref_ij > 0) {

          surcout_rel_ij <- (min_cout_deg - cout_ref_ij) / cout_ref_ij * 100
          volume_ij      <- flux_tonnes_total[i, j]

          if (!is.na(volume_ij) && volume_ij > 0) {

            # Arêtes "route" uniquement — on exclut les transbordements inter-véhicules.
            edges_routes_ij <- edges_path_deg[
              edges_path_deg <= max_idx_mm & lookup_type[edges_path_deg] == "route"
            ]

            if (length(edges_routes_ij) > 0) {
              idx_phys_ij <- lookup_physique[edges_routes_ij]
              # Filtrage défensif : indices hors plage sur arêtes dégénérées (III.2).
              idx_phys_ij <- idx_phys_ij[idx_phys_ij >= 1L & idx_phys_ij <= n_aretes_physiques]
              if (length(idx_phys_ij) > 0) {
                surcout_pondere_arete[idx_phys_ij] <-
                  surcout_pondere_arete[idx_phys_ij] + surcout_rel_ij * volume_ij
                volume_detourne_arete[idx_phys_ij] <-
                  volume_detourne_arete[idx_phys_ij] + volume_ij
              }
            }
          }
        }

        # edge_attrs_deg extrait une fois par origine i (hors boucle j)
        distance_km_degrade <- sum(edge_attrs_deg$length_km[edges_path_deg], na.rm = TRUE)
      }
    }
    
    idx_deg <- idx_deg + 1
    od_rows_degrade[[idx_deg]] <- list(
      id_origine      = i,
      id_destination  = j,
      nom_origine     = noeuds_entreposage$warehouse_name[i],
      nom_destination = noeuds_entreposage$warehouse_name[j],
      cout_degrade    = min_cout_deg,   # Inf si plus de chemin possible
      distance_km_degrade = distance_km_degrade,      
      connecte        = !is.infinite(min_cout_deg),  # FALSE = zones déconnectées
      co2_kg_degrade      = co2_kg_degrade,   # Émissions CO2 sur le chemin de contournement
      nox_g_degrade       = nox_g_degrade,    # Émissions NOx sur le chemin de contournement
      pm25_g_degrade      = pm25_g_degrade    # Émissions PM2.5 sur le chemin de contournement
    )
  }
  
  rm(dists_deg)                                                              
  origines_deja_traitees <- c(origines_deja_traitees, i)                    
  if (i %% 5 == 0) {                                                         
    invisible(gc(verbose = FALSE))                                           
    saveRDS(                                                                 
      list(od_rows_degrade        = od_rows_degrade,                        
           idx_deg                = idx_deg,                                
           origines_deja_traitees = origines_deja_traitees),                
      CHECKPOINT_OD_DEG                                                     
    )                                                                       
  }                                                                         
  
  if (i %% 5 == 0 || i == length(warehouse_nodes_base))
    cat("  OD dégradé :", round(i / length(warehouse_nodes_base) * 100, 1), "%\n")
}

# Suppression du checkpoint une fois la boucle terminée avec succès         
if (file.exists(CHECKPOINT_OD_DEG)) file.remove(CHECKPOINT_OD_DEG)         

od_degrade <- bind_rows(od_rows_degrade)

cat("✓ Matrice OD dégradée calculée\n\n")


################################################################################
# PARTIE IX.3 — CALCUL DES SURCOÛTS ET CLASSIFICATION DES IMPACTS
#
# On compare les deux matrices OD (avant / après perturbation) pour calculer :
#   - Le surcoût absolu (RWF supplémentaires par trajet)
#   - Le surcoût relatif (% d'augmentation)
#   - Le type d'impact (détour, déconnexion, inchangé)
#   - Les zones les plus touchées en cumulant leurs surcoûts
################################################################################

cat("── Calcul des surcoûts ──────────────────────────────────────────────\n\n")

# ── Fusion des deux matrices OD (référence + dégradée) ────────────────────────
# left_join() : pour chaque paire OD dans la matrice de référence, on récupère
# le coût dégradé correspondant. Les colonnes by = sont les clés de jointure.

od_compare <- od_long %>%
  left_join(
    od_degrade %>%
      select(id_origine, id_destination, cout_degrade, connecte,
             distance_km_degrade,
             co2_kg_degrade, nox_g_degrade, pm25_g_degrade),
    by = c("id_origine", "id_destination")
  ) %>%
  mutate(
    
    # Surcoût absolu : différence de coût entre la situation dégradée et normale.
    # Si la route est coupée (cout_degrade = Inf), le surcoût est NA
    # (on le traite séparément dans la variable "type_impact").
    surcout_absolu_rwf  = if_else(
      connecte,
      cout_degrade - cout_rwf,
      NA_real_
    ),
    
    # Surcoût relatif : augmentation en % par rapport au coût de référence.
    # NULLIF équivalent en R : on évite la division par zéro si cout_rwf = 0.
    surcout_relatif_pct = if_else(
      connecte & cout_rwf > 0,
      round((cout_degrade - cout_rwf) / cout_rwf * 100, 1),
      NA_real_
    ),
    
    # Classification du type d'impact pour chaque paire OD.
    # case_when() : équivalent R de if / else if / else.
    # L'ordre des conditions compte : la première condition vraie est retenue.
    type_impact = case_when(
      is.na(connecte) | !connecte   ~ "deconnecte",   # Plus aucun chemin possible
      surcout_absolu_rwf  == 0      ~ "inchange",     # Le chemin optimal ne passe pas par la zone perturbée
      surcout_relatif_pct < 10      ~ "faible",       # Détour minime (< 10%)
      surcout_relatif_pct < 50      ~ "modere",       # Détour notable (10-50%)
      surcout_relatif_pct < 100     ~ "fort",         # Détour majeur (50-100%)
      TRUE                          ~ "tres_fort"     # Doublement ou plus du coût
    ),
    
    # Conversion de type_impact en facteur ordonné pour les graphiques
    type_impact = factor(
      type_impact,
      levels = c("inchange", "faible", "modere", "fort", "tres_fort", "deconnecte")
    )
  )

# ── Enrichissement de od_compare avec les émissions supplémentaires ───────────
# Pour chaque paire OD affectée par la perturbation, on calcule les émissions
# de CO2 supplémentaires générées par l'allongement du trajet.
od_compare <- od_compare %>%
  mutate(
    
    # ── Distance réelle du détour (km supplémentaires) ────────────────────────
    # Positive si le chemin dégradé est plus long, nulle si inchangé,
    # NA si la zone est déconnectée.
    delta_distance_km = case_when(
      type_impact == "deconnecte" ~ NA_real_,
      type_impact == "inchange"   ~ 0,
      TRUE                        ~ distance_km_degrade - distance_km
    ),
    
    # ── Émissions supplémentaires ─────────────────────────────────────────────
    co2_surcout_kg = case_when(
      type_impact == "deconnecte" ~ NA_real_,
      type_impact == "inchange"   ~ 0,
      !is.na(co2_kg_degrade)      ~ pmax(0, co2_kg_degrade - co2_kg_trajet),
      TRUE                        ~ 0
    ),
    nox_surcout_g = case_when(
      type_impact == "deconnecte" ~ NA_real_,
      type_impact == "inchange"   ~ 0,
      !is.na(nox_g_degrade)      ~ pmax(0, nox_g_degrade - nox_g_trajet),
      TRUE                        ~ 0
    ),
    pm25_surcout_g = case_when(
      type_impact == "deconnecte" ~ NA_real_,
      type_impact == "inchange"   ~ 0,
      !is.na(pm25_g_degrade)      ~ pmax(0, pm25_g_degrade - pm25_g_trajet),
      TRUE                        ~ 0
    )
  )

# Rapport global enrichi
co2_surcout_total_kg  <- sum(od_compare$co2_surcout_kg,  na.rm = TRUE)
nox_surcout_total_g   <- sum(od_compare$nox_surcout_g,   na.rm = TRUE)
pm25_surcout_total_g  <- sum(od_compare$pm25_surcout_g,  na.rm = TRUE)
dist_surcout_total_km <- sum(od_compare$delta_distance_km, na.rm = TRUE)

n_paires_na <- sum(is.na(od_compare$co2_surcout_kg))

cat("── Émissions supplémentaires (émissions réelles des arêtes) ───\n")
cat("  Km supplémentaires total :",
    format(round(dist_surcout_total_km), big.mark = " "), "km\n")
cat("  CO2  supplémentaire      :",
    round(co2_surcout_total_kg  / 1000, 1), "tonnes\n")
cat("  NOx  supplémentaire      :",
    round(nox_surcout_total_g   / 1000, 1), "kg\n")
cat("  PM2.5 supplémentaire     :",
    round(pm25_surcout_total_g  / 1000, 1), "kg\n")
cat("  ⚠ Paires déconnectées (émissions non calculables) :",
    n_paires_na, "\n")
cat("  sur", DUREE_JOURS, "jours de perturbation\n\n")

# ── Sauvegarde dans DuckDB ────────────────────────────────────────────────────
# On stocke la table de comparaison dans DuckDB pour des requêtes SQL ultérieures.
# Le nom de la table inclut le nom du scénario pour permettre de stocker
# plusieurs scénarios simultanément.
nom_table_impact <- paste0("impact_", NOM_SCENARIO)
duck_write(od_compare, nom_table_impact)

# ── Statistiques globales d'impact ────────────────────────────────────────────
stats_impact <- od_compare %>%
  group_by(type_impact) %>%
  summarise(
    n_paires       = n(),
    pct_paires     = round(n() / nrow(od_compare) * 100, 1),
    surcout_moy    = round(mean(surcout_absolu_rwf,  na.rm = TRUE), 2),
    surcout_median = round(median(surcout_absolu_rwf, na.rm = TRUE), 2),
    .groups        = "drop"
  )

cat("Distribution des impacts par type :\n")
print(stats_impact)

# ── Zones les plus touchées ───────────────────────────────────────────────────
# Pour chaque zone, on cumule les surcoûts sur tous ses trajets (comme origine
# ET comme destination) pour mesurer son exposition totale à la perturbation.
surcouts_par_zone <- od_compare %>%
  filter(type_impact != "inchange") %>%
  group_by(Zone = nom_origine) %>%
  summarise(
    surcout_total_rwf  = round(sum(surcout_absolu_rwf,  na.rm = TRUE), 1),
    surcout_moyen_rwf  = round(mean(surcout_absolu_rwf, na.rm = TRUE), 2),
    n_paires_touchees  = n(),
    n_deconnexions     = sum(type_impact == "deconnecte"),
    pct_surcout_moyen  = round(mean(surcout_relatif_pct, na.rm = TRUE), 1),
    .groups            = "drop"
  ) %>%
  arrange(desc(surcout_total_rwf))

cat("\nTop 10 des zones les plus touchées (en tant qu'origine) :\n")
print(head(surcouts_par_zone, 10))
cat("\n")

# Même calcul côté destination (quelles zones reçoivent moins de fret ?)
surcouts_par_destination <- od_compare %>%
  filter(type_impact != "inchange") %>%
  group_by(Zone = nom_destination) %>%
  summarise(
    surcout_total_rwf = round(sum(surcout_absolu_rwf,  na.rm = TRUE), 1),
    n_deconnexions    = sum(type_impact == "deconnecte"),
    .groups           = "drop"
  ) %>%
  arrange(desc(surcout_total_rwf))

cat("Top 5 des zones les plus isolées (en tant que destination) :\n")
print(head(surcouts_par_destination, 5))
cat("\n")

# ── Pré-calcul de la matrice de fractions de flux perdus par paire OD ─────────
# Traduit chaque paire OD de od_compare en fraction [0, 1] du flux commercial
# considéré comme perdu pendant la perturbation :
#   - déconnecté            → 1.0  (flux totalement interrompu)
#   - surcoût de x%         → x/100 (élasticité-prix implicite = 1)
#   - inchangé ou NA        → 0.0
#
# Cette matrice n_warehouses × n_warehouses est sauvegardée dans PERSIST_VULNERAB.
od_lookup_perdu <- od_compare %>%
  select(id_origine, id_destination, type_impact, surcout_relatif_pct) %>%
  mutate(
    fraction_perdue = case_when(
      type_impact == "deconnecte" ~ 1.0,
      type_impact == "inchange"   ~ 0.0,
      is.na(surcout_relatif_pct)  ~ 0.0,
      TRUE ~ pmin(1.0, surcout_relatif_pct / 100)
    )
  )

fraction_perdue_zone <- matrix(0, nrow = n_warehouses, ncol = n_warehouses)
for (k in seq_len(nrow(od_lookup_perdu))) {
  i <- od_lookup_perdu$id_origine[k]
  j <- od_lookup_perdu$id_destination[k]
  fraction_perdue_zone[i, j] <- od_lookup_perdu$fraction_perdue[k]
}

cat("✓ Matrice fraction_perdue_zone calculée (",
    sum(fraction_perdue_zone > 0), "paires impactées)\n\n")


################################################################################
# PARTIE IX.4 — IDENTIFICATION DES ARÊTES CRITIQUES
#
# OBJECTIF : Trouver les arêtes qui, si elles sont supprimées individuellement,
#            causent le plus grand surcoût agrégé sur le réseau.
# MÉTHODE  : Pour chaque arête candidate, on simule sa suppression isolée
#            et on calcule le surcoût OD total. On classe ensuite les arêtes
#            par ordre décroissant de criticité.
#
# REMARQUE SUR LE TEMPS DE CALCUL :
#   Tester TOUTES les arêtes du réseau serait trop long (Dijkstra × 30 000 arêtes).
#   On se restreint aux arêtes "candidates" selon deux filtres :
#     1. Les arêtes sur le chemin optimal d'au moins une paire OD (utiles)
#     2. Parmi elles, les arêtes de fort volume de trafic (impactantes)
#   Ce sous-ensemble représente typiquement 5-15% du réseau total,
#   ce qui rend le calcul faisable en moins d'une heure.
#
# PARAMÈTRE :
#   N_TOP_ARETES_CRITIQUES — nombre d'arêtes à tester (par ordre de volume)
#   Augmenter ce nombre = analyse plus complète mais plus lente.
################################################################################

cat("── Analyse de criticité des arêtes ──────────────────────────────────\n\n")

# ── Sélection des arêtes candidates ───────────────────────────────────────────
# On prend les N arêtes avec le plus gros volume de trafic fret,
# car ce sont les candidates les plus susceptibles d'être critiques.
# Les arêtes sans trafic (routes jamais empruntées dans le modèle) sont exclues.
aretes_candidates <- aretes_reseau_sf %>%
  filter(!is.na(volume_tonnes), volume_tonnes > 0) %>%
  arrange(desc(volume_tonnes)) %>%
  slice_head(n = N_TOP_ARETES_CRITIQUES) %>%
  pull(arete_idx)

cat("  Arêtes candidates :", length(aretes_candidates),
    "(top", N_TOP_ARETES_CRITIQUES, "par volume de trafic)\n")

# ── Fonction de calcul du surcoût total pour une suppression individuelle ─────
# calculer_surcout_total() :
#   - Prend un vecteur d'indices d'arêtes physiques à supprimer
#   - Construit un graphe temporaire avec ces arêtes bloquées (poids = Inf)
#   - Recalcule les distances OD pour les paires les plus importantes
#   - Retourne le surcoût total agrégé (en RWF)
calculer_surcout_total <- function(indices_a_supprimer, graphe_ref, paires_imp) {

  # Blocage des arêtes perturbées dans toutes les couches véhicule.
  # R applique copy-on-modify : graphe_ref est copié localement à cette ligne,
  # l'original (graphe_criticite) reste intact pour l'itération suivante.
  idx_mm_temp <- which(lookup_type == "route" & lookup_physique %in% indices_a_supprimer)
  igraph::E(graphe_ref)$weight[idx_mm_temp] <- Inf

  # Regroupement des paires par origine unique.
  n_wh          <- length(warehouse_nodes_base)
  targets_all_c <- as.vector(sapply(seq_len(n_vehicules), function(v) node_multi(v, warehouse_nodes_base)))
  origines_uniq <- unique(paires_imp[, 1])

  surcout_cumule <- 0
  n_deconnexions <- 0L

  for (i_u in origines_uniq) {

    # Toutes les destinations de cette origine (diagonale déjà exclue)
    j_list <- paires_imp[paires_imp[, 1] == i_u, 2]
    if (length(j_list) == 0) next

    sources_u <- sapply(seq_len(n_vehicules), function(v) node_multi(v, warehouse_nodes_base[i_u]))

    # Un seul appel distances() pour toutes les destinations de l'origine i_u
    dists_u <- igraph::distances(
      graphe_ref,
      v       = sources_u,
      to      = targets_all_c,
      weights = igraph::E(graphe_ref)$weight
    )

    for (j_k in j_list) {
      cols_k         <- j_k + (seq_len(n_vehicules) - 1) * n_wh
      cout_degrade_k <- min(dists_u[, cols_k], na.rm = TRUE)

      if (is.infinite(cout_degrade_k)) {
        n_deconnexions <- n_deconnexions + 1L
        next
      }

      ref_k <- od_ref_map[paste0(i_u, "_", j_k)]
      if (is.na(ref_k) || ref_k == 0) next

      surcout_cumule <- surcout_cumule +
        max(0, cout_degrade_k - ref_k) * flux_tonnes_total[i_u, j_k]
    }
  }

  list(surcout = surcout_cumule, n_deconnexions = n_deconnexions)
}

# ── Calcul de la criticité pour chaque arête candidate ────────────────────────
# Paires importantes calculées une seule fois ici, avant la boucle.
paires_importantes_crit <- which(flux_tonnes_total > SEUIL_PAIRES_CRITICITE, arr.ind = TRUE)
paires_importantes_crit <- paires_importantes_crit[
  paires_importantes_crit[, 1] != paires_importantes_crit[, 2], , drop = FALSE
]

# Copie unique du graphe avant la boucle.
# R copiera graphe_criticite localement dans calculer_surcout_total() au moment
# de la modification des poids (copy-on-modify)
graphe_criticite <- recuperer_lourd("graphe_multimodal")

cat("  Paires OD importantes (seuil :", SEUIL_PAIRES_CRITICITE, "t) :",
    nrow(paires_importantes_crit), "\n")
cat("  Calcul de la criticité (", length(aretes_candidates),
    "arêtes × Dijkstra) — prend environ 2h...\n")

criticite_df <- tibble(
  arete_idx         = aretes_candidates,
  surcout_pondere   = NA_real_,
  n_deconnexions_caus = NA_integer_
)

pb_crit <- progress_bar$new(
  format = "  Criticité [:bar] :percent | ETA: :eta",
  total  = length(aretes_candidates),
  clear  = FALSE,
  width  = 60
)

for (k in seq_along(aretes_candidates)) {
  resultat_k <- calculer_surcout_total(aretes_candidates[k], graphe_criticite, paires_importantes_crit)
  criticite_df$surcout_pondere[k]     <- resultat_k$surcout
  criticite_df$n_deconnexions_caus[k] <- resultat_k$n_deconnexions
  pb_crit$tick()
}

# ── Enrichissement avec les attributs de chaque arête ─────────────────────────
# On récupère les attributs (road_type, longueur, etc.) pour interpréter
# les résultats de criticité.
criticite_df <- criticite_df %>%
  left_join(
    aretes_reseau_sf %>%
      st_drop_geometry() %>%
      select(arete_idx, osm_id, name, road_type, surface,
             longueur_m, volume_tonnes, cost_per_tkm),
    by = "arete_idx"
  ) %>%
  arrange(desc(surcout_pondere)) %>%
  mutate(
    rang              = row_number(),
    longueur_km       = round(longueur_m / 1000, 2),
    surcout_pondere_k = round(surcout_pondere / 1000, 1)   # En milliers RWF×tonnes
  )

# ── Sauvegarde de la table de criticité dans DuckDB ───────────────────────────
duck_write(criticite_df, paste0("criticite_aretes_", NOM_SCENARIO))

cat("\n✓ Top 10 des arêtes les plus critiques :\n")
print(
  criticite_df %>%
    slice_head(n = 10) %>%
    select(rang, osm_id, name, road_type, longueur_km,
           volume_tonnes, surcout_pondere_k, n_deconnexions_caus) %>%
    rename(Rang        = rang,
           OSM_ID      = osm_id,
           Nom         = name,
           Type        = road_type,
           Long_km     = longueur_km,
           Vol_t       = volume_tonnes,
           Criticite_k = surcout_pondere_k,
           Deconnex    = n_deconnexions_caus
    )
)
cat("\n")


# ── EXPORTS CSV et Parquet ────────────────────────────────────────────────────
# Export de la table de comparaison OD (avant / après)
dbExecute(con, paste0(
  "COPY (SELECT * FROM ", nom_table_impact, ") TO '",
  file.path(DIR_EXPORTS, paste0("impact_od_", NOM_SCENARIO, ".csv")),
  "' (FORMAT CSV, HEADER)"
))

# Export de la table de criticité des arêtes
dbExecute(con, paste0(
  "COPY (SELECT * FROM criticite_aretes_", NOM_SCENARIO, ") TO '",
  file.path(DIR_EXPORTS, paste0("criticite_aretes_", NOM_SCENARIO, ".csv")),
  "' (FORMAT CSV, HEADER)"
))

cat("✓ Exports CSV terminés\n\n")

# ── RAPPORT FINAL DE LA PARTIE IX ─────────────────────────────────────────────

cat("==========================================================\n")
cat("  RAPPORT — ANALYSE DE VULNÉRABILITÉ\n")
cat("==========================================================\n\n")
cat("Scénario        :", NOM_SCENARIO, "\n")
cat("Description     :", DESCRIPTION_SCENARIO, "\n")
cat("Durée estimée   :", DUREE_JOURS, "jours\n")
cat("Type d'événement:", TYPE_EVENEMENT, "\n\n")

cat("RÉSEAU PERTURBÉ :\n")
cat("  Arêtes coupées            :", n_perturb, "\n")
cat("  Longueur hors service     :", resume_perturb$longueur_km, "km\n")
cat("  Part du réseau total      :", resume_perturb$pct_du_reseau, "%\n\n")

cat("IMPACT SUR LES FLUX OD :\n")
cat("  Paires inchangées         :",
    sum(od_compare$type_impact == "inchange", na.rm = TRUE), "\n")
cat("  Paires avec détour faible :",
    sum(od_compare$type_impact == "faible", na.rm = TRUE), "\n")
cat("  Paires avec détour modéré :",
    sum(od_compare$type_impact == "modere", na.rm = TRUE), "\n")
cat("  Paires avec détour fort   :",
    sum(od_compare$type_impact == "fort", na.rm = TRUE), "\n")
cat("  Paires fortement impactées:",
    sum(od_compare$type_impact == "tres_fort", na.rm = TRUE), "\n")
cat("  Paires déconnectées       :",
    sum(od_compare$type_impact == "deconnecte", na.rm = TRUE), "\n\n")

cat("SURCOÛT MOYEN (paires affectées) :",
    round(mean(od_compare$surcout_absolu_rwf, na.rm = TRUE), 2), "RWF\n")
cat("SURCOÛT RELATIF MOYEN            :",
    round(mean(od_compare$surcout_relatif_pct, na.rm = TRUE), 1), "%\n\n")

cat("ARÊTES LES PLUS CRITIQUES (top 5) :\n")
print(
  criticite_df %>%
    slice_head(n = 5) %>%
    select(rang, road_type, longueur_km, volume_tonnes, surcout_pondere_k) %>%
    rename(Rang = rang, Type = road_type, Long_km = longueur_km,
           Vol_t = volume_tonnes, Criticite = surcout_pondere_k)
)

cat("\nFICHIERS GÉNÉRÉS (Partie IX) :\n")
cat("  • carte_reseau_degrade_",   NOM_SCENARIO, ".png\n", sep = "")
cat("  • carte_criticite_aretes_", NOM_SCENARIO, ".png\n", sep = "")
cat("  • carte_vulnerabilite_zones_", NOM_SCENARIO, ".png\n", sep = "")
cat("  • graphique_surcouts_",     NOM_SCENARIO, ".png\n", sep = "")
cat("  • impact_od_",              NOM_SCENARIO, ".csv\n", sep = "")
cat("  • criticite_aretes_",       NOM_SCENARIO, ".csv\n", sep = "")

# ==============================================================================
# SAUVEGARDE INTER-SCRIPTS
# ==============================================================================

saveRDS(
  list(
    od_compare                = od_compare,
    od_degrade                = od_degrade,
    criticite_df              = criticite_df,
    indices_aretes_perturbees = indices_aretes_perturbees,
    aretes_perturbees_sf      = aretes_perturbees_sf,    
    fraction_perdue_zone      = fraction_perdue_zone,    
    surcouts_par_zone         = surcouts_par_zone,       
    fraction_perdue_prov      = if (exists("fraction_perdue_prov")) fraction_perdue_prov else NULL,
    surcout_pondere_arete     = surcout_pondere_arete,
    volume_detourne_arete     = volume_detourne_arete,
    NOM_SCENARIO              = NOM_SCENARIO,
    date_creation             = Sys.time()
  ),
  PERSIST_VULNERAB
)

cat("✓ persist_vulnerabilite.rds\n\n")

# Libération des gros objets intermédiaires de la partie IX.
# graphe_degrade et graphe_criticite sont des copies du graphe multimodal (~500 Mo
# selon la taille du réseau). aretes_ems_cache et od_rows_degrade peuvent aussi
# être volumineux. Tout est déjà dans le .rds ci-dessus.
objets_a_liberer <- c(
  "graphe_degrade", "graphe_criticite",
  "aretes_ems_cache", "aretes_ems_idx",
  "od_rows_degrade", "od_degrade",
  "chemins_par_vehicule", "edge_attrs_deg",
  "paires_importantes_crit"
)
rm(list = intersect(objets_a_liberer, ls()))
invisible(gc(verbose = FALSE))
invisible(gc(verbose = FALSE))

cat("Lancer un script viz_*.R pour la suite.\n")