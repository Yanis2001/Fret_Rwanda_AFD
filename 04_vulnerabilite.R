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
reseau_rwanda         <- .fret$reseau_rwanda
volume_trafic         <- .fret$volume_trafic
volume_trafic_mm_s    <- .fret$volume_trafic_mm_s
volume_par_secteur    <- .fret$volume_par_secteur
flux_tonnes_total     <- readRDS(PERSIST_FLUX_FRET)$flux_tonnes_total
n_noeuds              <- .mm$n_noeuds
n_vehicules           <- .mm$n_vehicules
stocker_lourd("graphe_multimodal", .mm$graphe_multimodal)
lookup_type           <- .map$lookup_type
lookup_physique       <- .map$lookup_physique
lookup_vehicule       <- .map$lookup_vehicule
max_idx_mm            <- .map$max_idx_mm
node_multi <- function(v_idx, n_id) as.integer((v_idx - 1L) * n_noeuds + n_id)

# Chargement de la matrice OD de référence (cache existant)
.od_cache <- readRDS(file.path(DIR_OUTPUT, "od_cache.rds"))
od_long   <- .od_cache$od_long
rm(.geo, .ent, .mm, .map, .fret, .od_cache)

source("utils_fond_carte.R")
cat("✓ Objets chargés\n\n")

################################################################################
# PARTIE IX — ANALYSE DE VULNÉRABILITÉ ET DE CONTOURNEMENT
#
# OBJECTIF : Simuler la suppression d'une ou plusieurs arêtes du réseau
#            (routes inondées, glissements de terrain, etc.) et mesurer
#            l'impact sur les coûts de transport entre toutes les paires OD.
#
# STRUCTURE :
#   IX.2 — Identification des arêtes perturbées
#   IX.3 — Recalcul de la matrice OD sur le réseau dégradé
#   IX.4 — Calcul des surcoûts et classification des impacts
#   IX.5 — Identification des arêtes critiques (analyse de sensibilité)
#   IX.6 — Cartes et exports
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
#     reseau_rwanda      — réseau sfnetworks avec coûts et volumes
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
# PARTIE IX.2 — IDENTIFICATION DES ARÊTES PERTURBÉES
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
    st_transform(crs = 32735)    # UTM Zone 35S = système métrique Rwanda
  
  # st_buffer() crée un cercle de rayon RAYON_PERTURBATION_M autour du point.
  # Ce cercle représente la zone géographique affectée par l'événement.
  zone_perturbation_buffer <- st_buffer(point_perturbation,
                                        dist = RAYON_PERTURBATION_M)
  
  # Récupération de toutes les arêtes du réseau sous forme sf
  aretes_sf_mode_b <- reseau_rwanda %>%
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
    aretes_sf_mode_c <- reseau_rwanda %>%
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
aretes_reseau_sf <- reseau_rwanda %>%
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
       "  → Mode Buffer : les coordonnées GPS sont-elles dans le Rwanda ?\n",
       "  → Mode Manuel : les osm_id existent-ils dans le réseau ?\n",
       "  → Mode Raster : le seuil est-il trop élevé ?\n")
}

cat("\n✓ Arêtes perturbées identifiées :", n_perturb, "\n")

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
# PARTIE IX.3 — RECALCUL DE LA MATRICE OD SUR LE RÉSEAU DÉGRADÉ
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
  od_long$cout_usd,
  paste0(od_long$id_origine, "_", od_long$id_destination)
)
cat("  Table de référence OD pré-chargée (", length(od_ref_map), "paires)\n\n")

# ── Récupération des poids originaux du graphe multi-modal ────────────────────
# igraph::E() : accède aux arêtes (edges) du graphe.
# $weight : attribut "weight" de chaque arête (coût de transport en USD).
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

# On stocke les résultats dans une liste, puis on l'assemble en data.frame.
# La structure est identique à od_long (Partie VI) pour faciliter la comparaison.
od_rows_degrade <- list()
idx_deg         <- 0

# ── Chargement du checkpoint si disponible ────────────────────────────────────   
CHECKPOINT_OD_DEG <- file.path(DIR_OUTPUT, "od_degrade_checkpoint.rds")     
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
  
  for (j in seq_along(warehouse_nodes_base)) {
    if (i == j) next
    
    cols_j      <- j + (seq_len(n_vehicules) - 1) * length(warehouse_nodes_base)
    min_cout_deg <- min(dists_deg[, cols_j], na.rm = TRUE)
    
    # ── Reconstruction du chemin dégradé pour mesurer la distance réelle ──────
    distance_km_degrade <- NA_real_
    
    if (!is.infinite(min_cout_deg)) {
      best_idx_mat <- which(dists_deg[, cols_j] == min_cout_deg, arr.ind = TRUE)
      if (!is.matrix(best_idx_mat)) best_idx_mat <- matrix(best_idx_mat, nrow = 1)
      best_from_deg <- sources_i[best_idx_mat[1, 1]]
      best_to_deg   <- targets_all[cols_j[best_idx_mat[1, 2]]]
      
      path_deg <- igraph::shortest_paths(
        graphe_degrade,
        from    = best_from_deg,
        to      = best_to_deg,
        weights = igraph::E(graphe_degrade)$weight,
        output  = "epath"
      )
      edges_path_deg <- as.integer(path_deg$epath[[1]])
      
      # Calcul des émissions réelles sur le chemin de contournement
      co2_kg_degrade  <- NA_real_
      nox_g_degrade   <- NA_real_
      pm25_g_degrade  <- NA_real_
      
      if (length(edges_path_deg) > 0) {
        
        # Filtrer les arêtes "route" (pas les transbordements)
        edges_routes_deg <- edges_path_deg[lookup_type[edges_path_deg] == "route"]
        
        if (length(edges_routes_deg) > 0) {
          
          # Remontée vers les arêtes physiques et véhicules
          idx_phys_deg <- lookup_physique[edges_routes_deg]
          veh_id_deg   <- lookup_vehicule[edges_routes_deg]
          
          # Requête DuckDB : émissions réelles arête × véhicule
          paires_sql <- paste0(
            "SELECT co2_kg, nox_g, pm25_g FROM aretes_couts_tous ",
            "WHERE (arete_id, vehicule_id) IN (",
            paste(sprintf("(%d,'%s')", idx_phys_deg, veh_id_deg), collapse = ","),
            ")"
          )
          ems_deg <- duck_query(paires_sql)
          
          co2_kg_degrade <- sum(ems_deg$co2_kg,  na.rm = TRUE)
          nox_g_degrade  <- sum(ems_deg$nox_g,   na.rm = TRUE)
          pm25_g_degrade <- sum(ems_deg$pm25_g,  na.rm = TRUE)
        }
      }
      
      rm(path_deg)
      
      # ── Accumulation pour les itinéraires de contournement ──────────────────
      # Les paires inchangées (min_cout_deg ≈ cout_ref_ij) sont
      # ignorées car leur chemin de détour est identique au chemin de référence.
      if (!is.infinite(min_cout_deg) && length(edges_path_deg) > 0) {
        
        # Accès O(1) au coût de référence via la hash map (évite filter(od_long))
        cout_ref_ij <- od_ref_map[paste0(i, "_", j)]
        
        # Vérifications : paire connue + surcoût positif + cout_ref > 0 (évite / 0)
        if (!is.na(cout_ref_ij) && min_cout_deg > cout_ref_ij && cout_ref_ij > 0) {
          
          surcout_rel_ij <- (min_cout_deg - cout_ref_ij) / cout_ref_ij * 100
          volume_ij      <- flux_tonnes_total[i, j]
          
          if (!is.na(volume_ij) && volume_ij > 0) {
            
            # Extraction des arêtes "route" uniquement (on exclut les arêtes de
            # transbordement inter-véhicules qui n'ont pas d'équivalent physique).
            # lookup_type et lookup_physique ont été construits en Partie V.2.
            edges_routes_ij <- edges_path_deg[
              edges_path_deg <= max_idx_mm &
                lookup_type[edges_path_deg] == "route"
            ]
            
            if (length(edges_routes_ij) > 0) {
              
              idx_phys_ij <- lookup_physique[edges_routes_ij]
              # Filtrage défensif : on ne garde que les indices dans [1, n_aretes_physiques].
              # Des indices hors plage peuvent apparaître sur les arêtes dégénérées
              # créées par to_spatial_subdivision() (Partie III.2).
              idx_phys_ij <- idx_phys_ij[
                idx_phys_ij >= 1L & idx_phys_ij <= n_aretes_physiques
              ]
              
              if (length(idx_phys_ij) > 0) {
                
                # Accumulation pondérée : un flux de 5 000 t avec +50% de surcoût
                # pèse davantage qu'un flux de 50 t avec +200% dans le classement
                # final des axes de détour les plus sollicités.
                surcout_pondere_arete[idx_phys_ij] <-
                  surcout_pondere_arete[idx_phys_ij] + surcout_rel_ij * volume_ij
                volume_detourne_arete[idx_phys_ij] <-
                  volume_detourne_arete[idx_phys_ij] + volume_ij
              }
            }
          }
        }
      }
      
      if (length(edges_path_deg) > 0) {
        edge_data_deg   <- igraph::edge_attr(graphe_degrade)
        distance_km_degrade <- sum(
          edge_data_deg$length_km[edges_path_deg], na.rm = TRUE
        )
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
# PARTIE IX.4 — CALCUL DES SURCOÛTS ET CLASSIFICATION DES IMPACTS
#
# On compare les deux matrices OD (avant / après perturbation) pour calculer :
#   - Le surcoût absolu (USD supplémentaires par trajet)
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
    surcout_absolu_usd  = if_else(
      connecte,
      cout_degrade - cout_usd,
      NA_real_
    ),
    
    # Surcoût relatif : augmentation en % par rapport au coût de référence.
    # NULLIF équivalent en R : on évite la division par zéro si cout_usd = 0.
    surcout_relatif_pct = if_else(
      connecte & cout_usd > 0,
      round((cout_degrade - cout_usd) / cout_usd * 100, 1),
      NA_real_
    ),
    
    # Classification du type d'impact pour chaque paire OD.
    # case_when() : équivalent R de if / else if / else.
    # L'ordre des conditions compte : la première condition vraie est retenue.
    type_impact = case_when(
      is.na(connecte) | !connecte   ~ "deconnecte",   # Plus aucun chemin possible
      surcout_absolu_usd  == 0      ~ "inchange",     # Le chemin optimal ne passe pas par la zone perturbée
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
    surcout_moy    = round(mean(surcout_absolu_usd,  na.rm = TRUE), 2),
    surcout_median = round(median(surcout_absolu_usd, na.rm = TRUE), 2),
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
    surcout_total_usd  = round(sum(surcout_absolu_usd,  na.rm = TRUE), 1),
    surcout_moyen_usd  = round(mean(surcout_absolu_usd, na.rm = TRUE), 2),
    n_paires_touchees  = n(),
    n_deconnexions     = sum(type_impact == "deconnecte"),
    pct_surcout_moyen  = round(mean(surcout_relatif_pct, na.rm = TRUE), 1),
    .groups            = "drop"
  ) %>%
  arrange(desc(surcout_total_usd))

cat("\nTop 10 des zones les plus touchées (en tant qu'origine) :\n")
print(head(surcouts_par_zone, 10))
cat("\n")

# Même calcul côté destination (quelles zones reçoivent moins de fret ?)
surcouts_par_destination <- od_compare %>%
  filter(type_impact != "inchange") %>%
  group_by(Zone = nom_destination) %>%
  summarise(
    surcout_total_usd = round(sum(surcout_absolu_usd,  na.rm = TRUE), 1),
    n_deconnexions    = sum(type_impact == "deconnecte"),
    .groups           = "drop"
  ) %>%
  arrange(desc(surcout_total_usd))

cat("Top 5 des zones les plus isolées (en tant que destination) :\n")
print(head(surcouts_par_destination, 5))
cat("\n")


################################################################################
# PARTIE IX.5 — IDENTIFICATION DES ARÊTES CRITIQUES
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
#   - Retourne le surcoût total agrégé (en USD)

calculer_surcout_total <- function(indices_a_supprimer) {
  
  # Construction du graphe temporaire
  graphe_temp <- recuperer_lourd("graphe_multimodal")
  
  # Indices multi-modaux à bloquer (toutes couches véhicule)
  idx_mm_temp <- which(
    lookup_type     == "route" &
      lookup_physique %in% indices_a_supprimer
  )
  igraph::E(graphe_temp)$weight[idx_mm_temp] <- Inf
  
  # Paires OD à tester (uniquement les paires avec fort volume de fret)
  # flux_tonnes_total a été construit en Partie VIII
  paires_importantes <- which(flux_tonnes_total > SEUIL_PAIRES_CRITICITE,
                              arr.ind = TRUE)
  
  surcout_cumule <- 0
  n_deconnexions <- 0L
  
  for (k in seq_len(nrow(paires_importantes))) {
    i_k <- paires_importantes[k, 1]
    j_k <- paires_importantes[k, 2]
    if (i_k == j_k) next
    
    sources_k <- sapply(seq_len(n_vehicules),
                        function(v) node_multi(v, warehouse_nodes_base[i_k]))
    cols_k    <- j_k + (seq_len(n_vehicules) - 1) * length(warehouse_nodes_base)
    targets_k <- as.vector(sapply(seq_len(n_vehicules),
                                  function(v) node_multi(v, warehouse_nodes_base)))
    
    dists_k <- igraph::distances(
      graphe_temp,
      v       = sources_k,
      to      = targets_k[cols_k],
      weights = igraph::E(graphe_temp)$weight
    )
    
    cout_degrade_k <- min(dists_k, na.rm = TRUE)
    
    # Décompte des arêtes déconnectées du réseau
    if (is.infinite(cout_degrade_k)) {
      n_deconnexions <- n_deconnexions + 1L
      next
    }
    
    # Coût de référence pour cette paire (depuis od_long)
    ref_k <- od_long %>%
      filter(id_origine == i_k, id_destination == j_k) %>%
      pull(cout_usd)
    if (length(ref_k) == 0 || is.na(ref_k)) next
    
    delta_k <- max(0, cout_degrade_k - ref_k)
    # Pondération par le volume de fret : une arête qui détourne 10 000 tonnes
    # est plus critique qu'une arête qui détourne 10 tonnes au même surcoût.
    surcout_cumule <- surcout_cumule +
      delta_k * flux_tonnes_total[i_k, j_k]
  }
  
  list(surcout = surcout_cumule, n_deconnexions = n_deconnexions)
}

# ── Calcul de la criticité pour chaque arête candidate ────────────────────────
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
  resultat_k <- calculer_surcout_total(aretes_candidates[k])
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
    surcout_pondere_k = round(surcout_pondere / 1000, 1)   # En milliers USD×tonnes
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


################################################################################
# PARTIE IX.6 — CARTES ET EXPORTS
#
# Génère quatre sorties visuelles :
#   Carte A — Réseau dégradé : arêtes perturbées + impact sur les OD
#   Carte B — Arêtes critiques : classement des segments les plus sensibles
#   Carte C — Surcoûts par zone : gradient de vulnérabilité économique
#   Graphique — Distribution des surcoûts relatifs par type de route
#   Carte D — Nouvelles routes 
#   Graphique — Report modal.
################################################################################

cat("── Génération des cartes et exports ──────────────────────────────────\n\n")

# Palette spécifique aux types d'impact (cohérente avec la mind map)
PALETTE_IMPACT <- c(
  "inchange"   = "#CCCCCC",   # Gris — pas d'impact
  "faible"     = "#FFFFB2",   # Jaune pâle — détour < 10%
  "modere"     = "#FECC5C",   # Jaune-orange — détour 10-50%
  "fort"       = "#FD8D3C",   # Orange — détour 50-100%
  "tres_fort"  = "#E31A1C",   # Rouge vif — doublement du coût
  "deconnecte" = "#800026"    # Rouge foncé — zone coupée du réseau
)

# ── Préparation des couches spatiales ─────────────────────────────────────────

# Arêtes perturbées (pour les surligner sur la carte)
aretes_perturbees_sf <- aretes_reseau_sf %>%
  filter(arete_idx %in% indices_aretes_perturbees)

# Arêtes critiques (top N pour la Carte B)
N_ARETES_AFFICHEES <- min(200, nrow(criticite_df))
aretes_critiques_sf <- aretes_reseau_sf %>%
  filter(arete_idx %in% criticite_df$arete_idx[1:N_ARETES_AFFICHEES]) %>%
  left_join(
    criticite_df %>% select(arete_idx, rang, surcout_pondere_k),
    by = "arete_idx"
  )

# Points des zones colorés par impact (surcoût moyen relatif)
impact_par_zone_sf <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  st_as_sf() %>%
  left_join(
    surcouts_par_zone %>%
      select(Zone, pct_surcout_moyen, n_deconnexions, surcout_total_usd),
    by = c("warehouse_name" = "Zone")
  ) %>%
  mutate(
    pct_surcout_moyen = replace_na(pct_surcout_moyen, 0),
    surcout_total_usd = replace_na(surcout_total_usd, 0)
  )

# ── CARTE A : Réseau dégradé et zones d'impact ────────────────────────────────
cat("  Génération Carte A — réseau dégradé...\n")

# Zone tampon visible autour des arêtes perturbées (pour la localiser sur la carte)
# st_buffer() + st_union() : crée une zone en surbrillance autour des routes coupées
zone_impact_visible <- aretes_perturbees_sf %>%
  st_buffer(dist = 2000) %>%   # 2km de buffer pour être visible sur la carte
  st_union()

carte_reseau_degrade <- fond_carte() +
  
  # Réseau de base en gris clair
  tm_shape(aretes_reseau_sf) +
  tm_lines(col = "#DDDDDD", lwd = 0.4) +
  
  # Zone d'impact en surbrillance semi-transparente
  tm_shape(zone_impact_visible %>% st_as_sf()) +
  tm_polygons(
    fill       = "#FF6B6B",
    col        = "#CC0000",
    fill_alpha = 0.25,
    lwd        = 1.5,
    fill.legend = tm_legend(show = FALSE)
  ) +
  
  # Arêtes perturbées en rouge épais
  tm_shape(aretes_perturbees_sf) +
  tm_lines(col = "#CC0000", lwd = 3.5,
           col.legend = tm_legend(show = FALSE)) +
  
  # Points des zones avec couleur selon le surcoût moyen
  tm_shape(impact_par_zone_sf) +
  tm_dots(
    fill       = "pct_surcout_moyen",
    fill.scale = tm_scale_intervals(
      style  = "fixed",
      breaks = c(0, 5, 20, 50, 100, Inf),
      values = c("#CCCCCC", "#FFFFB2", "#FD8D3C", "#E31A1C", "#800026")
    ),
    fill.legend = tm_legend(title = "Surcoût moyen\n(% hausse)"),
    size = 0.8
  ) +
  
  tm_title(paste0("Réseau dégradé — ", NOM_SCENARIO,
                  "\n", DESCRIPTION_SCENARIO)) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_reseau_degrade,
  file.path(DIR_OUTPUT, paste0("carte_reseau_degrade_", NOM_SCENARIO, ".png")),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ Carte A sauvegardée\n")

# ── CARTE B : Arêtes critiques (top N classées par criticité) ─────────────────
cat("  Génération Carte B — arêtes critiques...\n")

carte_criticite <- fond_carte() +
  
  # Réseau de base en gris très clair
  tm_shape(aretes_reseau_sf) +
  tm_lines(col = "#EEEEEE", lwd = 0.3) +
  
  # Arêtes avec trafic, colorées par leur rang de criticité
  # (plus rouge = plus critique = suppression la plus coûteuse)
  tm_shape(aretes_critiques_sf) +
  tm_lines(
    col        = "rang",
    col.scale  = tm_scale_intervals(
      style  = "fixed",
      breaks = c(0, 5, 10, 15, 20, Inf),
      values = rev(c("#FFF5F0", "#FCBBA1", "#FC7050", "#EF3B2C", "#99000D"))
    ),
    col.legend = tm_legend(title = paste0("Rang de criticité\n(top ",
                                          N_ARETES_AFFICHEES, ")")),
    lwd        = 3
  ) +
  
  # Arêtes perturbées du scénario actuel
  tm_shape(aretes_perturbees_sf) +
  tm_lines(col = "#0000CC", lwd = 2,
           col.legend = tm_legend(show = FALSE)) +
  
  tm_title(paste0("Arêtes critiques du réseau — ",
                  "Top ", N_ARETES_AFFICHEES, " par surcoût pondéré")) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_criticite,
  file.path(DIR_OUTPUT, paste0("carte_criticite_aretes_", NOM_SCENARIO, ".png")),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ Carte B sauvegardée\n")

# ── CARTE C : Vulnérabilité économique des zones ──────────────────────────────
cat("  Génération Carte C — vulnérabilité des zones...\n")

# Vérification : y a-t-il des surcoûts à représenter ?
has_surcouts <- any(impact_par_zone_sf$surcout_total_usd > 0, na.rm = TRUE)
has_deconnex <- any(impact_par_zone_sf$n_deconnexions   > 0, na.rm = TRUE)

if (!has_surcouts) {
  cat("  ⚠ Aucun surcoût détecté pour ce scénario — carte C simplifiée\n")
}

carte_vulnerabilite <- fond_carte() +
  
  tm_shape(aretes_reseau_sf) +
  tm_lines(col = "#DDDDDD", lwd = 0.3) +
  
  # Taille des points proportionnelle au surcoût total (exposition économique)
  # Couleur selon la présence de déconnexions (rouge = zone coupée du réseau)
  tm_shape(impact_par_zone_sf) +
  {
    if (has_surcouts) {
      # Version complète : taille et couleur variables
      tm_dots(
        fill       = "n_deconnexions",
        fill.scale = tm_scale_intervals(
          breaks = c(-Inf, 0, 1, 5, Inf),
          values = c("#2166AC", "#FEE08B", "#F46D43", "#A50026")
        ),
        fill.legend = tm_legend(title = "Nb de destinations\ncoupées"),
        size        = "surcout_total_usd",
        size.scale  = tm_scale(values.range = c(0.3, 2.5)),
        size.legend = tm_legend(title = "Surcoût total\n(USD)")
      ) 
    } else {
      # Version dégradée : taille fixe, couleur selon type de zone
      tm_dots(
        fill        = "warehouse_type",
        fill.scale  = tm_scale(values = PALETTE_ZONE_TYPE),
        fill.legend = tm_legend(title = "Type de zone"),
        size        = 0.6
      )
    }
  } +
  
  # Arêtes perturbées pour référence
  tm_shape(aretes_perturbees_sf) +
  tm_lines(col = "#CC0000", lwd = 3) +
  
  tm_title(paste0("Vulnérabilité économique des zones\n",
                  NOM_SCENARIO, " — Durée estimée : ",
                  DUREE_JOURS, " jours")) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_vulnerabilite,
  file.path(DIR_OUTPUT, paste0("carte_vulnerabilite_zones_", NOM_SCENARIO, ".png")),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ Carte C sauvegardée\n")

# ── GRAPHIQUE : Distribution des surcoûts relatifs ────────────────────────────
cat("  Génération du graphique de distribution...\n")

g_surcouts <- od_compare %>%
  filter(!is.na(surcout_relatif_pct), surcout_relatif_pct > 0) %>%
  ggplot(aes(x = surcout_relatif_pct, fill = type_impact)) +
  geom_histogram(bins = 40, color = "white", linewidth = 0.2) +
  scale_fill_manual(
    values = PALETTE_IMPACT,
    name   = "Type d'impact"
  ) +
  scale_x_continuous(
    labels = scales::percent_format(scale = 1),
    breaks = c(0, 10, 25, 50, 75, 100, 150, 200)
  ) +
  labs(
    title    = paste0("Distribution des surcoûts de transport — ", NOM_SCENARIO),
    subtitle = paste0(DESCRIPTION_SCENARIO,
                      "\nDurée estimée : ", DUREE_JOURS, " jours"),
    x        = "Hausse du coût de transport (%)",
    y        = "Nombre de paires OD affectées"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title    = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#555555")
  )

ggsave(
  file.path(DIR_OUTPUT, paste0("graphique_surcouts_", NOM_SCENARIO, ".png")),
  g_surcouts, width = 11, height = 6, dpi = 300
)
cat("  ✓ Graphique sauvegardé\n\n")


# ==============================================================================
# CARTE D — Itinéraires de contournement, colorés par surcoût moyen
# ==============================================================================

cat("  Génération Carte D — routes de contournement...\n")

# Palette de surcoût : vert (faible surcoût) → bordeaux (surcoût extrême)
PALETTE_SURCOUT_DETOUR <- c(
  "Faible (<10%)"       = "#1A9850",
  "Modéré (10–30%)"     = "#FEE08B",
  "Fort (30–60%)"       = "#FD8D3C",
  "Très fort (60–100%)" = "#E31A1C",
  "Extrême (>100%)"     = "#67001F"
)

n_paires_reroutees_total <- sum(
  !is.na(od_ref_map[paste0(od_degrade$id_origine, "_", od_degrade$id_destination)]) &
    od_degrade$cout_degrade >
    od_ref_map[paste0(od_degrade$id_origine, "_", od_degrade$id_destination)]
)
cat("  Paires reroutées traitées (toutes) :", n_paires_reroutees_total, "\n")

# Construction de la couche géographique des arêtes de détour.
# On exclut les arêtes perturbées elles-mêmes : seules les NOUVELLES
# routes (hors zone de choc) sont affichées.
aretes_detour_sf <- aretes_reseau_sf %>%
  mutate(
    surcout_moyen   = surcout_moyen_detour,
    vol_detourne_t  = volume_detourne_arete
  ) %>%
  filter(
    vol_detourne_t > 0,
    !(arete_idx %in% indices_aretes_perturbees)
  ) %>%
  mutate(
    classe_surcout = case_when(
      surcout_moyen < 10  ~ "Faible (<10%)",
      surcout_moyen < 30  ~ "Modéré (10–30%)",
      surcout_moyen < 60  ~ "Fort (30–60%)",
      surcout_moyen < 100 ~ "Très fort (60–100%)",
      TRUE                ~ "Extrême (>100%)"
    ),
    classe_surcout = factor(
      classe_surcout,
      levels = names(PALETTE_SURCOUT_DETOUR)
    ),
    # Épaisseur de ligne proportionnelle au volume détourné (échelle log)
    lwd_detour = as.numeric(rescale(log10(vol_detourne_t + 1), to = c(0.6, 5)))
  )

carte_detour <- fond_carte() +
  
  # Réseau de base en gris très clair (contexte géographique)
  tm_shape(aretes_reseau_sf) +
  tm_lines(col = "#EEEEEE", lwd = 0.3) +
  
  # Itinéraires de contournement : couleur = surcoût moyen, épaisseur = volume
  tm_shape(aretes_detour_sf) +
  tm_lines(
    col        = "classe_surcout",
    col.scale  = tm_scale(values = PALETTE_SURCOUT_DETOUR),
    col.legend = tm_legend(title = "Surcoût moyen\n(flux reroutés)"),
    lwd        = "lwd_detour",
    lwd.scale  = tm_scale(values.range = c(0.6, 5)),
    lwd.legend = tm_legend(show = FALSE)
  ) +
  
  # Routes coupées en noir épais (référence visuelle)
  tm_shape(aretes_perturbees_sf) +
  tm_lines(
    col        = "#000000",
    lwd        = 4,
    col.legend = tm_legend(show = FALSE)
  ) +
  
  # Zones d'entrepôt
  tm_shape(coords_zones_sf) +
  tm_dots(
    fill        = "warehouse_type",
    fill.scale  = tm_scale(values = PALETTE_ZONE_TYPE),
    fill.legend = tm_legend(title = "Type de zone"),
    size        = 0.5
  ) +
  
  tm_title(paste0(
    "Itinéraires de contournement — ", NOM_SCENARIO,
    "\nCouleur = surcoût moyen pondéré | Épaisseur = volume détourné | Noir = routes coupées"
  )) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_detour,
  file.path(DIR_OUTPUT, paste0("carte_detours_", NOM_SCENARIO, ".png")),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ carte_detours_", NOM_SCENARIO, ".png\n\n", sep = "")

# ==============================================================================
# GRAPHIQUE — Report de trafic par type de route (avant vs après le choc)
# ==============================================================================

cat("  Génération du graphique de report par type de route...\n")

# ── Volumes de référence par type de route (avant choc) ───────────────────────
vol_ref_type <- aretes_reseau_sf %>%
  st_drop_geometry() %>%
  mutate(volume_tonnes = replace_na(volume_tonnes, 0)) %>%
  group_by(road_type) %>%
  summarise(vol_ref_t = sum(volume_tonnes), .groups = "drop")

# ── Volume de détour entrant par type de route (nouvelles routes utilisées) ───
# On n'inclut QUE les arêtes non coupées pour mesurer les routes qui
# ABSORBENT le trafic rerouté, pas celles qui le perdent.
vol_detour_type <- aretes_reseau_sf %>%
  st_drop_geometry() %>%
  mutate(vol_det = volume_detourne_arete[arete_idx]) %>%
  filter(!(arete_idx %in% indices_aretes_perturbees)) %>%
  group_by(road_type) %>%
  summarise(vol_detour_t = sum(vol_det, na.rm = TRUE), .groups = "drop")

# ── Volume perdu (sur routes coupées) par type de route ───────────────────────
vol_perdu_type <- aretes_reseau_sf %>%
  st_drop_geometry() %>%
  filter(arete_idx %in% indices_aretes_perturbees) %>%
  mutate(volume_tonnes = replace_na(volume_tonnes, 0)) %>%
  group_by(road_type) %>%
  summarise(vol_perdu_t = sum(volume_tonnes), .groups = "drop")

# ── Assemblage et calcul de la variation nette ────────────────────────────────
report_df <- vol_ref_type %>%
  left_join(vol_detour_type, by = "road_type") %>%
  left_join(vol_perdu_type,  by = "road_type") %>%
  replace_na(list(vol_detour_t = 0, vol_perdu_t = 0)) %>%
  mutate(
    road_type       = factor(road_type,
                             levels = c("motorway", "trunk", "primary",
                                        "secondary", "tertiary", "unclassified")),
    # Variation nette = trafic de détour entrant - trafic perdu (route coupée)
    variation_nette = vol_detour_t - vol_perdu_t,
    pct_variation   = round(variation_nette / pmax(vol_ref_t, 1) * 100, 1),
    # Position verticale du label : au-dessus de la barre la plus haute
    y_label         = pmax(vol_detour_t, vol_perdu_t) / 1000
  ) %>%
  filter(!is.na(road_type))

# ── Format long pour ggplot ───────────────────────────────────────────────────
report_long <- report_df %>%
  pivot_longer(
    cols      = c(vol_ref_t, vol_detour_t, vol_perdu_t),
    names_to  = "categorie",
    values_to = "volume_t"
  ) %>%
  mutate(
    categorie = recode(categorie,
                       "vol_ref_t"    = "Référence (avant choc)",
                       "vol_detour_t" = "Report entrant (détour)",
                       "vol_perdu_t"  = "Perdu (route coupée)"
    ),
    categorie = factor(categorie,
                       levels = c("Référence (avant choc)",
                                  "Report entrant (détour)",
                                  "Perdu (route coupée)"))
  )

# ── Graphique ─────────────────────────────────────────────────────────────────
g_report <- ggplot(report_long,
                   aes(x = road_type, y = volume_t / 1000, fill = categorie)) +
  
  geom_col(position = "dodge", width = 0.72) +
  
  # Annotation de la variation nette au-dessus des barres
  geom_text(
    data    = report_df,
    mapping = aes(
      x     = road_type,
      y     = y_label + max(report_df$y_label, na.rm = TRUE) * 0.03,
      label = paste0(ifelse(pct_variation >= 0, "+", ""), pct_variation, "%"),
      color = ifelse(variation_nette >= 0, "#006400", "#CC0000")
    ),
    inherit.aes = FALSE,
    vjust    = 0,
    size     = 3.5,
    fontface = "bold"
  ) +
  
  # Ligne de référence à 0 pour la lisibilité
  geom_hline(yintercept = 0, color = "#AAAAAA", linewidth = 0.4) +
  
  scale_fill_manual(
    values = c(
      "Référence (avant choc)"  = "#4393C3",
      "Report entrant (détour)" = "#2CA25F",
      "Perdu (route coupée)"    = "#D6604D"
    )
  ) +
  scale_color_identity() +
  scale_y_continuous(
    labels = scales::label_number(suffix = " kt"),
    expand = expansion(mult = c(0, 0.18))
  ) +
  
  labs(
    title    = paste0("Report de trafic par type de route — ", NOM_SCENARIO),
    subtitle = paste0(
      "Bleu = volume de référence · Vert = trafic de détour absorbé · ",
      "Rouge = trafic perdu sur route coupée\n",
      "Pourcentage = variation nette / volume de référence"
    ),
    x    = "Type de route",
    y    = "Volume (milliers de tonnes)",
    fill = NULL
  ) +
  
  theme_minimal(base_size = 12) +
  theme(
    plot.title      = element_text(face = "bold", size = 13),
    plot.subtitle   = element_text(color = "#666666", size = 9),
    legend.position = "top",
    panel.grid.minor = element_blank(),
    axis.text.x     = element_text(angle = 20, hjust = 1)
  )

ggsave(
  file.path(DIR_OUTPUT, paste0("graphique_report_type_route_", NOM_SCENARIO, ".png")),
  g_report,
  width = 11,
  height = 6,
  dpi = 300
)
cat("  ✓ graphique_report_type_route_", NOM_SCENARIO, ".png\n\n", sep = "")

# ── EXPORTS CSV et Parquet ────────────────────────────────────────────────────
# Export de la table de comparaison OD (avant / après)
dbExecute(con, paste0(
  "COPY (SELECT * FROM ", nom_table_impact, ") TO '",
  file.path(DIR_OUTPUT, paste0("impact_od_", NOM_SCENARIO, ".csv")),
  "' (FORMAT CSV, HEADER)"
))

# Export de la table de criticité des arêtes
dbExecute(con, paste0(
  "COPY (SELECT * FROM criticite_aretes_", NOM_SCENARIO, ") TO '",
  file.path(DIR_OUTPUT, paste0("criticite_aretes_", NOM_SCENARIO, ".csv")),
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
    round(mean(od_compare$surcout_absolu_usd, na.rm = TRUE), 2), "USD\n")
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
    fraction_perdue_prov      = if (exists("fraction_perdue_prov")) fraction_perdue_prov else NULL,
    surcout_pondere_arete     = surcout_pondere_arete,
    volume_detourne_arete     = volume_detourne_arete,
    NOM_SCENARIO              = NOM_SCENARIO,
    date_creation             = Sys.time()
  ),
  PERSIST_VULNERAB
)

cat("✓ persist_vulnerabilite.rds\n\n")
cat("Lancer 05_ario.R pour la suite.\n")