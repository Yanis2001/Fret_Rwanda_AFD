################################################################################
# BLOC 2 — DEM + RÉSEAU + TOPOLOGIE (VERSION QUASI INDUSTRIELLE)
# Objectif :
#   - Construire un réseau routier propre et exploitable
#   - Corriger les erreurs topologiques
#   - Garantir un graphe cohérent (connecté, sans arêtes invalides)
################################################################################

library(sf)
library(dplyr)
library(sfnetworks)
library(igraph)

################################################################################
# 1. CONSTRUCTION DU RÉSEAU À PARTIR DES ROUTES
################################################################################

build_network <- function(routes_sf) {
  
  # ── Vérification de l'entrée ────────────────────────────────────────────────
  if (!inherits(routes_sf, "sf")) {
    stop("Erreur : l'objet routes_sf doit être un objet sf")
  }
  
  # ── Conversion en LINESTRING simples ───────────────────────────────────────
  # Certaines routes OSM peuvent être des MULTILINESTRING → on les éclate
  routes_clean <- routes_sf %>%
    st_cast("LINESTRING", warn = FALSE) %>%
    filter(st_geometry_type(.) == "LINESTRING")
  
  # ── Création du réseau spatial ─────────────────────────────────────────────
  # Chaque ligne devient une arête du graphe
  reseau <- as_sfnetwork(routes_clean, directed = FALSE)
  
  return(reseau)
}

################################################################################
# 2. CORRECTION TOPOLOGIQUE DU RÉSEAU
################################################################################

clean_topology <- function(reseau) {
  
  # ── Étape 1 : subdivision ──────────────────────────────────────────────────
  # Ajoute des nœuds aux intersections manquantes
  # (ex : deux routes qui se croisent sans être connectées)
  reseau <- reseau %>%
    convert(to_spatial_subdivision)
  
  # ── Étape 2 : lissage ──────────────────────────────────────────────────────
  # Supprime les pseudo-nœuds inutiles (segments artificiels)
  reseau <- reseau %>%
    convert(to_spatial_smooth)
  
  return(reseau)
}

################################################################################
# 3. EXTRACTION DE LA COMPOSANTE GÉANTE
################################################################################

extract_giant_component <- function(reseau) {
  
  # ── Conversion en graphe igraph ────────────────────────────────────────────
  graph <- reseau %>% as_tbl_graph()
  
  # ── Calcul des composantes connexes ────────────────────────────────────────
  comp <- igraph::components(graph)
  
  # ── Identification de la plus grande composante ────────────────────────────
  id_geante <- which.max(comp$csize)
  
  # ── Sélection des nœuds appartenant à cette composante ─────────────────────
  noeuds_a_garder <- which(comp$membership == id_geante)
  
  # ── Filtrage du réseau ─────────────────────────────────────────────────────
  reseau_filtre <- reseau %>%
    activate("nodes") %>%
    filter(row_number() %in% noeuds_a_garder)
  
  return(reseau_filtre)
}

################################################################################
# 4. NETTOYAGE DES ARÊTES (POINT CRITIQUE)
################################################################################

clean_edges <- function(reseau) {
  
  # ── Calcul des longueurs réelles ───────────────────────────────────────────
  reseau <- reseau %>%
    activate("edges") %>%
    mutate(longueur_m = as.numeric(st_length(geometry)))
  
  # ── Comptage avant nettoyage ───────────────────────────────────────────────
  n_avant <- igraph::ecount(reseau)
  
  # ── Suppression des arêtes invalides ───────────────────────────────────────
  # On enlève :
  #   - longueurs NA (erreurs géométriques)
  #   - longueurs = 0 (segments dégénérés)
  #   - longueurs < 0.5m (bruit topologique)
  reseau <- reseau %>%
    filter(!is.na(longueur_m) & longueur_m > 0.5)
  
  # ── Comptage après nettoyage ───────────────────────────────────────────────
  n_apres <- igraph::ecount(reseau)
  
  cat("Nettoyage des arêtes :\n")
  cat(" - Avant :", n_avant, "\n")
  cat(" - Après :", n_apres, "\n")
  cat(" - Supprimées :", n_avant - n_apres, "\n\n")
  
  return(reseau)
}

################################################################################
# 5. VALIDATION DU RÉSEAU (ANTI-BUG)
################################################################################

validate_network <- function(reseau) {
  
  # ── Extraction des arêtes en sf ────────────────────────────────────────────
  aretes <- reseau %>%
    activate("edges") %>%
    st_as_sf()
  
  # ── Vérification critique : longueurs ──────────────────────────────────────
  n_invalides <- sum(is.na(aretes$longueur_m) | aretes$longueur_m == 0)
  
  if (n_invalides > 0) {
    stop(paste("Erreur critique :", n_invalides, "arêtes invalides détectées"))
  }
  
  # ── Vérification de la connectivité ────────────────────────────────────────
  comp <- igraph::components(reseau %>% as_tbl_graph())
  
  if (length(comp$csize) > 1) {
    warning("Attention : le réseau n'est pas entièrement connecté")
  }
  
  # ── Affichage des stats ────────────────────────────────────────────────────
  cat("Validation réseau OK :\n")
  cat(" - Nœuds :", igraph::vcount(reseau), "\n")
  cat(" - Arêtes :", igraph::ecount(reseau), "\n")
  cat(" - Composantes :", length(comp$csize), "\n\n")
}

################################################################################
# 6. PIPELINE GLOBAL DU BLOC 2
################################################################################

build_clean_network <- function(routes_sf) {
  
  cat("=== Construction du réseau ===\n\n")
  
  # 1. Construction brute du réseau
  reseau <- build_network(routes_sf)
  
  # 2. Correction topologique
  reseau <- clean_topology(reseau)
  
  # 3. Extraction composante géante
  reseau <- extract_giant_component(reseau)
  
  # 4. Nettoyage des arêtes (CRUCIAL)
  reseau <- clean_edges(reseau)
  
  # 5. Validation finale
  validate_network(reseau)
  
  cat("=== Réseau prêt pour analyse ===\n\n")
  
  return(reseau)
}

################################################################################
# UTILISATION
################################################################################

# reseau_rwanda <- build_clean_network(routes_rwanda)