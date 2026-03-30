################################################################################
# BLOC 1 — CHARGEMENT & NETTOYAGE DES ROUTES (VERSION QUASI INDUSTRIELLE)
################################################################################

library(sf)
library(dplyr)

################################################################################
# 1. CHARGEMENT DES DONNÉES
################################################################################

load_routes <- function(filepath) {
  
  cat("=== Chargement des données routes ===\n")
  
  # ── Lecture du fichier spatial (GeoPackage, Shapefile, etc.) ────────────────
  routes <- st_read(filepath, quiet = TRUE)
  
  # ── Vérification : objet sf ────────────────────────────────────────────────
  if (!inherits(routes, "sf")) {
    stop("Erreur : le fichier chargé n'est pas un objet sf")
  }
  
  cat("✓ Données chargées :", nrow(routes), "objets\n\n")
  
  return(routes)
}

################################################################################
# 2. NETTOYAGE DES GÉOMÉTRIES
################################################################################

clean_geometries <- function(routes) {
  
  cat("=== Nettoyage des géométries ===\n")
  
  # ── Suppression géométries NULL ────────────────────────────────────────────
  routes <- routes %>%
    filter(!st_is_empty(geometry))
  
  # ── Correction géométries invalides ────────────────────────────────────────
  routes <- st_make_valid(routes)
  
  # ── Suppression des géométries toujours invalides ──────────────────────────
  routes <- routes %>%
    filter(st_is_valid(.))
  
  cat("✓ Géométries nettoyées\n\n")
  
  return(routes)
}

################################################################################
# 3. NORMALISATION DES TYPES DE GÉOMÉTRIE
################################################################################

normalize_geometries <- function(routes) {
  
  cat("=== Normalisation des géométries ===\n")
  
  # ── Conversion en LINESTRING ───────────────────────────────────────────────
  routes <- routes %>%
    st_cast("LINESTRING", warn = FALSE)
  
  # ── Filtrer uniquement les lignes ──────────────────────────────────────────
  routes <- routes %>%
    filter(st_geometry_type(.) == "LINESTRING")
  
  cat("✓ Géométries converties en LINESTRING\n\n")
  
  return(routes)
}

################################################################################
# 4. GESTION DU CRS (SYSTÈME DE COORDONNÉES)
################################################################################

set_crs_projected <- function(routes, target_crs = 32735) {
  
  cat("=== Projection du système de coordonnées ===\n")
  
  # ── Vérification CRS existant ──────────────────────────────────────────────
  if (is.na(st_crs(routes))) {
    stop("Erreur : CRS manquant dans les données")
  }
  
  # ── Transformation en CRS projeté (mètres) ─────────────────────────────────
  routes <- st_transform(routes, crs = target_crs)
  
  cat("✓ CRS projeté appliqué (mètres)\n\n")
  
  return(routes)
}

################################################################################
# 5. NETTOYAGE DES ATTRIBUTS OSM
################################################################################

clean_attributes <- function(routes) {
  
  cat("=== Nettoyage des attributs ===\n")
  
  # ── Vérification présence champ highway ─────────────────────────────────────
  if (!"highway" %in% names(routes)) {
    stop("Erreur : colonne 'highway' absente")
  }
  
  # ── Suppression routes non pertinentes ─────────────────────────────────────
  routes <- routes %>%
    filter(!is.na(highway))
  
  # ── Option : filtrage par type de route ─────────────────────────────────────
  routes <- routes %>%
    filter(highway %in% c(
      "motorway", "trunk", "primary", "secondary",
      "tertiary", "unclassified", "residential"
    ))
  
  cat("✓ Attributs nettoyés\n\n")
  
  return(routes)
}

################################################################################
# 6. VALIDATION FINALE DES ROUTES
################################################################################

validate_routes <- function(routes) {
  
  cat("=== Validation des routes ===\n")
  
  # ── Vérification nombre d'objets ───────────────────────────────────────────
  if (nrow(routes) == 0) {
    stop("Erreur : aucune route après nettoyage")
  }
  
  # ── Vérification géométrie ─────────────────────────────────────────────────
  if (any(st_is_empty(routes))) {
    stop("Erreur : géométries vides détectées")
  }
  
  # ── Vérification CRS projeté ───────────────────────────────────────────────
  if (st_is_longlat(routes)) {
    warning("Attention : données en coordonnées géographiques (degrés)")
  }
  
  cat("✓ Routes valides\n")
  cat(" - Nombre :", nrow(routes), "\n\n")
}

################################################################################
# 7. PIPELINE GLOBAL BLOC 1
################################################################################

prepare_routes <- function(filepath) {
  
  cat("=== PRÉPARATION DES ROUTES ===\n\n")
  
  # 1. Chargement
  routes <- load_routes(filepath)
  
  # 2. Nettoyage géométrique
  routes <- clean_geometries(routes)
  
  # 3. Normalisation
  routes <- normalize_geometries(routes)
  
  # 4. Projection
  routes <- set_crs_projected(routes)
  
  # 5. Nettoyage attributaire
  routes <- clean_attributes(routes)
  
  # 6. Validation finale
  validate_routes(routes)
  
  cat("=== Routes prêtes pour le réseau ===\n\n")
  
  return(routes)
}

################################################################################
# UTILISATION
################################################################################

# routes_rwanda <- prepare_routes("data/routes_rwanda.gpkg")