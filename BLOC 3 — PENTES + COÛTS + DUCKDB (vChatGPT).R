################################################################################
# BLOC 3 — PENTES + COÛTS + DUCKDB (VERSION QUASI INDUSTRIELLE)
################################################################################

library(terra)
library(dplyr)
library(DBI)

################################################################################
# 1. CALCUL DES PENTES À PARTIR DU DEM
################################################################################

compute_slopes <- function(reseau, dem_raster) {
  
  cat("=== Calcul des pentes ===\n")
  
  # ── Vérification des entrées ───────────────────────────────────────────────
  if (!inherits(dem_raster, "SpatRaster")) {
    stop("Erreur : dem_raster doit être un SpatRaster (terra)")
  }
  
  # ── Extraction des arêtes en format sf ─────────────────────────────────────
  aretes <- reseau %>%
    activate("edges") %>%
    st_as_sf()
  
  # ── Extraction altitude début et fin de chaque arête ───────────────────────
  # On récupère les coordonnées des extrémités de chaque segment
  coords_start <- st_coordinates(st_startpoint(aretes))
  coords_end   <- st_coordinates(st_endpoint(aretes))
  
  # Conversion en format terra pour extraction raster
  pts_start <- vect(coords_start, geom = c("X", "Y"), crs = crs(dem_raster))
  pts_end   <- vect(coords_end,   geom = c("X", "Y"), crs = crs(dem_raster))
  
  # ── Extraction des altitudes depuis le DEM ─────────────────────────────────
  z_start <- terra::extract(dem_raster, pts_start)[,2]
  z_end   <- terra::extract(dem_raster, pts_end)[,2]
  
  # ── Calcul de la pente ─────────────────────────────────────────────────────
  # pente = (delta altitude / longueur horizontale)
  aretes <- aretes %>%
    mutate(
      z_start = z_start,
      z_end   = z_end,
      delta_z = z_end - z_start,
      slope   = delta_z / longueur_m   # pente en ratio
    )
  
  # ── Vérification ───────────────────────────────────────────────────────────
  if (any(is.na(aretes$slope))) {
    warning("Certaines pentes sont NA (zones sans données DEM)")
  }
  
  cat("✓ Pentes calculées\n\n")
  
  return(aretes)
}

################################################################################
# 2. EXPORT PROPRE VERS DUCKDB
################################################################################

export_edges_duckdb <- function(aretes_df, con) {
  
  cat("=== Export vers DuckDB ===\n")
  
  # ── Vérification critique ──────────────────────────────────────────────────
  n_invalides <- sum(is.na(aretes_df$longueur_m) | aretes_df$longueur_m == 0)
  
  if (n_invalides > 0) {
    stop("Erreur : arêtes invalides détectées avant export")
  }
  
  # ── Ajout identifiant unique ───────────────────────────────────────────────
  aretes_df <- aretes_df %>%
    mutate(arete_id = row_number())
  
  # ── Écriture dans DuckDB ───────────────────────────────────────────────────
  dbWriteTable(con, "aretes_base", aretes_df, overwrite = TRUE)
  
  cat("✓ Export terminé\n\n")
  
  return(aretes_df)
}

################################################################################
# 3. CRÉATION TABLE COÛTS GÉNÉRALISÉS
################################################################################

compute_costs_duckdb <- function(con) {
  
  cat("=== Calcul des coûts généralisés ===\n")
  
  # ── Requête SQL structurée ─────────────────────────────────────────────────
  dbExecute(con, "
  CREATE OR REPLACE TABLE aretes_couts AS
  WITH aretes_propres AS (
    SELECT *
    FROM aretes_base
    WHERE longueur_m IS NOT NULL AND longueur_m > 0
  )
  SELECT
    *,
    longueur_m / 1000.0 AS length_km
  FROM aretes_propres
  ")
  
  cat("✓ Table aretes_couts créée\n\n")
}

################################################################################
# 4. VALIDATION DES COÛTS
################################################################################

validate_costs <- function(con) {
  
  cat("=== Validation des coûts ===\n")
  
  res <- dbGetQuery(con, "
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN length_km IS NULL THEN 1 ELSE 0 END) AS n_null
    FROM aretes_couts
  ")
  
  cat("Total arêtes :", res$total, "\n")
  cat("length_km NULL :", res$n_null, "\n")
  
  if (res$n_null > 0) {
    stop("Erreur : des coûts sont NULL")
  }
  
  cat("✓ Coûts valides\n\n")
}

################################################################################
# 5. PIPELINE GLOBAL BLOC 3
################################################################################

run_block3 <- function(reseau, dem_raster, con) {
  
  cat("=== Lancement bloc 3 ===\n\n")
  
  # 1. Calcul des pentes
  aretes <- compute_slopes(reseau, dem_raster)
  
  # 2. Suppression géométrie pour SQL
  aretes_df <- aretes %>%
    st_drop_geometry()
  
  # 3. Export vers DuckDB
  aretes_df <- export_edges_duckdb(aretes_df, con)
  
  # 4. Calcul des coûts
  compute_costs_duckdb(con)
  
  # 5. Validation
  validate_costs(con)
  
  cat("=== Bloc 3 terminé ===\n\n")
  
  return(aretes_df)
}

################################################################################
# UTILISATION
################################################################################

# aretes_df <- run_block3(reseau_rwanda, dem_rwanda, con)