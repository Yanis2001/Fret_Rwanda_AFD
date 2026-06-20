################################################################################
# creer_raster_test.R
#
# OBJECTIF : Créer un raster synthétique de risque d'inondation pour tester
#            le Mode C (raster) du script 05_vulnerabilite.R.
#
# SORTIE   : data/raw/zones_inondables_rwanda.tif
#            Valeurs entre 0 et 1 (probabilité de risque d'inondation).
#            Zones à risque élevé (> 0.5) positionnées le long des principaux
#            cours d'eau et plaines inondables du Rwanda.
#
# UTILISATION :
#   1. Exécuter ce script une fois pour générer le raster.
#   2. Dans 00_parametres.R, mettre UTILISER_MODE_RASTER <- TRUE.
#   3. Lancer 05_vulnerabilite.R normalement.
################################################################################

library(terra)

cat("=== Création du raster de test zones inondables Rwanda ===\n\n")

# ── Emprise géographique du Rwanda (WGS84) ─────────────────────────────────────
# Bounding box approximatif du Rwanda + légère marge
xmin_rw <- 28.85
xmax_rw <- 30.92
ymin_rw <- -2.85
ymax_rw <- -1.04

# Résolution : 0.005° ≈ 500 m — suffisamment fin pour intersecter les routes
# sans être trop lourd pour un raster de test.
resolution <- 0.005

# Création du raster de base (toutes valeurs à 0 = aucun risque)
r_base <- rast(
  xmin = xmin_rw, xmax = xmax_rw,
  ymin = ymin_rw, ymax = ymax_rw,
  resolution = resolution,
  crs = "EPSG:4326"   # WGS84 — le script 04 reprojetera en UTM 35S
)
values(r_base) <- 0

cat("Dimensions du raster :", nrow(r_base), "lignes ×", ncol(r_base), "colonnes\n")
cat("Résolution           :", resolution, "° ≈ 500 m\n\n")

# ── Fonction utilitaire : ajouter une "tache" de risque élevé ─────────────────
# Crée un gradient gaussien de risque centré en (lon, lat) avec un rayon donné.
# Le risque décroît exponentiellement avec la distance au centre.
ajouter_zone_risque <- function(raster, lon, lat, rayon_deg, intensite = 0.9) {

  # Grille de coordonnées lon/lat pour chaque cellule du raster
  coords <- crds(raster)

  # Distance euclidienne en degrés depuis le centre (approximation planaire valide
  # sur la petite emprise du Rwanda)
  dist <- sqrt((coords[, 1] - lon)^2 + (coords[, 2] - lat)^2)

  # Profil gaussien : risque = intensite × exp(-0.5 × (dist/sigma)²)
  # sigma = rayon/2 pour que la valeur à la périphérie soit ~30% de l'intensité
  sigma  <- rayon_deg / 2
  risque <- intensite * exp(-0.5 * (dist / sigma)^2)

  # On prend le maximum entre la valeur existante et la nouvelle zone
  # (les zones se chevauchant gardent le risque le plus élevé)
  values(raster) <- pmax(values(raster), risque)

  raster
}

# ── Zones à risque élevé : principaux cours d'eau et plaines inondables ────────
#
# 1. Rivière Nyabarongo (centre Rwanda, axe N-S)
#    La Nyabarongo est le principal cours d'eau interne ; ses plaines
#    d'inondation sont les plus étendues du pays.
cat("Ajout zone 1 : plaine Nyabarongo (centre)\n")
r_base <- ajouter_zone_risque(r_base, lon = 29.78, lat = -1.95, rayon_deg = 0.15, intensite = 0.88)
r_base <- ajouter_zone_risque(r_base, lon = 29.65, lat = -2.20, rayon_deg = 0.12, intensite = 0.82)
r_base <- ajouter_zone_risque(r_base, lon = 29.55, lat = -2.45, rayon_deg = 0.10, intensite = 0.78)

# 2. Plaine de l'Akagera (est Rwanda — parc de l'Akagera, marais)
cat("Ajout zone 2 : plaines de l'Akagera (est)\n")
r_base <- ajouter_zone_risque(r_base, lon = 30.65, lat = -1.55, rayon_deg = 0.18, intensite = 0.92)
r_base <- ajouter_zone_risque(r_base, lon = 30.72, lat = -1.90, rayon_deg = 0.16, intensite = 0.85)
r_base <- ajouter_zone_risque(r_base, lon = 30.55, lat = -2.30, rayon_deg = 0.14, intensite = 0.80)

# 3. Rivière Kagera (nord-est Rwanda)
cat("Ajout zone 3 : vallée Kagera (nord-est)\n")
r_base <- ajouter_zone_risque(r_base, lon = 30.45, lat = -1.25, rayon_deg = 0.10, intensite = 0.75)
r_base <- ajouter_zone_risque(r_base, lon = 30.20, lat = -1.18, rayon_deg = 0.08, intensite = 0.70)

# 4. Rivière Ruzizi / lac Kivu (ouest Rwanda)
cat("Ajout zone 4 : rives lac Kivu / Ruzizi (ouest)\n")
r_base <- ajouter_zone_risque(r_base, lon = 29.00, lat = -2.50, rayon_deg = 0.09, intensite = 0.72)
r_base <- ajouter_zone_risque(r_base, lon = 29.05, lat = -2.20, rayon_deg = 0.08, intensite = 0.68)

# 5. Rivière Mwogo / Mukungwa (nord-ouest, proche Musanze)
cat("Ajout zone 5 : plaine Mukungwa (nord-ouest)\n")
r_base <- ajouter_zone_risque(r_base, lon = 29.58, lat = -1.50, rayon_deg = 0.08, intensite = 0.65)
r_base <- ajouter_zone_risque(r_base, lon = 29.40, lat = -1.60, rayon_deg = 0.07, intensite = 0.62)

# 6. Bas-fonds autour de Kigali (vallées urbaines)
cat("Ajout zone 6 : bas-fonds Kigali\n")
r_base <- ajouter_zone_risque(r_base, lon = 30.06, lat = -1.95, rayon_deg = 0.06, intensite = 0.60)

# ── Ajout d'un bruit de fond (texture réaliste) ────────────────────────────────
# Un faible niveau de risque diffus (~0.05–0.15) évite que les routes en dehors
# des zones principales aient toutes une valeur exactement nulle.
set.seed(2024)
n_cells <- ncell(r_base)
bruit   <- rast(r_base)
values(bruit) <- runif(n_cells, min = 0.02, max = 0.12)

r_final <- r_base + bruit
values(r_final) <- pmin(values(r_final), 1.0)   # Valeur max = 1
values(r_final) <- pmax(values(r_final), 0.0)   # Valeur min = 0

# ── Statistiques sommaires ─────────────────────────────────────────────────────
v      <- values(r_final)[!is.na(values(r_final))]
n_haut <- sum(v >= 0.5)
cat("\nStatistiques du raster final :\n")
cat("  Min               :", round(min(v),  3), "\n")
cat("  Max               :", round(max(v),  3), "\n")
cat("  Moyenne           :", round(mean(v), 3), "\n")
cat("  Médiane           :", round(median(v), 3), "\n")
cat("  Cellules ≥ 0.5    :", n_haut, "(", round(n_haut / length(v) * 100, 1), "% de l'emprise)\n\n")

# ── Sauvegarde ─────────────────────────────────────────────────────────────────
chemin_sortie <- "data/raw/zones_inondables_rwanda.tif"
writeRaster(r_final, chemin_sortie, overwrite = TRUE)

cat("✓ Raster sauvegardé :", chemin_sortie, "\n")
cat("  Dimensions :", nrow(r_final), "×", ncol(r_final), "cellules\n")
cat("  CRS        : EPSG:4326 (WGS84) — sera reprojeté en UTM 35S par 05_vulnerabilite.R\n\n")

cat("=== Pour utiliser ce raster dans 05_vulnerabilite.R ===\n")
cat("  Dans 00_parametres.R, mettre :\n")
cat("    UTILISER_MODE_RASTER <- TRUE\n")
cat("  Les autres paramètres par défaut sont déjà calibrés pour ce raster :\n")
cat("    SEUIL_RISQUE_RASTER    <- 0.5  (valeurs > 0.5 = zone à risque)\n")
cat("    PROPORTION_MIN_EXPOSEE <- 0.3  (30% de la route doit être à risque)\n")
cat("    PROP_ROUTES_INONDEES_RASTER <- 0.7 (70% des routes exposées coupées)\n\n")
