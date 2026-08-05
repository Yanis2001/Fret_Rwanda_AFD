################################################################################
# preparer_raster_inondation.R
#
# OBJECTIF : Construire le raster d'aléa inondation utilisé par le Mode C du
#            script 05_vulnerabilite.R, à partir des cartes d'aléa fluvial
#            mondiales du JRC / Copernicus EMS (GloFAS).
#
# ENTRÉE   : data/raw/glofas/ID<tuile>_<RP>_depth.tif
#            Deux tuiles de 10°×10° couvrent le Rwanda, qui est à cheval sur
#            les méridiens 20-30°E et 30-40°E :
#              - ID139_N0_E20 : 20-30°E, 0 à -10° de latitude
#              - ID151_N0_E30 : 30-40°E, 0 à -10° de latitude
#            Résolution 3 arc-sec (~90 m), valeurs = hauteur d'eau en MÈTRES,
#            CRS WGS84 (EPSG:4326). Licence CC-BY 4.0.
#
# SORTIE   : data/raw/zones_inondables_rwanda_glofas_rp<XXX>.tif
#            Un fichier par période de retour, mosaïqué et découpé sur
#            l'emprise du Rwanda. C'est ce fichier que pointe
#            CHEMIN_RASTER_RISQUE dans 00_parametres.R.
#
# ATTENTION — CHANGEMENT D'UNITÉ :
#   Le raster de test produit par creer_raster_test.R contenait des
#   PROBABILITÉS (0-1). Celui-ci contient des HAUTEURS D'EAU EN MÈTRES.
#   SEUIL_RISQUE_RASTER change donc de sens : 0.5 ne signifie plus
#   « 50 % de probabilité » mais « 50 cm d'eau sur la chaussée ».
#
# SOURCE ET CITATION :
#   Baugh, C., Colonese, J., D'Angelo, C., Dottori, F., Neal, J., Prudhomme, C.,
#   Salamon, P. (2024). Modelled flood inundation for different return period
#   scenarios at the global scale. European Commission, Joint Research Centre.
#   https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/CEMS-GLOFAS/flood_hazard/
#
# UTILISATION :
#   1. Exécuter ce script une fois (les tuiles doivent être présentes).
#   2. Dans 00_parametres.R, choisir GLOFAS_PERIODE_RETOUR (10, 100 ou 500).
#   3. Lancer 05_vulnerabilite.R normalement (UTILISER_MODE_RASTER <- TRUE).
################################################################################

library(terra)

cat("=== Préparation du raster d'aléa inondation (JRC / GloFAS) ===\n\n")

# ── Emprise de découpe ────────────────────────────────────────────────────────
# Bounding box du Rwanda élargie d'une marge : les postes frontières et les
# nœuds RoW se trouvent sur la frontière, et le réseau OSM extrait déborde
# légèrement du pays. Une marge évite de tronquer l'aléa sur ces zones.
DOSSIER_TUILES <- "data/raw/glofas"
TUILES         <- c("ID139_N0_E20", "ID151_N0_E30")
PERIODES       <- c(10, 100, 500)

emprise_rwanda <- ext(28.80, 30.95, -2.90, -1.00)   # xmin, xmax, ymin, ymax

cat("Emprise de découpe : lon", xmin(emprise_rwanda), "à", xmax(emprise_rwanda),
    "| lat", ymin(emprise_rwanda), "à", ymax(emprise_rwanda), "\n\n")

# ── Chargement d'un masque (eaux permanentes / profondeurs aberrantes) ────────
# Les masques sont fournis par le JRC sous le même découpage en tuiles que les
# cartes d'aléa. Même logique que pour l'aléa : découpe tuile par tuile, puis
# fusion. Renvoie NULL si les fichiers sont absents, pour que le script reste
# exécutable avec les seules tuiles de profondeur.
charger_masque <- function(suffixe) {
  morceaux <- list()
  for (tuile in TUILES) {
    chemin <- file.path(DOSSIER_TUILES, sprintf("%s_%s.tif", tuile, suffixe))
    if (!file.exists(chemin)) return(NULL)
    r <- rast(chemin)
    if (is.null(intersect(ext(r), emprise_rwanda))) next
    morceaux[[tuile]] <- crop(r, emprise_rwanda)
  }
  if (length(morceaux) == 0) return(NULL)
  if (length(morceaux) == 1) morceaux[[1]] else do.call(merge, unname(morceaux))
}

# ── Traitement d'une période de retour ────────────────────────────────────────
# Pour chaque période : on lit les deux tuiles, on les découpe AVANT de les
# fusionner (crop puis merge est bien plus économe en mémoire que l'inverse :
# on manipule ~2 500 × 2 300 pixels au lieu de deux grilles de 12 000²),
# puis on écrit le résultat.
preparer_periode <- function(rp) {

  cat("── Période de retour", rp, "ans ─────────────────────────────\n")

  # Lecture + découpe tuile par tuile. terra::crop() sur une tuile qui
  # n'intersecte pas l'emprise renverrait une erreur : on filtre donc en
  # amont sur l'intersection des emprises.
  morceaux <- list()
  for (tuile in TUILES) {
    chemin <- file.path(DOSSIER_TUILES, sprintf("%s_RP%d_depth.tif", tuile, rp))
    if (!file.exists(chemin)) {
      stop("Tuile manquante : ", chemin,
           "\n  Relancer le téléchargement depuis le JRC Data Store.")
    }
    r <- rast(chemin)
    if (is.null(intersect(ext(r), emprise_rwanda))) {
      cat("  – ", tuile, ": hors emprise, ignorée\n")
      next
    }
    morceaux[[tuile]] <- crop(r, emprise_rwanda)
    cat("  ✓ ", tuile, ": découpée (", nrow(morceaux[[tuile]]), "×",
        ncol(morceaux[[tuile]]), "pixels )\n")
  }

  # Fusion des morceaux. merge() prend la première valeur non-NA rencontrée ;
  # les tuiles étant jointives et non recouvrantes, l'ordre est sans effet.
  r_final <- if (length(morceaux) == 1) morceaux[[1]] else do.call(merge, unname(morceaux))

  # ── Retrait des eaux permanentes ────────────────────────────────────────────
  # Le JRC « patche » ses cartes d'aléa avec les plans d'eau permanents : le lac
  # Kivu, les lacs de l'Akagera et le lit mineur des rivières y apparaissent
  # comme inondés à TOUTES les périodes de retour. Sans retrait, l'étendue
  # inondée ne varie quasiment pas entre RP10 et RP500 (l'essentiel des pixels
  # « en eau » étant des lacs), ce qui viderait de son sens la gradation par
  # période de retour. On force donc ces pixels à 0 : seule l'eau EXCÉDENTAIRE,
  # celle qui déborde sur des terres normalement sèches, constitue l'aléa.
  masque_perm <- charger_masque("permanent_water")
  if (!is.null(masque_perm)) {
    n_avant <- sum(values(r_final) > 0, na.rm = TRUE)
    r_final <- mask(r_final, masque_perm, maskvalues = 1, updatevalue = 0)
    cat("  ✓  Eaux permanentes retirées :",
        format(n_avant - sum(values(r_final) > 0, na.rm = TRUE), big.mark = " "),
        "pixels\n")
  }

  # ── Retrait des profondeurs aberrantes ──────────────────────────────────────
  # Le JRC signale lui-même (README, note F02) les zones où le modèle prédit des
  # hauteurs > 10 m dans de petits chenaux (bassin < 3 000 km²), artefact de
  # résolution ou de puits artificiels dans le MNT. Sur un pays au relief aussi
  # marqué que le Rwanda ces artefacts sont fréquents, et ils couperaient des
  # routes sans raison physique.
  masque_spur <- charger_masque("spurious_depth_areas")
  if (!is.null(masque_spur)) {
    n_avant <- sum(values(r_final) > 0, na.rm = TRUE)
    r_final <- mask(r_final, masque_spur, maskvalues = 1, updatevalue = 0)
    cat("  ✓  Zones de profondeur aberrante retirées :",
        format(n_avant - sum(values(r_final) > 0, na.rm = TRUE), big.mark = " "),
        "pixels\n")
  }

  # Les pixels hors zone inondable sont à NA dans le produit JRC. Le 05 calcule
  # une proportion de points au-dessus du seuil : un NA y est traité comme
  # « non exposé » (na.rm = TRUE), donc on peut laisser les NA en l'état.
  # On les remplace tout de même par 0 pour que les statistiques ci-dessous et
  # toute visualisation du raster soient lisibles.
  r_final <- subst(r_final, NA, 0)

  # ── Statistiques de contrôle ────────────────────────────────────────────────
  v <- values(r_final)
  v <- v[!is.na(v)]
  n_tot <- length(v)
  cat("  Dimensions        :", nrow(r_final), "×", ncol(r_final), "pixels\n")
  cat("  Hauteur d'eau max :", round(max(v), 2), "m\n")
  for (s in c(0.3, 0.5, 1.0)) {
    cat(sprintf("  Pixels > %.1f m    : %s (%.2f %% de l'emprise)\n",
                s, format(sum(v > s), big.mark = " "), 100 * mean(v > s)))
  }

  # ── Écriture ────────────────────────────────────────────────────────────────
  sortie <- sprintf("data/raw/zones_inondables_rwanda_glofas_rp%03d.tif", rp)
  writeRaster(r_final, sortie, overwrite = TRUE)
  cat("  ✓ Écrit :", sortie, "(", round(file.size(sortie) / 1e6, 1), "Mo )\n\n")

  invisible(sortie)
}

for (rp in PERIODES) preparer_periode(rp)

cat("=== Terminé ===\n")
cat("  Dans 00_parametres.R : GLOFAS_PERIODE_RETOUR <- 100 (ou 10 / 500)\n")
cat("  Rappel : SEUIL_RISQUE_RASTER s'exprime désormais en MÈTRES d'eau.\n\n")
