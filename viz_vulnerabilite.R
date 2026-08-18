################################################################################
# viz_vulnerabilite.R
# RÔLE : Cartes de vulnérabilité (réseau dégradé, criticité, détours,
#        report modal) et graphiques de distribution des surcoûts.
#
# PEUT TOURNER SANS RELANCER LES SCRIPTS PRÉCÉDENTS si les .rds sont à jour.
#
# RELANCER 05_vulnerabilite.R avant ce script si :
#   → le scénario a changé (NOM_SCENARIO, OSM_IDS_PERTURBES_MANUEL,
#     CENTRE_PERTURBATION_*, RAYON_PERTURBATION_M, SEUIL_RISQUE_RASTER)
#   → DUREE_JOURS ou TYPE_EVENEMENT ont changé
#   → N_TOP_ARETES_CRITIQUES ou SEUIL_PAIRES_CRITICITE ont changé
#   → les flux de fret (persist_flux_fret.rds) ont changé
#     → dans ce cas relancer aussi 03_transport.R puis 04_affectation.R
#       avant 05_vulnerabilite.R
#
# RELANCER 02_couts.R + 03_transport.R + 04_affectation.R + 05_vulnerabilite.R si :
#   → le réseau routier lui-même a changé (nouveau PBF, nouvelles corrections)
#
# FICHIERS LUS : persist_geodata.rds, persist_entreposages.rds,
#                persist_reseau_fret.rds, persist_flux_fret.rds,
#                persist_vulnerabilite.rds
################################################################################

source("00_parametres.R")
fond_carte <- readRDS(file.path(DIR_CARTES, "persist_fond_carte.rds"))

.ent  <- readRDS(PERSIST_ENTREPOSAGES)
list2env(.ent, envir = .GlobalEnv)

.fret <- readRDS(PERSIST_RESEAU_FRET)
reseau    <- .fret$reseau   # version avec volumes fret
volumes_par_zone <- .fret$volumes_par_zone
rm(.fret)

# ==============================================================================
# Correction du device PNG pour tmap sur macOS sans XQuartz
# tmap v4 force type="cairo-png" en dur dans sa fonction interne plot_device.
# Sur macOS sans XQuartz installé, cairo n'est pas disponible : tmap_save()
# échoue silencieusement (avertissement "failed to load cairo DLL", aucun fichier
# créé, pas d'erreur levée). Ce bloc détecte l'absence de cairo et remplace
# automatiquement le device par type="quartz" (rendu natif macOS).
# Sur les systèmes où cairo fonctionne (Linux, macOS + XQuartz), le patch est
# silencieusement ignoré.
# ==============================================================================
local({
  # Test réel : tente d'ouvrir un device cairo-png et guette le warning d'échec.
  # withCallingHandlers intercepte l'avertissement sans interrompre l'exécution,
  # contrairement à tryCatch qui stopperait après le premier warning.
  .f      <- tempfile(fileext = ".png")
  .echec  <- FALSE
  withCallingHandlers(
    grDevices::png(.f, type = "cairo-png", width = 10, height = 10,
                   res = 72, units = "px"),
    warning = function(w) {
      .echec <<- TRUE
      invokeRestart("muffleWarning")   # Supprime l'affichage du warning
    }
  )
  try(dev.off(), silent = TRUE)
  unlink(.f, force = TRUE)
  
  if (.echec) {
    # cairo-png indisponible : patch de plot_device dans le namespace de tmap.
    # On capture la fonction originale dans .orig pour ne patcher que le cas PNG
    # et déléguer tous les autres formats (pdf, svg, tiff…) à l'implémentation
    # officielle, garantissant la compatibilité lors des mises à jour de tmap.
    .orig <- tmap:::plot_device
    .patched <- function(device, ext, filename, dpi, units_target) {
      if (is.null(device) && identical(ext, "png")) {
        force(dpi)
        force(units_target)
        return(function(..., width, height) {
          grDevices::png(..., type = "quartz", width = width, height = height,
                         res = dpi, units = units_target)
        })
      }
      .orig(device, ext, filename, dpi, units_target)
    }
    assignInNamespace("plot_device", .patched, ns = "tmap")
    cat("✓ tmap : device PNG patché (quartz au lieu de cairo-png, XQuartz absent)\n\n")
  }
})

# ── Reconstruction d'aretes_reseau_sf ─────────────────────────────────────────
# Couche sf de toutes les arêtes du réseau, avec un index entier arete_idx.
# Nécessaire pour : filtrer les arêtes perturbées/critiques, calculer les volumes
# par type de route, et construire les couches de détour.
# On repart de reseau (déjà chargé) plutôt que de le sauvegarder dans
# PERSIST_VULNERAB (objet géométrique lourd, ~50 Mo).
aretes_reseau_sf <- reseau %>%
  activate("edges") %>%
  st_as_sf() %>%
  mutate(arete_idx = row_number())

# ── Reconstruction de coords_zones_sf ─────────────────────────────────────────
# Points sf des zones d'entrepôt, utilisés sur la Carte D (itinéraires de détour).
# Version simplifiée sans taille_point — la Carte D n'en a pas besoin.
# type : type simplifié de la zone ("Frontière" vs "Ville"), pour la légende
# "Type" des cartes (00_parametres.R).
coords_zones_sf <- reseau %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  st_as_sf() %>%
  mutate(type = type_simplifie(warehouse_type))

.vuln <- readRDS(PERSIST_VULNERAB)
list2env(.vuln, envir = .GlobalEnv)
rm(.vuln)

# ── Reconstruction d'od_ref_map ───────────────────────────────────────────────
# Table de lookup : clé "i_j" → coût de référence (avant perturbation).
# Reconstruit depuis od_compare (disponible via list2env) pour éviter de
# recharger od_cache.rds 
od_ref_map <- setNames(
  od_compare$cout_rwf,
  paste0(od_compare$id_origine, "_", od_compare$id_destination)
)

surcout_moyen_detour <- surcout_pondere_arete / pmax(volume_detourne_arete, 1)

################################################################################
# PARTIE IX.5 — CARTES ET EXPORTS
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
impact_par_zone_sf <- reseau %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  st_as_sf() %>%
  left_join(
    surcouts_par_zone %>%
      select(Zone, pct_surcout_moyen, n_deconnexions, surcout_total_rwf),
    by = c("warehouse_name" = "Zone")
  ) %>%
  mutate(
    pct_surcout_moyen = replace_na(pct_surcout_moyen, 0),
    surcout_total_rwf = replace_na(surcout_total_rwf, 0),
    n_deconnexions    = replace_na(n_deconnexions, 0L),

    # type simplifié de la zone ("Frontière" vs "Ville"), pour la légende
    # "Type" utilisée quand aucun surcoût n'est disponible (00_parametres.R).
    type              = type_simplifie(warehouse_type),

    # ── Classe d'impact de la zone ────────────────────────────────────────────
    # Une zone largement déconnectée doit apparaître comme telle et NON dans la
    # classe « 0 à 5 % ». Son surcoût moyen n'est en effet calculé que sur les
    # rares liaisons qui lui restent — typiquement ses voisines immédiates, donc
    # peu ou pas affectées — et ressort proche de zéro. Colorer sur ce seul
    # indicateur peindrait en gris rassurant les zones les plus durement
    # touchées : celles qui ont perdu l'accès au reste du réseau.
    # Le seuil de 50 % des destinations sépare la perte de quelques liaisons
    # d'un véritable enclavement.
    part_deconnectee  = n_deconnexions / pmax(n_warehouses - 1, 1),
    classe_impact = case_when(
      part_deconnectee >= 0.5   ~ "Déconnectée",
      pct_surcout_moyen >= 100  ~ "Surcoût ≥ 100 %",
      pct_surcout_moyen >= 50   ~ "Surcoût 50–100 %",
      pct_surcout_moyen >= 20   ~ "Surcoût 20–50 %",
      pct_surcout_moyen >= 5    ~ "Surcoût 5–20 %",
      TRUE                      ~ "Surcoût < 5 %"
    ),
    classe_impact = factor(
      classe_impact,
      levels = c("Surcoût < 5 %", "Surcoût 5–20 %", "Surcoût 20–50 %",
                 "Surcoût 50–100 %", "Surcoût ≥ 100 %", "Déconnectée")
    )
  )

# Palette de la classe d'impact des zones : gradient jaune → rouge pour les
# surcoûts, et un noir distinct pour l'enclavement, qui n'est pas le haut d'une
# échelle de surcoût mais une rupture de nature différente.
PALETTE_CLASSE_ZONE <- c(
  "Surcoût < 5 %"    = "#CCCCCC",
  "Surcoût 5–20 %"   = "#FFFFB2",
  "Surcoût 20–50 %"  = "#FD8D3C",
  "Surcoût 50–100 %" = "#E31A1C",
  "Surcoût ≥ 100 %"  = "#800026",
  "Déconnectée"      = "#000000"
)

n_zones_deconnectees <- sum(impact_par_zone_sf$classe_impact == "Déconnectée")
if (n_zones_deconnectees > 0) {
  cat("  ⚠", n_zones_deconnectees,
      "zone(s) enclavée(s) (plus de 50 % de leurs destinations coupées)\n")
}

# ── CARTE A : Réseau dégradé et zones d'impact ────────────────────────────────
cat("  Génération Carte A — réseau dégradé...\n")

# Zone tampon visible autour des arêtes perturbées (pour la localiser sur la carte)
# st_buffer() + st_union() : crée une zone en surbrillance autour des routes coupées
zone_impact_visible <- aretes_perturbees_sf %>%
  st_buffer(dist = 2000) %>%   # 2km de buffer pour être visible sur la carte
  st_union()

# ── Poids de la perturbation ──────────────────────────────────────────────────
# Deux indicateurs pour situer l'ampleur du scénario, affichés dans la note de
# lecture de la Carte A :
#  1. part du linéaire total du réseau (tous tronçons, utilisés ou non) que
#     représentent les arêtes coupées ;
#  2. part du linéaire des « axes effectivement utilisés » — les tronçons qui
#     portaient déjà du fret (volume_tonnes > 0) avant la perturbation — qui se
#     retrouve coupée. Ce second indicateur est le plus parlant économiquement :
#     un scénario peut couper un faible pourcentage du réseau brut tout en
#     touchant une part importante des axes qui comptent réellement pour le fret.
km_total_reseau   <- sum(aretes_reseau_sf$length_km, na.rm = TRUE)
km_perturbe       <- sum(aretes_perturbees_sf$length_km, na.rm = TRUE)
pct_reseau_touche <- 100 * km_perturbe / km_total_reseau

aretes_utilisees_sf <- aretes_reseau_sf %>%
  filter(volume_tonnes > 0)
km_utilise           <- sum(aretes_utilisees_sf$length_km, na.rm = TRUE)
km_utilise_perturbe  <- sum(
  aretes_perturbees_sf$length_km[aretes_perturbees_sf$arete_idx %in% aretes_utilisees_sf$arete_idx],
  na.rm = TRUE
)
pct_axes_utilises_touches <- if (km_utilise > 0) 100 * km_utilise_perturbe / km_utilise else 0

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

  # Entrée de légende manuelle pour les arêtes perturbées : tm_lines() ci-dessus
  # utilise une couleur fixe (pas une variable), donc tmap ne génère pas de
  # légende automatique pour cette couche. tm_add_legend() ajoute une entrée
  # dédiée avec le même rouge, pour que la carte reste lisible sans légende.
  tm_add_legend(
    labels   = "Arêtes coupées / perturbées",
    col      = "#CC0000",
    lwd      = 3.5,
    type     = "lines",
    position = tm_pos_out("right", "center")
  ) +

  # Points des zones colorés par classe d'impact. On utilise une échelle
  # CATÉGORIELLE et non un découpage du surcoût moyen : l'enclavement n'est pas
  # un surcoût élevé, c'est une absence de coût définissable, et il doit se lire
  # comme tel sur la carte.
  tm_shape(impact_par_zone_sf) +
  tm_dots(
    fill        = "classe_impact",
    # labels : mêmes intitulés que les niveaux du facteur classe_impact, sauf
    # « Déconnectée » qui est explicitée pour que la légende porte sa propre
    # définition (zone ayant perdu l'accès à ≥ 50 % de ses destinations
    # habituelles — voir le calcul de part_deconnectee plus haut).
    fill.scale  = tm_scale_categorical(
      values = PALETTE_CLASSE_ZONE,
      labels = c(
        "Surcoût < 5 %",
        "Surcoût 5–20 %",
        "Surcoût 20–50 %",
        "Surcoût 50–100 %",
        "Surcoût ≥ 100 %",
        "Déconnectée (≥ 50 % des destinations habituelles coupées)"
      )
    ),
    # position/width fixés explicitement : avec l'intitulé long de la classe
    # « Déconnectée », l'algorithme de mise en page automatique de tmap peut
    # décider de replier la légende en bas (où elle chevauche la note de
    # lecture) plutôt qu'à droite. On force donc la légende à droite — alignée
    # verticalement au centre pour longer la carte comme avant, et non calée en
    # haut de page — avec une largeur suffisante pour que l'intitulé tienne sur
    # une seule colonne.
    fill.legend = tm_legend(
      title    = "Impact sur la zone",
      position = tm_pos_out("right", "center"),
      width    = 22
    ),
    size = 0.5
  ) +
  
  tm_title(paste0("Réseau dégradé — ", NOM_SCENARIO,
                  "\n", DESCRIPTION_SCENARIO)) +
  tm_credits(
    note_lecture(sprintf(
      "ce scénario coupe %d tronçons (%.1f%% du linéaire routier total ; %.1f%% du linéaire des axes qui portaient déjà du fret avant la perturbation) et enclave %d zone(s) (« Déconnectée »).",
      nrow(aretes_perturbees_sf), pct_reseau_touche, pct_axes_utilises_touches, n_zones_deconnectees
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_reseau_degrade,
  file.path(DIR_CARTES, paste0("carte_reseau_degrade_", NOM_SCENARIO, ".png")),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ Carte A sauvegardée\n")

# ── CARTE B : Arêtes critiques (top N classées par criticité) ─────────────────
cat("  Génération Carte B — arêtes critiques...\n")

# Libellé "tranche (xx,x %)" par tranche de rang, part du linéaire des
# arêtes affichées (top N) — insérée dans la légende comme pour la carte des
# pentes. Mêmes seuils que col.scale ci-dessous ; .drop = FALSE conserve les
# tranches vides (ex. si N_ARETES_AFFICHEES < 20) pour garder 5 catégories.
breaks_rang  <- c(0, 5, 10, 15, 20, Inf)
libelles_rang <- c("0 à 5", "5 à 10", "10 à 15", "15 à 20", "20 et plus")
km_par_rang <- aretes_critiques_sf %>%
  st_drop_geometry() %>%
  mutate(tranche_rang = cut(rang, breaks = breaks_rang, labels = libelles_rang)) %>%
  group_by(tranche_rang, .drop = FALSE) %>%
  summarise(km = sum(length_km, na.rm = TRUE), .groups = "drop")

labels_rang_pct <- sprintf(
  "%s (%s %%)",
  km_par_rang$tranche_rang,
  sub("\\.", ",", sprintf("%.1f", 100 * km_par_rang$km / sum(km_par_rang$km)))
)

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
      breaks = breaks_rang,
      values = rev(c("#FFF5F0", "#FCBBA1", "#FC7050", "#EF3B2C", "#99000D")),
      labels = labels_rang_pct
    ),
    col.legend = tm_legend(title = paste0("Rang de criticité\n(top ",
                                          N_ARETES_AFFICHEES, ")")),
    lwd        = 3
  ) +
  
  # Arêtes perturbées du scénario actuel. Colonne constante "type_ligne" pour
  # forcer une entrée de légende dédiée (bleu), distincte du dégradé de
  # criticité ci-dessus.
  tm_shape(aretes_perturbees_sf %>% mutate(type_ligne = "Arête perturbée du scénario")) +
  tm_lines(
    col        = "type_ligne",
    col.scale  = tm_scale_categorical(values = c("Arête perturbée du scénario" = "#0000CC")),
    col.legend = tm_legend(title = "Scénario"),
    lwd        = 2
  ) +

  tm_title(paste0("Arêtes critiques du réseau — ", NOM_SCENARIO,
                  "\nTop ", N_ARETES_AFFICHEES, " par surcoût pondéré")) +
  tm_credits(
    note_lecture(sprintf(
      "le tronçon classé n°1 génère, à lui seul coupé, un surcoût pondéré de %s (milliers RWF×tonnes).",
      format(round(aretes_critiques_sf$surcout_pondere_k[which.min(aretes_critiques_sf$rang)]), big.mark = " ")
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_criticite,
  file.path(DIR_CARTES, paste0("carte_criticite_aretes_", NOM_SCENARIO, ".png")),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ Carte B sauvegardée\n")

# ── CARTE C : Vulnérabilité économique des zones ──────────────────────────────
cat("  Génération Carte C — vulnérabilité des zones...\n")

# Vérification : y a-t-il des surcoûts à représenter ?
has_surcouts <- any(impact_par_zone_sf$surcout_total_rwf > 0, na.rm = TRUE)
has_deconnex <- any(impact_par_zone_sf$n_deconnexions   > 0, na.rm = TRUE)

if (!has_surcouts) {
  cat("  ⚠ Aucun surcoût détecté pour ce scénario — carte C simplifiée\n")
}

carte_vulnerabilite <- fond_carte() +
  
  tm_shape(aretes_reseau_sf) +
  tm_lines(col = "#DDDDDD", lwd = 0.3) +
  
  # Couleur selon la présence de déconnexions (rouge = zone coupée du réseau) ;
  # taille fixe (0,5), commune à tous les points OD des cartes du projet.
  tm_shape(impact_par_zone_sf) +
  {
    if (has_surcouts) {
      # Version complète : couleur selon le nombre de destinations coupées
      tm_dots(
        fill       = "n_deconnexions",
        fill.scale = tm_scale_intervals(
          breaks = c(-Inf, 0, 1, 5, Inf),
          values = c("#2166AC", "#FEE08B", "#F46D43", "#A50026")
        ),
        fill.legend = tm_legend(title = "Nb de destinations\ncoupées"),
        size        = 0.5
      )
    } else {
      # Version dégradée : aucun surcoût à représenter, couleur selon type de zone
      tm_dots(
        fill        = "type",
        fill.scale  = tm_scale(values = PALETTE_TYPE),
        fill.legend = tm_legend(title = "Type"),
        size        = 0.5
      )
    }
  } +
  
  # Arêtes perturbées pour référence
  tm_shape(aretes_perturbees_sf) +
  tm_lines(col = "#CC0000", lwd = 3) +
  
  tm_title(paste0("Vulnérabilité économique des zones\n",
                  NOM_SCENARIO, " — Durée estimée : ",
                  DUREE_JOURS, " jours")) +
  tm_credits(
    note_lecture(sprintf(
      "la zone %s supporte le plus fort surcoût total, %s RWF sur les %d jours du scénario.",
      str_remove(impact_par_zone_sf$warehouse_name[which.max(impact_par_zone_sf$surcout_total_rwf)], " - .*"),
      format(round(max(impact_par_zone_sf$surcout_total_rwf)), big.mark = " "),
      DUREE_JOURS
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_vulnerabilite,
  file.path(DIR_CARTES, paste0("carte_vulnerabilite_zones_", NOM_SCENARIO, ".png")),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ Carte C sauvegardée\n")

# ── GRAPHIQUE : Distribution des surcoûts relatifs ────────────────────────────
cat("  Génération du graphique de distribution...\n")

# Exemple de lecture chiffré (repris dans le caption ci-dessous) : nombre de
# paires OD dont le surcoût est proche de 50 %, calculé sur les données
# réellement affichées pour rester exact quel que soit le scénario.
n_exemple_surcout <- sum(od_compare$surcout_relatif_pct >= 45 &
                          od_compare$surcout_relatif_pct < 55, na.rm = TRUE)

donnees_surcout <- od_compare %>%
  filter(!is.na(surcout_relatif_pct), surcout_relatif_pct > 0)

# Bornes des barres calées sur les seuils de classification de type_impact
# (10 %, 50 %, 100 % — voir 05_vulnerabilite.R). Avec un simple bins = 40,
# les limites des barres tombaient au hasard par rapport à ces seuils : une
# barre pouvait alors contenir des paires OD "faible" et "modere" à la fois,
# ce qui empilait deux couleurs sur une même barre. On part d'une grille
# log-régulière à 40 points (même densité visuelle qu'avant) et on y insère
# les 3 seuils exacts, pour qu'aucune barre ne chevauche deux catégories.
grille_log <- 10^seq(log10(min(donnees_surcout$surcout_relatif_pct)),
                      log10(max(donnees_surcout$surcout_relatif_pct)),
                      length.out = 40)
seuils_classification <- c(10, 50, 100)
seuils_a_inserer <- seuils_classification[
  seuils_classification > min(donnees_surcout$surcout_relatif_pct) &
  seuils_classification < max(donnees_surcout$surcout_relatif_pct)
]
bornes_histogramme <- sort(unique(c(grille_log, seuils_a_inserer)))

g_surcouts <- donnees_surcout %>%
  ggplot(aes(x = surcout_relatif_pct, fill = type_impact)) +
  geom_histogram(breaks = bornes_histogramme, color = "white", linewidth = 0.2) +
  scale_fill_manual(
    values = PALETTE_IMPACT,
    name   = "Type d'impact"
  ) +
  # Échelle log : la distribution est très étalée à droite (quelques paires
  # dépassent 1000 % de surcoût) — des graduations linéaires jusqu'à 200 %
  # compressaient tous les labels à gauche et laissaient le reste du
  # graphique vide. Même traitement que distribution_trafic_par_secteur.png
  # (viz_fret.R) pour une distribution à la même forme très asymétrique.
  scale_x_log10(
    labels = scales::percent_format(scale = 1),
    breaks = c(1, 5, 10, 25, 50, 100, 250, 500, 1000)
  ) +
  labs(
    title    = paste0("Distribution des surcoûts de transport — ", NOM_SCENARIO),
    subtitle = paste0(DESCRIPTION_SCENARIO,
                      "\nDurée estimée : ", DUREE_JOURS, " jours"),
    x        = "Hausse du coût de transport (%, échelle log)",
    y        = "Nombre de paires OD affectées",
    caption  = note_lecture(sprintf(
      "dans %s, %d paires origine-destination ont subi une hausse de leur coût de transport proche de 50 %%.",
      NOM_SCENARIO, n_exemple_surcout
    ), largeur_car = 132)
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title    = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#555555")
  ) +
  THEME_NOTE_LECTURE

ggsave(
  file.path(DIR_CARTES, paste0("graphique_surcouts_", NOM_SCENARIO, ".png")),
  g_surcouts, width = 11, height = 7.2, dpi = 300
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
    surcout_moyen   = surcout_moyen_detour[arete_idx],
    vol_detourne_t  = volume_detourne_arete[arete_idx]
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

# Part du linéaire de détour par classe de surcoût, insérée dans la parenthèse
# déjà utilisée pour la borne de surcoût (ex. "Faible (<10%, 23,4 % du
# linéaire)") — même principe que pour la carte des pentes.
km_par_surcout <- aretes_detour_sf %>%
  st_drop_geometry() %>%
  group_by(classe_surcout, .drop = FALSE) %>%
  summarise(km = sum(length_km, na.rm = TRUE), .groups = "drop")

labels_surcout_pct <- setNames(
  sprintf("%s, %s %% du linéaire)",
          sub("\\)$", "", as.character(km_par_surcout$classe_surcout)),
          sub("\\.", ",", sprintf("%.1f", 100 * km_par_surcout$km / sum(km_par_surcout$km)))),
  as.character(km_par_surcout$classe_surcout)
)

aretes_detour_sf <- aretes_detour_sf %>%
  mutate(classe_surcout_pct = factor(
    labels_surcout_pct[as.character(classe_surcout)],
    levels = labels_surcout_pct[names(PALETTE_SURCOUT_DETOUR)]
  ))

palette_surcout_pct <- setNames(
  PALETTE_SURCOUT_DETOUR,
  labels_surcout_pct[names(PALETTE_SURCOUT_DETOUR)]
)

# ── Zone inondée du scénario (Mode C, IX.1) ───────────────────────────────────
# Reconstruit le masque d'inondation à partir du même raster GloFAS et du même
# seuil de hauteur d'eau que la détection des arêtes perturbées dans
# 05_vulnerabilite.R (Mode C) : permet de voir si les itinéraires de détour
# longent la zone inondée ou s'en écartent largement.
zone_inondation_ok <- FALSE
if (UTILISER_MODE_RASTER && file.exists(CHEMIN_RASTER_RISQUE)) {
  raster_risque_detour <- terra::rast(CHEMIN_RASTER_RISQUE) %>%
    terra::project("EPSG:32735", method = "bilinear")
  masque_inondation <- raster_risque_detour > SEUIL_RISQUE_RASTER
  masque_inondation[masque_inondation == 0] <- NA
  if (!all(is.na(terra::values(masque_inondation)))) {
    zone_inondation_sf <- terra::as.polygons(masque_inondation, dissolve = TRUE) %>%
      st_as_sf()
    zone_inondation_ok <- TRUE
  }
}

carte_detour <- fond_carte() +

  # Réseau de base en gris très clair (contexte géographique)
  tm_shape(aretes_reseau_sf) +
  tm_lines(col = "#EEEEEE", lwd = 0.3)

# Zone inondée en aplat bleu semi-transparent, sous les routes de détour pour
# qu'elles restent lisibles par-dessus.
if (zone_inondation_ok) {
  carte_detour <- carte_detour +
    tm_shape(zone_inondation_sf) +
    tm_polygons(
      fill        = "#3182BD",
      col         = "#3182BD",
      lwd         = 0.3,
      fill_alpha  = 0.35,
      fill.legend = tm_legend(show = FALSE)
    )
}

carte_detour <- carte_detour +

  # Itinéraires de contournement : couleur = surcoût moyen, épaisseur = volume
  tm_shape(aretes_detour_sf) +
  tm_lines(
    col        = "classe_surcout_pct",
    col.scale  = tm_scale(values = palette_surcout_pct),
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
    fill        = "type",
    fill.scale  = tm_scale(values = PALETTE_TYPE),
    fill.legend = tm_legend(title = "Type"),
    size        = 0.5
  ) +
  
  tm_title(paste0(
    "Itinéraires de contournement — ", NOM_SCENARIO,
    "\nCouleur = surcoût moyen pondéré | Épaisseur = volume détourné | Noir = routes coupées"
  )) +
  tm_credits(
    note_lecture(paste0(
      sprintf(
        "le tronçon de détour le plus sollicité absorbe %s tonnes reportées depuis les routes coupées.",
        format(round(max(aretes_detour_sf$vol_detourne_t)), big.mark = " ")
      ),
      if (zone_inondation_ok) sprintf(
        " Aplat bleu : zone inondée du scénario (hauteur d'eau simulée > %.1f m, GloFAS période de retour %d ans).",
        SEUIL_RISQUE_RASTER, GLOFAS_PERIODE_RETOUR
      ) else ""
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_detour,
  file.path(DIR_CARTES, paste0("carte_detours_", NOM_SCENARIO, ".png")),
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
    # Trafic qui continue de circuler sur les arêtes non coupées de ce type
    # (= socle commun aux barres "avant choc" et "après choc")
    vol_conserve_t  = vol_ref_t - vol_perdu_t,
    # Variation nette = trafic de détour entrant - trafic perdu (route coupée)
    variation_nette = vol_detour_t - vol_perdu_t,
    pct_variation   = round(variation_nette / pmax(vol_ref_t, 1) * 100, 1),
    # Position verticale du label : au-dessus de la barre empilée "après choc"
    # (conservé + détourné entrant), pour que sa hauteur totale corresponde
    # bien au pourcentage affiché
    y_label         = (vol_conserve_t + vol_detour_t) / 1000
  ) %>%
  filter(!is.na(road_type))

# ── Format long empilé ─────────────────────────────────────────────────────────
# Pour chaque type de route, deux barres empilées côte à côte :
#  - "Avant choc"  = trafic conservé (bleu) + trafic perdu (rouge)
#  - "Après choc"  = trafic conservé (bleu) + trafic détourné entrant (vert)
# Le socle "trafic conservé" est identique dans les deux barres, ce qui permet
# de comparer visuellement la hauteur totale avant/après (et donc le %).
report_long <- bind_rows(
  report_df %>% transmute(road_type, phase = "Avant choc",
                           composante = "Trafic conservé",
                           volume_t = vol_conserve_t),
  report_df %>% transmute(road_type, phase = "Avant choc",
                           composante = "Perdu (route coupée)",
                           volume_t = vol_perdu_t),
  report_df %>% transmute(road_type, phase = "Après choc",
                           composante = "Trafic conservé",
                           volume_t = vol_conserve_t),
  report_df %>% transmute(road_type, phase = "Après choc",
                           composante = "Report entrant (détour)",
                           volume_t = vol_detour_t)
) %>%
  mutate(
    phase      = factor(phase, levels = c("Avant choc", "Après choc")),
    composante = factor(composante,
                        levels = c("Trafic conservé",
                                   "Perdu (route coupée)",
                                   "Report entrant (détour)"))
  )

# ── Graphique ─────────────────────────────────────────────────────────────────
# Une facette par type de route, avec dans chacune deux barres empilées
# (avant / après choc) partageant le même socle "trafic conservé".
g_report <- ggplot(report_long,
                   aes(x = phase, y = volume_t / 1000, fill = composante)) +

  geom_col(position = "stack", width = 0.72) +

  facet_wrap(~ road_type, nrow = 1) +

  # Annotation de la variation nette au-dessus de la barre "après choc"
  geom_text(
    data    = report_df,
    mapping = aes(
      x     = "Après choc",
      y     = y_label + max(report_df$y_label, na.rm = TRUE) * 0.05,
      label = paste0(ifelse(pct_variation >= 0, "+", ""), pct_variation, "%"),
      color = ifelse(variation_nette >= 0, "#006400", "#CC0000")
    ),
    inherit.aes = FALSE,
    vjust    = 0,
    size     = 3.2,
    fontface = "bold"
  ) +

  # Ligne de référence à 0 pour la lisibilité
  geom_hline(yintercept = 0, color = "#AAAAAA", linewidth = 0.4) +

  scale_fill_manual(
    values = c(
      "Trafic conservé"         = "#4393C3",
      "Perdu (route coupée)"    = "#D6604D",
      "Report entrant (détour)" = "#2CA25F"
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
      "Bleu = trafic conservé (commun aux deux barres) · Rouge = trafic perdu ",
      "sur route coupée · Vert = trafic de détour absorbé\n",
      "Pourcentage = variation nette / volume de référence"
    ),
    x    = NULL,
    y    = "Volume (milliers de tonnes)",
    fill = NULL,
    caption = note_lecture(sprintf(
      "sur les routes %s, le trafic varie de %s%s %% par rapport à la référence après le choc.",
      report_df$road_type[which.max(abs(report_df$pct_variation))],
      ifelse(report_df$pct_variation[which.max(abs(report_df$pct_variation))] >= 0, "+", ""),
      report_df$pct_variation[which.max(abs(report_df$pct_variation))]
    ), largeur_car = 132)
  ) +

  theme_minimal(base_size = 12) +
  theme(
    plot.title      = element_text(face = "bold", size = 13),
    plot.subtitle   = element_text(color = "#666666", size = 9),
    legend.position = "top",
    panel.grid.minor = element_blank(),
    strip.text      = element_text(face = "bold")
  ) +
  THEME_NOTE_LECTURE

ggsave(
  file.path(DIR_CARTES, paste0("graphique_report_type_route_", NOM_SCENARIO, ".png")),
  g_report,
  width = 11,
  height = 6.8,
  dpi = 300
)
cat("  ✓ graphique_report_type_route_", NOM_SCENARIO, ".png\n\n", sep = "")

