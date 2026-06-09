################################################################################
# viz_reseau.R
# RÔLE : Cartes du réseau routier, coûts, pentes, démographie, RWI.
#
# PEUT TOURNER SANS RELANCER LES SCRIPTS PRÉCÉDENTS si les .rds sont à jour.
#
# RELANCER 01_reseau.R avant ce script si :
#   → le fichier PBF a changé (nouveau téléchargement OSM)
#   → de nouvelles zones d'entrepôt ont été ajoutées / modifiées
#   → les données WorldPop, NISR ou RPHC5 ont été mises à jour
#   → les paramètres RAYON_AGGLO_ENTREPOT_M ou BUFFER_POIDS_RWI_M ont changé
#   → les données RPHC5 d'emploi ont été mises à jour (emploi_zone_secteur)
#
# RELANCER 02_couts.R avant ce script si :
#   → les paramètres de flotte (params_flotte, vitesses_flotte, facteurs_pente)
#     ont changé
#   → les valeurs VEHICULE_REFERENCE ou TONNES_PAR_mrd_RWF ont changé
#   → le DEM (pentes) a été recalculé
#
# FICHIERS LUS : persist_geodata.rds, persist_reseau_base.rds (pour carte III),
#                persist_reseau_couts.rds, persist_entreposages.rds
################################################################################

source("00_parametres.R")
fond_carte <- readRDS(file.path(DIR_CARTES, "persist_fond_carte.rds"))

.ent  <- readRDS(PERSIST_ENTREPOSAGES)
list2env(.ent, envir = .GlobalEnv)

.res  <- readRDS(PERSIST_RESEAU_COUTS)
reseau_rwanda <- .res$reseau_rwanda
rm(.ent, .res)

.base         <- readRDS(PERSIST_RESEAU_BASE)
routes_rwanda <- .base$routes_rwanda
rm(.base)

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

# ── Carte 1 : vérification post-nettoyage ─────────────────────────────────────
# Cette carte est générée pour vérifier visuellement que le réseau routier
# a été correctement chargé et nettoyé. Chaque type de route apparaît dans
# une couleur différente (définie dans PALETTE_ROAD_TYPE).
# tm_lines() : représente les lignes (routes) avec une couleur selon "road_type".
# tm_scale() : définit comment mapper les valeurs de "road_type" aux couleurs.
carte_verif_routes <- fond_carte() +
  tm_shape(routes_rwanda) +
  tm_lines(
    col       = "road_type",
    col.scale = tm_scale(values = PALETTE_ROAD_TYPE),
    col.legend = tm_legend(title = "Type de route"),
    lwd = 1.2
  ) +
  tm_title("Réseau Routier du Rwanda\nContrôle post-nettoyage") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position = c("right", "top"))

# tmap_save() exporte la carte en fichier PNG haute résolution.
# width, height : dimensions en pixels. dpi = 300 : résolution pour impression.
tmap_save(carte_verif_routes,
          file.path(DIR_CARTES, "carte_verif_routes.png"),
          width = 3000, height = 2400, dpi = 300)

# Ce bloc if (FALSE) est volontairement désactivé (FALSE = ne jamais s'exécuter).
# Pour l'activer temporairement et afficher la carte interactive dans RStudio,
# il suffit de remplacer FALSE par TRUE et de relancer ce bloc.
# tmap_mode("view") active le mode interactif (carte zoomable dans le Viewer).
# tmap_mode("plot") remet le mode statique pour les exports PNG.
if (FALSE) {
  tmap_mode("view")
  print(carte_verif_routes)
  tmap_mode("plot")   # Remettre en mode statique pour la suite du script
}

cat("✓ Carte de vérification générée\n\n")

# ── Carte des arêtes perdues ────────────────────────────────────────────────
cat("Génération de la carte des arêtes perdues...\n")

if (file.exists(file.path(DIR_PERSIST, "persist_diag_reseau.rds"))) {
  .diag <- readRDS(file.path(DIR_PERSIST, "persist_diag_reseau.rds"))
  list2env(.diag, envir = .GlobalEnv)
  rm(.diag)
}

# Palette par type de route (cohérente avec la carte de vérification Partie 3)
if (exists("aretes_perdues")) {
carte_aretes_perdues <- fond_carte() +
  
  
  # Arêtes perdues colorées par type de route
  tm_shape(aretes_perdues) +
  tm_lines(
    col       = "road_type",
    col.scale = tm_scale(values = PALETTE_ROAD_TYPE),
    col.legend = tm_legend(title = "Type de route\n(arêtes perdues)"),
    lwd = 3
  ) +
  
  # Nœuds hors géante (points rouges) pour visualiser les isolats
  tm_shape(noeuds_hors_geante) +
  tm_dots(fill = "#CC0000", size = 0.2, fill_alpha = 0.5) +
  
  tm_title(paste0("Arêtes exclues de la composante géante\n(",
                  round(nrow(aretes_perdues) / n_aretes_avant * 100, 1),
                  "% du réseau)")) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_aretes_perdues,
  file.path(DIR_CARTES, "carte_aretes_perdues.png"),
  width = 3000, height = 2400, dpi = 300
)
if (FALSE) {
  tmap_mode("view")
  print(carte_aretes_perdues)
  tmap_mode("plot")
}

cat("✓ Carte des arêtes perdues sauvegardée\n\n")
}


cat("── Visualisation de la distribution démographique ───────────────────\n")

# ── Carte : population par zone sur le réseau ─────────────────────────────────
# Les entrepôts sont affichés comme des cercles dont le diamètre est
# proportionnel à la population (échelle log pour gérer les ordres de grandeur).
# Kigali (~1 M d'habitants) ne doit pas écraser visuellement les petites villes.

entrepots_pop_sf <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse, !is.na(population_zone)) %>%
  st_as_sf() 

carte_population <- fond_carte() +

  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#DDDDDD", lwd = 0.4) +

  # Contours des cellules de Voronoï : chaque zone d'entrepôt correspond à une
  # cellule dont les frontières délimitent l'espace rattaché à cet entrepôt.
  tm_shape(zones_voronoi) +
  tm_borders(col = "#999999", lwd = 0.7, lty = "dashed") +

  tm_shape(entrepots_pop_sf) +
  tm_dots(
    fill        = "population_zone",
    fill.scale  = tm_scale_intervals(
      style  = "quantile",
      n      = 5,
      values = "brewer.yl_or_rd"
    ),
    fill.legend = tm_legend(title = "Population\n(habitants)"),
    size        = 0.3
  ) +

  tm_title("Distribution démographique des zones d'entrepôt\nSources : NISR 2022 / WorldPop 2020 / OSM") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_population,
  file.path(DIR_CARTES, "carte_population_zones.png"),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ carte_population_zones.png\n")

# ── Identification de la zone de référence (max population = future réf. demande) ──
zone_ref_pop_approx <- diag_population %>%
  arrange(desc(population_zone)) %>%
  slice(1) %>%
  pull(nom_zone)

# Etiquette courte pour l'annotation (même troncature que Zone_court dans le plot)
zone_ref_pop_label <- str_trunc(str_remove(zone_ref_pop_approx, " - .*"), 25)

# ── Graphique : population par zone et par type ───────────────────────────────
g_pop <- diag_population %>%
  arrange(desc(population_zone)) %>%
  mutate(
    Zone_court = str_trunc(str_remove(nom_zone, " - .*"), 25),
    Zone_court = make.unique(Zone_court, sep = " #"),   # rend les labels uniques
    Zone_court = factor(Zone_court, levels = rev(Zone_court)),
    est_reference   = (nom_zone == zone_ref_pop_approx)
  ) %>%
  ggplot(aes(x = Zone_court, y = population_zone / 1000,
             fill = type_zone, alpha = source)) +
  geom_col(width = 0.75) +
  # Surlignage de la barre de référence par un contour rouge épais
  geom_col(
    data = ~ filter(., est_reference),
    aes(x = Zone_court, y = population_zone / 1000),
    fill  = NA,
    color = "#CC0000",
    linewidth = 1.2,
    width = 0.75,
    inherit.aes = FALSE
  ) +
  # Annotation textuelle positionnée à droite de la barre de référence
  annotate(
    "text",
    x     = zone_ref_pop_label,
    y     = max(diag_population$population_zone / 1000, na.rm = TRUE) * 0.55,
    label = "◄ Référence demande",
    hjust = 0,
    color = "#CC0000",
    size  = 3.2,
    fontface = "bold"
  ) +
  coord_flip() +
  scale_alpha_manual(
    values = c("NISR_2022"     = 1.0,
               "WorldPop_2020" = 0.8,
               "OSM"           = 0.65,
               "Fallback_1000" = 0.35),
    name   = "Source"
  ) +
  scale_fill_manual(values = PALETTE_ZONE_TYPE, name = "Type de zone") +
  scale_y_continuous(labels = scales::label_number(suffix = " k")) +
  labs(
    title    = "Population par zone d'entrepôt",
    subtitle = paste0(
      "Transparence = fiabilité de la source (opaque = NISR officiel)\n",
      "Contour rouge = zone de population maximale (référence MRIO)"
    ),
    x = NULL,
    y = "Population (milliers d'habitants)"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "right"
  )

ggsave(
  file.path(DIR_CARTES, "graphique_population_zones.png"),
  g_pop, width = 12, height = 8, dpi = 300
)
cat("  ✓ graphique_population_zones.png\n\n")

cat("── Visualisations RWI ────────────────────────────────────────────────\n")

# ── Préparation de la couche sf pour les entrepôts enrichis RWI ───────────────
entrepots_rwi_sf <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse, !is.na(p_rwi)) %>%
  st_as_sf() %>%
  mutate(
    taille_cercle = rescale(p_rwi, to = c(0.3, 2.5)),
    classe_rwi    = case_when(
      p_rwi >= 0.75 ~ "Très riche",
      p_rwi >= 0.50 ~ "Riche",
      p_rwi >= 0.25 ~ "Pauvre",
      TRUE          ~ "Très pauvre"
    ),
    classe_rwi = factor(
      classe_rwi,
      levels = c("Très pauvre", "Pauvre", "Riche", "Très riche")
    )
  )

# ── Carte : score p_rwi sur le réseau ─────────────────────────────────────────
# Le dégradé de couleur va du bleu foncé (zones pauvres) au rouge foncé (zones
# riches), ce qui est la convention cartographique habituelle pour les indices
# de richesse. La taille des points est proportionnelle au score p_rwi.
PALETTE_RWI <- c(
  "#08519C",   # Bleu foncé  — très pauvre (p_rwi < 0.25)
  "#6BAED6",   # Bleu clair  — pauvre      (p_rwi 0.25–0.50)
  "#FD8D3C",   # Orange      — riche       (p_rwi 0.50–0.75)
  "#A50026"    # Rouge foncé — très riche  (p_rwi > 0.75)
)

carte_rwi <- fond_carte() +

  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#DDDDDD", lwd = 0.4) +

  # Contours des cellules de Voronoï : chaque zone d'entrepôt correspond à une
  # cellule dont les frontières délimitent l'espace rattaché à cet entrepôt.
  tm_shape(zones_voronoi) +
  tm_borders(col = "#999999", lwd = 0.7, lty = "dashed") +

  tm_shape(entrepots_rwi_sf) +
  tm_dots(
    fill        = "p_rwi",
    fill.scale  = tm_scale_intervals(
      style  = "fixed",
      breaks = c(0, 0.25, 0.50, 0.75, 1.00),
      values = PALETTE_RWI
    ),
    fill.legend = tm_legend(title = "Richesse relative\n(p_rwi normalisé)"),
    size        = "taille_cercle",
    size.scale  = tm_scale(values.range = c(0.3, 2.5)),
    size.legend = tm_legend(show = FALSE)
  ) +
  
  tm_title(paste0(
    "Richesse relative des zones d'entrepôt\n",
    "Relative Wealth Index — Meta / CIESIN ",
    "(moyenne par cellule de Voronoï, pondérée population)"
  )) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_rwi,
  file.path(DIR_CARTES, "carte_rwi_zones.png"),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ carte_rwi_zones.png\n")

# ── Carte : raster RWI Rwanda (vue d'ensemble des données brutes) ─────────────
# Cette carte montre les données RWI pour TOUT le Rwanda (pas seulement les
# entrepôts), ce qui permet de visualiser les gradients spatiaux de richesse
# et de vérifier que les entrepôts sont bien positionnés dans leur contexte.
if (rwi_ok && nrow(rwi_sf) > 0) {
  
  carte_rwi_raster <- fond_carte() +
    
    tm_shape(rwi_sf) +
    tm_dots(
      fill        = "rwi",
      fill.scale  = tm_scale_intervals(
        style  = "quantile",
        n      = 5,
        values = c("#08519C", "#6BAED6", "#F7F7F7", "#FD8D3C", "#A50026")
      ),
      fill.legend = tm_legend(title = "Score RWI\n(brut, centré 0)"),
      size        = 0.05,    # Petite taille : beaucoup de points (~1700 cellules)
      fill_alpha  = 0.7
    ) +
    
    # Superposition des entrepôts pour le repérage
    tm_shape(entrepots_rwi_sf) +
    tm_dots(
      fill        = "warehouse_type",
      fill.scale  = tm_scale(values = PALETTE_ZONE_TYPE),
      fill.legend = tm_legend(title = "Type d'entrepôt"),
      size        = 0.5,
      col         = "white",
      lwd         = 1
    ) +
    
    tm_title("Données RWI brutes — Rwanda\n(chaque point = cellule de ~2,4 km²)") +
    tm_layout(legend.outside = TRUE, frame = TRUE) +
    tm_scalebar(position = c("left", "bottom")) +
    tm_compass(position  = c("right", "top"))
  
  tmap_save(
    carte_rwi_raster,
    file.path(DIR_CARTES, "carte_rwi_rwanda_brut.png"),
    width = 3000, height = 2400, dpi = 300
  )
  cat("  ✓ carte_rwi_rwanda_brut.png\n")
}

# ── Graphique : corrélation RWI × population ──────────────────────────────────
# Ce graphique met en relation les deux enrichissements (IV.4 et IV.5) pour
# vérifier leur cohérence : on s'attend à ce que les zones à forte population
# (Kigali, Musanze…) aient aussi des scores RWI élevés — mais pas toujours,
# car les zones frontalières peuvent avoir une population élevée et un RWI faible.
if ("population_zone" %in% names(entreposages_fictifs)) {
  
  g_rwi_pop <- diag_rwi %>%
    left_join(
      entreposages_fictifs %>%
        select(nom, population_zone),
      by = c("nom_zone" = "nom")
    ) %>%
    filter(!is.na(population_zone), population_zone > 0) %>%
    mutate(
      Zone_court = str_trunc(str_remove(nom_zone, " - .*"), 22),
      pop_log    = log10(population_zone)
    ) %>%
    ggplot(aes(x = pop_log, y = p_rwi,
               color = type_zone, label = Zone_court)) +
    
    geom_point(size = 4, alpha = 0.85) +
    ggrepel::geom_text_repel(
      size = 3, max.overlaps = 12,
      segment.color = "#AAAAAA"
    ) +
    
    # Droite de régression pour visualiser la tendance générale
    geom_smooth(
      method  = "lm",
      se      = TRUE,
      color   = "#333333",
      linetype = "dashed",
      alpha   = 0.15
    ) +
    
    scale_color_manual(values = PALETTE_ZONE_TYPE, name = "Type de zone") +
    scale_x_continuous(
      labels = function(x) format(10^x, big.mark = " ", scientific = FALSE),
      name   = "Population (échelle log₁₀)"
    ) +
    scale_y_continuous(
      limits = c(0, 1),
      name   = "Score de richesse relative (p_rwi)"
    ) +
    
    labs(
      title    = "Richesse relative (RWI) × Population par zone d'entrepôt",
      subtitle = paste0(
        "Les zones en haut à droite (riches ET peuplées) génèrent la plus forte demande\n",
        "Sources : Meta RWI (2021), NISR RPHC-5 (2022)"
      )
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title    = element_text(face = "bold"),
      plot.subtitle = element_text(color = "#555555", size = 10),
      legend.position = "right"
    )+
    # Cercle rouge autour du point de référence
    geom_point(
      data = ~ filter(., str_trunc(str_remove(nom_zone, " - .*"), 22) ==
                        str_trunc(str_remove(zone_ref_pop_approx, " - .*"), 22)),
      aes(x = pop_log, y = p_rwi),
      shape  = 21,
      size   = 6,
      color  = "#CC0000",
      fill   = NA,
      stroke = 1.5,
      inherit.aes = FALSE
    ) +
    # Légende textuelle du cercle
    annotate(
      "text",
      x     = log10(max(diag_population$population_zone, na.rm = TRUE)) * 0.97,
      y     = 0.05,
      label = paste0("◄ Population max\n  (référence MRIO)"),
      hjust = 1,
      color = "#CC0000",
      size  = 3.0,
      fontface = "italic"
    )
  
  ggsave(
    file.path(DIR_CARTES, "graphique_rwi_vs_population.png"),
    g_rwi_pop, width = 12, height = 7, dpi = 300
  )
  cat("  ✓ graphique_rwi_vs_population.png\n\n")
}

# ==============================================================================
# V.3 : Cartes de coûts et de pentes
# Génère une carte de cost_per_tkm par véhicule, deux cartes de ratio
# (lourd/camionnette, moyen/camionnette) et la carte des pentes.
# ==============================================================================

# On génère une carte par véhicule montrant le coût de transport sur chaque
# segment routier. Les segments rouges/bordeaux sont les plus coûteux
# (routes en mauvais état, pentes importantes, zone urbaine).

for (i in seq_len(nrow(VEHICULES_IDS))) {
  
  id_veh  <- VEHICULES_IDS$vehicule_id[i]
  nom_veh <- VEHICULES_IDS$nom[i]
  
  # Récupérer les coûts depuis DuckDB (une requête SQL, pas de R intermédiaire)
  couts_veh <- duck_query(glue::glue("
    SELECT arete_id, cost_per_tkm, speed_kmh
    FROM aretes_couts_tous
    WHERE vehicule_id = '{id_veh}'
    ORDER BY arete_id
  "))
  
  reseau_tmp <- reseau_rwanda %>%
    activate("edges") %>%
    mutate(
      cost_per_tkm          = couts_veh$cost_per_tkm,
      speed_kmh            = couts_veh$speed_kmh
    )
  
  # tm_scale_intervals() : découpe la variable continue (cost_per_tkm) en
  # intervalles discrets pour la légende.
  # style="quantile" : intervalles de taille égale en nombre d'observations
  # (chaque classe contient le même nombre d'arêtes).
  carte <- fond_carte() +
    tm_shape(reseau_tmp %>% activate("edges") %>% st_as_sf()) +
    tm_lines(
      col       = "cost_per_tkm",
      col.scale = tm_scale_intervals(style="quantile", n=4, values=PALETTE_COUTS),
      col.legend = tm_legend(title = "Coût (RWF/tkm)"),
      lwd = 1.5
    ) +
    tm_shape(entreposages_sf) + tm_dots(fill="black", size=0.2) +
    tm_title(paste("Coûts de Transport —", nom_veh)) +
    tm_layout(legend.outside=TRUE, frame=TRUE) +
    tm_scalebar(position=c("left","bottom")) +
    tm_compass(position=c("right","top"))
  
  nom_fichier <- paste0("carte_couts_", id_veh, ".png")
  tmap_save(carte, file.path(DIR_CARTES, nom_fichier), width=3000, height=2400, dpi=300)
  cat("  ✓", nom_fichier, "\n")
}


# On génère une deuxième série de cartes dans un format différent (pour le Viewer)
# en stockant les objets tmap dans une liste pour un affichage interactif optionnel.
cartes_vehicules <- list()

for (i in seq_len(nrow(VEHICULES_IDS))) {
  id_veh  <- VEHICULES_IDS$vehicule_id[i]
  nom_veh <- VEHICULES_IDS$nom[i]
  cat(id_veh, "\n")
  cat(nom_veh, "\n")
  
  couts_veh <- duck_query(glue::glue("
    SELECT arete_id, cost_per_tkm, speed_kmh
    FROM aretes_couts_tous
    WHERE vehicule_id = '{id_veh}'
    ORDER BY arete_id
  "))
  cat("Lignes récupérées :", nrow(couts_veh), "\n")
  
  reseau_tmp <- reseau_rwanda %>%
    activate("edges") %>%
    mutate(
      cost_per_tkm          = couts_veh$cost_per_tkm,
      speed_kmh            = couts_veh$speed_kmh
    )
  cat("reseau_tmp créé\n")
  
  # tm_scale(values.range = c(0.5, 5)) : remplace la légende de la largeur de ligne
  # par une échelle continue entre 0.5 (fin) et 5 (épais).
  cartes_vehicules[[id_veh]] <- fond_carte() +
    tm_shape(reseau_tmp %>% activate("edges") %>% st_as_sf()) +
    tm_lines(
      col        = "cost_per_tkm",
      col.scale  = tm_scale_intervals(style="quantile", n=5, values="brewer.yl_or_rd"),
      col.legend = tm_legend(title = "Coût (RWF/km)"),
      lwd = 1.5
    ) +
    tm_shape(entreposages_sf) + tm_dots(fill="black", size=0.2) +
    tm_title(paste("Coûts de Transport —", nom_veh))
}
if (FALSE){
  tmap_mode("view")
  cat("✓ Cartes créées :", paste(names(cartes_vehicules), collapse=", "), "\n")
  cat("  Pour afficher, entrer dans la console : print(cartes_vehicules[['camionnette']])\n")
  cat("                                          print(cartes_vehicules[['camion_moyen']])\n")
  cat("                                          print(cartes_vehicules[['camion_lourd']])\n")
  tmap_mode("plot")
}

# ── Carte comparative : ratio coût par km camion lourd vs camionnette ─────────
# Cette carte montre où le camion lourd est relativement plus avantageux (vert)
# ou plus désavantageux (rouge) par rapport à la camionnette.
# Ratio > 1 : le camion lourd coûte plus cher par tkm (pentes fortes, routes dégradées)
# Ratio < 1 : le camion lourd est plus avantageux (économies d'échelle sur grande route)
# Requête SQL directe : le calcul du ratio se fait entièrement dans DuckDB
ratio_df <- duck_query("
  SELECT
    a.arete_id,
    a.cost_per_tkm / NULLIF(b.cost_per_tkm, 0) AS ratio_lourd_vs_legere
  FROM
    (SELECT arete_id, cost_per_tkm FROM aretes_couts_tous WHERE vehicule_id = 'camion_lourd')  a
  JOIN
    (SELECT arete_id, cost_per_tkm FROM aretes_couts_tous WHERE vehicule_id = 'camionnette') b
  USING (arete_id)
  ORDER BY arete_id
")

if (nrow(ratio_df) > 0) {
  reseau_ratio <- reseau_rwanda %>%
    activate("edges") %>%
    mutate(ratio_lourd_vs_legere = ratio_df$ratio_lourd_vs_legere)
  
  carte_ratio <- fond_carte() +
    tm_shape(reseau_ratio %>% activate("edges") %>% st_as_sf()) +
    tm_lines(
      col       = "ratio_lourd_vs_legere",
      col.scale = tm_scale_intervals(style="quantile", n=5, values=PALETTE_RATIO),
      col.legend = tm_legend(title="Ratio coût\nlourd / camionnette"),
      lwd = 1.5
    ) +
    tm_title("Surcoût relatif — Camion lourd vs Camionnette") +
    tm_layout(legend.outside=TRUE, frame=TRUE) +
    tm_scalebar(position=c("left","bottom")) +
    tm_compass(position=c("right","top"))
  
  tmap_save(carte_ratio, file.path(DIR_CARTES,"carte_ratio_vehicules.png"),
            width=3000, height=2400, dpi=300)
  cat("  ✓ carte_ratio_vehicules.png\n")
}

cat("✓", nrow(VEHICULES_IDS), "cartes + 1 carte comparative générées\n\n")

if (FALSE){
  tmap_mode("view")
  print(carte_ratio)
  tmap_mode("plot")
}

# ── Carte comparative : ratio coût par tkm camion moyen vs camionnette ────────
ratio_moyen_df <- duck_query("
  SELECT
    a.arete_id,
    a.cost_per_tkm / NULLIF(b.cost_per_tkm, 0) AS ratio_moyen_vs_camionnette
  FROM
    (SELECT arete_id, cost_per_tkm FROM aretes_couts_tous WHERE vehicule_id = 'camion_moyen') a
  JOIN
    (SELECT arete_id, cost_per_tkm FROM aretes_couts_tous WHERE vehicule_id = 'camionnette') b
  USING (arete_id)
  ORDER BY arete_id
")

if (nrow(ratio_moyen_df) > 0) {
  reseau_ratio_moyen <- reseau_rwanda %>%
    activate("edges") %>%
    mutate(ratio_moyen_vs_camionnette = ratio_moyen_df$ratio_moyen_vs_camionnette)
  
  carte_ratio_moyen <- fond_carte() +
    tm_shape(reseau_ratio_moyen %>% activate("edges") %>% st_as_sf()) +
    tm_lines(
      col       = "ratio_moyen_vs_camionnette",
      col.scale = tm_scale_intervals(style="quantile", n=5, values=PALETTE_RATIO),
      col.legend = tm_legend(title="Ratio coût\nmoyen / camionnette"),
      lwd = 1.5
    ) +
    tm_title("Surcoût relatif — Camion moyen vs Camionnette") +
    tm_layout(legend.outside=TRUE, frame=TRUE) +
    tm_scalebar(position=c("left","bottom")) +
    tm_compass(position=c("right","top"))
  
  tmap_save(carte_ratio_moyen,
            file.path(DIR_CARTES,"carte_ratio_moyen_camionnette.png"),
            width=3000, height=2400, dpi=300)
  cat("  ✓ carte_ratio_moyen_camionnette.png\n")
}

if (FALSE) {
  tmap_mode("view")
  print(carte_ratio_moyen)
  tmap_mode("plot")
}

# ── Carte des pentes (indépendante du véhicule) ───────────────────────────────
# Cette carte ne dépend pas du type de véhicule : elle montre juste l'inclinaison
# du terrain sur chaque segment routier. Elle permet d'identifier visuellement
# les zones montagneuses (routes en rouge = pente forte > 8%).
carte_pentes <- fond_carte() +
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col="slope_category",
           col.scale = tm_scale(values = PALETTE_PENTE),
           col.legend=tm_legend(title="Catégorie de pente"), lwd=1.5) +
  tm_title("Pentes du Réseau Routier") +
  tm_layout(legend.outside=TRUE, frame=TRUE) +
  tm_scalebar(position=c("left","bottom")) +
  tm_compass(position=c("right","top"))

tmap_save(carte_pentes, file.path(DIR_CARTES,"carte_pentes_rwanda.png"),
          width=3000, height=2400, dpi=300)
cat("  ✓ carte_pentes_rwanda.png\n")

if (FALSE) {
  tmap_mode("view")
  print(carte_pentes)
  tmap_mode("plot")
}

# ==============================================================================
# V.4 : Cartes d'émissions de GES
# Génère deux cartes pour le véhicule de référence :
#   - intensité carbone (co2_kg_par_tkm) : routes les plus émettrices
#   - émissions de NOx (nox_g_par_tkm)   : proxy de pollution locale
# Et un graphique comparatif des émissions totales par véhicule.
# ==============================================================================

# ── Carte : intensité carbone (co2_kg_par_tkm) pour le véhicule de référence ──
# Cette carte identifie les segments routiers où chaque tonne-kilomètre
# transportée génère le plus de CO2 : pentes fortes, mauvaise surface,
# zones de congestion urbaine.
# Elle répond à la question : "Où décarboner le transport est-il le plus urgent ?"
carte_co2 <- fond_carte() +
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(
    col        = "co2_kg_par_tkm",
    col.scale  = tm_scale_intervals(style = "quantile", n = 5,
                                    values = PALETTE_EMISSIONS),
    col.legend = tm_legend(title = "CO₂\n(kg / tonne-km)"),
    lwd        = 1.5
  ) +
  tm_shape(entreposages_sf) + tm_dots(fill = "black", size = 0.2) +
  tm_title(paste0("Intensité carbone du réseau — ", VEHICULES_IDS$nom[
    VEHICULES_IDS$vehicule_id == VEHICULE_REFERENCE
  ])) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(carte_co2,
          file.path(DIR_CARTES, "carte_emissions_co2_par_tkm.png"),
          width = 3000, height = 2400, dpi = 300)
cat("  ✓ carte_emissions_co2_par_tkm.png\n")

# ── Carte : intensité NOx (nox_g_par_tkm) ─────────────────────────────────────
# Le NOx est le principal polluant local du transport routier diesel.
# Sa distribution spatiale diffère du CO2 : elle dépend davantage
# des normes Euro des moteurs (plus sévère en ville) et de la congestion
# (ralentissements → régime moteur sous-optimal → émissions NOx élevées).
carte_nox <- fond_carte() +
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(
    col        = "nox_g_par_tkm",
    col.scale  = tm_scale_intervals(style = "quantile", n = 5,
                                    values = PALETTE_EMISSIONS),
    col.legend = tm_legend(title = "NOx\n(g / tonne-km)"),
    lwd        = 1.5
  ) +
  tm_shape(entreposages_sf) + tm_dots(fill = "black", size = 0.2) +
  tm_title("Intensité NOx du réseau (pollution locale)") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(carte_nox,
          file.path(DIR_CARTES, "carte_emissions_nox_par_tkm.png"),
          width = 3000, height = 2400, dpi = 300)
cat("  ✓ carte_emissions_nox_par_tkm.png\n")

# ── Graphique : émissions totales comparées par véhicule ──────────────────────
# Ce graphique répond à : "Quel véhicule est le plus émetteur sur le réseau ?"
# en agrégeant les émissions absolues sur l'ensemble du réseau.
# La décomposition par composante (CO2, NOx, PM2.5) montre que le classement
# peut différer selon le polluant : un camion lourd émet plus de CO2 mais
# un camion plus vieux peut émettre disproportionnellement plus de PM2.5.
emissions_par_vehicule <- duck_query("
  SELECT
    vehicule_id,
    vehicule_nom,
    -- Émissions totales sur l'ensemble des arêtes du réseau
    ROUND(SUM(co2_kg)  / 1000, 1)  AS co2_total_t,    -- CO2 en tonnes
    ROUND(SUM(nox_g)   / 1000, 1)  AS nox_total_kg,   -- NOx en kg
    ROUND(SUM(pm25_g)  / 1000, 1)  AS pm25_total_kg,  -- PM2.5 en kg
    -- Intensités moyennes pondérées par la longueur de l'arête
    ROUND(AVG(co2_kg_par_tkm), 4)  AS co2_intensite_moy,
    ROUND(AVG(nox_g_par_tkm),  4)  AS nox_intensite_moy
  FROM aretes_couts_tous
  GROUP BY vehicule_id, vehicule_nom
  ORDER BY co2_total_t DESC
")

cat("\nÉmissions totales par véhicule (réseau complet) :\n")
print(emissions_par_vehicule)

# Graphique en barres groupées : CO2 / NOx / PM2.5 côte à côte par véhicule.
# On normalise chaque polluant sur sa propre échelle (valeur relative entre
# véhicules) car les ordres de grandeur sont très différents (tonnes vs grammes).
# scale() centre et normalise chaque colonne entre 0 et 1 (min-max scaling).
emissions_long <- emissions_par_vehicule %>%
  select(vehicule_nom, co2_total_t, nox_total_kg, pm25_total_kg) %>%
  pivot_longer(-vehicule_nom,
               names_to  = "Polluant",
               values_to = "Valeur") %>%
  # Renommage pour la légende du graphique
  mutate(
    Polluant = recode(Polluant,
                      "co2_total_t"   = "CO₂ (t)",
                      "nox_total_kg"  = "NOx (kg)",
                      "pm25_total_kg" = "PM2.5 (kg)"),
    # Normalisation min-max par polluant pour rendre les barres comparables
    # entre polluants qui n'ont pas les mêmes unités.
    # group_by + mutate permet ici de normaliser chaque polluant séparément.
    Valeur_norm = Valeur / max(Valeur)
  ) %>%
  group_by(Polluant) %>%
  mutate(Valeur_norm = Valeur / max(Valeur)) %>%
  ungroup()

g_emissions <- ggplot(emissions_long,
                      aes(x = vehicule_nom, y = Valeur_norm, fill = Polluant)) +
  geom_col(position = "dodge", width = 0.65) +
  scale_fill_manual(values = c("CO₂ (t)"    = "#D73027",
                               "NOx (kg)"   = "#FC8D59",
                               "PM2.5 (kg)" = "#4575B4")) +
  scale_y_continuous(labels = scales::percent_format()) +
  labs(
    title    = "Émissions relatives par véhicule et par polluant",
    subtitle = "Normalisé par rapport au véhicule le plus émetteur de chaque polluant (100%)",
    x        = NULL,
    y        = "Niveau relatif d'émission",
    fill     = "Polluant"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title    = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#666666"),
    legend.position = "top"
  )

ggsave(file.path(DIR_CARTES, "graphique_emissions_par_vehicule.png"),
       g_emissions, width = 10, height = 6, dpi = 300)
cat("  ✓ graphique_emissions_par_vehicule.png\n\n")


