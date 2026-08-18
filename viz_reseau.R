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
#   → les paramètres de flotte (params_flotte, params_flotte_type_route, facteurs_pente)
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
reseau <- .res$reseau
rm(.ent, .res)

.base         <- readRDS(PERSIST_RESEAU_BASE)
routes <- .base$routes
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
# Exemple chiffré repris dans la note de lecture ci-dessous.
tab_routes_v      <- table(routes$road_type)
type_dominant_v   <- names(which.max(tab_routes_v))
pct_dominant_v    <- round(100 * max(tab_routes_v) / sum(tab_routes_v), 1)

# Libellé "type (xx,x %)" par type de route, part en nombre de segments — même
# base que la note de lecture ci-dessous — insérée directement dans la légende
# (comme pour la carte des pentes).
labels_road_type_pct <- setNames(
  sprintf("%s (%s %%)", names(tab_routes_v),
          sub("\\.", ",", sprintf("%.1f", 100 * as.numeric(tab_routes_v) / sum(tab_routes_v)))),
  names(tab_routes_v)
)
noms_road_type_presents <- intersect(names(PALETTE_ROAD_TYPE), names(labels_road_type_pct))

routes <- routes %>%
  mutate(road_type_pct = factor(
    labels_road_type_pct[road_type],
    levels = labels_road_type_pct[noms_road_type_presents]
  ))

palette_road_type_pct <- setNames(
  PALETTE_ROAD_TYPE[noms_road_type_presents],
  labels_road_type_pct[noms_road_type_presents]
)

carte_verif_routes <- fond_carte() +
  tm_shape(routes) +
  tm_lines(
    col       = "road_type_pct",
    col.scale = tm_scale(values = palette_road_type_pct),
    col.legend = tm_legend(title = "Type de route"),
    lwd = 1.2
  ) +
  tm_title(paste0("Réseau Routier — ", NOM_PAYS, "\nContrôle post-nettoyage")) +
  tm_credits(
    note_lecture(sprintf(
      "%s %% des segments du réseau sont de type « %s ».",
      pct_dominant_v, type_dominant_v
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
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

# Libellé "type (xx,x %)" par type de route, part du linéaire des arêtes
# perdues (longueur_m, seule colonne de longueur disponible sur cet objet
# antérieur à la fusion des coûts) — insérée dans la légende comme pour la
# carte des pentes.
km_par_road_type_perdues <- aretes_perdues %>%
  st_drop_geometry() %>%
  group_by(road_type) %>%
  summarise(km = sum(longueur_m, na.rm = TRUE) / 1000, .groups = "drop")

labels_road_type_perdues_pct <- setNames(
  sprintf("%s (%s %%)", km_par_road_type_perdues$road_type,
          sub("\\.", ",", sprintf("%.1f", 100 * km_par_road_type_perdues$km / sum(km_par_road_type_perdues$km)))),
  km_par_road_type_perdues$road_type
)
noms_road_type_perdues_presents <- intersect(names(PALETTE_ROAD_TYPE), names(labels_road_type_perdues_pct))

aretes_perdues <- aretes_perdues %>%
  mutate(road_type_pct = factor(
    labels_road_type_perdues_pct[road_type],
    levels = labels_road_type_perdues_pct[noms_road_type_perdues_presents]
  ))

palette_road_type_perdues_pct <- setNames(
  PALETTE_ROAD_TYPE[noms_road_type_perdues_presents],
  labels_road_type_perdues_pct[noms_road_type_perdues_presents]
)

carte_aretes_perdues <- fond_carte() +


  # Arêtes perdues colorées par type de route
  tm_shape(aretes_perdues) +
  tm_lines(
    col       = "road_type_pct",
    col.scale = tm_scale(values = palette_road_type_perdues_pct),
    col.legend = tm_legend(title = "Type de route\n(arêtes perdues)"),
    lwd = 3
  ) +
  
  # Nœuds hors géante (points rouges) pour visualiser les isolats
  tm_shape(noeuds_hors_geante) +
  tm_dots(fill = "#CC0000", size = 0.2, fill_alpha = 0.5) +
  
  tm_title(paste0("Arêtes exclues de la composante géante\n(",
                  round(nrow(aretes_perdues) / n_aretes_avant * 100, 1),
                  "% du réseau)")) +
  tm_credits(
    note_lecture(sprintf(
      "%d arêtes et %d nœuds (points rouges) sont exclus de la composante connexe principale du réseau.",
      nrow(aretes_perdues), nrow(noeuds_hors_geante)
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
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

entrepots_pop_sf <- reseau %>%
  activate("nodes") %>%
  filter(is_warehouse, !is.na(population_zone)) %>%
  st_as_sf() %>%
  # Population en milliers d'habitants, arrondie à l'unité (ex. 950 pour
  # ~950 000 hab.) : évite les grands nombres sur la légende et les libellés.
  mutate(population_zone_k = round(population_zone / 1000))

# Part de la population modélisée qui provient de chaque source (WorldPop vs
# NISR, cf. diag_population$source, IV.6 de 01_reseau.R) : jointure par zone,
# puis pondération par population_zone pour refléter le poids réel de chaque
# source dans le total national, pas seulement le nombre de zones concernées.
part_source_pop <- entrepots_pop_sf %>%
  st_drop_geometry() %>%
  left_join(diag_population %>% select(nom_zone, source), by = c("warehouse_name" = "nom_zone")) %>%
  mutate(source_cat = case_when(
    str_detect(source, "^WorldPop") ~ "WorldPop",
    str_detect(source, "^NISR")     ~ "NISR",
    TRUE                            ~ "Autre"
  )) %>%
  group_by(source_cat) %>%
  summarise(pop = sum(population_zone), .groups = "drop") %>%
  mutate(pct = 100 * pop / sum(pop))

pct_worldpop_pop <- sum(part_source_pop$pct[part_source_pop$source_cat == "WorldPop"])
pct_nisr_pop     <- sum(part_source_pop$pct[part_source_pop$source_cat == "NISR"])

# WorldPop cité en premier ; NISR seulement si sa part n'est pas nulle (sinon
# une seule source est réellement à l'œuvre et il est trompeur de citer NISR).
libelle_sources_pop <- sprintf("WorldPop 2020 (%s %%)",
                                sub("\\.", ",", sprintf("%.1f", pct_worldpop_pop)))
if (pct_nisr_pop > 0) {
  libelle_sources_pop <- paste0(
    libelle_sources_pop, " / NISR 2022 (",
    sub("\\.", ",", sprintf("%.1f", pct_nisr_pop)), " %)"
  )
}

carte_population <- fond_carte() +

  tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#DDDDDD", lwd = 0.4) +

  # Contours des cellules de Voronoï : chaque zone d'entrepôt correspond à une
  # cellule dont les frontières délimitent l'espace rattaché à cet entrepôt.
  tm_shape(zones_voronoi) +
  tm_borders(col = "#999999", lwd = 0.7, lty = "dashed") +

  tm_shape(entrepots_pop_sf) +
  tm_dots(
    fill        = "population_zone_k",
    fill.scale  = tm_scale_intervals(
      style  = "quantile",
      n      = 5,
      values = "brewer.yl_or_rd"
    ),
    fill.legend = tm_legend(title = "Population\n(milliers d'habitants)"),
    size        = 0.5
  ) +

  tm_title(paste0("Distribution démographique des zones d'entrepôt\nSources : ",
                  libelle_sources_pop)) +
  tm_credits(
    note_lecture(sprintf(
      "la zone la plus peuplée, %s, compte %s milliers d'habitants.",
      str_remove(entrepots_pop_sf$warehouse_name[which.max(entrepots_pop_sf$population_zone_k)], " - .*"),
      format(max(entrepots_pop_sf$population_zone_k), big.mark = " ")
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
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

# ── Graphique : population par zone et par type ───────────────────────────────
g_pop <- diag_population %>%
  arrange(desc(population_zone)) %>%
  mutate(
    Zone_court = str_trunc(str_remove(nom_zone, " - .*"), 25),
    Zone_court = make.unique(Zone_court, sep = " #"),   # rend les labels uniques
    Zone_court = factor(Zone_court, levels = rev(Zone_court)),
    # type simplifié de la zone ("Frontière" vs "Ville"), pour la légende
    # "Type" ci-dessous (00_parametres.R).
    type            = type_simplifie(type_zone)
  ) %>%
  ggplot(aes(x = Zone_court, y = population_zone / 1000, fill = type)) +
  geom_col(width = 0.75) +
  coord_flip() +
  scale_fill_manual(values = PALETTE_TYPE, name = "Type") +
  scale_y_continuous(labels = scales::label_number(suffix = " k")) +
  labs(
    title    = "Population par zone d'entrepôt",
    x = NULL,
    y = "Population (milliers d'habitants)"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title = element_text(face = "bold"),
    axis.text.y = element_text(size = 7),
    legend.position = "right"
  )

# Hauteur adaptée au nombre de zones : chaque zone a besoin d'environ 0,11
# pouce pour que son étiquette reste lisible sans chevaucher ses voisines
# (avec ~91 zones, un format fixe de 8,6 pouces les fait toutes se chevaucher).
hauteur_g_pop <- max(8.6, 0.11 * nrow(diag_population))

ggsave(
  file.path(DIR_CARTES, "graphique_population_zones.png"),
  g_pop, width = 12, height = hauteur_g_pop, dpi = 300
)
cat("  ✓ graphique_population_zones.png\n\n")

cat("── Visualisations RWI ────────────────────────────────────────────────\n")

# ── Préparation de la couche sf pour les entrepôts enrichis RWI ───────────────
# type : type simplifié de la zone ("Frontière" vs "Ville"), pour la légende
# "Type" des cartes ci-dessous (00_parametres.R).
entrepots_rwi_sf <- reseau %>%
  activate("nodes") %>%
  filter(is_warehouse, !is.na(p_rwi)) %>%
  st_as_sf() %>%
  mutate(
    classe_rwi    = case_when(
      p_rwi >= 0.75 ~ "Très riche",
      p_rwi >= 0.50 ~ "Riche",
      p_rwi >= 0.25 ~ "Pauvre",
      TRUE          ~ "Très pauvre"
    ),
    classe_rwi = factor(
      classe_rwi,
      levels = c("Très pauvre", "Pauvre", "Riche", "Très riche")
    ),
    type = type_simplifie(warehouse_type)
  )

# ── Carte : score p_rwi sur le réseau ─────────────────────────────────────────
# Le dégradé de couleur va du bleu foncé (zones pauvres) au rouge foncé (zones
# riches), ce qui est la convention cartographique habituelle pour les indices
# de richesse.
PALETTE_RWI <- c(
  "#08519C",   # Bleu foncé  — très pauvre (p_rwi < 0.25)
  "#6BAED6",   # Bleu clair  — pauvre      (p_rwi 0.25–0.50)
  "#FD8D3C",   # Orange      — riche       (p_rwi 0.50–0.75)
  "#A50026"    # Rouge foncé — très riche  (p_rwi > 0.75)
)

carte_rwi <- fond_carte() +

  tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
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
    size        = 0.5
  ) +
  
  tm_title(paste0(
    "Richesse relative des zones d'entrepôt\n",
    "Relative Wealth Index — Meta / CIESIN ",
    "(moyenne par cellule de Voronoï, pondérée population)"
  )) +
  tm_credits(
    note_lecture(sprintf(
      "la zone la plus riche, %s, a un score de richesse relative de %.2f sur 1.",
      str_remove(entrepots_rwi_sf$warehouse_name[which.max(entrepots_rwi_sf$p_rwi)], " - .*"),
      max(entrepots_rwi_sf$p_rwi)
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_rwi,
  file.path(DIR_CARTES, "carte_rwi_zones.png"),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ carte_rwi_zones.png\n")

# ── Carte : raster RWI (vue d'ensemble des données brutes) ───────────────────
# Cette carte montre les données RWI pour tout le pays (pas seulement les
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
      size        = 0.07,    # Légèrement agrandi vs 0.05 pour mieux distinguer les
                              # couleurs, sans chevauchement (beaucoup de points, ~1700 cellules)
      fill_alpha  = 0.7
    ) +
    
    # Superposition des entrepôts pour le repérage
    tm_shape(entrepots_rwi_sf) +
    tm_dots(
      fill        = "type",
      fill.scale  = tm_scale(values = PALETTE_TYPE),
      fill.legend = tm_legend(title = "Type"),
      size        = 0.5,
      col         = "white",
      lwd         = 1
    ) +
    
    tm_title(paste0("Données RWI brutes — ", NOM_PAYS)) +
    tm_credits(
      note_lecture(sprintf(
        "la grille RWI brute compte %s cellules d'environ 2,4 km² sur l'ensemble du pays.",
        format(nrow(rwi_sf), big.mark = " ")
      )),
      position = tm_pos_out("center", "bottom", "left", "top"),
      size     = 0.65
    ) +
    tm_layout(legend.outside = TRUE, frame = TRUE) +
    tm_scalebar(position = c("left", "bottom")) +
    tm_compass(position  = c("right", "top"))

  tmap_save(
    carte_rwi_raster,
    file.path(DIR_CARTES, "carte_rwi_brut.png"),
    width = 3000, height = 2400, dpi = 300
  )
  cat("  ✓ carte_rwi_brut.png\n")
}

# ── Graphique : corrélation RWI × population ──────────────────────────────────
# Ce graphique met en relation les deux enrichissements (IV.4 et IV.5) pour
# vérifier leur cohérence : on s'attend à ce que les zones à forte population
# (Kigali, Musanze…) aient aussi des scores RWI élevés — mais pas toujours,
# car les zones frontalières peuvent avoir une population élevée et un RWI faible.
if ("population_zone" %in% names(entreposages_fictifs)) {
  
  df_rwi_pop_v <- diag_rwi %>%
    left_join(
      entreposages_fictifs %>%
        select(nom, population_zone),
      by = c("nom_zone" = "nom")
    ) %>%
    filter(!is.na(population_zone), population_zone > 0) %>%
    mutate(
      Zone_court = str_trunc(str_remove(nom_zone, " - .*"), 22),
      pop_log    = log10(population_zone),
      # type simplifié de la zone ("Frontière" vs "Ville"), pour la légende
      # "Type" ci-dessous (00_parametres.R).
      type       = type_simplifie(type_zone)
    )
  zone_rwi_pop_exemple_v <- df_rwi_pop_v %>% arrange(desc(population_zone)) %>% slice(1)

  g_rwi_pop <- df_rwi_pop_v %>%
    ggplot(aes(x = pop_log, y = p_rwi,
               color = type, label = Zone_court)) +
    
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
    
    scale_color_manual(values = PALETTE_TYPE, name = "Type") +
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
      ),
      caption = note_lecture(sprintf(
        "la zone la plus peuplée, %s (%s habitants), a un score de richesse relative de %.2f.",
        zone_rwi_pop_exemple_v$Zone_court,
        format(round(zone_rwi_pop_exemple_v$population_zone), big.mark = " "),
        zone_rwi_pop_exemple_v$p_rwi
      ), largeur_car = 144)
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title    = element_text(face = "bold"),
      plot.subtitle = element_text(color = "#555555", size = 10),
      legend.position = "right"
    ) +
    THEME_NOTE_LECTURE +
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
    g_rwi_pop, width = 12, height = 7.8, dpi = 300
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
  
  reseau_tmp <- reseau %>%
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
    tm_shape(entreposages_sf) + tm_dots(fill="black", size=0.5) +
    tm_title(paste0("Coûts de Transport — ", nom_veh, "\nCoût généralisé par tonne-kilomètre transportée")) +
    tm_credits(
      note_lecture(sprintf(
        "la moitié des tronçons ont un coût de transport en %s inférieur à %.1f RWF par tonne-kilomètre.",
        nom_veh, median(couts_veh$cost_per_tkm, na.rm = TRUE)
      )),
      position = tm_pos_out("center", "bottom", "left", "top"),
      size     = 0.65
    ) +
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
  
  reseau_tmp <- reseau %>%
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
    tm_shape(entreposages_sf) + tm_dots(fill="black", size=0.5) +
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
  reseau_ratio <- reseau %>%
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
    tm_credits(
      note_lecture(sprintf(
        "sur la moitié des tronçons, le camion lourd coûte moins de %.2f fois le coût de la camionnette par tonne-km.",
        median(ratio_df$ratio_lourd_vs_legere, na.rm = TRUE)
      )),
      position = tm_pos_out("center", "bottom", "left", "top"),
      size     = 0.65
    ) +
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
  reseau_ratio_moyen <- reseau %>%
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
    tm_credits(
      note_lecture(sprintf(
        "sur la moitié des tronçons, le camion moyen coûte moins de %.2f fois le coût de la camionnette par tonne-km.",
        median(ratio_moyen_df$ratio_moyen_vs_camionnette, na.rm = TRUE)
      )),
      position = tm_pos_out("center", "bottom", "left", "top"),
      size     = 0.65
    ) +
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
# La part de chaque catégorie dans le linéaire total est calculée puis insérée
# directement dans le libellé de la légende (ex. "plat (44,9 %)"), pour ne pas
# dépendre d'un texte de lecture séparé.
km_par_pente <- reseau %>% activate("edges") %>% as_tibble() %>%
  group_by(slope_category) %>%
  summarise(km = sum(length_km, na.rm = TRUE), .groups = "drop") %>%
  mutate(pct = km / sum(km) * 100)

# Libellés français (avec accents) des codes internes de PALETTE_PENTE.
LIBELLES_PENTE <- c(plat = "plat", legere = "légère", moderee = "modérée", forte = "forte")

# Libellé "catégorie (xx,x %)" par code de pente, virgule décimale française.
labels_pente_pct <- setNames(
  sprintf("%s (%s %%)",
          LIBELLES_PENTE[km_par_pente$slope_category],
          sub("\\.", ",", sprintf("%.1f", km_par_pente$pct))),
  km_par_pente$slope_category
)

# Colonne recodée avec les libellés enrichis, ordonnée du plus plat au plus
# raide (ordre de PALETTE_PENTE) plutôt que l'ordre alphabétique par défaut.
reseau_pentes <- reseau %>%
  activate("edges") %>%
  mutate(slope_category_pct = factor(
    labels_pente_pct[slope_category],
    levels = labels_pente_pct[names(PALETTE_PENTE)]
  ))

palette_pente_pct <- setNames(PALETTE_PENTE, labels_pente_pct[names(PALETTE_PENTE)])

carte_pentes <- fond_carte() +
  tm_shape(reseau_pentes %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col="slope_category_pct",
           col.scale = tm_scale(values = palette_pente_pct),
           col.legend=tm_legend(title="Catégorie de pente"), lwd=1.5) +
  tm_title("Pentes du Réseau Routier") +
  tm_layout(legend.outside=TRUE, frame=TRUE) +
  tm_scalebar(position=c("left","bottom")) +
  tm_compass(position=c("right","top"))

tmap_save(carte_pentes, file.path(DIR_CARTES,"carte_pentes.png"),
          width=3000, height=2400, dpi=300)
cat("  ✓ carte_pentes.png\n")

if (FALSE) {
  tmap_mode("view")
  print(carte_pentes)
  tmap_mode("plot")
}

# ── Carte : aléa inondation GloFAS (contexte physique brut) ──────────────────
# Contrairement à la carte de vulnérabilité du 05 (qui montre les CONSÉQUENCES
# d'une rupture de route), cette carte montre directement l'ALÉA lui-même : la
# hauteur d'eau simulée par GloFAS pour la période de retour retenue
# (GLOFAS_PERIODE_RETOUR, 00_parametres.R), avant toute intersection avec le
# réseau. Utilise le même raster que le Mode C du script 05_vulnerabilite.R.
if (UTILISER_MODE_RASTER && file.exists(CHEMIN_RASTER_RISQUE)) {

  cat("  Génération de la carte de l'aléa inondation GloFAS...\n")

  # Reprojection dans le CRS métrique commun à toutes les cartes (UTM 35S),
  # même traitement que pour la zone inondée affichée dans viz_vulnerabilite.R.
  raster_alea_brut <- terra::rast(CHEMIN_RASTER_RISQUE) %>%
    terra::project("EPSG:32735", method = "bilinear")
  names(raster_alea_brut) <- "hauteur_eau_m"

  # Statistique de cadrage calculée sur le raster BRUT (avant de mettre les
  # cellules à sec à NA ci-dessous), pour que le dénominateur reste le nombre
  # total de pixels du pays et non les seuls pixels inondés.
  valeurs_alea  <- terra::values(raster_alea_brut)
  n_pixels_tot  <- sum(!is.na(valeurs_alea))
  pct_expose    <- 100 * sum(valeurs_alea > SEUIL_RISQUE_RASTER, na.rm = TRUE) / n_pixels_tot

  # Les cellules à sec (hauteur d'eau = 0, l'immense majorité du territoire)
  # sont mises à NA pour rester transparentes : seule l'eau inondante ressort,
  # le fond de carte reste visible partout ailleurs.
  raster_alea <- raster_alea_brut
  raster_alea[raster_alea == 0] <- NA

  # Classes de hauteur d'eau ; la borne à 0,5 m reprend SEUIL_RISQUE_RASTER,
  # le seuil opérationnel utilisé par le Mode C du 05 pour couper une route.
  # Palette violette (et non bleue) pour ne pas confondre l'aléa simulé avec
  # les lacs et rivières, déjà représentés en bleu par fond_carte().
  BORNES_ALEA  <- c(0, 0.5, 1, 2, 5,
                     ceiling(max(terra::values(raster_alea), na.rm = TRUE)))
  PALETTE_ALEA <- c("#F2F0F7", "#CBC9E2", "#9E9AC8", "#756BB1", "#54278F")

  carte_alea_glofas <- fond_carte() +

    # Réseau en gris très clair, pour situer l'aléa par rapport aux routes
    tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
    tm_lines(col = "#CCCCCC", lwd = 0.3) +

    tm_shape(raster_alea) +
    tm_raster(
      col        = "hauteur_eau_m",
      col.scale  = tm_scale_intervals(style = "fixed", breaks = BORNES_ALEA,
                                       values = PALETTE_ALEA),
      col.legend = tm_legend(title = "Hauteur d'eau\nsimulée (m)")
    ) +

    tm_title(paste0(
      "Aléa inondation — crue de période de retour ", GLOFAS_PERIODE_RETOUR, " ans (GloFAS)\n",
      "Hauteur d'eau simulée, hors eaux permanentes | JRC / Copernicus EMS"
    )) +
    tm_credits(
      note_lecture(sprintf(
        "%.1f %% du territoire est exposé à une hauteur d'eau simulée supérieure au seuil opérationnel de %.1f m retenu pour couper une route (Mode C, 05_vulnerabilite.R).",
        pct_expose, SEUIL_RISQUE_RASTER
      )),
      position = tm_pos_out("center", "bottom", "left", "top"),
      size     = 0.65
    ) +
    tm_layout(legend.outside = TRUE, frame = TRUE) +
    tm_scalebar(position = c("left", "bottom")) +
    tm_compass(position  = c("right", "top"))

  tmap_save(
    carte_alea_glofas,
    file.path(DIR_CARTES, sprintf("carte_alea_glofas_rp%03d.png", GLOFAS_PERIODE_RETOUR)),
    width = 3000, height = 2400, dpi = 300
  )
  cat(sprintf("  ✓ carte_alea_glofas_rp%03d.png\n", GLOFAS_PERIODE_RETOUR))

  rm(raster_alea_brut, raster_alea, valeurs_alea, n_pixels_tot, pct_expose,
     BORNES_ALEA, PALETTE_ALEA, carte_alea_glofas)
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
  tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
  tm_lines(
    col        = "co2_kg_par_tkm",
    col.scale  = tm_scale_intervals(style = "quantile", n = 5,
                                    values = PALETTE_EMISSIONS),
    col.legend = tm_legend(title = "CO₂\n(kg / tonne-km)"),
    lwd        = 1.5
  ) +
  tm_shape(entreposages_sf) + tm_dots(fill = "black", size = 0.5) +
  tm_title(paste0("Intensité carbone du réseau — ", VEHICULES_IDS$nom[
    VEHICULES_IDS$vehicule_id == VEHICULE_REFERENCE
  ])) +
  tm_credits(
    note_lecture(sprintf(
      "sur la moitié des tronçons, transporter une tonne sur un km émet moins de %.3f kg de CO₂.",
      median(reseau %>% activate("edges") %>% pull(co2_kg_par_tkm), na.rm = TRUE)
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
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
  tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
  tm_lines(
    col        = "nox_g_par_tkm",
    col.scale  = tm_scale_intervals(style = "quantile", n = 5,
                                    values = PALETTE_EMISSIONS),
    col.legend = tm_legend(title = "NOx\n(g / tonne-km)"),
    lwd        = 1.5
  ) +
  tm_shape(entreposages_sf) + tm_dots(fill = "black", size = 0.5) +
  tm_title(paste0("Intensité NOx du réseau (pollution locale) — ", VEHICULES_IDS$nom[
    VEHICULES_IDS$vehicule_id == VEHICULE_REFERENCE
  ])) +
  tm_credits(
    note_lecture(sprintf(
      "sur la moitié des tronçons, transporter une tonne sur un km émet moins de %.3f g de NOx.",
      median(reseau %>% activate("edges") %>% pull(nox_g_par_tkm), na.rm = TRUE)
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
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
    fill     = "Polluant",
    caption  = note_lecture(sprintf(
      "sur le réseau complet, le %s émet %s tonnes de CO₂, le plus haut total des trois véhicules.",
      emissions_par_vehicule$vehicule_nom[which.max(emissions_par_vehicule$co2_total_t)],
      format(emissions_par_vehicule$co2_total_t[which.max(emissions_par_vehicule$co2_total_t)], big.mark = " ")
    ), largeur_car = 120)
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title    = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#666666"),
    legend.position = "top"
  ) +
  THEME_NOTE_LECTURE

ggsave(file.path(DIR_CARTES, "graphique_emissions_par_vehicule.png"),
       g_emissions, width = 10, height = 6.8, dpi = 300)
cat("  ✓ graphique_emissions_par_vehicule.png\n\n")

# ── Carte des districts administratifs ────────────────────────────────────────
# Carte de repère (sans donnée thématique) montrant les limites et les noms
# des districts administratifs, pour situer géographiquement les autres cartes
# du réseau. Utilise les frontières GADM niveau 2 (province = niveau 1,
# district = niveau 2), la même source que la jointure population NISR ×
# district de 01_reseau.R (IV.4.B). L'objet pays_districts_gadm n'est pas
# conservé dans les .rds inter-scripts (libéré en fin de 01_reseau.R pour
# limiter l'empreinte mémoire), donc on le retélécharge ici.
cat("── Carte des districts administratifs ──────────────────────────────\n")

districts_gadm <- tryCatch({
  geodata::gadm(country = "RWA", level = 2, path = tempdir()) %>%
    st_as_sf() %>%
    st_transform(crs = 32735) %>%
    select(district = NAME_2, province = NAME_1, geometry)
}, error = function(e) {
  cat("  ⚠ Téléchargement GADM échoué :", conditionMessage(e),
      "— carte des districts ignorée.\n\n")
  NULL
})

if (!is.null(districts_gadm)) {

  # ── Postes-frontière et pays voisin associé ──────────────────────────────
  # entreposages_fictifs (persisté par 01_reseau.R) contient une ligne par
  # nœud-entrepôt final, dont les postes-frontière (type == "frontiere",
  # définis dans entreposages_manuels, 00_parametres.R), avec leur pays voisin
  # dans la colonne "pays". nom_court retire la répétition du pays entre
  # parenthèses (déjà porté par "pays") pour une étiquette plus courte.
  postes_frontiere_sf <- entreposages_fictifs %>%
    filter(type == "frontiere") %>%
    mutate(
      nom_court = str_trim(str_remove(nom, "\\s*\\(.*\\)$")),
      etiquette = paste0(nom_court, "\n(", pays, ")")
    ) %>%
    st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
    st_transform(crs = 32735)

  # Remplissage par province (couleur) pour repérer visuellement les groupes
  # de districts, et étiquette du nom de district au centroïde de chaque
  # polygone (tm_text place l'étiquette au centroïde par défaut sur des
  # polygones). bgcol ajoute un halo blanc semi-transparent derrière le texte
  # pour qu'il reste lisible par-dessus les couleurs de remplissage. Les
  # postes-frontière (losanges rouges) et leur pays voisin sont ajoutés
  # au-dessus, en dernier, pour rester visibles par-dessus les polygones.
  carte_districts <- fond_carte() +
    tm_shape(districts_gadm) +
    tm_polygons(
      fill        = "province",
      fill.scale  = tm_scale(values = "brewer.set3"),
      fill.legend = tm_legend(title = "Province"),
      fill_alpha  = 0.55,
      col         = "#555555",
      lwd         = 0.8
    ) +
    tm_text(
      text        = "district",
      size        = 0.55,
      col         = "#222222",
      fontface    = "bold",
      bgcol       = "white",
      bgcol_alpha = 0.6
    ) +
    tm_shape(postes_frontiere_sf) +
    tm_symbols(
      shape        = 23,
      size         = 0.5,
      fill         = "#CC0000",
      col          = "black",
      lwd          = 0.6,
      fill.legend  = tm_legend(show = FALSE)
    ) +
    tm_text(
      text        = "etiquette",
      size        = 0.5,
      col         = "#CC0000",
      fontface    = "bold",
      ymod        = -1.1,
      bgcol       = "white",
      bgcol_alpha = 0.7
    ) +
    tm_title(paste0("Districts administratifs — ", NOM_PAYS)) +
    tm_credits(
      note_lecture(sprintf(
        "le pays compte %d districts répartis en %d provinces (frontières GADM) ; les %d losanges rouges sont les postes-frontière, avec le pays voisin associé.",
        nrow(districts_gadm), n_distinct(districts_gadm$province),
        nrow(postes_frontiere_sf)
      )),
      position = tm_pos_out("center", "bottom", "left", "top"),
      size     = 0.65
    ) +
    tm_layout(legend.outside = TRUE, frame = TRUE) +
    tm_scalebar(position = c("left", "bottom")) +
    tm_compass(position  = c("right", "top"))

  tmap_save(
    carte_districts,
    file.path(DIR_CARTES, "carte_districts.png"),
    width = 3000, height = 2400, dpi = 300
  )
  cat("  ✓ carte_districts.png\n\n")
}


