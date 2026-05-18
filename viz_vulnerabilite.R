################################################################################
# viz_vulnerabilite.R
# RÔLE : Cartes de vulnérabilité (réseau dégradé, criticité, détours,
#        report modal) et graphiques de distribution des surcoûts.
#
# PEUT TOURNER SANS RELANCER LES SCRIPTS PRÉCÉDENTS si les .rds sont à jour.
#
# RELANCER 04_vulnerabilite.R avant ce script si :
#   → le scénario a changé (NOM_SCENARIO, OSM_IDS_PERTURBES_MANUEL,
#     CENTRE_PERTURBATION_*, RAYON_PERTURBATION_M, SEUIL_RISQUE_RASTER)
#   → DUREE_JOURS ou TYPE_EVENEMENT ont changé
#   → N_TOP_ARETES_CRITIQUES ou SEUIL_PAIRES_CRITICITE ont changé
#   → les flux de fret (persist_flux_fret.rds) ont changé
#     → dans ce cas relancer aussi 03_transport.R avant 04_vulnerabilite.R
#
# RELANCER 02_couts.R + 03_transport.R + 04_vulnerabilite.R si :
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
reseau_rwanda    <- .fret$reseau_rwanda   # version avec volumes fret
volumes_par_zone <- .fret$volumes_par_zone
rm(.fret)

# ── Reconstruction d'aretes_reseau_sf ─────────────────────────────────────────
# Couche sf de toutes les arêtes du réseau, avec un index entier arete_idx.
# Nécessaire pour : filtrer les arêtes perturbées/critiques, calculer les volumes
# par type de route, et construire les couches de détour.
# On repart de reseau_rwanda (déjà chargé) plutôt que de le sauvegarder dans
# PERSIST_VULNERAB (objet géométrique lourd, ~50 Mo).
aretes_reseau_sf <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  mutate(arete_idx = row_number())

# ── Reconstruction de coords_zones_sf ─────────────────────────────────────────
# Points sf des zones d'entrepôt, utilisés sur la Carte D (itinéraires de détour).
# Version simplifiée sans taille_point — la Carte D n'en a pas besoin.
coords_zones_sf <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  st_as_sf()

.vuln <- readRDS(PERSIST_VULNERAB)
list2env(.vuln, envir = .GlobalEnv)
rm(.vuln)

# ── Reconstruction d'od_ref_map ───────────────────────────────────────────────
# Table de lookup : clé "i_j" → coût de référence (avant perturbation).
# Reconstruit depuis od_compare (disponible via list2env) pour éviter de
# recharger od_cache.rds 
od_ref_map <- setNames(
  od_compare$cout_usd,
  paste0(od_compare$id_origine, "_", od_compare$id_destination)
)

if (!exists("surcout_moyen_detour")) {
  surcout_moyen_detour <- surcout_pondere_arete / pmax(volume_detourne_arete, 1)
}

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
  file.path(DIR_CARTES, paste0("carte_reseau_degrade_", NOM_SCENARIO, ".png")),
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
  file.path(DIR_CARTES, paste0("carte_criticite_aretes_", NOM_SCENARIO, ".png")),
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
  file.path(DIR_CARTES, paste0("carte_vulnerabilite_zones_", NOM_SCENARIO, ".png")),
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
  file.path(DIR_CARTES, paste0("graphique_surcouts_", NOM_SCENARIO, ".png")),
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
  file.path(DIR_CARTES, paste0("graphique_report_type_route_", NOM_SCENARIO, ".png")),
  g_report,
  width = 11,
  height = 6,
  dpi = 300
)
cat("  ✓ graphique_report_type_route_", NOM_SCENARIO, ".png\n\n", sep = "")

