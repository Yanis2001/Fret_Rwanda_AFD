################################################################################
# viz_ario.R
# RÔLE : Graphiques ARIO-inventory (trajectoires de production par secteur et
#        province, pertes indirectes, heatmap de la matrice Z).
#
# PEUT TOURNER SANS RELANCER LES SCRIPTS PRÉCÉDENTS si les .rds sont à jour.
#
# RELANCER 05_ario.R avant ce script si :
#   → les paramètres ARIO ont changé (ARIO_PSI, ARIO_ALPHA_MAX,
#     ARIO_TAU_ALPHA, ARIO_INV_DUREE_JOURS, ARIO_HORIZON_JOURS)
#   → le scénario de perturbation a changé (relancer 04_vulnerabilite.R d'abord)
#   → les flux gravitaires ont changé (relancer 03_transport.R d'abord)
#
# RELANCER 01 → 02 → 03 → 04 → 05 en séquence complète si :
#   → le réseau physique a changé (nouveau PBF)
#
# FICHIERS LUS : persist_geodata.rds, persist_entreposages.rds,
#                persist_vulnerabilite.rds, persist_ario.rds
################################################################################

source("00_parametres.R")
fond_carte <- readRDS(file.path(DIR_CARTES, "persist_fond_carte.rds"))

.ario <- readRDS(PERSIST_ARIO)
list2env(.ario, envir = .GlobalEnv)
rm(.ario)

.vuln <- readRDS(PERSIST_VULNERAB)
aretes_perturbees_sf <- .vuln$aretes_perturbees_sf
rm(.vuln)

# Réseau uniquement pour la géométrie des arêtes (fond de la carte choroplèthe)
.res  <- readRDS(PERSIST_RESEAU_COUTS)
reseau_rwanda <- .res$reseau_rwanda
rm(.res)

# ==============================================================================
# X.6 : Visualisations et exports
# ==============================================================================

cat("── X.6 : Visualisations et exports ──────────────────────────────────\n\n")

# ── Graphique 1 : trajectoire de la production par secteur ────────────────────
# Production journalière agrégée par secteur, en écart % par rapport à
# l'équilibre pré-choc. Une ligne verticale rouge marque la fin de la
# perturbation (rétablissement des routes).
traj_secteurs <- trajectoire_secteurs_df %>%   # chargé via list2env(.ario, ...)
  pivot_longer(-jour, names_to = "Secteur", values_to = "Production_musd") %>%
  group_by(Secteur) %>%
  mutate(
    Production_ref = prod_ref_secteur[match(Secteur, SECTEURS)],
    Production_pct = (Production_musd / Production_ref - 1) * 100
  ) %>%
  ungroup()

g_traj_secteurs <- ggplot(traj_secteurs,
                          aes(x = jour, y = Production_pct, color = Secteur)) +
  geom_line(linewidth = 0.9) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "#666666") +
  geom_vline(xintercept = DUREE_JOURS, linetype = "dotted",
             color = "#CC0000", linewidth = 0.8) +
  annotate("text", x = DUREE_JOURS + 1, 
           y = max(traj_secteurs$Production_pct, na.rm = TRUE) * 0.95,
           label = "Fin perturbation", hjust = 0, color = "#CC0000", size = 3) +
  scale_color_manual(values = PALETTE_SECTEURS) +
  scale_y_continuous(labels = scales::percent_format(scale = 1, accuracy = 0.1)) +
  labs(
    title    = "ARIO-inventory — Trajectoire de la production par secteur",
    subtitle = paste0("Scénario : ", NOM_SCENARIO,
                      " — Écart en % par rapport à l'équilibre pré-choc"),
    x        = "Jour de simulation",
    y        = "Écart de production",
    color    = "Secteur"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title    = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#666666"),
    legend.position = "right"
  )

ggsave(
  file.path(DIR_CARTES,
            paste0("ario_trajectoire_secteurs_", NOM_SCENARIO, ".png")),
  g_traj_secteurs, width = 12, height = 7, dpi = 300
)
cat("  ✓ ario_trajectoire_secteurs_", NOM_SCENARIO, ".png\n", sep = "")


# ── Graphique 2 : trajectoire de la production par province ───────────────────
traj_provinces <- trajectoire_provinces_df %>%   # chargé via list2env(.ario, ...)
  pivot_longer(-jour, names_to = "Province", values_to = "Production_musd") %>%
  group_by(Province) %>%
  mutate(
    Production_ref = prod_ref_province[match(Province, noms_provinces)],
    Production_pct = (Production_musd / Production_ref - 1) * 100
  ) %>%
  ungroup()

g_traj_provinces <- ggplot(traj_provinces,
                           aes(x = jour, y = Production_pct, color = Province)) +
  geom_line(linewidth = 1) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "#666666") +
  geom_vline(xintercept = DUREE_JOURS, linetype = "dotted",
             color = "#CC0000", linewidth = 0.8) +
  scale_color_brewer(palette = "Dark2") +
  scale_y_continuous(labels = scales::percent_format(scale = 1, accuracy = 0.1)) +
  labs(
    title    = "ARIO-inventory — Trajectoire de la production par province",
    subtitle = paste0("Scénario : ", NOM_SCENARIO),
    x        = "Jour de simulation",
    y        = "Écart de production",
    color    = "Province"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "right"
  )

ggsave(
  file.path(DIR_CARTES,
            paste0("ario_trajectoire_provinces_", NOM_SCENARIO, ".png")),
  g_traj_provinces, width = 12, height = 6, dpi = 300
)
cat("  ✓ ario_trajectoire_provinces_", NOM_SCENARIO, ".png\n", sep = "")


# ── Graphique 3 : pertes indirectes par secteur (barres) ──────────────────────
g_pertes_secteurs <- tableau_pertes_secteur %>%
  ggplot(aes(x = reorder(Secteur, Perte_indirecte_musd),
             y = Perte_indirecte_musd,
             fill = Secteur)) +
  geom_col(width = 0.7, show.legend = FALSE) +
  geom_text(aes(label = paste0(round(Perte_pct, 1), "%")),
            hjust = -0.1, size = 3.5) +
  coord_flip(clip = "off") +
  scale_fill_manual(values = PALETTE_SECTEURS) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.15))) +
  labs(
    title    = "Pertes indirectes par secteur (ARIO-inventory)",
    subtitle = paste0("Scénario : ", NOM_SCENARIO),
    x        = NULL,
    y        = "Perte cumulée sur l'horizon (M USD)"
  ) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"))

ggsave(
  file.path(DIR_CARTES,
            paste0("ario_pertes_secteurs_", NOM_SCENARIO, ".png")),
  g_pertes_secteurs, width = 11, height = 6, dpi = 300
)
cat("  ✓ ario_pertes_secteurs_", NOM_SCENARIO, ".png\n", sep = "")


# ── Carte : pertes indirectes par province (choropleth) ───────────────────────
# Avec seulement 5 provinces, une carte choroplèthe (polygones colorés par
# valeur) est bien plus lisible qu'une carte par points.
provinces_pertes_sf <- provinces_ario %>%
  left_join(
    tableau_pertes_province %>%
      select(Province, Perte_indirecte_musd, Perte_pct, Choc_capacite_pct),
    by = c("nom_province" = "Province")
  ) %>%
  mutate(
    Perte_indirecte_musd = replace_na(Perte_indirecte_musd, 0),
    Perte_pct            = replace_na(Perte_pct, 0)
  )

carte_pertes_ario <- fond_carte() +
  
  # Provinces colorées par % de perte indirecte
  tm_shape(provinces_pertes_sf) +
  tm_polygons(
    fill       = "Perte_pct",
    fill.scale = tm_scale_intervals(
      style  = "fixed",
      breaks = c(0, 0.5, 2, 5, 10, Inf),
      values = c("#F7F7F7", "#FDD49E", "#FDBB84", "#E34A33", "#7F0000")
    ),
    fill.legend = tm_legend(title = "Perte indirecte\n(% production)"),
    col = "#444444",
    lwd = 0.8,
    fill_alpha = 0.85
  ) +
  
  # Réseau de base par-dessus pour le contexte
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#888888", lwd = 0.3) +
  
  # Arêtes perturbées du scénario (même couleur que Cartes A et C de viz_vulnerabilite.R)
  tm_shape(aretes_perturbees_sf) +
  tm_lines(col = "#CC0000", lwd = 2.5,
           col.legend = tm_legend(show = FALSE)) +
  
  tm_title(paste0("Pertes indirectes par province — ARIO-inventory\n",
                  NOM_SCENARIO, " (horizon : ", ARIO_HORIZON_JOURS, " jours)")) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_pertes_ario,
  file.path(DIR_CARTES,
            paste0("carte_ario_pertes_provinces_", NOM_SCENARIO, ".png")),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ carte_ario_pertes_provinces_", NOM_SCENARIO, ".png\n", sep = "")


# ── Heatmap : flux interindustriels inter-provinces (matrice Z agrégée) ───────
# Avec 40 industries, on peut visualiser la matrice complète sous forme
# de heatmap. C'est utile pour comprendre la structure des dépendances.
Z_df <- Z %>%
  as.data.frame() %>%
  rownames_to_column("Industrie_fournisseur") %>%
  pivot_longer(-Industrie_fournisseur,
               names_to = "Industrie_cliente",
               values_to = "Flux") %>%
  mutate(
    Flux_log = ifelse(Flux > 0, log10(Flux + 1), NA),
    # Ordre des industries pour la heatmap (par province puis par secteur)
    Industrie_fournisseur = factor(Industrie_fournisseur,
                                   levels = industries_idx$industrie_id),
    Industrie_cliente     = factor(Industrie_cliente,
                                   levels = industries_idx$industrie_id)
  )

g_heatmap_Z <- ggplot(Z_df, aes(x = Industrie_cliente, y = Industrie_fournisseur,
                                fill = Flux_log)) +
  geom_tile(color = "white", linewidth = 0.2) +
  scale_fill_gradient(low = "#FFF7BC", high = "#7F0000",
                      na.value = "#EEEEEE",
                      name = "log₁₀(flux + 1)\n(M USD/jour)") +
  labs(
    title    = "Matrice des flux interindustriels (Z) — état initial",
    subtitle = "Province × secteur, échelle log",
    x        = "Industrie cliente",
    y        = "Industrie fournisseur"
  ) +
  theme_minimal(base_size = 8) +
  theme(
    axis.text.x   = element_text(angle = 90, hjust = 1, vjust = 0.5, size = 6),
    axis.text.y   = element_text(size = 6),
    plot.title    = element_text(face = "bold", size = 13),
    plot.subtitle = element_text(color = "#666666"),
    panel.grid    = element_blank()
  )

ggsave(
  file.path(DIR_CARTES, "ario_heatmap_matrice_Z.png"),
  g_heatmap_Z, width = 14, height = 12, dpi = 300
)
cat("  ✓ ario_heatmap_matrice_Z.png\n")
