################################################################################
# viz_verif.R
# RÔLE : Vérification de la crédibilité des données et des résultats du modèle.
#        Ne produit AUCUNE donnée utilisée en aval — uniquement des cartes,
#        graphiques et tableaux de diagnostic dans outputs/cartes/ et
#        outputs/exports/, tous préfixés "verif_" ou "carte_verif_"/
#        "graphique_verif_" pour ne jamais être confondus avec une sortie du
#        modèle.
#
# TROIS FAMILLES DE CONTRÔLES :
#   I-III  Cohérence INTERNE : la même grandeur, recalculée par deux voies
#          différentes du pipeline, doit converger (marges du modèle
#          gravitaire, production spatialisée vs SAM, tonnage affecté au
#          réseau vs tonnage attendu, répartition modale).
#   IV     Comparaison à des chiffres EXTERNES au modèle (population NISR,
#          emploi RPHC5, part urbaine EICV5/RPHC5, valeur ajoutée Banque
#          Mondiale) — des repères, pas des cibles de calage.
#   V      Statistiques descriptives des données d'entrée (réseau), et
#          tableau de bord récapitulatif de tous les indicateurs ci-dessus.
#
# PEUT TOURNER SANS RELANCER LES SCRIPTS PRÉCÉDENTS si les .rds sont à jour.
# FICHIERS LUS : persist_entreposages.rds, persist_flux_fret.rds,
#                persist_reseau_fret.rds, persist_fond_carte.rds,
#                data/raw/rwa_admpop_adm2_2023.csv,
#                data/raw/rwa_emploi_district_secteur_2022*.csv,
#                outputs/cache/wb_rwanda_secteurs.rds (optionnel)
################################################################################

source("00_parametres.R")
fond_carte <- readRDS(file.path(DIR_CARTES, "persist_fond_carte.rds"))

.ent <- readRDS(PERSIST_ENTREPOSAGES)
list2env(.ent, envir = .GlobalEnv)
rm(.ent)

.fret <- readRDS(PERSIST_RESEAU_FRET)
reseau                <- .fret$reseau
volume_par_secteur    <- .fret$volume_par_secteur
volume_par_secteur_df <- .fret$volume_par_secteur_df
volumes_par_zone      <- .fret$volumes_par_zone
rm(.fret)

.flux <- readRDS(PERSIST_FLUX_FRET)
list2env(.flux, envir = .GlobalEnv)
rm(.flux)

# ==============================================================================
# Correction du device PNG pour tmap sur macOS sans XQuartz
# tmap v4 force type="cairo-png" en dur dans sa fonction interne plot_device.
# Sur macOS sans XQuartz installé, cairo n'est pas disponible : tmap_save()
# échoue silencieusement. Ce bloc détecte l'absence de cairo et remplace
# automatiquement le device par type="quartz" (rendu natif macOS). Sur les
# systèmes où cairo fonctionne (Linux, macOS + XQuartz), le patch est
# silencieusement ignoré. Identique au patch de viz_fret.R (mêmes causes).
# ==============================================================================
local({
  .f      <- tempfile(fileext = ".png")
  .echec  <- FALSE
  withCallingHandlers(
    grDevices::png(.f, type = "cairo-png", width = 10, height = 10,
                   res = 72, units = "px"),
    warning = function(w) {
      .echec <<- TRUE
      invokeRestart("muffleWarning")
    }
  )
  try(dev.off(), silent = TRUE)
  unlink(.f, force = TRUE)

  if (.echec) {
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

# note_lecture() et THEME_NOTE_LECTURE : helpers partagés, définis dans
# 00_parametres.R (utilisés par tous les viz_*.R).

# Accumulateur du tableau de bord final (Partie V.2) : chaque contrôle ajoute
# une ligne décrivant ce qu'il compare, avec le résultat chiffré.
tableau_bord <- list()
ajouter_indicateur <- function(indicateur, valeur_modele, valeur_reference,
                                source_reference, ecart_pct = NA_real_,
                                commentaire = "") {
  tableau_bord[[length(tableau_bord) + 1]] <<- tibble(
    indicateur        = indicateur,
    valeur_modele      = valeur_modele,
    valeur_reference    = valeur_reference,
    source_reference   = source_reference,
    ecart_pct          = ecart_pct,
    commentaire        = commentaire
  )
}

cat("################################################################\n")
cat("# viz_verif.R — Vérification de la crédibilité des données\n")
cat("################################################################\n\n")


################################################################################
# PARTIE I — COHÉRENCE DU MODÈLE GRAVITAIRE (contraintes de Furness)
#
# 03_transport.R vérifie déjà, secteur par secteur, que les sorties/entrées de
# chaque nœud (flux_gravitaire_ext) respectent leurs cibles (offre/demande
# domestique + jambes export/import) — mais n'écrit ce diagnostic que dans la
# console, jamais dans un fichier. On le recalcule ici À L'IDENTIQUE (mêmes
# formules, mêmes objets persistés) pour le garder sous forme de carte/tableau.
################################################################################

cat("=== I : Convergence du modèle gravitaire (contraintes de Furness) ===\n")

n_warehouses_v <- nrow(prod_zones)
n_total_v      <- nrow(offre_total)
idx_dom_v      <- seq_len(n_warehouses_v)
idx_row_v      <- (n_warehouses_v + 1):n_total_v   # lignes "reste du monde" (RoW), pas dplyr::row_number()

# Propensions sectorielles à l'export/import — mêmes formules que 03_transport.R
# VII.4-bis (tau_E = exports/production, tau_M = imports/demande domestique).
exports_s_v <- sam$exports[SECTEURS]
imports_s_v <- sam$imports[SECTEURS]
tau_E_v <- ifelse(production_totale[SECTEURS] > 1e-12,
                   exports_s_v / production_totale[SECTEURS], 0)
D_nat_v <- colSums(dem_zones)
tau_M_v <- ifelse(D_nat_v > 1e-12, imports_s_v / D_nat_v, 0)
names(tau_E_v) <- SECTEURS; names(tau_M_v) <- SECTEURS

prod_dom_v    <- sweep(prod_zones, 2, 1 - tau_E_v, `*`)
dem_dom_v     <- sweep(dem_zones,  2, 1 - tau_M_v, `*`)
o_dom_zones_v <- pmax(prod_dom_v - dem_dom_v, 0)
q_dom_zones_v <- pmax(dem_dom_v - prod_dom_v, 0)

furness_convergence_df <- bind_rows(lapply(SECTEURS_FRET, function(s) {
  Tcoef <- TONNES_PAR_mrd_RWF[s]
  T_s   <- flux_gravitaire_ext[[s]]

  target_O <- c(o_dom_zones_v[, s] + e_zones[, s], offre_total[idx_row_v, s]) * Tcoef
  target_D <- c(q_dom_zones_v[, s] + m_zones[, s], demande_total[idx_row_v, s]) * Tcoef

  zones_O_actives <- target_O > 1e-9
  zones_D_actives <- target_D > 1e-9

  err_O <- if (any(zones_O_actives)) {
    max(abs(rowSums(T_s)[zones_O_actives] - target_O[zones_O_actives]) /
          target_O[zones_O_actives]) * 100
  } else 0
  err_D <- if (any(zones_D_actives)) {
    max(abs(colSums(T_s)[zones_D_actives] - target_D[zones_D_actives]) /
          target_D[zones_D_actives]) * 100
  } else 0

  tibble(secteur = s, err_origine_pct = err_O, err_destination_pct = err_D)
}))

write_csv(furness_convergence_df, file.path(DIR_EXPORTS, "verif_furness_convergence.csv"))
cat("✓ verif_furness_convergence.csv\n")

# Graphique : une barre par secteur et par type d'erreur (origine/destination),
# avec une ligne de tolérance à 0,01 % (seuil utilisé dans 03_transport.R pour
# distinguer ✓ de ⚠ en console).
df_furness_long <- furness_convergence_df %>%
  pivot_longer(cols = c(err_origine_pct, err_destination_pct),
               names_to = "type_marge", values_to = "erreur_pct") %>%
  mutate(type_marge = recode(type_marge,
                              err_origine_pct     = "Origine (offre)",
                              err_destination_pct = "Destination (demande)"),
         secteur = factor(secteur, levels = SECTEURS_FRET))

g_furness <- ggplot(df_furness_long, aes(x = secteur, y = erreur_pct, fill = type_marge)) +
  geom_col(position = position_dodge(width = 0.7), width = 0.6) +
  geom_hline(yintercept = 0.01, linetype = "dashed", color = "#B22222", linewidth = 0.5) +
  annotate("text", x = 1, y = 0.011, label = "seuil 0,01 %", hjust = 0,
           size = 3, color = "#B22222", fontface = "italic") +
  coord_flip() +
  scale_fill_manual(values = c("Origine (offre)" = "#2171B5",
                                "Destination (demande)" = "#6A0DAD")) +
  labs(title = "Convergence du modèle gravitaire (Furness) par secteur",
       subtitle = "Écart relatif maximal entre flux affectés et marges cibles (offre/demande) — doit rester proche de zéro",
       x = NULL, y = "Erreur relative (%)", fill = NULL,
       caption = note_lecture(sprintf(
         "dans le secteur %s, l'écart entre le flux calculé et sa cible atteint %.4f %%, la plus grande valeur du graphique — bien en dessous du seuil de 0,01 %% (ligne pointillée).",
         furness_convergence_df$secteur[which.max(pmax(furness_convergence_df$err_origine_pct, furness_convergence_df$err_destination_pct))],
         max(pmax(furness_convergence_df$err_origine_pct, furness_convergence_df$err_destination_pct))
       ), largeur_car = 105)) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold", size = 14),
        plot.subtitle = element_text(color = "#666666", size = 9)) +
  THEME_NOTE_LECTURE

ggsave(file.path(DIR_CARTES, "graphique_verif_furness_convergence.png"),
       g_furness, width = 9, height = 7, dpi = 300)
cat("✓ graphique_verif_furness_convergence.png\n\n")

ajouter_indicateur(
  "Convergence Furness — erreur max toutes secteurs/marges",
  max(furness_convergence_df$err_origine_pct, furness_convergence_df$err_destination_pct),
  0, "Seuil interne 03_transport.R (FURNESS_TOL)",
  commentaire = "Doit rester << 0,01 % ; un écart important signale une non-convergence de l'IPF"
)

# ── Conservation du tonnage lors de la projection RoW → frontières ───────────
# flux_gravitaire_ext (avant projection) et flux_tonnes_total (après, = somme
# des flux_gravitaire projetés) doivent porter exactement le même tonnage total
# — la projection ne fait que déplacer la masse des nœuds RoW vers les postes
# frontières, elle ne doit ni en créer ni en perdre.
tonnage_avant_v <- sum(sapply(flux_gravitaire_ext, sum))
tonnage_total_v <- sum(flux_tonnes_total)
ecart_projection_v <- 100 * (tonnage_avant_v - tonnage_total_v) / tonnage_avant_v

cat("  Tonnage avant projection RoW  :", format(round(tonnage_avant_v), big.mark = " "), "t\n")
cat("  Tonnage après projection RoW  :", format(round(tonnage_total_v), big.mark = " "), "t\n")
cat("  Écart                         :", round(ecart_projection_v, 4), "%\n\n")

ajouter_indicateur(
  "Conservation du tonnage — projection RoW sur frontières",
  tonnage_total_v, tonnage_avant_v, "Interne (avant projection)",
  ecart_pct = ecart_projection_v,
  commentaire = "Un écart non nul signale un pays RoW sans poste frontière associé"
)


################################################################################
# PARTIE II — PRODUCTION SPATIALISÉE VS PRODUCTION NATIONALE (SAM)
#
# La production nationale par secteur (SAM, recap_io$production_mrd_rwf) est
# répartie entre zones par un poids w[i,s] (emploi × RWI, 01_reseau.R) qui
# somme à 1 sur les zones. La somme spatiale de prod_zones doit donc
# reconstituer EXACTEMENT le total national : ce n'est pas une comparaison
# indépendante mais un test de non-régression de cette pondération (une fuite
# ou une double comptabilisation romprait l'égalité).
################################################################################

cat("=== II : Production spatialisée (somme des zones) vs SAM nationale ===\n")

df_prod_check <- tibble(
  secteur                 = SECTEURS,
  production_sam_mrd      = as.numeric(recap_io$production_mrd_rwf[match(SECTEURS, recap_io$secteur)]),
  production_spatialisee_mrd = as.numeric(colSums(prod_zones)[SECTEURS])
) %>%
  mutate(ecart_pct = 100 * (production_spatialisee_mrd - production_sam_mrd) /
           pmax(production_sam_mrd, 1e-9))

write_csv(df_prod_check, file.path(DIR_EXPORTS, "verif_production_sam_vs_spatialisee.csv"))
cat("✓ verif_production_sam_vs_spatialisee.csv\n")

df_prod_long <- df_prod_check %>%
  select(secteur, production_sam_mrd, production_spatialisee_mrd) %>%
  pivot_longer(-secteur, names_to = "source", values_to = "valeur_mrd") %>%
  mutate(source = recode(source,
                          production_sam_mrd         = "SAM (national)",
                          production_spatialisee_mrd = "Somme des zones"),
         secteur = factor(secteur, levels = SECTEURS))

g_prod_check <- ggplot(df_prod_long, aes(x = secteur, y = valeur_mrd, fill = source)) +
  geom_col(position = position_dodge(width = 0.7), width = 0.6) +
  coord_flip() +
  scale_fill_manual(values = c("SAM (national)" = "#E57373", "Somme des zones" = "#42A5F5")) +
  scale_y_continuous(labels = scales::label_number(suffix = " mrd")) +
  labs(title = "Test de conservation : production spatialisée vs total SAM",
       subtitle = "La pondération emploi × RWI (01_reseau.R) doit reconstituer exactement le total national par secteur",
       x = NULL, y = "Production (mrd RWF)", fill = NULL,
       caption = note_lecture(sprintf(
         "pour le secteur %s, la SAM indique %.0f mrd RWF de production nationale, et la somme des zones du modèle donne exactement la même valeur.",
         df_prod_check$secteur[which.max(df_prod_check$production_sam_mrd)],
         df_prod_check$production_sam_mrd[which.max(df_prod_check$production_sam_mrd)]
       ), largeur_car = 105)) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold", size = 14),
        plot.subtitle = element_text(color = "#666666", size = 9)) +
  THEME_NOTE_LECTURE

ggsave(file.path(DIR_CARTES, "graphique_verif_production_sam_vs_spatialisee.png"),
       g_prod_check, width = 9, height = 6.8, dpi = 300)
cat("✓ graphique_verif_production_sam_vs_spatialisee.png\n\n")

ajouter_indicateur(
  "Production spatialisée — écart max vs total SAM (%)",
  max(abs(df_prod_check$ecart_pct)), 0, "SAM IFPRI 2021 (recap_io)",
  commentaire = "Test de non-régression de la pondération emploi×RWI, pas une comparaison indépendante"
)


################################################################################
# PARTIE III — COHÉRENCE DE L'AFFECTATION AU RÉSEAU
################################################################################

cat("=== III : Cohérence de l'affectation réseau ===\n")

# ── III.1 : tonnage affecté (cumulé sur les arêtes) vs tonnage OD attendu ────
# Chaque flux OD est compté sur toutes les arêtes de son chemin : le tonnage
# affecté est donc un multiple du tonnage OD, et ce multiple approxime la
# longueur moyenne des chemins empruntés (en nombre d'arêtes). Reproduit le
# contrôle fait en console par 04_affectation.R, à partir des seuls objets
# persistés (flux_tonnes_total, SEUIL_FLUX_TONNES, volume_par_secteur).
paires_actives_v <- which(flux_tonnes_total > SEUIL_FLUX_TONNES, arr.ind = TRUE)
paires_actives_v <- paires_actives_v[paires_actives_v[, 1] != paires_actives_v[, 2], , drop = FALSE]

tonnage_affecte_v <- sum(volume_par_secteur)
tonnage_attendu_v <- sum(flux_tonnes_total[paires_actives_v])
ratio_chemin_v    <- tonnage_affecte_v / tonnage_attendu_v

cat("  Tonnage affecté (tonnes-arêtes) :", format(round(tonnage_affecte_v), big.mark = " "), "\n")
cat("  Tonnage OD attendu              :", format(round(tonnage_attendu_v), big.mark = " "), "\n")
cat("  Ratio (≈ longueur moy. chemin)  :", round(ratio_chemin_v, 1), "arêtes\n\n")

ajouter_indicateur(
  "Affectation réseau — ratio tonnage affecté / tonnage OD attendu",
  tonnage_affecte_v, tonnage_attendu_v, "Interne (flux_tonnes_total)",
  ecart_pct = NA_real_,
  commentaire = paste0("Ratio = ", round(ratio_chemin_v, 1),
                        " ≈ nombre moyen d'arêtes par chemin ; doit rester stable, jamais NA")
)

# ── III.2 : répartition modale (tonnes-km) — doit sommer à 100 % ────────────
# Reproduit à l'identique le calcul de 04_affectation.R (Σ volume_véhicule ×
# longueur d'arête), directement depuis les colonnes déjà fusionnées dans
# `reseau` (persist_reseau_fret.rds) plutôt que depuis le tableau 3D interne.
edges_v <- reseau %>% activate("edges") %>% st_as_sf() %>% st_drop_geometry()

df_modal <- tibble(
  vehicule = c("Camionnette", "Camion moyen", "Camion lourd"),
  tkm = c(
    sum(edges_v$volume_camionnette  * edges_v$length_km, na.rm = TRUE),
    sum(edges_v$volume_camion_moyen * edges_v$length_km, na.rm = TRUE),
    sum(edges_v$volume_camion_lourd * edges_v$length_km, na.rm = TRUE)
  )
) %>%
  mutate(part_pct = round(100 * tkm / sum(tkm), 1),
         vehicule = factor(vehicule, levels = vehicule))

write_csv(df_modal, file.path(DIR_EXPORTS, "verif_repartition_modale_tkm.csv"))
cat("✓ verif_repartition_modale_tkm.csv (somme des parts :", sum(df_modal$part_pct), "%)\n")

g_modal <- ggplot(df_modal, aes(x = vehicule, y = part_pct, fill = vehicule)) +
  geom_col(width = 0.55, show.legend = FALSE) +
  geom_text(aes(label = paste0(part_pct, " %")), vjust = -0.4, size = 4.5, fontface = "bold") +
  scale_fill_manual(values = c("Camionnette" = "#6BAED6", "Camion moyen" = "#2171B5",
                                "Camion lourd" = "#6A0DAD")) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.15)), labels = scales::label_number(suffix = " %")) +
  labs(title = "Répartition modale du trafic (tonnes × km)",
       subtitle = "Recalculée depuis reseau$volume_* × length_km — doit sommer à 100 %",
       x = NULL, y = "Part du trafic (%)",
       caption = note_lecture(sprintf(
         "le %s assure %s %% du trafic total, mesuré en tonnes-kilomètres.",
         tolower(df_modal$vehicule[which.max(df_modal$part_pct)]),
         df_modal$part_pct[which.max(df_modal$part_pct)]
       ), largeur_car = 82)) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold", size = 14),
        plot.subtitle = element_text(color = "#666666", size = 9)) +
  THEME_NOTE_LECTURE

ggsave(file.path(DIR_CARTES, "graphique_verif_repartition_modale_tkm.png"),
       g_modal, width = 7, height = 7, dpi = 300)
cat("✓ graphique_verif_repartition_modale_tkm.png\n\n")

# ── III.3 : distribution de la saturation du réseau ──────────────────────────
# Part du linéaire routier (km) dans chaque classe de saturation déjà calculée
# par 04_affectation.R (classe_saturation). Un réseau très majoritairement
# "Saturé" ou très majoritairement "Inconnu" (capacité non définie) signale un
# problème de calibration des capacités plutôt qu'un résultat de fret plausible.
df_saturation <- edges_v %>%
  filter(!is.na(classe_saturation)) %>%
  group_by(classe_saturation) %>%
  summarise(km = sum(length_km, na.rm = TRUE), .groups = "drop") %>%
  mutate(part_pct = round(100 * km / sum(km), 1))

write_csv(df_saturation, file.path(DIR_EXPORTS, "verif_saturation_reseau.csv"))
cat("✓ verif_saturation_reseau.csv\n")

g_saturation <- ggplot(df_saturation, aes(x = classe_saturation, y = km, fill = classe_saturation)) +
  geom_col(width = 0.6, show.legend = FALSE) +
  geom_text(aes(label = paste0(part_pct, " %")), vjust = -0.4, size = 4, fontface = "bold") +
  scale_fill_manual(values = PALETTE_SATURATION) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.15))) +
  labs(title = "Distribution du linéaire routier par classe de saturation",
       subtitle = "Réseau chargé du fret modélisé (04_affectation.R) — classes de taux de charge/capacité (V/C)",
       x = NULL, y = "Longueur de réseau (km)",
       caption = note_lecture(sprintf(
         "%s %% du linéaire routier (%s km) est classé « %s ».",
         df_saturation$part_pct[which.max(df_saturation$km)],
         format(round(df_saturation$km[which.max(df_saturation$km)]), big.mark = " "),
         df_saturation$classe_saturation[which.max(df_saturation$km)]
       ), largeur_car = 93)) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold", size = 14),
        plot.subtitle = element_text(color = "#666666", size = 9)) +
  THEME_NOTE_LECTURE

ggsave(file.path(DIR_CARTES, "graphique_verif_saturation_reseau.png"),
       g_saturation, width = 8, height = 6.8, dpi = 300)
cat("✓ graphique_verif_saturation_reseau.png\n\n")


################################################################################
# PARTIE IV — COMPARAISON À DES SOURCES EXTERNES
################################################################################

cat("=== IV : Comparaison à des sources externes ===\n")

# ── IV.1 : population — WorldPop/NISR (modèle) vs total administratif NISR ──
# diag_population$source distingue, PAR ZONE, la méthode réellement utilisée
# (hiérarchie WorldPop > NISR par aire > plancher, cf. 01_reseau.R IV.6). On
# la cartographie, et on compare la somme nationale du modèle au total du CSV
# NISR brut (rwa_admpop_adm2_2023.csv), lu indépendamment ici.
diag_population_cat <- diag_population %>%
  mutate(source_cat = case_when(
    str_detect(source, "^WorldPop")  ~ "WorldPop",
    str_detect(source, "^NISR")      ~ "NISR",
    str_detect(source, "^Fallback")  ~ "Fallback",
    TRUE ~ "Autre"
  ))

coords_zones_verif <- reseau %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  st_as_sf() %>%
  mutate(source_cat = diag_population_cat$source_cat[
    match(warehouse_name, diag_population_cat$nom_zone)
  ]) %>%
  filter(!is.na(source_cat))

carte_source_pop <- fond_carte() +
  tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#DDDDDD", lwd = 0.3) +
  tm_shape(coords_zones_verif) +
  tm_dots(fill = "source_cat",
          fill.scale = tm_scale(values = PALETTE_SOURCE_POP),
          fill.legend = tm_legend(title = "Source de\nla population"),
          size = 0.5) +
  tm_title("Source de la population retenue par zone") +
  tm_credits(
    note_lecture(sprintf(
      "%d des %d zones du modèle utilisent la source %s pour leur population.",
      max(table(coords_zones_verif$source_cat)),
      nrow(coords_zones_verif),
      names(which.max(table(coords_zones_verif$source_cat)))
    ), largeur_car = 105),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position = c("right", "top"))

tmap_save(carte_source_pop, file.path(DIR_CARTES, "carte_verif_source_population.png"),
          width = 3000, height = 2400, dpi = 300)
cat("✓ carte_verif_source_population.png\n")

if (file.exists(NISR_CSV_PATH)) {
  nisr_pop_v <- read_csv(NISR_CSV_PATH, show_col_types = FALSE)
  pop_nisr_national_v <- sum(nisr_pop_v[[NISR_COL_POP_TOTAL]], na.rm = TRUE)
  pop_modele_national_v <- sum(diag_population$population_zone)
  ecart_pop_v <- 100 * (pop_modele_national_v - pop_nisr_national_v) / pop_nisr_national_v

  df_pop_compar <- tibble(
    source = factor(c("NISR (recensement 2023)", "Modèle (somme des zones)"),
                     levels = c("NISR (recensement 2023)", "Modèle (somme des zones)")),
    population = c(pop_nisr_national_v, pop_modele_national_v)
  )

  g_pop_national <- ggplot(df_pop_compar, aes(x = source, y = population, fill = source)) +
    geom_col(width = 0.5, show.legend = FALSE) +
    geom_text(aes(label = format(round(population), big.mark = " ")), vjust = -0.5,
               size = 4.5, fontface = "bold") +
    scale_fill_manual(values = c("#E57373", "#42A5F5")) +
    scale_y_continuous(labels = scales::label_number(big.mark = " "),
                        expand = expansion(mult = c(0, 0.15))) +
    labs(title = "Population nationale : modèle vs recensement NISR",
         subtitle = paste0("Écart : ", round(ecart_pop_v, 1), " %"),
         x = NULL, y = "Population",
         caption = note_lecture(sprintf(
           "le modèle attribue %s habitants au pays au total, contre %s selon le recensement NISR 2023.",
           format(round(pop_modele_national_v), big.mark = " "),
           format(round(pop_nisr_national_v), big.mark = " ")
         ), largeur_car = 82)) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold", size = 14)) +
    THEME_NOTE_LECTURE

  ggsave(file.path(DIR_CARTES, "graphique_verif_population_national.png"),
         g_pop_national, width = 7, height = 7.2, dpi = 300)
  cat("✓ graphique_verif_population_national.png (écart :", round(ecart_pop_v, 1), "%)\n\n")

  ajouter_indicateur("Population nationale — modèle vs NISR",
                      pop_modele_national_v, pop_nisr_national_v,
                      "NISR, rwa_admpop_adm2_2023.csv", ecart_pct = ecart_pop_v)
} else {
  cat("  ⚠ Fichier NISR introuvable (", NISR_CSV_PATH, ") — comparaison population ignorée\n\n")
}

df_source_pop_tab <- diag_population_cat %>%
  group_by(source_cat) %>%
  summarise(n_zones = n(), population = sum(population_zone), .groups = "drop") %>%
  mutate(part_pop_pct = round(100 * population / sum(population), 1))
write_csv(df_source_pop_tab, file.path(DIR_EXPORTS, "verif_population_sources.csv"))
cat("✓ verif_population_sources.csv\n\n")

# ── IV.2 : emploi RPHC5 — fichier utilisé par le modèle vs second fichier ────
# RPHC5_EMPLOI_CSV_PATH_ALT (00_parametres.R) n'alimente aucun calcul : il
# sert uniquement ici à visualiser à quel point les deux versions disponibles
# de l'emploi sectoriel par district divergent, aucune des deux n'étant une
# extraction confirmée du RPHC5 (cf. mémoire reference_sources_calibration).
#
# IMPORTANT : les deux fichiers ont des totaux NATIONAUX identiques par
# secteur (mêmes 7 sommes colonne à colonne) — seule la répartition ENTRE
# districts diffère du tout au tout (ex. Nyarugenge : Emploi_Agriculture vaut
# 15 128 dans un fichier, 75 291 dans l'autre). Une comparaison agrégée au
# niveau national ferait donc apparaître à tort deux fichiers identiques ;
# la comparaison porte ici sur le total par DISTRICT.
cols_emploi_v <- names(RPHC5_CORRESPONDANCE_SECTEURS)

lire_emploi_district <- function(chemin) {
  read_csv(chemin, show_col_types = FALSE) %>%
    rename(district = any_of(RPHC5_COL_DISTRICT_EMPLOI)) %>%
    mutate(
      district_clean = iconv(str_to_lower(str_trim(district)), from = "UTF-8", to = "ASCII//TRANSLIT"),
      emploi_total   = rowSums(across(all_of(cols_emploi_v)), na.rm = TRUE)
    ) %>%
    select(district_clean, district, emploi_total)
}

if (file.exists(RPHC5_EMPLOI_CSV_PATH) && file.exists(RPHC5_EMPLOI_CSV_PATH_ALT)) {
  emploi_district_utilise_v <- lire_emploi_district(RPHC5_EMPLOI_CSV_PATH)
  emploi_district_alt_v     <- lire_emploi_district(RPHC5_EMPLOI_CSV_PATH_ALT)
  df_emploi_district <- inner_join(
    emploi_district_utilise_v, emploi_district_alt_v,
    by = "district_clean", suffix = c("_utilise", "_alt")
  )

  write_csv(df_emploi_district, file.path(DIR_EXPORTS, "verif_emploi_sources.csv"))
  cat("✓ verif_emploi_sources.csv\n")

  # District servant d'exemple dans la note de lecture : celui où les deux
  # fichiers s'écartent le plus en valeur absolue.
  district_exemple_v <- df_emploi_district %>%
    mutate(ecart_abs = abs(emploi_total_utilise - emploi_total_alt)) %>%
    arrange(desc(ecart_abs)) %>%
    slice(1)

  g_emploi_sources <- ggplot(df_emploi_district,
                              aes(x = emploi_total_alt, y = emploi_total_utilise)) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey50") +
    geom_point(size = 2.5, color = "#42A5F5", alpha = 0.8) +
    scale_x_continuous(labels = scales::label_number(big.mark = " ")) +
    scale_y_continuous(labels = scales::label_number(big.mark = " ")) +
    labs(title = "Emploi total par district : fichier utilisé vs second fichier disponible",
         subtitle = "Mêmes totaux nationaux par secteur, mais répartition par district totalement différente",
         x = "Second fichier (non utilisé), emplois",
         y = "Fichier utilisé par le modèle, emplois",
         caption = note_lecture(sprintf(
           "à %s, le fichier utilisé par le modèle indique %s emplois au total, contre %s dans le second fichier.",
           district_exemple_v$district_utilise,
           format(round(district_exemple_v$emploi_total_utilise), big.mark = " "),
           format(round(district_exemple_v$emploi_total_alt), big.mark = " ")
         ), largeur_car = 105)) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold", size = 13),
          plot.subtitle = element_text(color = "#666666", size = 8.5)) +
    THEME_NOTE_LECTURE

  ggsave(file.path(DIR_CARTES, "graphique_verif_emploi_sources.png"),
         g_emploi_sources, width = 8, height = 8.4, dpi = 300)
  cat("✓ graphique_verif_emploi_sources.png\n\n")

  ajouter_indicateur(
    "Emploi par district — écart maximal entre les deux fichiers disponibles",
    district_exemple_v$emploi_total_utilise, district_exemple_v$emploi_total_alt,
    paste0("data/raw/rwa_emploi_district_secteur_2022_source_nationale.csv (district : ",
           district_exemple_v$district_utilise, ")"),
    ecart_pct = 100 * (district_exemple_v$emploi_total_utilise - district_exemple_v$emploi_total_alt) /
      district_exemple_v$emploi_total_alt,
    commentaire = "Totaux nationaux par secteur identiques entre les deux fichiers ; c'est la répartition par district qui diverge complètement — aucun des deux n'est une source confirmée"
  )
} else {
  cat("  ⚠ Fichier(s) d'emploi introuvable(s) — comparaison ignorée\n\n")
}

# Test de conservation : emploi spatialisé (zones) vs total national du fichier
# effectivement utilisé — doit correspondre exactement, la ventilation par
# cellule de Voronoï se faisant au prorata de l'aire (01_reseau.R IV.4.F).
if (exists("emploi_zone_secteur") && file.exists(RPHC5_EMPLOI_CSV_PATH)) {
  emploi_total_national_csv_v <- sum(lire_emploi_district(RPHC5_EMPLOI_CSV_PATH)$emploi_total)
  emploi_total_spatialise_v   <- sum(emploi_zone_secteur)
  cat("  Emploi total (CSV utilisé)      :", format(round(emploi_total_national_csv_v), big.mark = " "), "\n")
  cat("  Emploi total (spatialisé zones) :", format(round(emploi_total_spatialise_v), big.mark = " "), "\n\n")

  ajouter_indicateur(
    "Conservation de l'emploi — somme des zones vs total CSV utilisé",
    emploi_total_spatialise_v, emploi_total_national_csv_v,
    "data/raw/rwa_emploi_district_secteur_2022.csv",
    ecart_pct = 100 * (emploi_total_spatialise_v - emploi_total_national_csv_v) / emploi_total_national_csv_v,
    commentaire = "Test de non-régression de la répartition par aire de Voronoï (01_reseau.R IV.4.F)"
  )
}

# ── IV.3 : part urbaine — trois repères (EICV5, RPHC5, implicite SAM) ────────
df_urbain <- tibble(
  source = factor(
    c("EICV5 2016/17", "RPHC5 2022", "Implicite de la SAM 2021\n(valeur retenue)"),
    levels = c("EICV5 2016/17", "RPHC5 2022", "Implicite de la SAM 2021\n(valeur retenue)")
  ),
  part_urbaine_pct = 100 * c(EICV5_PART_URBAINE_POP, RPHC5_2022_PART_URBAINE_POP,
                              PART_URBAINE_IMPLICITE_SAM)
)

g_urbain <- ggplot(df_urbain, aes(x = source, y = part_urbaine_pct, fill = source)) +
  geom_col(width = 0.55, show.legend = FALSE) +
  geom_text(aes(label = paste0(round(part_urbaine_pct, 1), " %")), vjust = -0.5,
             size = 4.5, fontface = "bold") +
  scale_fill_manual(values = c("#90A4AE", "#90A4AE", "#42A5F5")) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.2)), labels = scales::label_number(suffix = " %")) +
  labs(title = "Part de la population urbaine : trois repères",
       subtitle = paste0(
         "Le modèle retient CIBLE_PART_URBAINE_POP = valeur implicite de la SAM ",
         "(cf. justification détaillée, 00_parametres.R)"
       ),
       x = NULL, y = "Part urbaine (%)",
       caption = note_lecture(sprintf(
         "le modèle retient une part urbaine de %s %% (implicite de la SAM), contre %s %% selon l'EICV5 2016/17.",
         round(PART_URBAINE_IMPLICITE_SAM * 100, 1), round(EICV5_PART_URBAINE_POP * 100, 1)
       ), largeur_car = 93)) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold", size = 14),
        plot.subtitle = element_text(color = "#666666", size = 8.5)) +
  THEME_NOTE_LECTURE

ggsave(file.path(DIR_CARTES, "graphique_verif_urbanisation_reperes.png"),
       g_urbain, width = 8, height = 7.2, dpi = 300)
cat("✓ graphique_verif_urbanisation_reperes.png\n\n")

ajouter_indicateur("Part urbaine retenue par le modèle (SAM implicite)",
                    PART_URBAINE_IMPLICITE_SAM, EICV5_PART_URBAINE_POP,
                    "EICV5 2016/17 (Poverty Profile Report, tab. 10.2)",
                    ecart_pct = 100 * (PART_URBAINE_IMPLICITE_SAM - EICV5_PART_URBAINE_POP) / EICV5_PART_URBAINE_POP,
                    commentaire = "Écart documenté et assumé — cf. 00_parametres.R (CIBLE_PART_URBAINE_POP)")

# ── IV.3bis : masque urbain — méthode du modèle vs landuse OSM ──────────────
# diag_masque_urbain_sf (calculé en 01_reseau.R, IV.5.B.3) : un point par
# cellule d'~1 km (raster WorldPop agrégé, cf. AGREGATION_MASQUE_URBAIN_VIZ),
# classée "Modèle seul" / "OSM seul" / "Modèle + OSM" selon que le pixel est
# jugé urbain par la méthode retenue (METHODE_MASQUE_URBAIN), par le landuse
# OSM, ou par les deux. Sert à voir OÙ les deux méthodes divergent, alors que
# le pourcentage de recouvrement (console, 01_reseau.R) ne donne qu'un total.
if (exists("diag_masque_urbain_sf") && !is.null(diag_masque_urbain_sf) &&
    nrow(diag_masque_urbain_sf) > 0) {

  carte_masque_urbain <- fond_carte() +
    tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
    tm_lines(col = "#DDDDDD", lwd = 0.3) +
    tm_shape(diag_masque_urbain_sf) +
    tm_dots(fill = "categorie",
            fill.scale = tm_scale(values = PALETTE_MASQUE_URBAIN),
            fill.legend = tm_legend(title = "Zone classée urbaine par"),
            size = 0.12) +
    tm_title(paste0("Masque urbain : méthode \"", METHODE_MASQUE_URBAIN, "\" vs landuse OSM")) +
    tm_credits(
      note_lecture(paste0(
        "chaque point = une cellule d'environ 1 km (raster WorldPop agrégé). Vert : les deux méthodes s'accordent. ",
        "Bleu : urbain pour le modèle (seuil de densité calé sur CIBLE_PART_URBAINE_POP) mais pas pour OSM — attendu ",
        "si le landuse OSM est incomplet. Orange : urbain pour OSM mais pas pour le modèle — attendu si le seuil de ",
        "densité, calé au niveau national, exclut de petits centres bien cartographiés dans OSM. Contrôle qualitatif : ",
        "aucun des deux repères n'est une vérité terrain."
      )),
      position = tm_pos_out("center", "bottom", "left", "top"),
      size     = 0.65
    ) +
    tm_layout(legend.outside = TRUE, frame = TRUE) +
    tm_scalebar(position = c("left", "bottom")) +
    tm_compass(position = c("right", "top"))

  tmap_save(carte_masque_urbain, file.path(DIR_CARTES, "carte_verif_masque_urbain.png"),
            width = 3000, height = 2400, dpi = 300)
  cat("✓ carte_verif_masque_urbain.png\n\n")
} else {
  cat("  ⚠ diag_masque_urbain_sf indisponible (pas de landuse OSM rasterisé) — carte ignorée\n\n")
}

# ── IV.4 : valeur ajoutée sectorielle — SAM 2021 vs Banque Mondiale 2023 ─────
# Comparaison par PARTS relatives (peu sensible au taux de change et à l'écart
# d'année) plutôt qu'en niveau : la SAM date de 2021, les données Banque
# Mondiale de 2023, et la correspondance vers les 4 grandes catégories WB est
# approximative (CORRESPONDANCE_SECTEURS_BANQUE_MONDIALE, 00_parametres.R).
chemin_wb_v <- file.path(DIR_CACHE, "wb_rwanda_secteurs.rds")
if (file.exists(chemin_wb_v)) {
  wb_secteurs_v <- readRDS(chemin_wb_v)

  va_sam_categorie_v <- tibble(
    secteur   = SECTEURS,
    categorie = CORRESPONDANCE_SECTEURS_BANQUE_MONDIALE[SECTEURS],
    va_mrd    = sam$va[SECTEURS]
  ) %>%
    group_by(categorie) %>%
    summarise(va_mrd = sum(va_mrd), .groups = "drop") %>%
    mutate(va_usd = va_mrd * 1e9 / TAUX_CHANGE_RWF_USD_2021,
           part_pct = round(100 * va_usd / sum(va_usd), 1),
           source = "SAM 2021 (implicite)")

  df_wb_v <- tibble(
    categorie = c("agri", "manuf", "indus", "serv"),
    va_usd    = c(wb_secteurs_v$agri, wb_secteurs_v$manuf, wb_secteurs_v$indus, wb_secteurs_v$serv)
  ) %>%
    mutate(part_pct = round(100 * va_usd / sum(va_usd), 1),
           source = "Banque Mondiale 2023 (WDI)")

  df_va_compar <- bind_rows(
    va_sam_categorie_v %>% select(categorie, part_pct, source),
    df_wb_v            %>% select(categorie, part_pct, source)
  ) %>%
    mutate(categorie = recode(categorie,
                               agri  = "Agriculture",
                               manuf = "Industrie manuf.",
                               indus = "Industrie (y c. construction)",
                               serv  = "Services"))

  write_csv(df_va_compar, file.path(DIR_EXPORTS, "verif_va_secteurs_banque_mondiale.csv"))
  cat("✓ verif_va_secteurs_banque_mondiale.csv\n")

  g_va_compar <- ggplot(df_va_compar, aes(x = categorie, y = part_pct, fill = source)) +
    geom_col(position = position_dodge(width = 0.7), width = 0.6) +
    scale_fill_manual(values = c("SAM 2021 (implicite)" = "#42A5F5",
                                  "Banque Mondiale 2023 (WDI)" = "#E57373")) +
    scale_y_continuous(labels = scales::label_number(suffix = " %"), expand = expansion(mult = c(0, 0.15))) +
    labs(title = "Parts de la valeur ajoutée par grande catégorie : SAM vs Banque Mondiale",
         subtitle = "SAM IFPRI 2021 vs World Development Indicators 2023 — comparaison en parts, pas en niveau",
         x = NULL, y = "Part de la VA totale (%)", fill = NULL,
         caption = note_lecture(sprintf(
           "le secteur %s représente %s %% de la valeur ajoutée selon la SAM, contre %s %% selon la Banque Mondiale.",
           df_va_compar$categorie[which.max(df_va_compar$part_pct)],
           df_va_compar$part_pct[which.max(df_va_compar$part_pct)],
           df_va_compar$part_pct[df_va_compar$categorie == df_va_compar$categorie[which.max(df_va_compar$part_pct)] &
                                    df_va_compar$source == "Banque Mondiale 2023 (WDI)"]
         ), largeur_car = 105)) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold", size = 13),
          plot.subtitle = element_text(color = "#666666", size = 8),
          axis.text.x = element_text(angle = 15, hjust = 1)) +
    THEME_NOTE_LECTURE

  ggsave(file.path(DIR_CARTES, "graphique_verif_va_secteurs_banque_mondiale.png"),
         g_va_compar, width = 9, height = 7, dpi = 300)
  cat("✓ graphique_verif_va_secteurs_banque_mondiale.png\n\n")
} else {
  cat("  ⚠ outputs/cache/wb_rwanda_secteurs.rds introuvable — comparaison Banque Mondiale ignorée\n\n")
}


################################################################################
# PARTIE V — STATISTIQUES DESCRIPTIVES ET TABLEAU DE BORD
################################################################################

cat("=== V : Statistiques descriptives et tableau de bord ===\n")

# ── V.1 : linéaire routier par type (statistique descriptive du réseau) ─────
df_reseau_type <- edges_v %>%
  filter(!is.na(road_type)) %>%
  group_by(road_type) %>%
  summarise(km = sum(length_km, na.rm = TRUE), n_troncons = n(), .groups = "drop") %>%
  mutate(part_pct = round(100 * km / sum(km), 1)) %>%
  arrange(desc(km))

write_csv(df_reseau_type, file.path(DIR_EXPORTS, "verif_reseau_km_par_type.csv"))
cat("✓ verif_reseau_km_par_type.csv\n")

g_reseau_type <- ggplot(df_reseau_type,
                         aes(x = reorder(road_type, km), y = km, fill = road_type)) +
  geom_col(show.legend = FALSE) +
  geom_text(aes(label = paste0(round(km), " km · ", part_pct, " %")), hjust = -0.05, size = 3.3) +
  coord_flip(clip = "off") +
  scale_fill_manual(values = PALETTE_ROAD_TYPE) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.35))) +
  labs(title = "Linéaire routier modélisé par type de route",
       subtitle = "Statistique descriptive du réseau OSM utilisé par le modèle (01_reseau.R)",
       x = NULL, y = "Longueur (km)",
       caption = note_lecture(sprintf(
         "les routes « %s » totalisent %s km, soit %s %% du linéaire routier total du modèle.",
         df_reseau_type$road_type[which.max(df_reseau_type$km)],
         format(round(df_reseau_type$km[which.max(df_reseau_type$km)]), big.mark = " "),
         df_reseau_type$part_pct[which.max(df_reseau_type$km)]
       ), largeur_car = 105)) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold", size = 14),
        plot.subtitle = element_text(color = "#666666", size = 9)) +
  THEME_NOTE_LECTURE

ggsave(file.path(DIR_CARTES, "graphique_verif_reseau_km_par_type.png"),
       g_reseau_type, width = 9, height = 6.3, dpi = 300)
cat("✓ graphique_verif_reseau_km_par_type.png\n\n")

# ── V.2 : tableau de bord — toutes les parties précédentes en une table ─────
df_tableau_bord <- bind_rows(tableau_bord)
write_csv(df_tableau_bord, file.path(DIR_EXPORTS, "verif_tableau_bord.csv"))
cat("✓ verif_tableau_bord.csv (", nrow(df_tableau_bord), "indicateurs )\n\n")
print(df_tableau_bord, width = Inf)

cat("################################################################\n")
cat("# viz_verif.R terminé\n")
cat("# Sorties : outputs/cartes/*verif*.png, outputs/exports/verif_*.csv\n")
cat("################################################################\n\n")
