################################################################################
# viz_fret.R
# RÔLE : Cartes et graphiques des flux de fret (trafic, répartition modale,
#        composition sectorielle, heatmaps, Sankey, secteur dominant).
#
# PEUT TOURNER SANS RELANCER LES SCRIPTS PRÉCÉDENTS si les .rds sont à jour.
#
# RELANCER 01_reseau.R + 02_couts.R + 03_transport.R avant ce script si :
#   → les paramètres BETA_SECTEUR ont changé (sensibilité gravitaire)
#   → la matrice A ou production_totale ont changé (table IO)
#   → DEMANDE_FINALE_SAM ou COMMERCE_EXTERIEUR_NISR ont changé
#   → SEUIL_FLUX_TONNES a changé (filtre affectation)
#   → de nouvelles zones d'entrepôt ont été ajoutées ou retirées
#   → les données RPHC5 d'emploi ont été mises à jour (emploi_zone_secteur_all)
#
# RELANCER uniquement 03_transport.R (sans 01 ni 02) si :
#   → seuls les paramètres gravitaires (BETA, PART_*) ont changé
#     et que le réseau physique est inchangé
#
# FICHIERS LUS : persist_geodata.rds, persist_entreposages.rds,
#                persist_flux_fret.rds, persist_reseau_fret.rds
################################################################################

source("00_parametres.R")
fond_carte <- readRDS(file.path(DIR_CARTES, "persist_fond_carte.rds"))

.ent  <- readRDS(PERSIST_ENTREPOSAGES)
list2env(.ent, envir = .GlobalEnv)

.fret <- readRDS(PERSIST_RESEAU_FRET)
reseau         <- .fret$reseau
volume_par_secteur    <- .fret$volume_par_secteur
volume_par_secteur_df <- .fret$volume_par_secteur_df
rm(.fret)

.flux <- readRDS(PERSIST_FLUX_FRET)
list2env(.flux, envir = .GlobalEnv)
rm(.flux)

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

# ==============================================================================
# VIII.2 : Visualisations
# Génère 5 sorties graphiques : carte du trafic fret, carte de répartition
# modale, graphiques sectoriels et heatmap de la matrice OD.
# ==============================================================================

# --- Préparation des couches spatiales ---

# Forcer les colonnes numériques explicitement
# rescale() (du package scales) : normalise une variable entre deux valeurs cibles.
# Ici, log10(volume) est mis à l'échelle entre 0.5 et 5 pour définir
# l'épaisseur des lignes sur la carte (lignes plus épaisses = trafic plus élevé).
aretes_fret <- reseau %>%
  activate("edges") %>%
  st_as_sf() %>%
  filter(volume_tonnes > 0) %>%
  mutate(
    volume_tonnes = as.numeric(volume_tonnes),
    volume_log    = as.numeric(log10(volume_tonnes + 1)),
    lwd_val       = as.numeric(rescale(log10(volume_tonnes + 1), to = c(0.5, 5)))
  )

# Coordonnées des zones
coords_zones_sf <- reseau %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  st_as_sf()

# match() : associe chaque nœud d'entrepôt à son index dans noeuds_entreposage
# pour récupérer les volumes de trafic.
# type : type simplifié de la zone ("Frontière" vs "Ville"), utilisé pour la
# légende "Type" sur les cartes ci-dessous (00_parametres.R).
coords_zones_sf <- coords_zones_sf %>%
  mutate(
    match_idx = match(warehouse_name, noeuds_entreposage$warehouse_name),
    type      = type_simplifie(warehouse_type)
  ) %>%
  filter(!is.na(match_idx)) %>%
  arrange(match_idx)

cat("✓ Couches préparées\n")
cat("  Arêtes fret actives:", nrow(aretes_fret), "\n")
cat("  Volume min:", round(min(aretes_fret$volume_tonnes)), "t\n")
cat("  Volume max:", round(max(aretes_fret$volume_tonnes)), "t\n\n")


# ============================================================
# CARTE 4 : Intensité du trafic fret sur le réseau routier
# tm_scale_continuous() au lieu de tm_scale_intervals()
# ============================================================

cat("Génération de la carte du trafic fret...\n")

# La largeur de ligne est proportionnelle au volume de trafic (échelle log).
# Les nœuds sont colorés selon le type de zone et dimensionnés selon le
# volume total généré/consommé.
carte_fret <- fond_carte() +
  
  # Réseau de base en gris très clair
  tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#DDDDDD", lwd = 0.3) +
  
  # Arêtes avec trafic
  tm_shape(aretes_fret) +
  tm_lines(
    col = "volume_tonnes",
    col.scale = tm_scale_intervals(style="quantile", n=4, values=PALETTE_FRET),
    col.legend = tm_legend(title = "Volume fret\n(tonnes)"),
    lwd = "lwd_val",
    lwd.scale = tm_scale(values.range = c(0.4, 5)),
    lwd.legend = tm_legend(show = FALSE)
  ) +
  
  # Points des zones
  tm_shape(coords_zones_sf) +
  tm_dots(
    fill = "type",
    fill.scale = tm_scale(values = PALETTE_TYPE),
    fill.legend = tm_legend(title = "Type"),
    size = 0.5
  ) +

  tm_title(paste0("Intensité du Trafic Fret\nModèle gravitaire — ", NOM_PAYS)) +
  tm_credits(
    note_lecture(sprintf(
      "le tronçon le plus chargé porte %s tonnes de fret, tous secteurs confondus.",
      format(round(max(aretes_fret$volume_tonnes)), big.mark = " ")
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position = c("right", "top"))

tmap_save(carte_fret,
          file.path(DIR_CARTES,"carte_trafic_fret.png"),
          width = 3000, height = 2400, dpi = 300)
cat("✓ Carte trafic fret sauvegardée\n")

# ============================================================
# CARTE 4bis : Saturation du réseau (goulots d'étranglement V/C)
# Colore chaque tronçon selon son taux de saturation (charge/capacité)
# calculé par la fonction d'encombrement en 03_transport.R.
# Vert = fluide, rouge = saturé (V/C ≥ 1). L'épaisseur croît avec V/C
# pour faire ressortir visuellement les goulots.
# ============================================================

# Garde-fou : la colonne taux_saturation n'existe que si 03_transport.R a été
# relancé AVEC la fonction d'encombrement (CONGESTION). Si le réseau chargé est
# antérieur, on saute la carte avec un message plutôt que de planter.
colonnes_aretes <- reseau %>% activate("edges") %>% as_tibble() %>% names()

if ("taux_saturation" %in% colonnes_aretes) {

  cat("Génération de la carte de saturation...\n")

  # Couche des arêtes empruntées, avec saturation et épaisseur proportionnelle.
  aretes_saturation <- reseau %>%
    activate("edges") %>%
    st_as_sf() %>%
    filter(volume_tonnes > 0) %>%
    mutate(
      taux_saturation = as.numeric(taux_saturation),
      # Épaisseur de ligne croissante avec la saturation (échelle linéaire)
      lwd_val = as.numeric(rescale(taux_saturation, to = c(0.5, 5)))
    )

  # Libellé "classe (xx,x %)" par classe de saturation, part du linéaire des
  # arêtes chargées de fret — insérée dans la légende comme pour la carte des
  # pentes.
  km_par_saturation <- aretes_saturation %>%
    st_drop_geometry() %>%
    group_by(classe_saturation) %>%
    summarise(km = sum(length_km, na.rm = TRUE), .groups = "drop")

  labels_saturation_pct <- setNames(
    sprintf("%s (%s %%)", km_par_saturation$classe_saturation,
            sub("\\.", ",", sprintf("%.1f", 100 * km_par_saturation$km / sum(km_par_saturation$km)))),
    km_par_saturation$classe_saturation
  )
  noms_saturation_presentes <- intersect(names(PALETTE_SATURATION), names(labels_saturation_pct))

  aretes_saturation <- aretes_saturation %>%
    mutate(classe_saturation_pct = factor(
      labels_saturation_pct[classe_saturation],
      levels = labels_saturation_pct[noms_saturation_presentes]
    ))

  palette_saturation_pct <- setNames(
    PALETTE_SATURATION[noms_saturation_presentes],
    labels_saturation_pct[noms_saturation_presentes]
  )

  carte_saturation <- fond_carte() +

    # Réseau de base en gris très clair (contexte géographique)
    tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
    tm_lines(col = "#DDDDDD", lwd = 0.3) +

    # Arêtes colorées par classe de saturation (catégoriel, comme warehouse_type)
    tm_shape(aretes_saturation) +
    tm_lines(
      col        = "classe_saturation_pct",
      col.scale  = tm_scale(values = palette_saturation_pct),
      col.legend = tm_legend(title = "Saturation (V/C)"),
      lwd        = "lwd_val",
      lwd.scale  = tm_scale(values.range = c(0.4, 5)),
      lwd.legend = tm_legend(show = FALSE)
    ) +

    # Points des zones (mêmes couleurs que la carte globale)
    tm_shape(coords_zones_sf) +
    tm_dots(
      fill        = "type",
      fill.scale  = tm_scale(values = PALETTE_TYPE),
      fill.legend = tm_legend(title = "Type"),
      size        = 0.5
    ) +

    tm_title(paste0("Saturation du réseau de fret (V/C)\nFonction d'encombrement — ",
                    NOM_PAYS)) +
    tm_credits(
      note_lecture(sprintf(
        "%d tronçons sont saturés (V/C ≥ 1), sur %d tronçons chargés de fret.",
        sum(aretes_saturation$taux_saturation > 1, na.rm = TRUE), nrow(aretes_saturation)
      )),
      position = tm_pos_out("center", "bottom", "left", "top"),
      size     = 0.65
    ) +
    tm_layout(legend.outside = TRUE, frame = TRUE) +
    tm_scalebar(position = c("left", "bottom")) +
    tm_compass(position  = c("right", "top"))

  tmap_save(carte_saturation,
            file.path(DIR_CARTES, "carte_saturation_reseau.png"),
            width = 3000, height = 2400, dpi = 300)
  cat("  ✓ carte_saturation_reseau.png\n")
  cat("    Tronçons saturés (V/C>1) :",
      sum(aretes_saturation$taux_saturation > 1, na.rm = TRUE), "\n\n")

} else {
  cat("⚠ Carte de saturation ignorée : colonne taux_saturation absente.\n")
  cat("  → Relancer 03_transport.R avec CONGESTION = TRUE.\n\n")
}

# ============================================================
# CARTE 5 : Intensité du fret PAR SECTEUR
# Une carte par secteur économique, même style que carte_trafic_fret
# mais en filtrant le volume pour ne garder que les tonnes du secteur.
# ============================================================
# Ces cartes sont utiles pour identifier visuellement quelles parties du
# réseau portent quel type de marchandise. Par exemple : l'Agriculture
# devrait se concentrer autour des marchés ruraux, tandis que la
# Construction devrait être concentrée autour de Kigali et des SEZ.
# ============================================================

cat("\nGénération des cartes sectorielles de trafic...\n")

# On récupère les arêtes du réseau avec leur géométrie (on en a besoin
# pour afficher sur la carte). On y attache ensuite les volumes sectoriels
# stockés dans volume_par_secteur_df (construit en Partie VIII.1).
# volume_par_secteur_df a une ligne par arête physique et une colonne
# par secteur, nommée "vol_t_<Secteur>" (ex : "vol_t_Agriculture").
aretes_geom_base <- reseau %>%
  activate("edges") %>%
  st_as_sf()

# Sanity check : le nombre de lignes doit correspondre exactement entre
# la géométrie du réseau et le tableau des volumes sectoriels.
# Si ce n'est pas le cas, c'est qu'il y a eu un désalignement quelque part
# (ex : filtrage d'arêtes après construction de volume_par_secteur_df).
stopifnot(nrow(aretes_geom_base) == nrow(volume_par_secteur_df))

# On attache les colonnes sectorielles à la couche géométrique.
# bind_cols() accole les colonnes de volume_par_secteur_df (une par secteur)
# à la table des arêtes. Résultat : chaque arête a maintenant 8 colonnes
# supplémentaires (une par secteur) contenant le tonnage sectoriel.
aretes_avec_secteurs <- bind_cols(aretes_geom_base, volume_par_secteur_df)

for (s in SECTEURS_FRET) {
  
  # Nom de la colonne sectorielle dans le tableau
  # (cohérent avec le préfixe "vol_t_" défini en Partie VIII.1)
  col_secteur <- paste0("vol_t_", s)
  
  # Extraction des arêtes avec un trafic non nul pour CE secteur uniquement.
  # .data[[col_secteur]] : syntaxe tidyverse pour utiliser une colonne dont
  # le nom est stocké dans une variable (ici col_secteur).
  # On convertit ensuite en variable lisible "vol_t" pour simplifier la suite.
  aretes_fret_s <- aretes_avec_secteurs %>%
    filter(.data[[col_secteur]] > 0) %>%
    mutate(
      vol_t     = as.numeric(.data[[col_secteur]]),
      vol_log   = as.numeric(log10(vol_t + 1)),
      lwd_val   = as.numeric(rescale(log10(vol_t + 1), to = c(0.5, 5)))
    )
  
  # Si aucun trafic pour ce secteur (rare mais possible pour Services),
  # on passe au secteur suivant sans générer de carte vide.
  if (nrow(aretes_fret_s) == 0) {
    cat("  ⚠", s, ": aucun trafic sectoriel, carte non générée\n")
    next
  }
  
  # Construction de la carte sur le même modèle que carte_trafic_fret,
  # mais avec la variable vol_t au lieu de volume_tonnes.
  carte_s <- fond_carte() +
    
    # Réseau de base en gris très clair (contexte géographique)
    tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
    tm_lines(col = "#DDDDDD", lwd = 0.3) +
    
    # Arêtes avec trafic pour ce secteur
    tm_shape(aretes_fret_s) +
    tm_lines(
      col        = "vol_t",
      col.scale  = tm_scale_intervals(style = "quantile", n = 4,
                                      values = PALETTE_FRET),
      col.legend = tm_legend(title = paste0("Volume ", s, "\n(tonnes)")),
      lwd        = "lwd_val",
      lwd.scale  = tm_scale(values.range = c(0.4, 5)),
      lwd.legend = tm_legend(show = FALSE)
    ) +
    
    # Points des zones (mêmes couleurs que la carte globale)
    tm_shape(coords_zones_sf) +
    tm_dots(
      fill        = "type",
      fill.scale  = tm_scale(values = PALETTE_TYPE),
      fill.legend = tm_legend(title = "Type"),
      size        = 0.5
    ) +

    tm_title(paste0("Intensité du Trafic Fret — Secteur ", s,
                    "\nModèle gravitaire — ", NOM_PAYS)) +
    tm_credits(
      note_lecture(sprintf(
        "le tronçon le plus chargé en %s porte %s tonnes.",
        s, format(round(max(aretes_fret_s$vol_t)), big.mark = " ")
      )),
      position = tm_pos_out("center", "bottom", "left", "top"),
      size     = 0.65
    ) +
    tm_layout(legend.outside = TRUE, frame = TRUE) +
    tm_scalebar(position = c("left", "bottom")) +
    tm_compass(position  = c("right", "top"))
  
  # Sauvegarde avec un nom de fichier qui inclut le nom du secteur
  # (ex : carte_trafic_fret_Agriculture.png)
  nom_fichier_s <- paste0("carte_trafic_fret_", s, ".png")
  tmap_save(
    carte_s,
    file.path(DIR_CARTES, nom_fichier_s),
    width = 3000, height = 2400, dpi = 300
  )
  cat("  ✓", nom_fichier_s, "\n")
}

cat("✓", length(SECTEURS), "cartes sectorielles générées\n\n")


# ============================================================
# CARTE 6 : Secteur DOMINANT par arête
# Pour chaque arête, on identifie le secteur qui y transporte le plus
# de tonnes et on colore l'arête selon ce secteur dominant.
# ============================================================
# Cette carte complète les cartes sectorielles individuelles en donnant
# une vue synthétique : quelles routes sont "spécialisées" dans quel secteur ?
# On verra par exemple que certaines routes sont dominées par l'Agriculture
# (routes rurales vers marchés), d'autres par le Commerce (corridors
# internationaux), d'autres par l'Industrie (proximité des SEZ).
# ============================================================

cat("Génération de la carte du secteur dominant...\n")

# apply(X, MARGIN = 1, FUN) : applique FUN sur chaque ligne de la matrice.
# which.max() retourne l'indice de la valeur maximale d'un vecteur.
# Résultat : pour chaque arête (ligne), on obtient l'indice du secteur
# (colonne) qui a le plus gros volume.
# Exemple : si l'arête 5 a des volumes (10, 0, 500, 20, 0, 5, 0, 2) par secteur,
# which.max() retourne 3 (Agro_industrie domine).
idx_secteur_dominant <- apply(volume_par_secteur, 1, function(ligne) {
  if (all(ligne == 0)) return(NA_integer_)   # Arête sans trafic → NA
  which.max(ligne)
})

# Conversion de l'indice numérique vers le nom du secteur.
# SECTEURS[NA] retourne NA, donc on garde les arêtes sans trafic en NA.
secteur_dominant <- SECTEURS[idx_secteur_dominant]

# Part du secteur dominant dans le trafic total de l'arête.
# Utile pour distinguer les arêtes "spécialisées" (99% d'un seul secteur)
# des arêtes "mixtes" (30-40% réparti sur plusieurs secteurs).
# rowSums() : somme par ligne → total toutes catégories confondues.
total_arete <- rowSums(volume_par_secteur)
part_dominant <- ifelse(
  total_arete > 0,
  # mapply() applique une fonction à deux vecteurs en parallèle.
  # Ici : pour chaque arête i, on prend volume_par_secteur[i, idx_dominant[i]]
  mapply(function(i, j) if (is.na(j)) NA else volume_par_secteur[i, j],
         seq_len(nrow(volume_par_secteur)),
         idx_secteur_dominant) / total_arete * 100,
  NA
)

# On attache ces deux colonnes à la géométrie
aretes_dominant_sf <- aretes_geom_base %>%
  mutate(
    secteur_dominant = secteur_dominant,
    part_dominant    = part_dominant
  ) %>%
  filter(!is.na(secteur_dominant))

# PALETTE_SECTEURS est définie dans 00_parametres.R (chargé via source() en début de script).

# Libellé "secteur (xx,x %)" par secteur dominant, part du linéaire des
# arêtes avec trafic — insérée dans la légende comme pour la carte des pentes.
km_par_secteur_dominant <- aretes_dominant_sf %>%
  st_drop_geometry() %>%
  group_by(secteur_dominant) %>%
  summarise(km = sum(length_km, na.rm = TRUE), .groups = "drop")

labels_secteur_dominant_pct <- setNames(
  sprintf("%s (%s %%)", km_par_secteur_dominant$secteur_dominant,
          sub("\\.", ",", sprintf("%.1f", 100 * km_par_secteur_dominant$km / sum(km_par_secteur_dominant$km)))),
  km_par_secteur_dominant$secteur_dominant
)
noms_secteurs_dominants_presents <- intersect(names(PALETTE_SECTEURS), names(labels_secteur_dominant_pct))

aretes_dominant_sf <- aretes_dominant_sf %>%
  mutate(secteur_dominant_pct = factor(
    labels_secteur_dominant_pct[secteur_dominant],
    levels = labels_secteur_dominant_pct[noms_secteurs_dominants_presents]
  ))

palette_secteur_dominant_pct <- setNames(
  PALETTE_SECTEURS[noms_secteurs_dominants_presents],
  labels_secteur_dominant_pct[noms_secteurs_dominants_presents]
)

carte_dominant <- fond_carte() +

  # Réseau de base en gris clair
  tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#EEEEEE", lwd = 0.3) +

  # Arêtes colorées par secteur dominant
  # Largeur proportionnelle au volume total (pas sectoriel) pour garder
  # l'information sur l'intensité globale.
  tm_shape(aretes_dominant_sf) +
  tm_lines(
    col        = "secteur_dominant_pct",
    col.scale  = tm_scale(values = palette_secteur_dominant_pct),
    col.legend = tm_legend(title = "Secteur\ndominant"),
    lwd        = 1.5
  ) +
  
  tm_title("Secteur dominant par arête\n(secteur le plus représenté en tonnes)") +
  tm_credits(
    note_lecture(sprintf(
      "le secteur %s domine sur %d tronçons, plus qu'aucun autre secteur.",
      names(which.max(table(aretes_dominant_sf$secteur_dominant))),
      max(table(aretes_dominant_sf$secteur_dominant))
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_dominant,
  file.path(DIR_CARTES, "carte_secteur_dominant.png"),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ carte_secteur_dominant.png\n\n")

# ── Carte : émissions CO2 affectées sur le réseau ─────────────────────────────
# Cette carte combine l'information de trafic (volume de fret) et d'émissions
# unitaires (co2_kg_par_tkm) pour montrer OÙ les émissions se concentrent.
# Une arête très chargée sur une route plate bien bitumée peut émettre moins
# qu'une arête peu chargée sur une piste pentue en mauvais état.
# C'est l'information de politique publique la plus utile : on y voit où une
# réhabilitation routière (surface → bitumée) aurait le plus grand impact carbone.
aretes_ges <- reseau %>%
  activate("edges") %>%
  st_as_sf() %>%
  filter(emissions_co2_t > 0)

carte_ges_affecte <- fond_carte() +
  tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#DDDDDD", lwd = 0.3) +
  tm_shape(aretes_ges) +
  tm_lines(
    col        = "emissions_co2_t",
    col.scale  = tm_scale_intervals(style = "quantile", n = 5,
                                    values = PALETTE_EMISSIONS),
    col.legend = tm_legend(title = "Émissions CO₂\n(tonnes, cumulées)"),
    lwd        = 1.5
  ) +
  tm_shape(coords_zones_sf) +
  tm_dots(fill = "type",
          fill.scale  = tm_scale(values = PALETTE_TYPE),
          fill.legend = tm_legend(title = "Type"),
          size = 0.5) +
  tm_title("Émissions CO₂ du Fret — Répartition sur le réseau") +
  tm_credits(
    note_lecture(sprintf(
      "le tronçon le plus émetteur cumule %s tonnes de CO₂.",
      format(round(max(aretes_ges$emissions_co2_t)), big.mark = " ")
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(carte_ges_affecte,
          file.path(DIR_CARTES, "carte_emissions_co2_affecte.png"),
          width = 3000, height = 2400, dpi = 300)
cat("✓ Carte émissions CO2 affectées sauvegardée\n\n")


# ============================================================
# GRAPHIQUE : Top 20 des arêtes les plus chargées × composition sectorielle
# Heatmap qui montre, pour les 20 arêtes au plus fort trafic total,
# la répartition sectorielle du tonnage.
# ============================================================
# Ce graphique complète les cartes en quantifiant précisément la
# composition du trafic sur les axes critiques. Utile pour identifier
# les goulots d'étranglement : si une seule arête porte 30% des flux
# d'Agriculture et 20% des flux de Commerce, sa défaillance aurait
# un impact sectoriel multiple.
# ============================================================

cat("Génération de la heatmap top 20 arêtes × secteurs...\n")

# Construction du tableau : une ligne par arête, colonnes = secteurs
# On part de volume_par_secteur_df (construit en Partie VIII.1).
top_aretes_df <- volume_par_secteur_df %>%
  # Ajout d'une colonne avec l'indice de l'arête (pour pouvoir y faire
  # référence plus tard si besoin) et la somme totale.
  mutate(
    arete_id    = row_number(),
    total_t     = rowSums(across(starts_with("vol_t_")))
  ) %>%
  # On ne garde que les 20 arêtes au plus fort trafic total
  arrange(desc(total_t)) %>%
  slice_head(n = 20) %>%
  # On récupère quelques attributs pour enrichir les labels
  left_join(
    aretes_geom_base %>%
      st_drop_geometry() %>%
      mutate(arete_id = row_number()) %>%
      select(arete_id, name, road_type),
    by = "arete_id"
  ) %>%
  # Création d'un label lisible : nom de la route si disponible, sinon ID
  # coalesce(x, y) : renvoie x si non-NA, sinon y (évite d'afficher "NA")
  mutate(
    label_raw = paste0(
      coalesce(name, paste0("Arête #", arete_id)),
      " (", road_type, ")"
    ),
    label_raw = str_trunc(label_raw, 40),
    # Rendre les labels uniques avant de créer le factor
    label = make.unique(label_raw, sep = " #"),
    label = factor(label, levels = rev(label))
  )

# Passage au format long pour ggplot (une ligne = une cellule de la heatmap)
top_aretes_long <- top_aretes_df %>%
  select(label, starts_with("vol_t_")) %>%
  pivot_longer(
    -label,
    names_to  = "Secteur",
    values_to = "Volume_t"
  ) %>%
  # On retire le préfixe "vol_t_" pour avoir un nom de secteur propre dans la légende
  mutate(Secteur = str_remove(Secteur, "^vol_t_"))

# Graphique : heatmap avec valeurs numériques affichées dans les cases
g_top_aretes <- ggplot(top_aretes_long,
                       aes(x = Secteur, y = label, fill = Volume_t)) +
  geom_tile(color = "white", linewidth = 0.4) +
  # Texte dans chaque case : volume en milliers de tonnes, format compact
  geom_text(
    aes(label = ifelse(Volume_t > 0,
                       format(round(Volume_t / 1000, 1), nsmall = 1),
                       "")),
    size  = 2.8,
    color = "black"
  ) +
  scale_fill_gradient(
    low      = "#FFF7EC",
    high     = "#7F0000",
    na.value = "#F5F5F5",
    name     = "Volume\n(tonnes)"
  ) +
  labs(
    title    = "Top 20 des arêtes les plus chargées × composition sectorielle",
    subtitle = "Volumes affichés en milliers de tonnes",
    x        = "Secteur",
    y        = NULL,
    caption  = note_lecture(sprintf(
      "sur l'arête « %s », le secteur %s porte %s tonnes, la case la plus chargée du graphique.",
      top_aretes_long$label[which.max(top_aretes_long$Volume_t)],
      top_aretes_long$Secteur[which.max(top_aretes_long$Volume_t)],
      format(round(max(top_aretes_long$Volume_t)), big.mark = " ")
    ), largeur_car = 144)
  ) +
  theme_minimal(base_size = 10) +
  theme(
    axis.text.x   = element_text(angle = 45, hjust = 1),
    plot.title    = element_text(face = "bold", size = 13),
    plot.subtitle = element_text(color = "#666666"),
    panel.grid    = element_blank()
  ) +
  THEME_NOTE_LECTURE

ggsave(
  file.path(DIR_CARTES, "heatmap_top_aretes_secteurs.png"),
  g_top_aretes,
  width = 12, height = 9.8, dpi = 300
)
cat("  ✓ heatmap_top_aretes_secteurs.png\n\n")


# ============================================================
# GRAPHIQUE : Composition sectorielle × type de route
# Barres empilées montrant, pour chaque type de route (motorway, trunk,
# primary, secondary, tertiary, unclassified), la répartition
# sectorielle du tonnage transporté.
# ============================================================
# Ce graphique répond à la question : "les différents secteurs utilisent-ils
# les mêmes types d'infrastructure, ou y a-t-il une spécialisation ?".
# On s'attend à voir :
#   - Construction et Mines sur les routes primary/trunk (poids lourds)
#   - Agriculture et Agro-industrie sur les routes tertiary/unclassified
#     (dernier kilomètre rural, collecte)
#   - Services et Commerce plus équilibrés
# ============================================================

cat("Génération du graphique composition sectorielle × type de route...\n")

# On attache road_type à chaque arête, puis on agrège les tonnages
# par (type de route, secteur).
compo_par_type_route <- aretes_geom_base %>%
  st_drop_geometry() %>%
  mutate(arete_id = row_number()) %>%
  select(arete_id, road_type) %>%
  # Jointure avec les volumes sectoriels via l'indice de ligne
  bind_cols(volume_par_secteur_df) %>%
  # Passage au format long pour ggplot
  pivot_longer(
    starts_with("vol_t_"),
    names_to  = "Secteur",
    values_to = "Volume_t"
  ) %>%
  mutate(Secteur = str_remove(Secteur, "^vol_t_")) %>%
  # Agrégation par (road_type, Secteur)
  group_by(road_type, Secteur) %>%
  summarise(Volume_t = sum(Volume_t, na.rm = TRUE), .groups = "drop") %>%
  # On ne garde que les types de route avec au moins un peu de trafic
  group_by(road_type) %>%
  filter(sum(Volume_t) > 0) %>%
  ungroup()

# Calcul des parts sectorielles (chaque barre somme à 100%)
compo_par_type_route <- compo_par_type_route %>%
  group_by(road_type) %>%
  mutate(
    total_type = sum(Volume_t),
    part_pct   = Volume_t / total_type * 100
  ) %>%
  ungroup()

# Ordre des types de route : du plus haut niveau (motorway) au plus bas
# (unclassified). factor() avec levels défini impose cet ordre sur l'axe X.
ordre_road_type <- c("motorway", "trunk", "primary", "secondary",
                     "tertiary", "unclassified")
compo_par_type_route <- compo_par_type_route %>%
  mutate(road_type = factor(road_type, levels = ordre_road_type))

g_compo_route <- ggplot(compo_par_type_route,
                        aes(x = road_type, y = part_pct, fill = Secteur)) +
  geom_col(position = "stack", width = 0.7) +
  scale_fill_manual(values = PALETTE_SECTEURS) +
  scale_y_continuous(labels = scales::percent_format(scale = 1)) +
  labs(
    title    = "Composition sectorielle du trafic par type de route",
    subtitle = paste0("Part de chaque secteur dans le tonnage total transporté, ",
                      "par niveau hiérarchique du réseau"),
    x        = "Type de route",
    y        = "Part sectorielle (%)",
    fill     = "Secteur",
    caption  = note_lecture(sprintf(
      "sur les routes %s, le secteur %s représente %s %% du tonnage transporté.",
      compo_par_type_route$road_type[which.max(compo_par_type_route$part_pct)],
      compo_par_type_route$Secteur[which.max(compo_par_type_route$part_pct)],
      round(compo_par_type_route$part_pct[which.max(compo_par_type_route$part_pct)], 1)
    ), largeur_car = 132)
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title    = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#666666"),
    axis.text.x   = element_text(angle = 20, hjust = 1)
  ) +
  THEME_NOTE_LECTURE

ggsave(
  file.path(DIR_CARTES, "graphique_compo_secteurs_type_route.png"),
  g_compo_route,
  width = 11, height = 6.8, dpi = 300
)
cat("  ✓ graphique_compo_secteurs_type_route.png\n\n")


# ============================================================
# GRAPHIQUE : Distribution du volume par arête, par secteur (facet grid)
# Histogrammes en échelle log pour montrer la concentration du trafic
# au sein de chaque secteur.
# ============================================================
# Intérêt : un secteur dont le trafic est concentré sur très peu d'arêtes
# (distribution très asymétrique) est plus vulnérable aux perturbations
# que celui réparti largement. C'est un indicateur qualitatif de résilience.
# L'axe X est en échelle log car la distribution du trafic routier est
# typiquement très asymétrique (loi de puissance : quelques axes portent
# l'essentiel du flux).
# ============================================================

cat("Génération des distributions de trafic par secteur...\n")

distrib_secteurs <- volume_par_secteur_df %>%
  mutate(arete_id = row_number()) %>%
  pivot_longer(
    starts_with("vol_t_"),
    names_to  = "Secteur",
    values_to = "Volume_t"
  ) %>%
  mutate(Secteur = str_remove(Secteur, "^vol_t_")) %>%
  # On ne garde que les arêtes avec un trafic non nul
  # (sinon log10(0) = -Inf et l'histogramme plante)
  filter(Volume_t > 0)

g_distrib <- ggplot(distrib_secteurs, aes(x = Volume_t, fill = Secteur)) +
  geom_histogram(bins = 30, color = "white", linewidth = 0.2) +
  # facet_wrap() : une sous-figure par secteur. scales = "fixed" (par défaut)
  # impose la même échelle X et Y à toutes les sous-figures, pour pouvoir
  # comparer visuellement les secteurs entre eux (hauteur des barres,
  # étendue du volume) sans être trompé par des échelles différentes.
  # ncol calculé pour obtenir une grille la plus carrée possible
  facet_wrap(~ Secteur, ncol = ceiling(sqrt(N_SECTEURS))) +
  scale_x_log10(
    labels = scales::label_number(big.mark = " "),
    breaks = c(1, 10, 100, 1000, 10000, 100000)
  ) +
  scale_fill_manual(values = PALETTE_SECTEURS, guide = "none") +
  labs(
    title    = "Distribution du volume par arête, par secteur",
    subtitle = paste0("Échelle log — une distribution étroite indique une ",
                      "concentration du trafic sur quelques axes"),
    x        = "Volume par arête (tonnes, échelle log)",
    y        = "Nombre d'arêtes",
    caption  = note_lecture(sprintf(
      "le secteur %s compte %d arêtes actives, contre %d pour le secteur %s.",
      names(which.max(table(distrib_secteurs$Secteur))), max(table(distrib_secteurs$Secteur)),
      min(table(distrib_secteurs$Secteur)), names(which.min(table(distrib_secteurs$Secteur)))
    ), largeur_car = 156)
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title    = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#666666"),
    strip.text    = element_text(face = "bold"),
    axis.text.x   = element_text(angle = 30, hjust = 1)
  ) +
  THEME_NOTE_LECTURE

ggsave(
  file.path(DIR_CARTES, "distribution_trafic_par_secteur.png"),
  g_distrib,
  width = 13, height = 7.8, dpi = 300
)
cat("  ✓ distribution_trafic_par_secteur.png\n\n")


# ── Carte : part du camion lourd par arête ────────────────────────────────────
aretes_avec_trafic <- reseau %>%
  activate("edges") %>%
  st_as_sf() %>%
  filter(volume_tonnes > 0)

carte_modal <- fond_carte() +
  tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#DDDDDD", lwd = 0.3) +
  tm_shape(aretes_avec_trafic) +
  tm_lines(
    col       = "part_camion_lourd",
    col.scale = tm_scale_intervals(
      style  = "fixed",
      breaks = c(0, 25, 50, 75, 100),
      values = c("#1A9850","#FEE090","#FC8D59","#D73027")
    ),
    col.legend = tm_legend(title = "Part camion\nlourd (%)"),
    lwd = 1.5
  ) +
  tm_title("Répartition modale — Part du camion lourd") +
  tm_credits(
    note_lecture(sprintf(
      "sur la moitié des tronçons chargés de fret, le camion lourd assure plus de %.0f %% du tonnage transporté.",
      median(aretes_avec_trafic$part_camion_lourd, na.rm = TRUE)
    )),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  ) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left","bottom")) +
  tm_compass(position  = c("right","top"))

tmap_save(carte_modal,
          file.path(DIR_CARTES,"carte_repartition_modale.png"),
          width = 3000, height = 2400, dpi = 300)
cat("✓ Carte répartition modale sauvegardée\n")

# ============================================================
# GRAPHIQUE 1 : Flux par secteur économique
# ============================================================

cat("Génération des graphiques statistiques...\n")

g1 <- flux_par_secteur_df %>%
  ggplot(aes(x = reorder(Secteur, Flux_total_tonnes),
             y = Flux_total_tonnes / 1000,
             fill = Secteur)) +
  geom_col(show.legend = FALSE, width = 0.75) +
  geom_text(aes(label = paste0(format(round(Flux_total_tonnes / 1000, 0),
                                      big.mark = " "), " kt")),
            hjust = -0.1, size = 3.5, color = "#333333") +
  coord_flip(clip = "off") +
  scale_fill_manual(values = PALETTE_SECTEURS) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.15))) +
  labs(
    title    = "Flux commerciaux interzonaux par secteur",
    subtitle = paste0("Modèle gravitaire — ", NOM_PAYS),
    x        = NULL,
    y        = "Flux total inter-zones (milliers de tonnes)",
    caption  = note_lecture(sprintf(
      "le secteur %s totalise %s kt de flux inter-zones, le plus élevé des secteurs de fret.",
      flux_par_secteur_df$Secteur[which.max(flux_par_secteur_df$Flux_total_tonnes)],
      format(round(max(flux_par_secteur_df$Flux_total_tonnes) / 1000), big.mark = " ")
    ), largeur_car = 132)
  ) +
  theme_minimal(base_size = 13) +
  theme(
    plot.title         = element_text(face = "bold", size = 15),
    plot.subtitle      = element_text(color = "#666666"),
    panel.grid.major.y = element_blank(),
    panel.grid.minor   = element_blank()
  ) +
  THEME_NOTE_LECTURE

ggsave(file.path(DIR_CARTES,"graphique_flux_secteurs.png"),
       g1, width = 11, height = 6.8, dpi = 300)
cat("✓ Graphique flux secteurs sauvegardé\n")


# ============================================================
# ORDRE CANONIQUE DES ENTREPÔTS (population décroissante)
# Utilisé partout où TOUS les entrepôts sont affichés (offre/demande par zone,
# heatmap OD, compositions sectorielles à 100%), afin qu'ils soient toujours
# rangés de la même façon : du plus peuplé (en haut) au moins peuplé (en bas).
# pop_i provient de persist_entreposages et est aligné ligne à ligne sur
# offre_zones ; on le nomme par zone pour un appariement robuste PAR NOM (et non
# par position), réutilisable quel que soit l'ordre des lignes de la matrice.
# ============================================================
stopifnot(length(pop_i) == nrow(offre_zones))
pop_par_zone <- setNames(pop_i, rownames(offre_zones))

# Libellé court et lisible d'une zone, identique à celui des graphes de
# composition sectorielle : on retire le suffixe descriptif après " - " (souvent
# le nom de district/secteur administratif) et l'éventuelle parenthèse, puis on
# tronque à 22 caractères. But : des barres lisibles malgré des noms d'entrepôts
# longs, et une troncature cohérente d'un graphique « tous entrepôts » à l'autre.
zone_court <- function(z) str_trunc(str_remove(str_remove(z, " - .*"), " \\(.*"), 22)


# ============================================================
# GRAPHIQUE 2 : Offre vs Demande par zone (en tonnes)
# ============================================================

# ── Version en tonnes ─────────────────────────────────────────────────────────
# Les tables offre_zones et demande_zones dans DuckDB sont en mrd RWF par secteur.
# On les convertit en tonnes via io_table.tonnes_par_mrd_rwf (facteur sectoriel),
# puis on somme sur les secteurs pour obtenir le tonnage total par zone.
recap_zones_tonnes <- duck_query("
  SELECT
    o.zone,
    ROUND(SUM(o.offre_mrd_rwf   * t.tonnes_par_mrd_rwf), 0) AS offre_totale_tonnes,
    ROUND(SUM(d.demande_mrd_rwf * t.tonnes_par_mrd_rwf), 0) AS demande_totale_tonnes
  FROM offre_zones  o
  JOIN demande_zones d ON o.zone = d.zone AND o.secteur = d.secteur
  JOIN io_table     t ON o.secteur = t.secteur
  GROUP BY o.zone
  ORDER BY offre_totale_tonnes DESC
")

ref_offre_t_court   <- zone_court(recap_zones_tonnes$zone[which.max(recap_zones_tonnes$offre_totale_tonnes)])
ref_demande_t_court <- zone_court(recap_zones_tonnes$zone[which.max(recap_zones_tonnes$demande_totale_tonnes)])

g2_tonnes <- recap_zones_tonnes %>%
  pivot_longer(
    cols      = c(offre_totale_tonnes, demande_totale_tonnes),
    names_to  = "Type_flux",
    values_to = "Valeur"
  ) %>%
  mutate(
    Zone_court = zone_court(zone),
    # Population de la zone : sert à ordonner les barres (population décroissante).
    Pop        = pop_par_zone[zone],
    Type_flux  = recode(Type_flux,
                        "offre_totale_tonnes"   = "offre",
                        "demande_totale_tonnes" = "demande")
  ) %>%
  ggplot(aes(x = reorder(Zone_court, Pop),
             y = Valeur / 1000,
             fill = Type_flux)) +
  geom_col(position = "dodge", width = 0.7) +
  geom_col(
    data = ~ filter(., Zone_court == ref_offre_t_court, Type_flux == "offre"),
    aes(x = reorder(Zone_court, Pop), y = Valeur / 1000),
    fill = NA, color = "#1976D2", linewidth = 1.3,
    position = "dodge", width = 0.7,
    inherit.aes = FALSE
  ) +
  geom_col(
    data = ~ filter(., Zone_court == ref_demande_t_court, Type_flux == "demande"),
    aes(x = reorder(Zone_court, Pop), y = Valeur / 1000),
    fill = NA, color = "#D32F2F", linewidth = 1.3,
    position = "dodge", width = 0.7,
    inherit.aes = FALSE
  ) +
  coord_flip() +
  scale_fill_manual(values = c("offre" = "#1976D2", "demande" = "#D32F2F")) +
  scale_y_continuous(labels = scales::label_number(suffix = " kt", big.mark = " ")) +
  labs(
    title    = "Offre et Demande par zone économique (tonnes)",
    subtitle = paste0(
      "Modèle gravitaire — ", NOM_PAYS, "\n",
      "Contour bleu = max offre ('", ref_offre_t_court, "') | ",
      "Contour rouge = max demande ('", ref_demande_t_court, "')"
    ),
    x    = NULL,
    y    = "Valeur (milliers de tonnes)",
    fill = NULL,
    caption = note_lecture(sprintf(
      "la zone %s a la plus forte offre nette, avec %s kt ; la zone %s a la plus forte demande nette, avec %s kt.",
      ref_offre_t_court, format(round(max(recap_zones_tonnes$offre_totale_tonnes) / 1000), big.mark = " "),
      ref_demande_t_court, format(round(max(recap_zones_tonnes$demande_totale_tonnes) / 1000), big.mark = " ")
    ), largeur_car = 156)
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 14),
    plot.subtitle      = element_text(color = "#666666", size = 9),
    # Texte d'axe Y réduit : avec ~91 zones empilées, une petite taille évite que
    # les libellés d'entrepôts ne se chevauchent verticalement.
    axis.text.y        = element_text(size = 7),
    legend.position    = "top",
    panel.grid.major.y = element_blank()
  ) +
  THEME_NOTE_LECTURE

# height = 18 : ~91 zones × 2 barres (dodge) → grande hauteur pour espacer les
# libellés de l'axe Y et garantir leur lisibilité (pas de chevauchement vertical).
ggsave(file.path(DIR_CARTES, "graphique_offre_demande_tonnes.png"),
       g2_tonnes, width = 13, height = 18.8, dpi = 300)
cat("✓ Graphique offre/demande (tonnes) sauvegardé\n")


# ============================================================
# GRAPHIQUE 3 : Heatmap de la matrice OD
# Noms courts uniques via make.unique()
# ============================================================

noms_courts_raw <- noeuds_entreposage$warehouse_name %>%
  str_remove(" - .*") %>%
  str_remove(" \\(.*") %>%
  str_trunc(18)

noms_courts <- make.unique(noms_courts_raw, sep = "_")  # Kigali, Kigali_1, Kigali_2

# Ordre d'affichage des axes : population décroissante (cohérent avec les autres
# graphiques « tous entrepôts »). noms_courts est aligné ligne à ligne sur
# noeuds_entreposage, donc sur pop_i ; order(pop_i, décroissant) donne le bon tri.
ordre_pop_court <- noms_courts[order(pop_i, decreasing = TRUE)]

# flux_total inclut les nœuds RoW (lignes/colonnes au-delà de n_warehouses).
# On extrait uniquement le bloc domestique × domestique pour la heatmap.
n_dom <- length(noms_courts)
flux_dom <- flux_total[seq_len(n_dom), seq_len(n_dom), drop = FALSE]

flux_heatmap <- flux_dom %>%
  as.data.frame() %>%
  setNames(noms_courts) %>%
  mutate(Origine = noms_courts) %>%
  pivot_longer(-Origine, names_to = "Destination", values_to = "Flux") %>%
  mutate(
    Flux_log    = ifelse(Flux > 0, log10(Flux), NA),
    # rev() pour l'axe Y : avec ggplot, le 1er niveau est en bas ; on inverse
    # pour que l'entrepôt le plus peuplé soit en haut. Axe X : plus peuplé à gauche.
    Origine     = factor(Origine,     levels = rev(ordre_pop_court)),
    Destination = factor(Destination, levels = ordre_pop_court)
  )

g3 <- ggplot(flux_heatmap,
             aes(x = Destination, y = Origine, fill = Flux_log)) +
  geom_tile(color = "white", linewidth = 0.4) +
  scale_fill_gradient(
    low      = "#FFF7EC",
    high     = "#7F0000",
    na.value = "#F5F5F5",
    name     = "log₁₀\n(tonnes)"
  ) +
  labs(
    title    = "Matrice des flux commerciaux interzonaux",
    subtitle = paste0("Modèle gravitaire — ", NOM_PAYS, " (log₁₀ tonnes)"),
    x        = "Destination",
    y        = "Origine",
    caption  = note_lecture(sprintf(
      "de %s vers %s, le flux atteint %s tonnes, le plus élevé de la matrice.",
      flux_heatmap$Origine[which.max(flux_heatmap$Flux)],
      flux_heatmap$Destination[which.max(flux_heatmap$Flux)],
      format(round(max(flux_heatmap$Flux, na.rm = TRUE)), big.mark = " ")
    ), largeur_car = 156)
  ) +
  theme_minimal(base_size = 10) +
  theme(
    axis.text.x     = element_text(angle = 45, hjust = 1, size = 8),
    axis.text.y     = element_text(size = 8),
    plot.title      = element_text(face = "bold", size = 13),
    plot.subtitle   = element_text(color = "#666666"),
    panel.grid      = element_blank(),
    legend.position = "right"
  ) +
  THEME_NOTE_LECTURE

ggsave(file.path(DIR_CARTES,"heatmap_flux_od.png"),
       g3, width = 13, height = 11.8, dpi = 300)
cat("✓ Heatmap flux OD sauvegardée\n")


# ============================================================
# GRAPHIQUE 4 : Composition sectorielle de l'offre et de la demande par zone
#
# IMPORTANT — ce que tracent ces graphiques :
#   • offre_zones / demande_zones = soldes NETS par zone (max(0, x−d) et
#     max(0, d−x)). En NET, les secteurs fortement consommés sur place sont
#     « nettés » : la composition est mécaniquement dominée par les secteurs
#     d'export peu absorbés localement (Mines, cultures de rente) côté offre,
#     et par les secteurs nets importateurs côté demande. Ce n'est PAS la
#     composition du fret qui circule, mais celle du surplus / besoin net.
#   • prod_zones / dem_zones = flux BRUTS (production locale et demande totale
#     avant netting). Ils reflètent la vraie structure économique de la zone.
#
# On fournit donc, via deux helpers réutilisables :
#   - une vue NETTE à 100% (toutes les zones) pour offre et demande ;
#   - une vue BRUTE en valeurs absolues, limitée à 3 entrepôts (le plus gros,
#     le médian, le plus petit) pour rester lisible.
# Versions en valeur (mrd RWF) et en tonnage physique (facteur sectoriel
# TONNES_PAR_mrd_RWF, défini dans 00_parametres.R) côté demande.
# ============================================================

cat("Génération des graphiques de composition sectorielle (offre & demande)...\n")

# Facteur de conversion sectoriel mrd RWF → tonnes, aligné sur l'ordre SECTEURS.
tonnes_factor <- TONNES_PAR_mrd_RWF[SECTEURS]
# Convertit une matrice (zones × secteurs) de mrd RWF en tonnes, colonne par colonne.
en_tonnes <- function(mat) sweep(mat, 2, tonnes_factor[colnames(mat)], `*`)

# ── Helper A : composition empilée à 100% par zone (toutes les zones) ─────────
# mat : matrice zones × secteurs (mrd RWF ou tonnes) = composante DOMESTIQUE
# (surplus net offre_zones, ou déficit net demande_zones), tracée en aplat.
# mat_hachure (optionnel) : composante de COMMERCE EXTÉRIEUR empilée par-dessus,
# mêmes couleurs sectorielles mais HACHURÉE (imports côté offre, exports côté
# demande). On normalise à 100% le total (domestique + commerce extérieur) de
# chaque zone. Intérêt : par l'identité ressources-emplois de la SAM,
#   Σ_zones offre + imports = Σ_zones demande + exports   (par secteur),
# donc les graphes offre et demande ont la MÊME composition sectorielle agrégée
# (répartie différemment selon les entrepôts) — on visualise ainsi ce qui passe
# par chaque jambe gravitaire (domestique vs export/import).
# Une barre = une zone, rangée par POPULATION DÉCROISSANTE (plus peuplée en haut)
# pour un ordre identique d'un graphique « tous entrepôts » à l'autre.
# make.unique() évite que deux entrepôts au libellé tronqué identique soient
# fusionnés silencieusement dans une seule barre.
graphe_compo_100 <- function(mat, titre, soustitre, fichier,
                             mat_hachure = NULL,
                             lab_solide = "domestique", lab_hachure = "commerce ext.") {
  noms <- make.unique(str_trunc(str_remove(rownames(mat), " - .*"), 22), sep = "_")
  pop  <- pop_par_zone[rownames(mat)]   # population alignée sur les lignes de mat

  # Passage au format long de la composante domestique (Bloc = solide).
  vers_long <- function(m, bloc) {
    d <- as.data.frame(unname(m)); colnames(d) <- colnames(m); d$Zone <- noms
    pivot_longer(d, -Zone, names_to = "Secteur", values_to = "Valeur") %>%
      mutate(Bloc = bloc)
  }
  df_long <- vers_long(mat, lab_solide)
  if (!is.null(mat_hachure)) df_long <- bind_rows(df_long, vers_long(mat_hachure, lab_hachure))

  df_long <- df_long %>%
    group_by(Zone) %>%
    mutate(tot  = sum(Valeur, na.rm = TRUE),
           Part = ifelse(tot > 0, Valeur / tot * 100, 0)) %>%
    ungroup() %>%
    mutate(
      # Niveaux par population croissante : avec coord_flip(), l'entrepôt le plus
      # peuplé se retrouve en haut (lecture haut→bas = population décroissante).
      Zone    = factor(Zone, levels = noms[order(pop)]),
      Secteur = factor(Secteur, levels = SECTEURS),
      Bloc    = factor(Bloc, levels = c(lab_solide, lab_hachure)),
      # Ordre d'empilement : secteurs d'abord, dans l'ordre SECTEURS, et à
      # l'intérieur de chaque secteur le bloc domestique puis le bloc commerce
      # extérieur → pour un même secteur, la portion domestique et sa portion
      # commerce extérieur se suivent directement dans la barre (au lieu d'être
      # regroupées en deux blocs opposés, tout le domestique puis tout le
      # commerce extérieur).
      ordre   = match(Secteur, SECTEURS) * 10L + (as.integer(Bloc) - 1L)
    )

  # On retire de la légende (et des couleurs affichées) les secteurs dont le
  # tonnage total (toutes zones, domestique + commerce ext. confondus) est nul :
  # ils n'apparaîtraient de toute façon jamais dans les barres, inutile de les
  # lister à côté du graphique.
  secteurs_actifs <- df_long %>%
    group_by(Secteur) %>%
    summarise(tot = sum(Valeur, na.rm = TRUE), .groups = "drop") %>%
    filter(tot > 0) %>%
    pull(Secteur) %>%
    as.character()
  df_long <- df_long %>%
    filter(Secteur %in% secteurs_actifs) %>%
    mutate(Secteur = factor(Secteur, levels = intersect(SECTEURS, secteurs_actifs)))

  if (is.null(mat_hachure)) {
    # Cas sans commerce extérieur : aplat simple (comportement d'origine).
    g <- ggplot(df_long, aes(Zone, Part, fill = Secteur, group = ordre)) +
      geom_col(width = 0.85, position = position_stack(reverse = TRUE))
  } else {
    # Cas avec commerce extérieur : aplat (domestique) + hachures (commerce ext.).
    g <- ggplot(df_long, aes(Zone, Part, fill = Secteur, pattern = Bloc, group = ordre)) +
      geom_col_pattern(
        width           = 0.85,
        position        = position_stack(reverse = TRUE),
        pattern_colour  = NA,          # pas de contour de hachure
        pattern_fill    = "grey15",    # couleur des hachures (sur fond = couleur secteur)
        pattern_density = 0.35,
        pattern_spacing = 0.009,
        pattern_angle   = 45,
        colour = NA, linewidth = 0
      ) +
      scale_pattern_manual(values = setNames(c("none", "stripe"),
                                             c(lab_solide, lab_hachure)),
                           name = NULL) +
      guides(pattern = guide_legend(override.aes = list(fill = "grey75"),
                                    order = 2),
             fill    = guide_legend(order = 1))
  }

  g <- g +
    coord_flip() +
    scale_fill_manual(values = PALETTE_SECTEURS) +
    scale_y_continuous(labels = scales::percent_format(scale = 1)) +
    labs(title = titre, subtitle = soustitre, x = NULL,
         y = "Part dans le total de la zone (%)", fill = "Secteur",
         caption = note_lecture({
           .rmax <- df_long[which.max(df_long$Part), ]
           sprintf("dans la zone %s, le secteur %s représente %s %% du total.",
                   .rmax$Zone, .rmax$Secteur, round(.rmax$Part, 1))
         }, largeur_car = 168)) +
    theme_minimal(base_size = 11) +
    theme(plot.title    = element_text(face = "bold", size = 13),
          plot.subtitle = element_text(color = "#666666", size = 9),
          legend.position = "right") +
    THEME_NOTE_LECTURE
  # Barres plus hautes quand il y a des hachures, pour qu'elles restent visibles.
  ggsave(file.path(DIR_CARTES, fichier), g,
         width = 14, height = if (is.null(mat_hachure)) 8.8 else 11.8, dpi = 300)
  cat("  ✓", fichier, "\n")
}

# ── Helper B : flux bruts OFFRE vs DEMANDE en miroir, centré sur zéro ─────────
# Combine en un seul graphique la production brute x[i,s] (offre, tracée à
# GAUCHE de zéro) et la demande totale brute d[i,s] (demande, tracée à DROITE),
# pour les 3 mêmes entrepôts (indices de ligne fournis dans `sel`, étiquetés
# par `rang` — ceux min/médian/max de la demande totale).
# Pour chaque secteur, la partie COMMUNE aux deux (min(x,d) : ce qui pourrait
# être produit ET consommé sur place, sans passer par le réseau) est tracée en
# APLAT des deux côtés. L'EXCÉDENT du côté qui dépasse — max(0,x−d) côté offre
# (surplus exportable) ou max(0,d−x) côté demande (besoin non couvert
# localement) — est tracé en HACHURÉ, uniquement du côté concerné : c'est
# l'implémentation graphique de offre_zones/demande_zones (le solde net utilisé
# par le modèle gravitaire, cf. 03_transport.R).
# Technique : on empile deux couches geom_col sur le même x. La couche du
# dessous va jusqu'à l'extension TOTALE (x ou d, signée) et porte le motif
# hachuré ; la couche du dessus, opaque et sans hachure, ne va que jusqu'à la
# partie commune. Elle masque donc entièrement la couche du dessous côté
# commun, et ne laisse apparaître le hachuré que sur l'excédent — sans qu'il
# soit nécessaire de gérer explicitement où placer le motif.
# Une facette par zone, échelle libre (scales = "free_x") : la composition
# reste lisible malgré l'écart d'ampleur (le total offre/demande de chaque zone
# est rappelé en titre de facette). unite : libellé d'unité ("mrd RWF" ou
# "tonnes").
graphe_brut3_diverge <- function(mat_offre, mat_demande, unite, sel, rang,
                                  titre, soustitre, fichier,
                                  secteurs_affiches = SECTEURS_FRET) {
  tot_offre   <- rowSums(mat_offre,   na.rm = TRUE)
  tot_demande <- rowSums(mat_demande, na.rm = TRUE)
  noms <- str_trunc(str_remove(rownames(mat_demande), " - .*"), 22)
  lab  <- sprintf("%s — %s  (offre : %s %s · demande : %s %s)",
                  rang, noms[sel],
                  format(round(tot_offre[sel]),   big.mark = " "), unite,
                  format(round(tot_demande[sel]), big.mark = " "), unite)

  off <- mat_offre[sel, secteurs_affiches, drop = FALSE]
  dem <- mat_demande[sel, secteurs_affiches, drop = FALSE]
  # Partie commune aux deux côtés, secteur par secteur et entrepôt par
  # entrepôt : min(offre, demande). Le facteur TONNES_PAR_mrd_RWF étant le même
  # des deux côtés pour un secteur donné, ce min commute avec la conversion en
  # tonnes déjà appliquée en amont (en_tonnes()).
  commun <- pmin(off, dem)

  # Passage au format long ; Valeur est signée : négative pour l'offre (barres
  # vers la gauche après coord_flip), positive pour la demande (vers la droite).
  vers_long <- function(m, cote) {
    d <- as.data.frame(unname(m)); colnames(d) <- secteurs_affiches
    d$Zone <- factor(lab, levels = lab)
    pivot_longer(d, -Zone, names_to = "Secteur", values_to = "Valeur") %>%
      mutate(Cote   = cote,
             Valeur = if (cote == "Offre") -Valeur else Valeur)
  }
  df_ext <- bind_rows(vers_long(off,    "Offre"), vers_long(dem, "Demande"))
  df_int <- bind_rows(vers_long(commun, "Offre"), vers_long(commun, "Demande"))

  # rev(secteurs_affiches) : après coord_flip(), le premier secteur se retrouve en haut.
  ord_secteur <- rev(secteurs_affiches)
  df_ext <- df_ext %>% mutate(Secteur = factor(Secteur, levels = ord_secteur))
  df_int <- df_int %>% mutate(Secteur = factor(Secteur, levels = ord_secteur))

  g <- ggplot() +
    geom_col_pattern(
      data            = df_ext,
      mapping         = aes(Secteur, Valeur, fill = Secteur),
      width           = 0.8,
      pattern         = "stripe",
      pattern_colour  = NA,
      pattern_fill    = "grey15",
      pattern_density = 0.35,
      pattern_spacing = 0.012,
      pattern_angle   = 45,
      colour = NA, linewidth = 0
    ) +
    geom_col(
      data    = df_int,
      mapping = aes(Secteur, Valeur, fill = Secteur),
      width   = 0.8, colour = NA
    ) +
    geom_hline(yintercept = 0, colour = "grey30", linewidth = 0.4) +
    facet_wrap(~ Zone, ncol = 1, scales = "free_x") +
    coord_flip() +
    scale_fill_manual(values = PALETTE_SECTEURS, guide = "none") +
    # scales::number() (plutôt que format()) évite le retour en notation
    # scientifique (1e+06) que format() choisit pour les grands nombres ronds.
    scale_y_continuous(labels = function(v) scales::number(abs(v), accuracy = 1, big.mark = " ")) +
    labs(
      title    = titre,
      subtitle = soustitre,
      x        = NULL,
      y        = paste0("Offre (", unite, ")   ←        →   Demande (", unite, ")"),
      caption  = note_lecture(sprintf(
        paste0("partie pleine = min(offre, demande) par secteur (satisfaisable ",
               "localement) ; partie hachurée = excédent du côté qui dépasse ",
               "(surplus exportable côté offre, besoin non couvert localement ",
               "côté demande). Pour %s, le secteur %s a l'excédent le plus élevé, ",
               "%s %s."),
        as.character(df_ext$Zone[which.max(abs(df_ext$Valeur) - abs(df_int$Valeur))]),
        df_ext$Secteur[which.max(abs(df_ext$Valeur) - abs(df_int$Valeur))],
        format(round(max(abs(df_ext$Valeur) - abs(df_int$Valeur))), big.mark = " "),
        unite
      ), largeur_car = 132)
    ) +
    theme_minimal(base_size = 11) +
    theme(plot.title    = element_text(face = "bold", size = 13),
          plot.subtitle = element_text(color = "#666666", size = 9),
          strip.text    = element_text(face = "bold")) +
    THEME_NOTE_LECTURE
  ggsave(file.path(DIR_CARTES, fichier), g, width = 11, height = 8.8, dpi = 300)
  cat("  ✓", fichier, "\n")
}

# Composante de commerce extérieur (hachurée) ajoutée si e_zones/m_zones sont
# persistés (03_transport.R) : imports côté offre, exports côté demande. Par
# l'identité SAM Σoffre+imports = Σdemande+exports, les graphes offre et demande
# ont alors la même composition sectorielle agrégée (sur des entrepôts différents).
hach_ok <- exists("e_zones") && exists("m_zones")
if (!hach_ok) cat("  ⚠ e_zones/m_zones absents — relancer 03_transport.R pour les hachures commerce ext.\n")

# ── 1) OFFRE — surplus net (aplat) + imports (hachuré), à 100%, en tonnage ────
graphe_compo_100(
  en_tonnes(offre_zones),
  "Composition sectorielle — surplus net (aplat) + imports (hachuré), par zone — tonnes",
  paste0("Modèle MRIO — ", NOM_PAYS,
         " · surplus domestique + imports, convertis en tonnes (facteur TONNES_PAR_mrd_RWF)"),
  "graphique_offre_composition_tonnes.png",
  mat_hachure = if (hach_ok) en_tonnes(m_zones) else NULL,
  lab_solide = "surplus domestique", lab_hachure = "imports"
)

# ── 2) DEMANDE — déficit net (aplat) + exports (hachuré), à 100%, en tonnage ──
graphe_compo_100(
  en_tonnes(demande_zones),
  "Composition sectorielle — déficit net (aplat) + exports (hachuré), par zone — tonnes",
  paste0("Modèle MRIO — ", NOM_PAYS,
         " · déficit domestique + exports, convertis en tonnes (facteur TONNES_PAR_mrd_RWF)"),
  "graphique_demande_composition_tonnes.png",
  mat_hachure = if (hach_ok) en_tonnes(e_zones) else NULL,
  lab_solide = "déficit domestique", lab_hachure = "exports"
)

# ── 4) à 6) FLUX BRUTS sur 3 entrepôts FIXES : ceux min/médian/max de la DEMANDE
# Mêmes 3 entrepôts affichés côté production (offre brute x) et côté demande
# (demande brute d), pour visualiser directement l'effet de max(0, d − x).
# Nécessite prod_zones ET dem_zones (bruts) persistés par 03_transport.R.
if (exists("prod_zones") && exists("dem_zones")) {
  # Zones à population nulle exclues de la sélection : ce sont les postes-
  # frontière "passage" (sans cellule de Voronoï propre, cf. 01_reseau.R IV.6),
  # dont le profil offre/demande n'est pas représentatif d'un territoire habité.
  # diag_population est aligné ligne à ligne sur noeuds_entreposage, donc sur
  # les lignes de prod_zones/dem_zones (mêmes indices 1..n_warehouses).
  zones_peuplees <- which(diag_population$population_zone > 0)

  # Sélection par la demande totale brute (rowSums dem_zones) : max, médiane, min,
  # uniquement parmi les zones peuplées.
  dem_tot  <- rowSums(dem_zones, na.rm = TRUE)
  ord_d    <- zones_peuplees[order(dem_tot[zones_peuplees])]
  sel_dem  <- c(ord_d[length(ord_d)], ord_d[ceiling(length(ord_d) / 2)], ord_d[1])
  rang_dem <- c("Demande max", "Demande médiane", "Demande min")

  # 4) OFFRE (production locale x) vs DEMANDE (demande totale d) en miroir,
  # centré sur zéro, sur les 3 mêmes entrepôts — tonnes.
  # secteurs_affiches = SECTEURS_FRET (défaut) : on n'affiche que les secteurs
  # qui échangent effectivement du tonnage sur le réseau (TONNES_PAR_mrd_RWF >
  # 0), pour ne pas polluer le graphe de barres à zéro.
  graphe_brut3_diverge(
    en_tonnes(prod_zones), en_tonnes(dem_zones), "tonnes", sel_dem, rang_dem,
    "Flux brut par secteur — offre (production locale) vs demande (entrepôts min/médian/max de la demande) — tonnes",
    paste0("Modèle MRIO — ", NOM_PAYS,
           " · production et demande brutes converties en tonnes (facteur sectoriel TONNES_PAR_mrd_RWF)"),
    "graphique_offre_demande_brut_tonnes_3entrepots.png"
  )
} else {
  cat("  ⚠ prod_zones/dem_zones absents du persist — relancer 03_transport.R pour les graphes bruts\n")
}

cat("✓ Graphiques de composition sectorielle (offre & demande) sauvegardés\n\n")

# ==============================================================================
# DIAGRAMME DE SANKEY — Flux de fret : Origine → Secteur → Destination
# ==============================================================================

cat("Génération du diagramme de Sankey...\n")

# ── Agrégation par (district origine × secteur × district destination) ──────
# On travaille à l'échelle des DISTRICTS administratifs (warehouse_district,
# 01_reseau.R) plutôt que des 120 zones individuelles, pour garantir la
# lisibilité du diagramme tout en restant à un niveau géographique plus fin et
# plus parlant que le type de zone (frontière, industrie, etc.).
# Les indices de ligne/colonne de flux_gravitaire[[s]] correspondent
# directement aux lignes de noeuds_entreposage (construit en IV.3).

sankey_raw <- map_dfr(SECTEURS_FRET, function(s) {
  mat_s <- flux_gravitaire[[s]]
  # which() avec arr.ind = TRUE retourne une matrice à 2 colonnes :
  # colonne 1 = indice de ligne (origine), colonne 2 = indice de colonne (destination)
  idx <- which(mat_s > 0 & row(mat_s) != col(mat_s), arr.ind = TRUE)
  if (nrow(idx) == 0) return(tibble())
  tibble(
    flux_t               = mat_s[idx],
    district_origine     = noeuds_entreposage$warehouse_district[idx[, 1]],
    district_destination = noeuds_entreposage$warehouse_district[idx[, 2]],
    secteur              = s
  )
}) %>%
  group_by(district_origine, secteur, district_destination) %>%
  summarise(flux_t = sum(flux_t, na.rm = TRUE), .groups = "drop") %>%
  # Seuil de lisibilité : on ne garde que les flux représentant au moins
  # 0.1% du tonnage total pour éviter les micro-rubans illisibles
  filter(flux_t > sum(flux_t) * 0.001)

# Ordre des districts sur les axes 1 et 3 : décroissant selon le volume total
# transporté (origine + destination confondues), pour que les plus gros
# centres logistiques apparaissent en haut du diagramme.
ordre_districts <- sankey_raw %>%
  pivot_longer(cols = c(district_origine, district_destination),
               values_to = "district") %>%
  group_by(district) %>%
  summarise(flux_t = sum(flux_t, na.rm = TRUE), .groups = "drop") %>%
  arrange(desc(flux_t)) %>%
  pull(district)

sankey_raw <- sankey_raw %>%
  mutate(
    district_origine     = factor(district_origine,     levels = ordre_districts),
    secteur              = factor(secteur,               levels = SECTEURS),
    district_destination = factor(district_destination, levels = ordre_districts)
  )

# ── Part urbaine par district, pour colorer les blocs des axes 1 et 3 ────────
# part_urbaine[i] = population du nœud-entrepôt i résidant dans le masque
# urbain retenu par le modèle (01_reseau.R, Partie IV.5.B), rapportée à sa
# population totale. pop_groupe_zone (chargé depuis persist_entreposages.rds)
# donne la population par nœud × groupe SAM (colonnes u1..u5 = quintiles
# urbains, r1..r5 = quintiles ruraux) ; ses lignes sont dans le même ordre que
# noeuds_entreposage, ce qui permet de rattacher directement chaque nœud à son
# warehouse_district.
# La part urbaine du district = somme de la population urbaine de ses nœuds /
# somme de sa population totale (moyenne pondérée par la population de chaque
# nœud, pas une simple moyenne des parts individuelles).
part_urbaine_district <- tibble(
  district    = noeuds_entreposage$warehouse_district,
  pop_urbaine = rowSums(pop_groupe_zone[, paste0("u", 1:5), drop = FALSE]),
  pop_totale  = rowSums(pop_groupe_zone)
) %>%
  group_by(district) %>%
  summarise(part_urbaine = sum(pop_urbaine) / sum(pop_totale), .groups = "drop")

# Vecteur nommé district → part urbaine, utilisé plus bas via after_stat()
# pour colorer uniquement les blocs des axes district (les blocs de l'axe
# secteur ne matchent aucun nom et reçoivent NA, donc la couleur neutre
# na.value du gradient).
LOOKUP_PART_URBAINE_DISTRICT <- setNames(
  part_urbaine_district$part_urbaine,
  part_urbaine_district$district
)

# Vecteur nommé district → couleur de texte (blanc sur fond bleu foncé quand
# la part urbaine dépasse 50 %, gris foncé sinon), pour que le nom du district
# reste lisible quel que soit le remplissage du bloc.
LOOKUP_TEXTE_DISTRICT <- setNames(
  ifelse(part_urbaine_district$part_urbaine > 0.5, "white", "#222222"),
  part_urbaine_district$district
)

g_sankey <- ggplot(
  sankey_raw,
  aes(
    axis1 = district_origine,
    axis2 = secteur,
    axis3 = district_destination,
    y     = flux_t / 1000        # Conversion en milliers de tonnes
  )
) +
  # Rubans : chaque ruban = un flux (district_origine, secteur, district_destination)
  # curve_type = "cubic" donne des courbes de Bézier lisses
  geom_alluvium(
    aes(fill = secteur),
    width      = 1/5,
    alpha      = 0.65,
    curve_type = "cubic"
  ) +
  scale_fill_manual(values = PALETTE_SECTEURS, name = "Secteur") +
  # ggnewscale::new_scale_fill() : ouvre une deuxième échelle de couleur pour
  # les couches suivantes, indépendante de celle des rubans (secteur). Sans
  # cet appel, un seul ggplot ne peut pas avoir à la fois un fill discret
  # (secteur) et un fill continu (part urbaine) sur le même aesthetic "fill".
  ggnewscale::new_scale_fill() +
  # Blocs : un par valeur unique sur chaque axe. LOOKUP_PART_URBAINE_DISTRICT[stratum]
  # ne renvoie une valeur que pour les blocs des axes district (axis1/axis3) ;
  # les blocs de l'axe secteur (axis2) ne matchent aucun nom de district et
  # reçoivent NA, donc la couleur na.value du gradient ci-dessous.
  geom_stratum(
    aes(fill = after_stat(LOOKUP_PART_URBAINE_DISTRICT[as.character(stratum)])),
    width     = 1/5,
    color     = "#333333",
    linewidth = 0.4
  ) +
  # after_stat(stratum) : récupère le nom de la strate depuis le stat interne.
  # Taille de police réduite car les axes district comptent une trentaine de
  # strates (un par district administratif). Couleur de texte définie via
  # LOOKUP_TEXTE_DISTRICT (blanc/gris foncé) pour rester lisible sur le
  # gradient de part urbaine ; replace_na() couvre les blocs de l'axe secteur,
  # qui ne matchent aucun nom de district.
  geom_text(
    stat     = "stratum",
    aes(
      label  = after_stat(stratum),
      colour = after_stat(replace_na(
        LOOKUP_TEXTE_DISTRICT[as.character(stratum)], "#222222"
      ))
    ),
    size     = 2.5,
    fontface = "bold"
  ) +
  scale_colour_identity(guide = "none") +
  # Notation explicitée dans le titre de légende : la part urbaine est un
  # ratio de population (population résidant dans le masque urbain du modèle
  # ÷ population totale du district), affiché en pourcentage entre 0 et 100 %.
  # na.value = couleur des blocs de l'axe secteur (non concernés par cette
  # échelle) et des districts sans warehouse_district connu (GADM indisponible).
  scale_fill_gradient(
    low      = "#FFFFFF",
    high     = "#08306B",
    na.value = "white",
    limits   = c(0, 1),
    labels   = scales::percent,
    name     = "Part urbaine\n(pop. en zone urbaine\n÷ pop. totale du district)"
  ) +
  scale_x_discrete(
    limits = c("district_origine", "secteur", "district_destination"),
    labels = c("District d'origine", "Secteur", "District de destination"),
    expand = expansion(add = 0.15)
  ) +
  scale_y_continuous(
    labels = scales::label_number(suffix = " kt"),
    name   = "Volume (milliers de tonnes)"
  ) +
  labs(
    title    = "Flux de fret interzonaux — Diagramme de Sankey",
    subtitle = paste0(
      "Agrégation par district et secteur économique · ",
      format(round(sum(sankey_raw$flux_t) / 1e6, 1)), " Mt modélisées"
    ),
    x = NULL,
    caption = note_lecture(sprintf(
      "le plus gros ruban relie le district « %s », via le secteur %s, au district « %s », avec %s kt.",
      sankey_raw$district_origine[which.max(sankey_raw$flux_t)],
      sankey_raw$secteur[which.max(sankey_raw$flux_t)],
      sankey_raw$district_destination[which.max(sankey_raw$flux_t)],
      format(round(max(sankey_raw$flux_t) / 1000), big.mark = " ")
    ), largeur_car = 168)
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title      = element_text(face = "bold", size = 14),
    plot.subtitle   = element_text(color = "#666666"),
    panel.grid      = element_blank(),
    axis.text.x     = element_text(size = 11, face = "bold"),
    axis.text.y     = element_text(size = 9),
    legend.position = "right"
  ) +
  THEME_NOTE_LECTURE

# Hauteur de 12 pouces : la trentaine de districts sur les axes 1 et 3 a besoin
# de cet espace vertical pour rester lisible.
ggsave(
  file.path(DIR_CARTES, "sankey_flux_fret.png"),
  g_sankey,
  width  = 14,
  height = 12,
  dpi    = 300
)
cat("✓ sankey_flux_fret.png\n\n")


# ============================================================
# GRAPHIQUE : Production locale vs demande par secteur (bilan MRIO)
#
# Objectif : visualiser pour chaque secteur l'écart entre la production
# nationale totale et la demande totale (intermédiaire + finale), issu
# du modèle MRIO. L'écart = surplus exportable net vers les autres zones.
# ============================================================

cat("Génération du graphique production vs demande (bilan MRIO)...\n")

# ── Calcul des agrégats sectoriels depuis DuckDB ──────────────────────────────
# On relit les tables offre_zones et demande_zones pour obtenir les totaux
# par secteur (somme sur toutes les zones), reflet direct du bilan MRIO.
bilan_mrio <- duck_query("
  SELECT
    o.secteur,
    ROUND(SUM(o.offre_mrd_rwf),   2) AS surplus_total_mrd_rwf,
    ROUND(SUM(d.demande_mrd_rwf), 2) AS deficit_total_mrd_rwf
  FROM offre_zones  o
  JOIN demande_zones d ON o.zone = d.zone AND o.secteur = d.secteur
  GROUP BY o.secteur
  ORDER BY surplus_total_mrd_rwf DESC
")

# Imports et exports SAM par secteur (mrd RWF), issus de sam calculé dans
# 00_parametres.R. Les imports entrent dans l'offre totale (supply-side), les
# exports dans la demande totale (demand-side) : avec cette convention, les deux
# colonnes sont égales par construction (équilibre comptable de la SAM).
commerce_sam <- tibble(
  secteur     = SECTEURS,
  imports_sam = as.numeric(sam$imports[SECTEURS]),
  exports_sam = as.numeric(sam$exports[SECTEURS])
)

df_mrio <- bilan_mrio %>%
  left_join(commerce_sam, by = "secteur") %>%
  mutate(
    # Offre totale = surplus domestique inter-zones + imports internationaux
    offre_totale_mrd_rwf   = surplus_total_mrd_rwf + imports_sam,
    # Demande totale = déficit domestique inter-zones + exports internationaux
    demande_totale_mrd_rwf = deficit_total_mrd_rwf + exports_sam,
    # Écart résiduel : doit être ≈ 0 pour chaque secteur si la SAM est équilibrée
    ecart_mrd_rwf = offre_totale_mrd_rwf - demande_totale_mrd_rwf,
    Secteur = factor(secteur, levels = secteur[order(offre_totale_mrd_rwf)])
  )

# ── Format long pour ggplot : 4 composantes ───────────────────────────────────
# Chaque secteur donne 4 lignes (surplus, imports, déficit, exports).
# On calcule une position X décalée manuellement pour grouper les deux barres
# côte à côte (offre à gauche, demande à droite) tout en les empilant :
# ggplot2 ne supporte pas nativement stacked+grouped, d'où le décalage manuel.
DEMI_LARGEUR <- 0.2  # demi-écart entre les deux barres (en unités d'axe)

df_mrio_comp <- df_mrio %>%
  select(Secteur, ecart_mrd_rwf, offre_totale_mrd_rwf, demande_totale_mrd_rwf,
         surplus_total_mrd_rwf, imports_sam, deficit_total_mrd_rwf, exports_sam) %>%
  pivot_longer(
    c(surplus_total_mrd_rwf, imports_sam, deficit_total_mrd_rwf, exports_sam),
    names_to = "composante", values_to = "valeur"
  ) %>%
  mutate(
    cote = if_else(composante %in% c("surplus_total_mrd_rwf", "imports_sam"),
                   "Offre", "Demande"),
    composante = recode(composante,
      "surplus_total_mrd_rwf" = "Surplus inter-zones",
      "imports_sam"           = "Imports SAM",
      "deficit_total_mrd_rwf" = "Déficit inter-zones",
      "exports_sam"           = "Exports SAM"
    ),
    # Ordre d'empilement : composante domestique en bas, commerce en haut
    composante = factor(composante,
      levels = c("Surplus inter-zones", "Imports SAM",
                 "Déficit inter-zones", "Exports SAM")),
    x_num = as.numeric(Secteur),
    x_pos = x_num + if_else(cote == "Offre", -DEMI_LARGEUR, DEMI_LARGEUR)
  )

# Couleurs : bleu foncé / bleu clair pour l'offre, rouge foncé / orange pour la demande
COULEURS_COMPOSANTES <- c(
  "Surplus inter-zones" = "#1565C0",
  "Imports SAM"         = "#90CAF9",
  "Déficit inter-zones" = "#C62828",
  "Exports SAM"         = "#FFAB91"
)

# ── Graphique ─────────────────────────────────────────────────────────────────
g_prod_ech <- ggplot(df_mrio_comp,
                     aes(x = x_pos, y = valeur, fill = composante)) +
  geom_col(width = DEMI_LARGEUR * 2 * 0.9, position = "stack") +
  # Annotation de l'écart résiduel au bout de la barre demande (côté droit).
  # Doit être ≈ 0 ; un écart non nul signale un déséquilibre de calibration.
  geom_text(
    data = df_mrio %>% mutate(x_pos = as.numeric(Secteur) + DEMI_LARGEUR),
    aes(x     = x_pos,
        y     = demande_totale_mrd_rwf,
        label = paste0(ifelse(ecart_mrd_rwf >= 0, "+", ""),
                       round(ecart_mrd_rwf, 1))),
    hjust       = -0.15,
    size        = 3.0,
    color       = "#555555",
    fontface    = "italic",
    inherit.aes = FALSE
  ) +
  # Axe Y converti en axe X après coord_flip : labels = noms des secteurs
  scale_x_continuous(
    breaks = seq_len(nlevels(df_mrio$Secteur)),
    labels = levels(df_mrio$Secteur)
  ) +
  coord_flip(clip = "off") +
  scale_fill_manual(values = COULEURS_COMPOSANTES, name = NULL) +
  scale_y_continuous(
    labels = scales::label_number(suffix = " mrd RWF", scale = 1),
    expand = expansion(mult = c(0, 0.20))
  ) +
  labs(
    title    = "Bilan MRIO complet par secteur — offre vs demande totales",
    subtitle = paste0(
      "Gauche : surplus inter-zones (bleu foncé) + imports SAM (bleu clair)  —  ",
      "Droite : déficit inter-zones (rouge) + exports SAM (orange).\n",
      "Annotation = écart résiduel (mrd RWF) ; doit être ≈ 0 si la SAM est équilibrée."
    ),
    x = NULL,
    y = "Volume agrégé (mrd RWF)",
    caption = note_lecture(sprintf(
      "pour le secteur %s, l'offre totale atteint %s mrd RWF, contre %s mrd RWF de demande totale.",
      df_mrio$Secteur[which.max(df_mrio$offre_totale_mrd_rwf)],
      round(df_mrio$offre_totale_mrd_rwf[which.max(df_mrio$offre_totale_mrd_rwf)], 0),
      round(df_mrio$demande_totale_mrd_rwf[which.max(df_mrio$offre_totale_mrd_rwf)], 0)
    ), largeur_car = 144)
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 14),
    plot.subtitle      = element_text(color = "#666666", size = 9),
    legend.position    = "top",
    panel.grid.major.y = element_blank()
  ) +
  THEME_NOTE_LECTURE

ggsave(
  file.path(DIR_CARTES, "graphique_bilan_mrio.png"),
  g_prod_ech,
  width  = 12,
  height = 7.8,
  dpi    = 300
)
cat("✓ graphique_bilan_mrio.png\n\n")

################################################################################
# VIII.9 — Validation SAM : coût de transport impliqué par le modèle vs trc
#
# Principe (issu de la discussion méthodologique sur les prix de base) :
#   Si le modèle est cohérent avec la comptabilité nationale, alors :
#   Σ_ij T_ij × c_ij ≈ trc_SAM
#   où T_ij = flux en tonnes entre zones i et j (modèle gravitaire),
#        c_ij = coût routier Dijkstra en RWF/tonne (matrice_od DuckDB),
#     trc_SAM = marges de distribution de la SAM IFPRI 2021 (mrd RWF).
#
#   Le modèle ne captant que les coûts de transport routier domestique,
#   on attend coût_modèle < trc_SAM. L'écart représente :
#     - les marges de commerce (grossiste, détail) — non modélisées,
#     - les coûts logistiques import/export (hors réseau domestique).
#   Le ratio coût_modèle / trc_SAM est un indicateur de cohérence interne.
#
# Produit deux graphiques :
#   graphique_validation_trc.png          — comparaison globale modèle vs SAM
#   graphique_validation_trc_secteurs.png — décomposition sectorielle du coût
################################################################################

cat("=== VIII.9 : Validation SAM (coût transport vs trc) ===\n")

# ── Coûts O-D depuis DuckDB ──────────────────────────────────────────────────
# cout_rwf = coût Dijkstra en RWF par tonne transportée entre les zones i et j.
# Calculé dans 03_transport.R pour le véhicule optimal (plus court chemin).
od_couts_val <- duck_query("
  SELECT nom_origine, nom_destination, cout_rwf
  FROM matrice_od
  WHERE cout_rwf > 0
")

# ── Valeur trc depuis la SAM ──────────────────────────────────────────────────
# trc = compte « Transaction costs » de la SAM IFPRI 2021.
# Il agrège TOUTES les marges versées entre producteur et acheteur :
# transport routier + grossiste + détail. C'est la borne supérieure naturelle
# du coût de transport modélisable.
.brut_val      <- as.data.frame(readxl::read_excel(
  SAM_XLSX_PATH, sheet = SAM_FEUILLE, col_names = FALSE, .name_repair = "minimal"
))
.codes_col_val <- as.character(unlist(.brut_val[1, ], use.names = FALSE))
.codes_row_val <- as.character(.brut_val[[2]])
.num_val       <- function(r, c) {
  v <- suppressWarnings(as.numeric(.brut_val[r, c]))
  if (is.na(v)) 0 else v
}
trc_sam_mrd <- .num_val(
  which(.codes_row_val == SAM_COMPTE_MARGES)[1],
  which(.codes_col_val == "total")[1]
)
rm(.brut_val, .codes_col_val, .codes_row_val, .num_val)
cat("  trc SAM IFPRI 2021 :", round(trc_sam_mrd, 1), "mrd RWF\n")

# ── Coût de transport modélisé, décomposé par secteur ────────────────────────
# Pour chaque secteur s avec fret physique (TONNES_PAR_mrd_RWF[s] > 0) :
#   T_s[i,j] est directement en tonnes (flux_gravitaire contient des tonnes,
#   la conversion mrd RWF → tonnes est effectuée dans 03_transport.R avant
#   la persistance — ne pas multiplier à nouveau par TONNES_PAR_mrd_RWF).
#   Coût_s (mrd RWF) = Σ_ij T_s[i,j] (t) × c_ij (RWF/t) / 1e9
# Les paires O-D sans route connue (RoW, zones non connectées) sont exclues
# par le filtre !is.na(cout_rwf) après la jointure gauche.
flux_sec_val <- dplyr::bind_rows(lapply(SECTEURS, function(s) {
  if (TONNES_PAR_mrd_RWF[s] == 0) return(NULL)
  mat <- flux_gravitaire[[s]]   # déjà en tonnes
  as.data.frame(as.table(mat), stringsAsFactors = FALSE) %>%
    rename(nom_origine = Var1, nom_destination = Var2, flux_t = Freq) %>%
    filter(flux_t > 0, nom_origine != nom_destination) %>%
    mutate(secteur = s)
})) %>%
  left_join(od_couts_val, by = c("nom_origine", "nom_destination")) %>%
  filter(!is.na(cout_rwf)) %>%
  mutate(cout_mrd = flux_t * cout_rwf / 1e9)

# Agrégation par secteur, triée par coût décroissant pour lecture du graphique
cout_sec_df <- flux_sec_val %>%
  group_by(secteur) %>%
  summarise(cout_mrd = sum(cout_mrd, na.rm = TRUE), .groups = "drop") %>%
  arrange(desc(cout_mrd)) %>%
  mutate(secteur = factor(secteur, levels = secteur))

cout_model_mrd  <- sum(cout_sec_df$cout_mrd)
part_transport  <- cout_model_mrd / trc_sam_mrd * 100
cat("  Coût transport modélisé :", round(cout_model_mrd, 1), "mrd RWF\n")
cat("  Part du trc SAM         :", round(part_transport, 1), "%\n\n")

# ── Graphique 1 — Comparaison globale : modèle vs trc SAM ────────────────────
# Deux barres côte à côte montrant le coût total modélisé (bleu) et le trc SAM
# (rouge). L'écart visuel illustre la part non modélisée (marges de commerce).
df_compar_val <- tibble(
  source = factor(
    c("trc SAM\n(transport + commerce)", "Coût transport\nréseau modélisé"),
    levels = c("trc SAM\n(transport + commerce)", "Coût transport\nréseau modélisé")
  ),
  valeur  = c(trc_sam_mrd, cout_model_mrd),
  couleur = c("#E57373", "#42A5F5")
)

g_valid_compar <- ggplot(df_compar_val, aes(x = source, y = valeur, fill = source)) +
  geom_col(width = 0.5, show.legend = FALSE) +
  geom_text(
    aes(label = paste0(round(valeur, 0), " mrd RWF")),
    vjust = -0.5, size = 4.5, fontface = "bold"
  ) +
  # Ligne de référence horizontale au niveau du coût modélisé,
  # pour lire visuellement la part qu'il représente dans le trc SAM.
  geom_hline(
    yintercept = cout_model_mrd,
    linetype   = "dashed",
    color      = "#42A5F5",
    linewidth  = 0.6
  ) +
  annotate(
    "text",
    x     = 0.55,
    y     = cout_model_mrd * 1.08,
    label = paste0(round(part_transport, 1), "% du trc"),
    color = "#1565C0",
    size  = 3.8,
    fontface = "italic",
    hjust = 0
  ) +
  scale_fill_manual(values = c("#E57373", "#42A5F5")) +
  scale_y_continuous(
    labels = scales::label_number(suffix = " mrd RWF"),
    expand = expansion(mult = c(0, 0.15))
  ) +
  labs(
    title    = "Validation SAM : coût de transport impliqué vs marges trc",
    subtitle = paste0(
      "trc SAM IFPRI 2021 = marges de distribution totales ",
      "(transport routier + marges grossiste/détail).\n",
      "Le modèle ne capturant que le transport routier domestique, ",
      "coût modélisé < trc est attendu."
    ),
    x = NULL,
    y = "Valeur (mrd RWF)",
    caption = note_lecture(sprintf(
      "le coût de transport modélisé, %s mrd RWF, représente %s %% des marges trc de la SAM.",
      round(cout_model_mrd, 0), round(part_transport, 1)
    ), largeur_car = 96)
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title    = element_text(face = "bold", size = 14),
    plot.subtitle = element_text(color = "#666666", size = 9),
    axis.text.x   = element_text(size = 11)
  ) +
  THEME_NOTE_LECTURE

ggsave(
  file.path(DIR_CARTES, "graphique_validation_trc.png"),
  g_valid_compar,
  width  = 8,
  height = 6.8,
  dpi    = 300
)
cat("✓ graphique_validation_trc.png\n")

# ── Graphique 2 — Décomposition sectorielle du coût modélisé ─────────────────
# Barres horizontales montrant la contribution de chaque secteur au coût total.
# Utilise PALETTE_SECTEURS (défini dans 00_parametres.R) pour la cohérence
# visuelle avec les autres graphiques du projet.
g_valid_sec <- ggplot(cout_sec_df,
                      aes(x = secteur, y = cout_mrd,
                          fill = as.character(secteur))) +
  geom_col(show.legend = FALSE) +
  geom_text(
    aes(label = paste0(round(cout_mrd, 1), " mrd")),
    hjust = -0.1,
    size  = 3.5
  ) +
  coord_flip(clip = "off") +
  scale_fill_manual(values = PALETTE_SECTEURS) +
  scale_y_continuous(
    labels = scales::label_number(suffix = " mrd RWF"),
    expand = expansion(mult = c(0, 0.22))
  ) +
  labs(
    title    = "Décomposition sectorielle du coût de transport modélisé",
    subtitle = paste0(
      "Total modélisé : ", round(cout_model_mrd, 1), " mrd RWF",
      "  (", round(part_transport, 1), "% du trc SAM).\n"
    ),
    x = NULL,
    y = "Coût de transport (mrd RWF)",
    caption = note_lecture(sprintf(
      "le secteur %s représente %s mrd RWF de coût de transport, le plus élevé des secteurs de fret.",
      cout_sec_df$secteur[which.max(cout_sec_df$cout_mrd)],
      round(max(cout_sec_df$cout_mrd), 1)
    ), largeur_car = 120)
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 14),
    plot.subtitle      = element_text(color = "#666666", size = 9),
    panel.grid.major.y = element_blank()
  ) +
  THEME_NOTE_LECTURE

ggsave(
  file.path(DIR_CARTES, "graphique_validation_trc_secteurs.png"),
  g_valid_sec,
  width  = 10,
  height = 6.8,
  dpi    = 300
)
cat("✓ graphique_validation_trc_secteurs.png\n\n")

################################################################################
# VIII.10 — Carte des classes géo-sociales
#
# OBJECTIF : Visualiser la segmentation géo-sociale du territoire utilisée par
# le modèle MRIO pour spatialiser la demande finale des ménages.
#
# CE QUE REPRÉSENTE LA CARTE : depuis le passage à la classification au PIXEL
# (01_reseau.R IV.5.B), une cellule de Voronoï ne porte plus UN groupe SAM mais
# un MÉLANGE : sa population est répartie entre les 10 groupes (strate urbain /
# rural × quintile national de consommation). On produit donc deux vues :
#   • carte_classes_geosociales      : le groupe MAJORITAIRE de chaque cellule
#     (lecture catégorielle, comparable aux versions précédentes de la figure) ;
#   • carte_quintile_moyen           : le quintile MOYEN pondéré par la
#     population, qui restitue le dégradé continu que produit la nouvelle
#     méthode et que la carte catégorielle écrase.
#
# Le RWI (Relative Wealth Index) mesure le niveau de vie relatif ; il sert de
# variable de classement pour le découpage en quintiles nationaux.
################################################################################

cat("=== VIII.10 : Carte des classes géo-sociales ===\n")

# ── Composition géo-sociale de chaque cellule ─────────────────────────────────
# On réutilise directement pop_groupe_zone produite par 01_reseau.R : la carte
# montre alors EXACTEMENT ce que le modèle de transport utilise, sans reproduire
# de logique de classification (source unique de vérité).
if (!exists("pop_groupe_zone") || is.null(pop_groupe_zone) ||
    !is.matrix(pop_groupe_zone) ||
    nrow(pop_groupe_zone) != nrow(noeuds_entreposage)) {
  stop("pop_groupe_zone absente du persist — relancer 01_reseau.R : la carte des ",
       "classes géo-sociales repose sur la classification au pixel (IV.5.B).")
}

pgz <- pop_groupe_zone[, c(paste0("r", 1:5), paste0("u", 1:5)), drop = FALSE]

# Groupe majoritaire = colonne de population maximale ; part_majoritaire mesure
# à quel point la cellule est homogène (1 = une seule classe, 0,2 = très mixte).
idx_max          <- max.col(pgz, ties.method = "first")
groupe_code      <- colnames(pgz)[idx_max]
part_majoritaire <- pgz[cbind(seq_len(nrow(pgz)), idx_max)] / rowSums(pgz)

# Quintile moyen pondéré : Σ_q q × pop[q] / pop_totale, toutes strates confondues.
quintile_num  <- as.integer(sub("^[ru]", "", colnames(pgz)))
quintile_moy  <- as.vector(pgz %*% quintile_num) / rowSums(pgz)
# Part de la population de la cellule vivant en strate urbaine.
part_urbaine_cellule <- rowSums(pgz[, paste0("u", 1:5), drop = FALSE]) / rowSums(pgz)

groupe_viz <- paste0(ifelse(substr(groupe_code, 1, 1) == "u", "Urbain Q", "Rural Q"),
                     sub("^[ru]", "", groupe_code))

# ── Jointure avec zones_voronoi ───────────────────────────────────────────────
# noeuds_entreposage est ordonné comme warehouse_id = row_number() → jointure
# directe sans ambiguïté. zones_voronoi ne contient déjà qu'un polygone par
# nœud "ville" (01_reseau.R Partie IV.3-bis/IV.6) : les postes-frontière
# "passage" (warehouse_passage_uniquement) n'y figurent pas, faute de cellule
# propre. Les postes-frontière "ville" (Voronoï + commerce intérieur), eux,
# ont un vrai polygone et sont donc affichés comme n'importe quelle autre zone.
voronoi_geo <- zones_voronoi %>%
  left_join(
    tibble(
      warehouse_id = noeuds_entreposage$warehouse_id,
      warehouse_name = noeuds_entreposage$warehouse_name,
      warehouse_type = noeuds_entreposage$warehouse_type,
      groupe_geosocial = groupe_viz,
      quintile_moyen   = quintile_moy,
      part_majoritaire = part_majoritaire,
      part_urbaine     = part_urbaine_cellule
    ),
    by = "warehouse_id"
  )

# Ordre d'affichage : Rural Q1…Q5 puis Urbain Q1…Q5 (du plus pauvre au plus riche)
niveaux_groupe <- c(paste0("Rural Q",  1:5), paste0("Urbain Q", 1:5))
voronoi_geo <- voronoi_geo %>%
  mutate(groupe_geosocial = factor(groupe_geosocial, levels = niveaux_groupe))

# ── Palette de couleurs ───────────────────────────────────────────────────────
# Vert (rural) et bleu (urbain), 5 nuances du plus clair (Q1 pauvre) au plus
# foncé (Q5 riche), pour distinguer simultanément strate et niveau de vie.
PALETTE_GEOSOCIAL <- c(
  "Rural Q1"  = "#C7E9C0",
  "Rural Q2"  = "#74C476",
  "Rural Q3"  = "#31A354",
  "Rural Q4"  = "#006D2C",
  "Rural Q5"  = "#00441B",
  "Urbain Q1" = "#C6DBEF",
  "Urbain Q2" = "#6BAED6",
  "Urbain Q3" = "#2171B5",
  "Urbain Q4" = "#08519C",
  "Urbain Q5" = "#08306B"
)

# ── Exemple de lecture : classe majoritaire de la zone associée à Kigali ─────
# Plusieurs zones peuvent porter "Kigali" dans leur nom d'entrepôt (découpage
# infra-urbain) ; on retient celle de plus forte population (pop_i, alignée
# ligne à ligne sur noeuds_entreposage) comme représentative de l'agglomération.
idx_kigali <- which(grepl("Kigali", noeuds_entreposage$warehouse_name))
if (length(idx_kigali) > 0) {
  id_kigali     <- noeuds_entreposage$warehouse_id[idx_kigali[which.max(pop_i[idx_kigali])]]
  groupe_kigali <- voronoi_geo$groupe_geosocial[voronoi_geo$warehouse_id == id_kigali]

  # Traduction du code de groupe (ex. "Urbain Q5") en formulation lisible.
  strate_kigali    <- ifelse(grepl("^Urbain", groupe_kigali), "urbains", "ruraux")
  q_kigali         <- as.integer(sub(".*Q", "", groupe_kigali))
  libelle_quintile <- c("quintile inférieur", "2e quintile", "3e quintile",
                         "4e quintile", "quintile supérieur")[q_kigali]

  texte_lecture_kigali <- note_lecture(sprintf(
    "la classe géosociale majoritaire de la zone associée à Kigali est la classe des %s du %s des revenus.",
    strate_kigali, libelle_quintile
  ))
} else {
  texte_lecture_kigali <- ""
}

# ── Carte tmap ────────────────────────────────────────────────────────────────
carte_geosocial <- fond_carte() +
  tm_shape(voronoi_geo) +
  tm_polygons(
    fill        = "groupe_geosocial",
    fill.scale  = tm_scale_categorical(values = PALETTE_GEOSOCIAL),
    fill.legend = tm_legend(
      title    = "Classe géo-sociale",
      position = tm_pos_out("right", "center")
    ),
    col        = "#FFFFFF",
    lwd        = 0.2,
    col_alpha  = 0.6
  ) +
  tm_title("Classes géo-sociales — groupe majoritaire de chaque zone") +
  tm_credits(
    paste0(
      texte_lecture_kigali, "\n",
      "Groupe majoritaire : chaque zone porte en réalité un mélange de groupes ",
      "(classification au pixel). Part moyenne du groupe majoritaire : ",
      round(mean(voronoi_geo$part_majoritaire) * 100), " %.\n",
      "Quintiles de consommation découpés sur un classement NATIONAL unique ",
      "(définition EICV5), pondérés par la population WorldPop.\n",
      "RWI = Relative Wealth Index (Meta / Chi et al., 2022)."),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  )

tmap_save(
  carte_geosocial,
  file.path(DIR_CARTES, "carte_classes_geosociales.png"),
  width  = 2200,
  height = 1800,
  dpi    = 300
)
cat("  ✓ carte_classes_geosociales.png\n")

# ── Carte du quintile moyen (dégradé continu) ─────────────────────────────────
# La carte catégorielle ci-dessus écrase l'apport de la méthode : elle ne montre
# que la classe dominante. Celle-ci restitue le mélange, en moyennant le numéro
# de quintile par la population de la zone.
carte_quintile_moyen <- fond_carte() +
  tm_shape(voronoi_geo) +
  tm_polygons(
    fill        = "quintile_moyen",
    fill.scale  = tm_scale_continuous(values = "brewer.yl_gn_bu"),
    fill.legend = tm_legend(
      title    = "Quintile moyen\n(pondéré population)",
      position = tm_pos_out("right", "center")
    ),
    col        = "#FFFFFF",
    lwd        = 0.2,
    col_alpha  = 0.6
  ) +
  tm_title("Niveau de vie moyen par zone — quintile national pondéré par la population") +
  tm_credits(
    paste0("Moyenne du numéro de quintile (1 = plus pauvre … 5 = plus riche) ",
           "sur la population de la zone.\n",
           "Valeur non entière = zone socialement mixte."),
    position = tm_pos_out("center", "bottom", "left", "top"),
    size     = 0.65
  )

tmap_save(
  carte_quintile_moyen,
  file.path(DIR_CARTES, "carte_quintile_moyen.png"),
  width  = 2200,
  height = 1800,
  dpi    = 300
)
cat("  ✓ carte_quintile_moyen.png\n\n")

