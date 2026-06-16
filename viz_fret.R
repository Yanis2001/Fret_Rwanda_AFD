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
volumes_par_zone      <- .fret$volumes_par_zone
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
coords_zones_sf <- coords_zones_sf %>%
  mutate(match_idx = match(warehouse_name, noeuds_entreposage$warehouse_name)) %>%
  filter(!is.na(match_idx)) %>%
  arrange(match_idx)

# Taille des points proportionnelle au volume total de la zone (en scale log)
coords_zones_sf <- coords_zones_sf %>%
  mutate(
    offre_kt      = as.numeric(volumes_par_zone$Offre_kt[
      match(warehouse_name, volumes_par_zone$Zone)]),
    demande_kt    = as.numeric(volumes_par_zone$Demande_kt[
      match(warehouse_name, volumes_par_zone$Zone)]),
    total_kt      = offre_kt + demande_kt,
    taille_point  = as.numeric(rescale(log10(total_kt + 1), to = c(0.3, 1.8)))
  )

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
    fill = "warehouse_type",
    fill.scale = tm_scale(values = PALETTE_ZONE_TYPE),
    fill.legend = tm_legend(title = "Type de zone"),
    size = "taille_point",
    size.scale = tm_scale(values.range = c(0.3, 1.8)),
    size.legend = tm_legend(show = FALSE)
  ) +
  
  tm_title(paste0("Intensité du Trafic Fret\nModèle gravitaire — ", NOM_PAYS)) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position = c("right", "top"))

tmap_save(carte_fret,
          file.path(DIR_CARTES,"carte_trafic_fret.png"),
          width = 3000, height = 2400, dpi = 300)
cat("✓ Carte trafic fret sauvegardée\n")

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

# Boucle sur tous les secteurs pour générer une carte par secteur.
# seq_along(SECTEURS) génère les indices 1, 2, ..., N_SECTEURS.
for (s in SECTEURS) {
  
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
      fill        = "warehouse_type",
      fill.scale  = tm_scale(values = PALETTE_ZONE_TYPE),
      fill.legend = tm_legend(title = "Type de zone"),
      size        = "taille_point",
      size.scale  = tm_scale(values.range = c(0.3, 1.8)),
      size.legend = tm_legend(show = FALSE)
    ) +
    
    tm_title(paste0("Intensité du Trafic Fret — Secteur ", s,
                    "\nModèle gravitaire — ", NOM_PAYS)) +
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

carte_dominant <- fond_carte() +
  
  # Réseau de base en gris clair
  tm_shape(reseau %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#EEEEEE", lwd = 0.3) +
  
  # Arêtes colorées par secteur dominant
  # Largeur proportionnelle au volume total (pas sectoriel) pour garder
  # l'information sur l'intensité globale.
  tm_shape(aretes_dominant_sf) +
  tm_lines(
    col        = "secteur_dominant",
    col.scale  = tm_scale(values = PALETTE_SECTEURS),
    col.legend = tm_legend(title = "Secteur\ndominant"),
    lwd        = 1.5
  ) +
  
  tm_title("Secteur dominant par arête\n(secteur le plus représenté en tonnes)") +
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
  tm_dots(fill = "warehouse_type",
          fill.scale  = tm_scale(values = PALETTE_ZONE_TYPE),
          fill.legend = tm_legend(title = "Type de zone"),
          size = 0.4) +
  tm_title("Émissions CO₂ du Fret — Répartition sur le réseau") +
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
    y        = NULL
  ) +
  theme_minimal(base_size = 10) +
  theme(
    axis.text.x   = element_text(angle = 45, hjust = 1),
    plot.title    = element_text(face = "bold", size = 13),
    plot.subtitle = element_text(color = "#666666"),
    panel.grid    = element_blank()
  )

ggsave(
  file.path(DIR_CARTES, "heatmap_top_aretes_secteurs.png"),
  g_top_aretes,
  width = 12, height = 9, dpi = 300
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
    fill     = "Secteur"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title    = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#666666"),
    axis.text.x   = element_text(angle = 20, hjust = 1)
  )

ggsave(
  file.path(DIR_CARTES, "graphique_compo_secteurs_type_route.png"),
  g_compo_route,
  width = 11, height = 6, dpi = 300
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
  # facet_wrap() : une sous-figure par secteur. scales = "free_y" permet à
  # chaque sous-figure d'avoir sa propre échelle Y (certains secteurs ont
  # beaucoup moins d'arêtes actives que d'autres).
  # ncol calculé pour obtenir une grille la plus carrée possible
  facet_wrap(~ Secteur, scales = "free_y", ncol = ceiling(sqrt(N_SECTEURS))) +
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
    y        = "Nombre d'arêtes"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title    = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#666666"),
    strip.text    = element_text(face = "bold"),
    axis.text.x   = element_text(angle = 30, hjust = 1)
  )

ggsave(
  file.path(DIR_CARTES, "distribution_trafic_par_secteur.png"),
  g_distrib,
  width = 13, height = 7, dpi = 300
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
    y        = "Flux total inter-zones (milliers de tonnes)"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    plot.title         = element_text(face = "bold", size = 15),
    plot.subtitle      = element_text(color = "#666666"),
    panel.grid.major.y = element_blank(),
    panel.grid.minor   = element_blank()
  )

ggsave(file.path(DIR_CARTES,"graphique_flux_secteurs.png"),
       g1, width = 11, height = 6, dpi = 300)
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


# ============================================================
# GRAPHIQUE 2 : Offre vs Demande par zone
# ============================================================

# ── Zones au sommet d'offre / demande (pour mise en évidence sur le graphique) ──
# str_trunc(28) est la troncature utilisée dans Zone_court ci-dessous.
ref_offre_court   <- str_trunc(recap_zones$zone[which.max(recap_zones$offre_totale_mrd_rwf)],   28)
ref_demande_court <- str_trunc(recap_zones$zone[which.max(recap_zones$demande_totale_mrd_rwf)], 28)

g2 <- recap_zones %>%
  pivot_longer(
    cols      = c(offre_totale_mrd_rwf, demande_totale_mrd_rwf),
    names_to  = "Type_flux",
    values_to = "Valeur"
  ) %>%
  mutate(
    Zone_court = str_trunc(zone, 28),
    # Population de la zone : sert à ordonner les barres (population décroissante).
    Pop        = pop_par_zone[zone],
    Type_flux  = recode(Type_flux,
                        "offre_totale_mrd_rwf"   = "offre",
                        "demande_totale_mrd_rwf" = "demande")
  ) %>%
  ggplot(aes(x = reorder(Zone_court, Pop),
             y = Valeur,
             fill = Type_flux)) +
  geom_col(position = "dodge", width = 0.7) +
  # Contour bleu sur la zone à la plus forte offre (cohérent avec fill offre = bleu)
  geom_col(
    data = ~ filter(., Zone_court == ref_offre_court, Type_flux == "offre"),
    aes(x = reorder(Zone_court, Pop), y = Valeur),
    fill = NA, color = "#1976D2", linewidth = 1.3,
    position = "dodge", width = 0.7,
    inherit.aes = FALSE
  ) +
  # Contour rouge sur la zone à la plus forte demande (cohérent avec fill demande = rouge)
  geom_col(
    data = ~ filter(., Zone_court == ref_demande_court, Type_flux == "demande"),
    aes(x = reorder(Zone_court, Pop), y = Valeur),
    fill = NA, color = "#D32F2F", linewidth = 1.3,
    position = "dodge", width = 0.7,
    inherit.aes = FALSE
  ) +
  coord_flip() +
  scale_fill_manual(values = c("offre" = "#1976D2", "demande" = "#D32F2F")) +
  labs(
    title    = "Offre et Demande par zone économique",
    subtitle = paste0(
      "Modèle gravitaire — ", NOM_PAYS, "\n",
      "Contour bleu = max offre ('", ref_offre_court, "') | ",
      "Contour rouge = max demande ('", ref_demande_court, "')"
    ),
    x    = NULL,
    y    = "Valeur (milliards RWF)",
    fill = NULL
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 14),
    plot.subtitle      = element_text(color = "#666666", size = 9),
    legend.position    = "top",
    panel.grid.major.y = element_blank()
  )

ggsave(file.path(DIR_CARTES, "graphique_offre_demande_mrd_rwf.png"),
       g2, width = 13, height = 8, dpi = 300)
cat("✓ Graphique offre/demande (mrd RWF) sauvegardé\n")


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

ref_offre_t_court   <- str_trunc(recap_zones_tonnes$zone[which.max(recap_zones_tonnes$offre_totale_tonnes)],   28)
ref_demande_t_court <- str_trunc(recap_zones_tonnes$zone[which.max(recap_zones_tonnes$demande_totale_tonnes)], 28)

g2_tonnes <- recap_zones_tonnes %>%
  pivot_longer(
    cols      = c(offre_totale_tonnes, demande_totale_tonnes),
    names_to  = "Type_flux",
    values_to = "Valeur"
  ) %>%
  mutate(
    Zone_court = str_trunc(zone, 28),
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
    fill = NULL
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 14),
    plot.subtitle      = element_text(color = "#666666", size = 9),
    legend.position    = "top",
    panel.grid.major.y = element_blank()
  )

ggsave(file.path(DIR_CARTES, "graphique_offre_demande_tonnes.png"),
       g2_tonnes, width = 13, height = 8, dpi = 300)
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
    y        = "Origine"
  ) +
  theme_minimal(base_size = 10) +
  theme(
    axis.text.x     = element_text(angle = 45, hjust = 1, size = 8),
    axis.text.y     = element_text(size = 8),
    plot.title      = element_text(face = "bold", size = 13),
    plot.subtitle   = element_text(color = "#666666"),
    panel.grid      = element_blank(),
    legend.position = "right"
  )

ggsave(file.path(DIR_CARTES,"heatmap_flux_od.png"),
       g3, width = 13, height = 11, dpi = 300)
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
# mat : matrice zones × secteurs (mrd RWF ou tonnes). Une barre = une zone,
# normalisée à 100%, rangée par POPULATION DÉCROISSANTE (la plus peuplée en haut)
# pour un ordre identique d'un graphique « tous entrepôts » à l'autre.
# make.unique() évite que deux entrepôts au libellé tronqué identique (ex. deux
# « Zone industrielle … ») soient fusionnés silencieusement dans une seule barre.
graphe_compo_100 <- function(mat, titre, soustitre, fichier) {
  noms <- make.unique(str_trunc(str_remove(rownames(mat), " - .*"), 22), sep = "_")
  pop  <- pop_par_zone[rownames(mat)]   # population alignée sur les lignes de mat
  df <- as.data.frame(unname(mat)); colnames(df) <- colnames(mat); df$Zone <- noms
  df_long <- df %>%
    pivot_longer(-Zone, names_to = "Secteur", values_to = "Valeur") %>%
    group_by(Zone) %>%
    mutate(tot  = sum(Valeur, na.rm = TRUE),
           Part = ifelse(tot > 0, Valeur / tot * 100, 0)) %>%
    ungroup() %>%
    # Niveaux par population croissante : avec coord_flip(), l'entrepôt le plus
    # peuplé se retrouve en haut (lecture haut→bas = population décroissante).
    mutate(Zone = factor(Zone, levels = noms[order(pop)]))
  g <- ggplot(df_long, aes(Zone, Part, fill = Secteur)) +
    geom_col(width = 0.85) +
    coord_flip() +
    scale_fill_manual(values = PALETTE_SECTEURS) +
    scale_y_continuous(labels = scales::percent_format(scale = 1)) +
    labs(title = titre, subtitle = soustitre, x = NULL,
         y = "Part dans le total de la zone (%)", fill = "Secteur") +
    theme_minimal(base_size = 11) +
    theme(plot.title    = element_text(face = "bold", size = 13),
          plot.subtitle = element_text(color = "#666666", size = 9),
          legend.position = "right")
  ggsave(file.path(DIR_CARTES, fichier), g, width = 14, height = 8, dpi = 300)
  cat("  ✓", fichier, "\n")
}

# ── Helper B : flux brut par secteur pour 3 zones imposées ────────────────────
# Trace le flux brut de chaque secteur (valeurs absolues, non normalisées) pour
# les 3 entrepôts dont les indices de ligne sont fournis dans `sel` (et étiquetés
# par `rang`). On impose les MÊMES 3 entrepôts (ceux min/médian/max de la demande)
# aux graphes offre et demande, pour lire directement l'effet du netting
# demande = max(0, d − x) en comparant production brute x et demande brute d.
# Une facette par zone, échelle libre (scales = "free_x") : la composition reste
# lisible malgré l'écart d'ampleur (le total de chaque zone est rappelé en titre
# de facette). unite : libellé d'unité ("mrd RWF" ou "tonnes").
graphe_brut3 <- function(mat, unite, sel, rang, titre, soustitre, fichier) {
  totaux <- rowSums(mat, na.rm = TRUE)
  noms <- str_trunc(str_remove(rownames(mat), " - .*"), 22)
  lab  <- sprintf("%s — %s  (total : %s %s)",
                  rang, noms[sel],
                  format(round(totaux[sel]), big.mark = " "), unite)
  sub <- mat[sel, , drop = FALSE]
  df  <- as.data.frame(unname(sub)); colnames(df) <- colnames(mat)
  df$Zone <- factor(lab, levels = lab)
  df_long <- df %>%
    pivot_longer(-Zone, names_to = "Secteur", values_to = "Valeur") %>%
    # rev(SECTEURS) : après coord_flip(), Agriculture se retrouve en haut.
    mutate(Secteur = factor(Secteur, levels = rev(SECTEURS)))
  g <- ggplot(df_long, aes(Secteur, Valeur, fill = Secteur)) +
    geom_col(width = 0.8) +
    facet_wrap(~ Zone, ncol = 1, scales = "free_x") +
    coord_flip() +
    scale_fill_manual(values = PALETTE_SECTEURS, guide = "none") +
    scale_y_continuous(labels = scales::label_number(big.mark = " ")) +
    labs(title = titre, subtitle = soustitre, x = NULL,
         y = paste0("Flux brut (", unite, ")")) +
    theme_minimal(base_size = 11) +
    theme(plot.title    = element_text(face = "bold", size = 13),
          plot.subtitle = element_text(color = "#666666", size = 9),
          strip.text    = element_text(face = "bold"))
  ggsave(file.path(DIR_CARTES, fichier), g, width = 11, height = 8, dpi = 300)
  cat("  ✓", fichier, "\n")
}

# ── 1) OFFRE — surplus net exportable, composition à 100%, toutes les zones ───
# (ex-« graphique_composition_sectorielle » ; descriptif clarifié.)
graphe_compo_100(
  offre_zones,
  "Composition sectorielle du surplus NET exportable, par zone",
  paste0("Modèle MRIO — ", NOM_PAYS,
         " · offre = max(0, production − demande locale), en valeur (mrd RWF)\n",
         "Attention : domination mécanique des secteurs d'export (Mines, cultures ",
         "de rente) — les secteurs consommés localement sont nettés. ≠ composition du fret."),
  "graphique_composition_sectorielle.png"
)

# ── 2) DEMANDE — besoin NET (déficit importé), composition à 100%, en valeur ──
graphe_compo_100(
  demande_zones,
  "Composition sectorielle du besoin NET (déficit importé), par zone — valeur",
  paste0("Modèle MRIO — ", NOM_PAYS,
         " · demande = max(0, demande locale − production), en valeur (mrd RWF)"),
  "graphique_demande_composition_mrd_rwf.png"
)

# ── 3) DEMANDE — besoin NET, composition à 100%, en tonnage physique ──────────
graphe_compo_100(
  en_tonnes(demande_zones),
  "Composition sectorielle du besoin NET (déficit importé), par zone — tonnes",
  paste0("Modèle MRIO — ", NOM_PAYS,
         " · besoin net converti en tonnes (facteur sectoriel TONNES_PAR_mrd_RWF)"),
  "graphique_demande_composition_tonnes.png"
)

# ── 4) à 6) FLUX BRUTS sur 3 entrepôts FIXES : ceux min/médian/max de la DEMANDE
# Mêmes 3 entrepôts affichés côté production (offre brute x) et côté demande
# (demande brute d), pour visualiser directement l'effet de max(0, d − x).
# Nécessite prod_zones ET dem_zones (bruts) persistés par 03_transport.R.
if (exists("prod_zones") && exists("dem_zones")) {
  # Sélection par la demande totale brute (rowSums dem_zones) : max, médiane, min.
  dem_tot  <- rowSums(dem_zones, na.rm = TRUE)
  ord_d    <- order(dem_tot)
  sel_dem  <- c(ord_d[length(ord_d)], ord_d[ceiling(length(ord_d) / 2)], ord_d[1])
  rang_dem <- c("Demande max", "Demande médiane", "Demande min")

  # 4) OFFRE brute (production locale x) sur les 3 entrepôts de la demande.
  graphe_brut3(
    prod_zones, "mrd RWF", sel_dem, rang_dem,
    "Flux brut par secteur — production locale (entrepôts min/médian/max de la demande)",
    paste0("Modèle MRIO — ", NOM_PAYS,
           " · production brute x[i,s] avant netting · mêmes entrepôts que le graphe demande"),
    "graphique_offre_brut_3entrepots.png"
  )
  # 5) DEMANDE brute (demande totale d) sur les mêmes 3 entrepôts — valeur.
  graphe_brut3(
    dem_zones, "mrd RWF", sel_dem, rang_dem,
    "Flux brut par secteur — demande totale (entrepôts min/médian/max de la demande) — valeur",
    paste0("Modèle MRIO — ", NOM_PAYS,
           " · demande brute d[i,s] (interm. + finale) avant netting"),
    "graphique_demande_brut_mrd_rwf_3entrepots.png"
  )
  # 6) DEMANDE brute sur les mêmes 3 entrepôts — tonnage physique.
  graphe_brut3(
    en_tonnes(dem_zones), "tonnes", sel_dem, rang_dem,
    "Flux brut par secteur — demande totale (entrepôts min/médian/max de la demande) — tonnes",
    paste0("Modèle MRIO — ", NOM_PAYS,
           " · demande brute convertie en tonnes (facteur sectoriel TONNES_PAR_mrd_RWF)"),
    "graphique_demande_brut_tonnes_3entrepots.png"
  )
} else {
  cat("  ⚠ prod_zones/dem_zones absents du persist — relancer 03_transport.R pour les graphes bruts\n")
}

cat("✓ Graphiques de composition sectorielle (offre & demande) sauvegardés\n\n")

# ==============================================================================
# DIAGRAMME DE SANKEY — Flux de fret : Origine → Secteur → Destination
# ==============================================================================

cat("Génération du diagramme de Sankey...\n")

# ── Agrégation par (type de zone origine × secteur × type de zone destination) ──
# On travaille à l'échelle des TYPES de zones (6 types) plutôt que des 120 zones
# individuelles, pour garantir la lisibilité du diagramme.
# Les indices de ligne/colonne de flux_gravitaire[[s]] correspondent
# directement aux lignes de noeuds_entreposage (construit en IV.3).

sankey_raw <- map_dfr(SECTEURS, function(s) {
  mat_s <- flux_gravitaire[[s]]   
  # which() avec arr.ind = TRUE retourne une matrice à 2 colonnes :
  # colonne 1 = indice de ligne (origine), colonne 2 = indice de colonne (destination)
  idx <- which(mat_s > 0 & row(mat_s) != col(mat_s), arr.ind = TRUE)
  if (nrow(idx) == 0) return(tibble())
  tibble(
    flux_t           = mat_s[idx],
    type_origine     = noeuds_entreposage$warehouse_type[idx[, 1]],
    type_destination = noeuds_entreposage$warehouse_type[idx[, 2]],
    secteur          = s
  )
}) %>%
  group_by(type_origine, secteur, type_destination) %>%
  summarise(flux_t = sum(flux_t, na.rm = TRUE), .groups = "drop") %>%
  # Seuil de lisibilité : on ne garde que les flux représentant au moins
  # 0.1% du tonnage total pour éviter les micro-rubans illisibles
  filter(flux_t > sum(flux_t) * 0.001) %>%
  mutate(
    type_origine     = factor(type_origine,     levels = names(PALETTE_ZONE_TYPE)),
    secteur          = factor(secteur,           levels = SECTEURS),
    type_destination = factor(type_destination, levels = names(PALETTE_ZONE_TYPE))
  )

g_sankey <- ggplot(
  sankey_raw,
  aes(
    axis1 = type_origine,
    axis2 = secteur,
    axis3 = type_destination,
    y     = flux_t / 1000        # Conversion en milliers de tonnes
  )
) +
  # Rubans : chaque ruban = un flux (type_origine, secteur, type_destination)
  # curve_type = "cubic" donne des courbes de Bézier lisses
  geom_alluvium(
    aes(fill = secteur),
    width      = 1/5,
    alpha      = 0.65,
    curve_type = "cubic"
  ) +
  # Blocs : un par valeur unique sur chaque axe
  geom_stratum(
    width     = 1/5,
    fill      = "white",
    color     = "#333333",
    linewidth = 0.4
  ) +
  # after_stat(stratum) : récupère le nom de la strate depuis le stat interne
  geom_text(
    stat     = "stratum",
    aes(label = after_stat(stratum)),
    size     = 3.2,
    fontface = "bold",
    color    = "#222222"
  ) +
  scale_fill_manual(values = PALETTE_SECTEURS, name = "Secteur") +
  scale_x_discrete(
    limits = c("type_origine", "secteur", "type_destination"),
    labels = c("Type d'origine", "Secteur", "Type de destination"),
    expand = expansion(add = 0.15)
  ) +
  scale_y_continuous(
    labels = scales::label_number(suffix = " kt"),
    name   = "Volume (milliers de tonnes)"
  ) +
  labs(
    title    = "Flux de fret interzonaux — Diagramme de Sankey",
    subtitle = paste0(
      "Agrégation par type de zone et secteur économique · ",
      format(round(sum(sankey_raw$flux_t) / 1e6, 1)), " Mt modélisées"
    ),
    x = NULL
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title      = element_text(face = "bold", size = 14),
    plot.subtitle   = element_text(color = "#666666"),
    panel.grid      = element_blank(),
    axis.text.x     = element_text(size = 11, face = "bold"),
    axis.text.y     = element_text(size = 9),
    legend.position = "right"
  )

ggsave(
  file.path(DIR_CARTES, "sankey_flux_fret.png"),
  g_sankey,
  width  = 14,
  height = 8,
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
    y = "Volume agrégé (mrd RWF)"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 14),
    plot.subtitle      = element_text(color = "#666666", size = 9),
    legend.position    = "top",
    panel.grid.major.y = element_blank()
  )

ggsave(
  file.path(DIR_CARTES, "graphique_bilan_mrio.png"),
  g_prod_ech,
  width  = 12,
  height = 7,
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
    y = "Valeur (mrd RWF)"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title    = element_text(face = "bold", size = 14),
    plot.subtitle = element_text(color = "#666666", size = 9),
    axis.text.x   = element_text(size = 11)
  )

ggsave(
  file.path(DIR_CARTES, "graphique_validation_trc.png"),
  g_valid_compar,
  width  = 8,
  height = 6,
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
      "  (", round(part_transport, 1), "% du trc SAM).\n",
      "Secteurs sans fret physique (Transport, Énergie_eau, Services) exclus."
    ),
    x = NULL,
    y = "Coût de transport (mrd RWF)"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 14),
    plot.subtitle      = element_text(color = "#666666", size = 9),
    panel.grid.major.y = element_blank()
  )

ggsave(
  file.path(DIR_CARTES, "graphique_validation_trc_secteurs.png"),
  g_valid_sec,
  width  = 10,
  height = 6,
  dpi    = 300
)
cat("✓ graphique_validation_trc_secteurs.png\n\n")

