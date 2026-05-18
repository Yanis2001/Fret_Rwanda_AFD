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
#   → PART_ECHANGEABLE ou PART_DEMANDE_FINALE ont changé
#   → SEUIL_FLUX_TONNES a changé (filtre affectation)
#   → de nouvelles zones d'entrepôt ont été ajoutées ou retirées
#   → les profils PROFILS_OFFRE ou PROFILS_DEMANDE ont été modifiés
#   → POIDS_PROFIL_EMPLOI_RPHC5 ou K_RWI_OFFRE ont changé
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
reseau_rwanda         <- .fret$reseau_rwanda
volume_par_secteur    <- .fret$volume_par_secteur
volume_par_secteur_df <- .fret$volume_par_secteur_df
volumes_par_zone      <- .fret$volumes_par_zone
rm(.fret)

.flux <- readRDS(PERSIST_FLUX_FRET)
list2env(.flux, envir = .GlobalEnv)
rm(.flux)

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
aretes_fret <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  filter(volume_tonnes > 0) %>%
  mutate(
    volume_tonnes = as.numeric(volume_tonnes),
    volume_log    = as.numeric(log10(volume_tonnes + 1)),
    lwd_val       = as.numeric(rescale(log10(volume_tonnes + 1), to = c(0.5, 5)))
  )

# Coordonnées des zones
coords_zones_sf <- reseau_rwanda %>%
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
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
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
  
  tm_title("Intensité du Trafic Fret\nModèle gravitaire - Rwanda") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position = c("right", "top"))

tmap_save(carte_fret,
          file.path(DIR_CARTES,"carte_trafic_fret.png"),
          width = 3000, height = 2400, dpi = 300)
cat("✓ Carte trafic fret sauvegardée\n")

# ============================================================
# CARTES 4bis : Intensité du fret PAR SECTEUR
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
aretes_geom_base <- reseau_rwanda %>%
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
    tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
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
                    "\nModèle gravitaire - Rwanda")) +
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
# CARTE 4ter : Secteur DOMINANT par arête
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

# Palette pour les secteurs (une couleur distincte par secteur).
# RColorBrewer::brewer.pal(8, "Set2") donne 8 couleurs contrastées
# adaptées à des catégories non ordonnées.
PALETTE_SECTEURS <- setNames(
  RColorBrewer::brewer.pal(N_SECTEURS, "Set2"),
  SECTEURS
)

carte_dominant <- fond_carte() +
  
  # Réseau de base en gris clair
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
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
aretes_ges <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  filter(emissions_co2_t > 0)

carte_ges_affecte <- fond_carte() +
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
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
  scale_fill_brewer(palette = "Set2") +
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
  facet_wrap(~ Secteur, scales = "free_y", ncol = 4) +
  scale_x_log10(
    labels = scales::label_number(big.mark = " "),
    breaks = c(1, 10, 100, 1000, 10000, 100000)
  ) +
  scale_fill_brewer(palette = "Set2", guide = "none") +
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
aretes_avec_trafic <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  filter(volume_tonnes > 0)

carte_modal <- fond_carte() +
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
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
  ggplot(aes(x = reorder(Secteur, Flux_total_musd),
             y = Flux_total_musd,
             fill = Secteur)) +
  geom_col(show.legend = FALSE, width = 0.75) +
  geom_text(aes(label = paste0(Flux_total_musd, " M$")),
            hjust = -0.1, size = 3.5, color = "#333333") +
  coord_flip(clip = "off") +
  scale_fill_brewer(palette = "Set2") +
  scale_y_continuous(expand = expansion(mult = c(0, 0.15))) +
  labs(
    title    = "Flux commerciaux interzonaux par secteur",
    subtitle = "Modèle gravitaire - Rwanda (données fictives réalistes)",
    x        = NULL,
    y        = "Flux total inter-zones (millions USD)"
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
# GRAPHIQUE 2 : Offre vs Demande par zone
# ============================================================

# ── Noms courts des zones de référence (mêmes règles de troncature que Zone_court) ──
# str_trunc(28) est la troncature utilisée dans recap_zones → Zone_court.
# On aligne les noms pour pouvoir les identifier sur l'axe Y du graphique.
ref_offre_court   <- str_trunc(nom_ref_offre,   28)
ref_demande_court <- str_trunc(nom_ref_demande, 28)

g2 <- recap_zones %>%
  pivot_longer(
    cols      = c(offre_totale_musd, demande_totale_musd),
    names_to  = "Type_flux",
    values_to = "Valeur"
  ) %>%
  mutate(
    Zone_court = str_trunc(zone, 28),
    Type_flux  = recode(Type_flux,
                        "offre_totale_musd"   = "offre",
                        "demande_totale_musd" = "demande")
  ) %>%
  ggplot(aes(x = reorder(Zone_court, Valeur),
             y = Valeur,
             fill = Type_flux)) +
  geom_col(position = "dodge", width = 0.7) +
  # Contour rouge sur la barre de la zone de référence offre
  geom_col(
    data = ~ filter(., Zone_court == ref_offre_court, Type_flux == "offre"),
    aes(x = reorder(Zone_court, Valeur), y = Valeur),
    fill = NA, color = "#CC0000", linewidth = 1.3,
    position = "dodge", width = 0.7,
    inherit.aes = FALSE
  ) +
  # Contour bleu foncé sur la barre de la zone de référence demande
  geom_col(
    data = ~ filter(., Zone_court == ref_demande_court, Type_flux == "demande"),
    aes(x = reorder(Zone_court, Valeur), y = Valeur),
    fill = NA, color = "#003399", linewidth = 1.3,
    position = "dodge", width = 0.7,
    inherit.aes = FALSE
  ) +
  coord_flip() +
  scale_fill_manual(values = c("offre" = "#1976D2", "demande" = "#D32F2F")) +
  labs(
    title    = "Offre et Demande par zone économique",
    subtitle = paste0(
      "Modèle gravitaire - Rwanda (données fictives réalistes)\n",
      "Contour rouge = référence offre ('", nom_ref_offre, "') | ",
      "Contour bleu = référence demande ('", nom_ref_demande, "')"
    ),
    x    = NULL,
    y    = "Valeur (millions USD)",
    fill = NULL
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 14),
    plot.subtitle      = element_text(color = "#666666", size = 9),
    legend.position    = "top",
    panel.grid.major.y = element_blank()
  )

ggsave(file.path(DIR_CARTES,"graphique_offre_demande.png"),
       g2, width = 13, height = 8, dpi = 300)
cat("✓ Graphique offre/demande sauvegardé\n")


# ============================================================
# GRAPHIQUE 3 : Heatmap de la matrice OD
# Noms courts uniques via make.unique()
# ============================================================

noms_courts_raw <- noeuds_entreposage$warehouse_name %>%
  str_remove(" - .*") %>%
  str_remove(" \\(.*") %>%
  str_trunc(18)

noms_courts <- make.unique(noms_courts_raw, sep = "_")  # Kigali, Kigali_1, Kigali_2

flux_heatmap <- flux_total %>%
  as.data.frame() %>%
  setNames(noms_courts) %>%
  mutate(Origine = noms_courts) %>%
  pivot_longer(-Origine, names_to = "Destination", values_to = "Flux") %>%
  mutate(
    Flux_log    = ifelse(Flux > 0, log10(Flux), NA),
    Origine     = factor(Origine,     levels = rev(noms_courts)),
    Destination = factor(Destination, levels = noms_courts)
  )

g3 <- ggplot(flux_heatmap,
             aes(x = Destination, y = Origine, fill = Flux_log)) +
  geom_tile(color = "white", linewidth = 0.4) +
  scale_fill_gradient(
    low      = "#FFF7EC",
    high     = "#7F0000",
    na.value = "#F5F5F5",
    name     = "log₁₀\n(M USD)"
  ) +
  labs(
    title    = "Matrice des flux commerciaux interzonaux",
    subtitle = "Modèle gravitaire - Rwanda (log₁₀ M USD)",
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
# GRAPHIQUE 4 : Composition sectorielle des flux par zone
# ============================================================

offre_long <- as.data.frame(offre_zones) %>%
  rownames_to_column("Zone") %>%
  pivot_longer(-Zone, names_to = "Secteur", values_to = "Offre_musd") %>%
  mutate(Zone_court = str_trunc(str_remove(Zone, " - .*"), 22))

g4 <- offre_long %>%
  group_by(Zone_court) %>%
  mutate(Part_pct = Offre_musd / sum(Offre_musd) * 100) %>%
  ungroup() %>%
  ggplot(aes(x = reorder(Zone_court, -Offre_musd),
             y = Part_pct,
             fill = Secteur)) +
  geom_col(width = 0.8) +
  coord_flip() +
  scale_fill_brewer(palette = "Set2") +
  scale_y_continuous(labels = scales::percent_format(scale = 1)) +
  labs(
    title    = "Composition sectorielle de l'offre par zone",
    subtitle = "Modèle gravitaire - Rwanda",
    x        = NULL,
    y        = "Part dans l'offre totale (%)",
    fill     = "Secteur"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title      = element_text(face = "bold", size = 13),
    legend.position = "right"
  )

ggsave(file.path(DIR_CARTES,"graphique_composition_sectorielle.png"),
       g4, width = 14, height = 8, dpi = 300)
cat("✓ Graphique composition sectorielle sauvegardé\n\n")

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
  mat_s <- flux_gravitaire[[s]] * TONNES_PAR_musd[s]
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
  scale_fill_brewer(palette = "Set2", name = "Secteur") +
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

