################################################################################
# 05_ario.R
# RÔLE : Modèle ARIO-inventory (Hallegatte 2014) — pertes économiques
#        indirectes propagées par les interdépendances sectorielles et
#        spatiales suite à la perturbation simulée en 04_vulnerabilite.R.
# ENTRÉES  : persist_vulnerabilite.rds, persist_entreposages.rds,
#            persist_flux_fret.rds + DuckDB (tables IO)
# SORTIES  : persist_ario.rds + graphiques PNG + CSV
# DÉPEND DE : 00_parametres.R, 01_reseau.R, 03_transport.R, 04_vulnerabilite.R
################################################################################

source("00_parametres.R")

cat("=== Chargement des objets ===\n")

.geo   <- readRDS(PERSIST_GEODATA)
.ent   <- readRDS(PERSIST_ENTREPOSAGES)
.flux  <- readRDS(PERSIST_FLUX_FRET)
.res   <- readRDS(PERSIST_RESEAU_FRET)
.vuln  <- readRDS(PERSIST_VULNERAB)

list2env(.geo,  envir = .GlobalEnv)
list2env(.ent,  envir = .GlobalEnv)
flux_gravitaire           <- .flux$flux_gravitaire
flux_tonnes_total         <- .flux$flux_tonnes_total
offre_zones               <- .flux$offre_zones
demande_zones             <- .flux$demande_zones
reseau_rwanda             <- .res$reseau_rwanda
od_compare                <- .vuln$od_compare
fraction_perdue_zone      <- .vuln$fraction_perdue_zone
indices_aretes_perturbees <- .vuln$indices_aretes_perturbees
aretes_perturbees_sf      <- .vuln$aretes_perturbees_sf
NOM_SCENARIO              <- .vuln$NOM_SCENARIO

rm(.geo, .ent, .flux, .res, .vuln)

source("utils_fond_carte.R")
cat("✓ Objets chargés\n\n")

################################################################################
# PARTIE X — MODÈLE ARIO-INVENTORY (HALLEGATTE 2014) — AGRÉGÉ PAR PROVINCE
#
# OBJECTIF : Quantifier les pertes économiques INDIRECTES (effets de second
#            tour) générées par la rupture du réseau routier simulée en
#            Partie IX. La Partie IX a calculé les surcoûts de transport ;
#            cette Partie X simule la propagation de ces chocs à travers
#            les interdépendances sectorielles et spatiales de l'économie.
#
# RÉFÉRENCE : Hallegatte, S. (2014). Modeling the Role of Inventories and
#             Heterogeneity in the Assessment of the Economic Costs of
#             Natural Disasters. Risk Analysis, 34(1), 152-167.
#             Working Paper World Bank WPS6047 (2012) :
#             https://documents1.worldbank.org/curated/en/410441468142479058/
#             pdf/WPS6047.pdf
#
# ── PRINCIPE DU MODÈLE ARIO ───────────────────────────────────────────────────
# Le modèle ARIO étend la table Input-Output classique avec trois mécanismes
# qui captent les rigidités économiques de court terme et permettent ainsi
# d'aller plus loin que la simple analyse statique de Leontief :
#
#   1. INVENTAIRES (Inventories) — Chaque "industrie" (couple région × secteur)
#      détient un stock d'inputs de chaque autre secteur. Ces stocks lui
#      permettent de continuer à produire pendant quelques jours même si
#      ses fournisseurs sont coupés. La taille initiale des stocks vaut
#      n_j jours de consommation au rythme normal.
#
#   2. HÉTÉROGÉNÉITÉ (paramètre ψ — psi) — Au sein d'un secteur, les biens
#      et services ne sont pas parfaitement substituables. Une pénurie d'un
#      type d'acier ne se compense pas par un autre type d'acier. Le
#      paramètre ψ ∈ [0, 1] contrôle à quel point une réduction des stocks
#      pénalise la production :
#        ψ = 0   → biens parfaitement substituables (réduction tolérée)
#        ψ = 0.8 → valeur de référence Hallegatte 2014
#        ψ = 1   → biens totalement spécialisés (toute pénurie = arrêt)
#
#   3. SURPRODUCTION (overproduction α) — Quand la demande dépasse la
#      capacité, les industries peuvent temporairement augmenter leur
#      production (heures sup, délai de maintenance, importations
#      exceptionnelles) jusqu'à un plafond α_max = 1.25 (+25% au-dessus
#      de la normale). C'est ce qui modélise la "résilience" de l'économie.
#
# ── INTERPRÉTATION DU CHOC DE TRANSPORT ───────────────────────────────────────
# Une rupture de route ne détruit pas de capital productif comme un ouragan,
# mais elle a deux effets équivalents (CHOC COMBINÉ, fidèle à Hallegatte) :
#
#   A. CHOC DE CAPACITÉ PRODUCTIVE (Δ_P)
#      Une province dont une grande partie des flux entrants est coupée ne
#      peut pas produire à plein régime : ses fournisseurs n'arrivent plus
#      à la livrer. On modélise cela comme une réduction Δ_P de sa capacité,
#      équivalente à une destruction temporaire de capital.
#
#   B. CHOC D'INVENTAIRES
#      Le fret bloqué entre i et j pendant DUREE_JOURS représente un volume
#      de marchandises qui n'arrive jamais à destination. On l'impute comme
#      une déduction directe sur les stocks de l'industrie destinataire.
#
# ── GRANULARITÉ SPATIALE ──────────────────────────────────────────────────────
# Le modèle est agrégé au niveau des PROVINCES (5 au Rwanda) plutôt qu'au
# niveau des zones d'entrepôt. Choix motivé par :
#   - cohérence avec les statistiques officielles (NISR diffuse par province) ;
#   - allègement numérique : 5 × 8 = 40 industries (plutôt que 120 × 8 = 960) ;
#   - lisibilité des résultats : on peut interpréter directement par province ;
#   - robustesse : moins de risque de "production = 0" pour des entités qui
#     ne sont actives que dans certains secteurs.
#
# Les flux gravitaires zone × zone sont agrégés en flux province × province
# via une matrice d'agrégation M.
#
# ── DÉPENDANCES ───────────────────────────────────────────────────────────────
#   Partie II.3  → rwanda_provinces, rwanda_boundary (polygones administratifs)
#   Partie IV.3  → noeuds_entreposage, entreposages_sf (positions des zones)
#   Partie VII.1 → A (matrice IO nationale), production_totale, demande_finale
#   Partie VII.2 → offre_zones, demande_zones (matrices zone × secteur)
#   Partie VII.3 → flux_gravitaire[[s]] (flux sectoriels zone × zone, M USD/an)
#   Partie VIII  → flux_tonnes_total
#   Partie IX    → od_compare (surcoûts par paire OD),
#                  indices_aretes_perturbees, DUREE_JOURS, NOM_SCENARIO,
#                  aretes_perturbees_sf
################################################################################

cat("==========================================================\n")
cat("  PARTIE X — MODÈLE ARIO-INVENTORY (HALLEGATTE 2014)\n")
cat("==========================================================\n\n")


# ==============================================================================
# X.1 : Préparation des provinces et agrégation des zones
#
# On veut une couche provinciale fiable, avec au moins 4-5 polygones nommés.
# Trois sources possibles :
#   1. rwanda_provinces (Partie II.3, depuis OSM admin_level = 4)
#   2. GADM niveau 1 (téléchargé à la volée si OSM insuffisant)
#   3. Fallback : rwanda_national en une seule "province" (cas dégradé)
# ==============================================================================

cat("── X.1 : Préparation des provinces ──────────────────────────────────\n\n")

# ── Sélection de la source des provinces ──────────────────────────────────────
# On considère la couche OSM utilisable si elle contient au moins 4 polygones
# distincts avec une colonne `name` non vide. Sinon, on tente GADM niveau 1.
# GADM (Global Administrative Areas) est généralement plus fiable et stable
# que les frontières administratives OSM.

provinces_ario <- NULL

# Test 1 : la couche OSM est-elle utilisable ?
osm_provinces_ok <- !is.null(rwanda_provinces) &&
  nrow(rwanda_provinces) >= 4 &&
  "name" %in% names(rwanda_provinces) &&
  sum(!is.na(rwanda_provinces$name) & rwanda_provinces$name != "") >= 4

if (osm_provinces_ok) {
  
  cat("  Source : OSM (admin_level = 4)\n")
  provinces_ario <- rwanda_provinces %>%
    filter(!is.na(name), name != "") %>%
    # Si plusieurs polygones portent le même nom (multipart découpé), on les fusionne
    group_by(name) %>%
    summarise(geometry = st_union(geometry), .groups = "drop") %>%
    rename(nom_province = name) %>%
    st_make_valid()
  
} else {
  
  # Test 2 : tentative de téléchargement GADM niveau 1
  cat("  Source OSM insuffisante — tentative GADM niveau 1...\n")
  
  provinces_ario <- tryCatch({
    geodata::gadm(country = "RWA", level = 1, path = tempdir()) %>%
      st_as_sf() %>%
      st_transform(crs = 32735) %>%
      st_make_valid() %>%
      # NAME_1 est le champ standard GADM pour le niveau 1
      select(nom_province = NAME_1, geometry)
  }, error = function(e) {
    cat("  ⚠ Téléchargement GADM échoué :", conditionMessage(e), "\n")
    NULL
  })
  
  # Test 3 : fallback ultime, une seule "province" = Rwanda
  if (is.null(provinces_ario) || nrow(provinces_ario) == 0) {
    cat("  ⚠ Aucune source de provinces disponible — fallback Rwanda entier\n")
    provinces_ario <- rwanda_national %>%
      mutate(nom_province = "Rwanda")
  }
}

n_provinces <- nrow(provinces_ario)
noms_provinces <- provinces_ario$nom_province

cat("  ✓", n_provinces, "provinces retenues :",
    paste(noms_provinces, collapse = ", "), "\n\n")


# ── Récupération des géométries des zones d'entrepôt ──────────────────────────
# noeuds_entreposage est un tibble (sans géométrie). Pour la jointure spatiale
# avec les provinces, on récupère la géométrie depuis le réseau routier.
noeuds_entreposage_sf <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  st_as_sf() %>%
  mutate(zone_idx = row_number())

# Sanity check : alignement avec noeuds_entreposage
stopifnot(nrow(noeuds_entreposage_sf) == n_warehouses)


# ── Affectation de chaque zone à une province (jointure spatiale) ─────────────
# st_join() avec join = st_within et largest = TRUE :
#   - st_within : chaque zone hérite des attributs de la province qui la contient
#   - largest = TRUE : si une zone est exactement sur une frontière, on prend
#     la province dont le polygone est le plus grand (départage)
#
# Fallback : pour les zones qui ne tombent dans aucune province
# (postes frontières juste à l'extérieur des polygones OSM), on utilise
# st_nearest_feature pour les rattacher à la province la plus proche.

cat("  Affectation des zones aux provinces...\n")

zones_avec_province <- noeuds_entreposage_sf %>%
  st_join(provinces_ario, join = st_within, largest = TRUE)

# Recensement des zones non rattachées (cas frontières)
manquants_idx <- which(is.na(zones_avec_province$nom_province))
if (length(manquants_idx) > 0) {
  idx_proche <- st_nearest_feature(
    zones_avec_province[manquants_idx, ],
    provinces_ario
  )
  zones_avec_province$nom_province[manquants_idx] <-
    provinces_ario$nom_province[idx_proche]
  cat("    Zones rattachées par fallback nearest :",
      length(manquants_idx), "\n")
}

# ── Vecteur d'allocation zone → province (index) ──────────────────────────────
# zone_to_prov[i] = indice de la province à laquelle appartient la zone i
# match() : pour chaque nom_province, trouve sa position dans noms_provinces.
zone_to_prov <- match(zones_avec_province$nom_province, noms_provinces)

# Décompte des zones par province (pour diagnostic)
zones_par_province <- table(zones_avec_province$nom_province)
cat("  Zones par province :\n")
print(zones_par_province)
cat("\n")


# ── Matrice d'agrégation M (n_provinces × n_warehouses) ───────────────────────
# M[P, i] = 1 si l'entrepôt i appartient à la province P, 0 sinon.
# Cette matrice est très pratique pour agréger n'importe quelle matrice
# entrepôt × entrepôt en matrice province × province via :
#   matrice_prov = M %*% matrice_warehouses %*% t(M)
# Elle agrège aussi les vecteurs warehouses via :
#   vecteur_prov = M %*% vecteur_warehouses
M_agg <- matrix(0, nrow = n_provinces, ncol = n_warehouses,
                dimnames = list(noms_provinces, NULL))
for (i in seq_len(n_warehouses)) {
  M_agg[zone_to_prov[i], i] <- 1
}

cat("  Matrice d'agrégation construite (", n_provinces, "×", n_warehouses, ")\n\n")

# ==============================================================================
# X.2 : Construction de l'état initial (équilibre pré-perturbation)
#
# On construit les structures de données du modèle ARIO au niveau provincial :
#   - x_0      : vecteur de production initiale par industrie (p = N × S)
#   - Z        : matrice des flux interindustriels en M USD/jour (p × p)
#   - A_full   : matrice des coefficients techniques (Z divisée par x_0)
#   - Y        : vecteur de demande finale (p)
#   - S_0      : matrice d'inventaires initiaux (N_SECTEURS × p)
#
# Une "industrie" est un couple (province P, secteur s). On utilise un index
# linéaire f = (P - 1) × N_SECTEURS + s pour numéroter les p industries.
# ==============================================================================

cat("── X.2 : Construction de l'état initial ──────────────────────────────\n\n")

# ── Index des industries ──────────────────────────────────────────────────────
# expand.grid() : toutes les combinaisons province × secteur.
# Avec n_provinces = 5 et N_SECTEURS = 8, on obtient p = 40 industries.
industries_idx <- expand.grid(
  province = noms_provinces,
  secteur  = SECTEURS,
  stringsAsFactors = FALSE
) %>%
  as_tibble() %>%
  mutate(
    industrie_id = paste0(province, "__", secteur),
    province_idx = match(province, noms_provinces),
    secteur_idx  = match(secteur, SECTEURS),
    # Index linéaire f, indexé par province (extérieur) puis secteur (intérieur)
    # → toutes les industries d'une même province sont contigües
    f = (province_idx - 1) * N_SECTEURS + secteur_idx
  ) %>%
  arrange(f)

p <- nrow(industries_idx)
cat("  Industries (province × secteur) :", p,
    "(", n_provinces, "provinces ×", N_SECTEURS, "secteurs)\n")

# Index linéaire rapide : idx_f[P, s] = f
# Permet de retrouver f à partir d'un couple (province, secteur) en O(1).
idx_f <- matrix(0, nrow = n_provinces, ncol = N_SECTEURS,
                dimnames = list(noms_provinces, SECTEURS))
for (k in seq_len(p)) {
  idx_f[industries_idx$province_idx[k], industries_idx$secteur_idx[k]] <- k
}


# ── Agrégation des matrices offre/demande zone → province ─────────────────────
# offre_zones est une matrice n_warehouses × N_SECTEURS (M USD/an).
# On agrège vers le niveau province : offre_prov[P, s] = somme des zones de P.
# La multiplication matricielle M_agg %*% offre_zones réalise cette agrégation.
offre_prov   <- M_agg %*% offre_zones      # n_provinces × N_SECTEURS, M USD/an
demande_prov <- M_agg %*% demande_zones    # idem

cat("  Offre nationale agrégée   :",
    round(sum(offre_prov), 1), "M USD/an\n")
cat("  Demande nationale agrégée :",
    round(sum(demande_prov), 1), "M USD/an\n")


# ── Vecteur de production initiale x_0 (M USD/jour) ───────────────────────────
# Pour chaque industrie f = (P, s), la production initiale est l'offre de
# la province P dans le secteur s, divisée par 365 pour passer en flux journalier.
# ARIO travaille en flux journaliers (δt = 1 jour).
x_0 <- numeric(p)
for (k in seq_len(p)) {
  P_k <- industries_idx$province_idx[k]
  s_k <- industries_idx$secteur_idx[k]
  x_0[k] <- offre_prov[P_k, s_k] / 365
}
names(x_0) <- industries_idx$industrie_id

cat("  Production initiale totale:",
    round(sum(x_0) * 365, 1), "M USD/an\n")


# ── Agrégation des flux gravitaires zone × zone → province × province ─────────
# Pour chaque secteur s, flux_gravitaire[[s]] est une matrice n × n des flux
# annuels en M USD entre paires de zones. On les agrège au niveau provincial
# via la formule : flux_prov = M %*% flux %*% t(M).
# Cette opération conserve les flux INTRA-PROVINCIAUX (zones de la même
# province échangent entre elles) sur la diagonale.

flux_prov_par_secteur <- vector("list", N_SECTEURS)
names(flux_prov_par_secteur) <- SECTEURS

for (s in seq_len(N_SECTEURS)) {
  nom_s <- SECTEURS[s]
  flux_prov_par_secteur[[nom_s]] <- as.matrix(
    M_agg %*% flux_gravitaire[[nom_s]] %*% t(M_agg)
  )
  dimnames(flux_prov_par_secteur[[nom_s]]) <- list(noms_provinces, noms_provinces)
}

flux_prov_total <- Reduce(`+`, flux_prov_par_secteur)

cat("  Flux interindustriels agrégés (", N_SECTEURS, "matrices ",
    n_provinces, "×", n_provinces, ")\n")
cat("  Flux total inter-provinces :",
    round(sum(flux_prov_total), 1), "M USD/an\n")


# ── Matrice des flux interindustriels Z (M USD/jour) ──────────────────────────
# Z[f, f'] = montant que l'industrie f' achète à l'industrie f.
# 
# Construction : pour chaque flux gravitaire de s entre P et Q, on répartit
# ce flux entre les secteurs clients s' en Q selon la matrice technique A
# (hypothèse : structure technique nationale homogène, faute de MRIO).
#   Z[(P,s), (Q,s')] = flux_prov_par_secteur[[s]][P, Q] × A[s, s'] / 365

Z <- matrix(0, nrow = p, ncol = p,
            dimnames = list(industries_idx$industrie_id,
                            industries_idx$industrie_id))

for (s in seq_len(N_SECTEURS)) {
  nom_s <- SECTEURS[s]
  flux_s <- flux_prov_par_secteur[[nom_s]]
  
  for (P in seq_len(n_provinces)) {
    for (Q in seq_len(n_provinces)) {
      if (flux_s[P, Q] == 0) next
      
      flux_s_PQ_jour <- flux_s[P, Q] / 365   # M USD/jour
      
      for (s_prime in seq_len(N_SECTEURS)) {
        a_ss_prime <- A[s, s_prime]    # A est indexée [fournisseur, client]
        if (a_ss_prime == 0) next
        
        f_source <- idx_f[P, s]
        f_cible  <- idx_f[Q, s_prime]
        Z[f_source, f_cible] <- Z[f_source, f_cible] + flux_s_PQ_jour * a_ss_prime
      }
    }
  }
}

cat("  Matrice Z construite (", sum(Z > 0), "cellules non-nulles sur",
    p^2, ")\n")


# ── Matrice des coefficients techniques A_full (p × p) ────────────────────────
# A_full[f, f'] = Z[f, f'] / x_0[f'] = quantité produite par f nécessaire
# pour produire 1 unité par f'.
# Si x_0[f'] = 0, l'industrie f' est inactive et on met le coefficient à 0.
A_full <- matrix(0, nrow = p, ncol = p,
                 dimnames = dimnames(Z))
for (f_prime in seq_len(p)) {
  if (x_0[f_prime] > 0) {
    A_full[, f_prime] <- Z[, f_prime] / x_0[f_prime]
  }
}

# ── Vecteur de demande finale Y ───────────────────────────────────────────────
# Y[f] = x_0[f] - somme des livraisons interindustrielles depuis f
# C'est ce qui reste de la production après livraison aux autres industries :
# ménages, gouvernement, exportations, investissements.
# pmax(0, ...) : sécurité numérique au cas où des arrondis donneraient
# une valeur légèrement négative.
Y <- pmax(0, x_0 - rowSums(Z))
names(Y) <- industries_idx$industrie_id

cat("  Demande finale totale Y    :",
    round(sum(Y) * 365, 1), "M USD/an (",
    round(sum(Y) / sum(x_0) * 100, 1), "% de la production)\n")


# ── Matrice de coefficients techniques agrégés par secteur fournisseur ────────
# A_sectoriel[s, f] = somme des A[s_input, f] pour s_input = s.
# Comme A est indexée par secteur (pas par couple région × secteur), et que
# tous les couples industriels ont la même structure technique sectorielle :
#   A_sectoriel[s, f] = A[s, secteur(f)] pour chaque f
# Cette matrice est utilisée pour les contraintes d'inventaire (Eq. 9 et 10
# de Hallegatte 2014).
A_sectoriel <- matrix(0, nrow = N_SECTEURS, ncol = p,
                      dimnames = list(SECTEURS, industries_idx$industrie_id))
for (f in seq_len(p)) {
  s_f <- industries_idx$secteur_idx[f]
  A_sectoriel[, f] <- A[, s_f]
}


# ── Matrice d'inventaires initiaux S_0 (N_SECTEURS × p) ───────────────────────
# S_0[s_input, f] = stock du bien produit par s_input détenu par l'industrie f
#                 = n_j × A_sectoriel[s_input, f] × x_0[f]
# Interprétation : f détient n_j[s_input] jours de stock de produits du
# secteur s_input, au niveau de consommation requis par sa production x_0[f].
#
# Note : les inventaires sont indexés par SECTEUR fournisseur (pas par couple
# région × secteur), suivant Hallegatte 2014. L'origine spatiale précise des
# stocks n'est pas tracée, mais elle l'est dans les commandes (matrice O).
S_0 <- matrix(0, nrow = N_SECTEURS, ncol = p,
              dimnames = list(SECTEURS, industries_idx$industrie_id))
n_j_matrix <- matrix(ARIO_INV_DUREE_JOURS, nrow = N_SECTEURS, ncol = p,
                     dimnames = list(SECTEURS, industries_idx$industrie_id))

for (f in seq_len(p)) {
  S_0[, f] <- n_j_matrix[, f] * A_sectoriel[, f] * x_0[f]
}

cat("  Inventaires initiaux totaux:",
    round(sum(S_0), 1), "M USD-jours\n\n")


# ==============================================================================
# X.3 : Traduction du choc de transport en chocs ARIO
#
# La Partie IX a produit od_compare (paire OD × type d'impact). On en extrait :
#
#   (A) CHOC DE CAPACITÉ Δ_P (par province)
#       Pour chaque province Q, on regarde tous les flux entrants depuis
#       toutes les provinces P, secteur par secteur. On calcule la fraction
#       moyenne pondérée de ces flux qui est perdue ou très renchérie.
#       Cette Δ s'applique uniformément à toutes les industries de Q.
#
#   (B) CHOC D'INVENTAIRE
#       Le fret bloqué pendant DUREE_JOURS représente des stocks qui
#       n'arrivent jamais à destination. On déduit ce volume des inventaires
#       initiaux de chaque industrie cliente.
#
# La "fraction perdue" d'une paire OD est définie comme :
#   - déconnecté         → 1.0 (100% du flux perdu)
#   - surcoût relatif x% → x/100 (plafonné à 1)
#   - inchangé           → 0
# C'est une hypothèse simplificatrice : on suppose qu'un surcoût de 50%
# fait que 50% du flux ne se fait plus. Cette élasticité-prix de 1 est
# raisonnable pour des marchandises à faible valeur ajoutée, mais peut être
# raffinée par secteur en analyse de sensibilité (paramètre future).
# ==============================================================================

cat("── X.3 : Traduction des chocs de transport ──────────────────────────\n\n")

# ── Construction du tableau de référence des fractions perdues ────────────────
# Pour chaque paire OD (origine zone, destination zone) dans od_compare, on
# calcule sa fraction perdue.
od_lookup <- od_compare %>%
  select(id_origine, id_destination, type_impact, surcout_relatif_pct) %>%
  mutate(
    fraction_perdue = case_when(
      type_impact == "deconnecte" ~ 1.0,
      type_impact == "inchange"   ~ 0.0,
      is.na(surcout_relatif_pct)  ~ 0.0,
      TRUE ~ pmin(1.0, surcout_relatif_pct / 100)
    )
  )

# ── Matrice fraction perdue par paire de zones (n × n) ────────────────────────
# fraction_perdue_zone[i, j] = fraction du flux entre zone i et zone j perdue
# pendant la perturbation. Sert ensuite à agréger au niveau provincial.
fraction_perdue_zone <- matrix(0, nrow = n_warehouses, ncol = n_warehouses)
for (k in seq_len(nrow(od_lookup))) {
  i <- od_lookup$id_origine[k]
  j <- od_lookup$id_destination[k]
  fraction_perdue_zone[i, j] <- od_lookup$fraction_perdue[k]
}


# ── (A) Calcul du choc de capacité Δ_P par province ───────────────────────────
# Pour chaque province Q, on calcule Δ_Q comme la moyenne des fractions
# perdues sur ses flux entrants, pondérée par le volume des flux.
#
# Formule : Δ_Q = Σ_i (flux_in[i, j∈Q] × fraction_perdue[i, j]) /
#                 Σ_i (flux_in[i, j∈Q])
# Pour des raisons numériques (un seul Δ par province qui s'applique à toutes
# ses industries), on pondère par les volumes en tonnes (Partie VIII) plutôt
# que les flux monétaires sectoriels.

cat("  Calcul des chocs de capacité par province...\n")

Delta_P <- numeric(n_provinces)
names(Delta_P) <- noms_provinces

for (P in seq_len(n_provinces)) {
  
  # Indices des zones appartenant à la province P
  zones_P <- which(zone_to_prov == P)
  if (length(zones_P) == 0) next
  
  # Flux entrants vers chaque zone de la province P (provenant de toutes les
  # autres zones, intra ou inter-provinces)
  flux_in_P    <- flux_tonnes_total[, zones_P, drop = FALSE]
  flux_total_P <- sum(flux_in_P)
  
  if (flux_total_P == 0) {
    Delta_P[P] <- 0
    next
  }
  
  # Pour chaque flux entrant, on récupère sa fraction perdue
  # Note : fraction_perdue_zone[i, j] est indexé par zone, pas par province
  flux_x_fraction <- flux_in_P * fraction_perdue_zone[, zones_P, drop = FALSE]
  
  Delta_P[P] <- sum(flux_x_fraction) / flux_total_P
}

# Δ_f pour chaque industrie : tous les secteurs d'une même province héritent
# du même Δ. Vecteur de longueur p.
Delta_f <- numeric(p)
for (f in seq_len(p)) {
  P_f <- industries_idx$province_idx[f]
  Delta_f[f] <- Delta_P[P_f]
}
names(Delta_f) <- industries_idx$industrie_id

cat("  Choc de capacité par province :\n")
print(round(Delta_P * 100, 2))
cat("\n")


# ── (B) Calcul du choc d'inventaire ───────────────────────────────────────────
# Pour chaque flux gravitaire de s entre P et Q :
#   volume_bloque_PQs = flux_prov_par_secteur[s][P, Q] × (DUREE_JOURS / 365)
#                       × fraction_perdue_moyenne_PQ
# Ce volume est réparti entre les secteurs clients s' en Q via A[s, s'].
#
# fraction_perdue_moyenne_PQ : fraction pondérée par les flux des zones
# constituant chaque province. On la pré-calcule au niveau provincial.

cat("  Calcul des chocs d'inventaire...\n")

# Pré-calcul : fraction perdue moyenne par paire de provinces (P, Q)
# Pondérée par les flux totaux entre les zones de ces provinces.
fraction_perdue_prov <- matrix(0, nrow = n_provinces, ncol = n_provinces,
                               dimnames = list(noms_provinces, noms_provinces))

for (P in seq_len(n_provinces)) {
  for (Q in seq_len(n_provinces)) {
    zones_P <- which(zone_to_prov == P)
    zones_Q <- which(zone_to_prov == Q)
    if (length(zones_P) == 0 || length(zones_Q) == 0) next
    
    flux_PQ <- flux_tonnes_total[zones_P, zones_Q, drop = FALSE]
    fraction_PQ <- fraction_perdue_zone[zones_P, zones_Q, drop = FALSE]
    
    if (sum(flux_PQ) > 0) {
      fraction_perdue_prov[P, Q] <- sum(flux_PQ * fraction_PQ) / sum(flux_PQ)
    }
  }
}

# Matrice des chocs d'inventaire à appliquer : choc_inv[s_input, f]
# = somme du volume de s_input qui devait arriver à f mais qui est bloqué
choc_inv <- matrix(0, nrow = N_SECTEURS, ncol = p,
                   dimnames = list(SECTEURS, industries_idx$industrie_id))

for (s in seq_len(N_SECTEURS)) {
  nom_s <- SECTEURS[s]
  flux_s_prov <- flux_prov_par_secteur[[nom_s]]   # M USD/an
  
  for (P in seq_len(n_provinces)) {
    for (Q in seq_len(n_provinces)) {
      if (flux_s_prov[P, Q] == 0 || fraction_perdue_prov[P, Q] == 0) next
      
      # Volume bloqué sur cette paire, ce secteur, pendant DUREE_JOURS
      volume_bloque <- flux_s_prov[P, Q] *
        (DUREE_JOURS / 365) *
        fraction_perdue_prov[P, Q]
      
      # Répartition du volume bloqué entre les secteurs clients s' en Q
      for (s_prime in seq_len(N_SECTEURS)) {
        a_ss_prime <- A[s, s_prime]
        if (a_ss_prime == 0) next
        
        f_cible <- idx_f[Q, s_prime]
        choc_inv[s, f_cible] <- choc_inv[s, f_cible] +
          volume_bloque * a_ss_prime
      }
    }
  }
}

# Application du choc : on retire le volume bloqué des inventaires initiaux.
# pmax(0, ...) : un stock ne peut pas devenir négatif.
S_choque <- pmax(0, S_0 - choc_inv)

cat("  Volume total bloqué (choc inv):",
    round(sum(choc_inv), 2), "M USD\n")
cat("  Part des inventaires détruits  :",
    round(sum(choc_inv) / sum(S_0) * 100, 2), "%\n\n")


# ==============================================================================
# X.4 : Simulation dynamique (boucle journalière vectorisée)
#
# À chaque pas de temps t (jour), on enchaîne les étapes :
#   1. Capacité de production : x_cap(t) = α(t) × (1 - Δ(t)) × x_0
#   2. Demande totale         : D(t)     = Σ_f' O[f, f'](t) + Y[f]
#   3. Production optimale    : x_opt(t) = min(D(t), x_cap(t))
#   4. Contraintes inventaire : x_a(t)   = x_opt(t) × min_s(S/S_req)
#      avec S_req = n_j × A_sectoriel × x_opt et tolérance ψ
#   5. Rationing proportionnel: les clients sont servis au prorata de x_a/D
#   6. Maj inventaires        : S(t+1)   = S(t) + livraisons - consommation
#   7. Maj commandes          : O(t+1) selon écart à la cible n_j × A × x_opt
#   8. Maj surproduction      : α(t+1) selon indicateur de rareté
#   9. Récupération de Δ      : Δ(t+1) décroît exponentiellement après le choc
#
# Toutes les opérations sont vectorisées sur les p industries.
# Avec p = 40 et T = 730 jours max, la boucle tourne en quelques secondes.
# ==============================================================================

cat("── X.4 : Simulation dynamique ARIO ──────────────────────────────────\n\n")

# ── État initial des variables dynamiques ─────────────────────────────────────
x_t     <- x_0                     # Production actuelle (init = équilibre)
alpha_t <- rep(ARIO_ALPHA_BASE, p) # Surproduction (init = 1.0)
Delta_t <- Delta_f                  # Choc capacité (init = valeur calculée en X.3)
S_t     <- S_choque                 # Inventaires (init = après choc inv.)
O_t     <- Z                        # Commandes (init = flux interindustriels)


# ── Allocation des matrices de trajectoire ────────────────────────────────────
# On stocke les trajectoires agrégées par secteur ET par province pour les
# visualisations, plus la matrice complète des productions pour les exports.
production_par_jour_secteur  <- matrix(0, nrow = ARIO_HORIZON_JOURS, ncol = N_SECTEURS,
                                       dimnames = list(NULL, SECTEURS))
production_par_jour_province <- matrix(0, nrow = ARIO_HORIZON_JOURS, ncol = n_provinces,
                                       dimnames = list(NULL, noms_provinces))
demande_non_satisfaite       <- matrix(0, nrow = ARIO_HORIZON_JOURS, ncol = N_SECTEURS,
                                       dimnames = list(NULL, SECTEURS))
alpha_par_jour                <- matrix(0, nrow = ARIO_HORIZON_JOURS, ncol = N_SECTEURS,
                                        dimnames = list(NULL, SECTEURS))
delta_par_jour                <- numeric(ARIO_HORIZON_JOURS)


# ── Pré-calculs pour la boucle ────────────────────────────────────────────────
# Indices des industries appartenant à chaque secteur, chaque province
idx_par_secteur  <- lapply(seq_len(N_SECTEURS),
                           function(s) which(industries_idx$secteur_idx == s))
idx_par_province <- lapply(seq_len(n_provinces),
                           function(P) which(industries_idx$province_idx == P))

# Vecteur τ_s par industrie (chaque industrie hérite du τ_s du secteur qu'elle produit)
tau_s_par_f <- ARIO_TAU_S[industries_idx$secteur_idx]


cat("  Simulation sur", ARIO_HORIZON_JOURS, "jours (p =", p, "industries)...\n")

pb_ario <- progress_bar$new(
  format = "  ARIO sim [:bar] :percent | ETA: :eta",
  total  = ARIO_HORIZON_JOURS, clear = FALSE, width = 60
)

# Petit epsilon pour éviter les divisions par zéro
eps <- 1e-12

for (t in seq_len(ARIO_HORIZON_JOURS)) {
  
  # ── Étape 1 : Capacité de production ────────────────────────────────────────
  x_cap <- alpha_t * (1 - Delta_t) * x_0
  
  # ── Étape 2 : Demande totale dirigée vers chaque industrie ──────────────────
  # D[f] = Σ_f' O[f, f'] + Y[f]
  # rowSums() : pour chaque f, somme sur tous les clients f'
  D_t <- rowSums(O_t) + Y
  
  # ── Étape 3 : Production optimale (sans contrainte inventaire) ──────────────
  x_opt <- pmin(D_t, x_cap)
  
  # ── Étape 4 : Contraintes d'inventaire (Eq. 9-10 de Hallegatte 2014) ────────
  # S_required[s, f] = n_j[s] × A_sectoriel[s, f] × x_opt[f]
  # Quand le stock S[s, f] < ψ × S_required[s, f], la production est limitée :
  #   x_a[f] = x_opt[f] × min_s(S[s, f] / (ψ × S_required[s, f]))
  # Plafond : x_a ≤ x_opt (ne dépasse jamais l'optimal).
  S_required <- n_j_matrix * A_sectoriel *
    matrix(x_opt, nrow = N_SECTEURS, ncol = p, byrow = TRUE)
  
  ratio_inv <- S_t / pmax(ARIO_PSI * S_required, eps)
  ratio_inv[S_required == 0] <- Inf   # Pas de contrainte si l'industrie ne consomme pas ce secteur
  ratio_inv <- pmin(ratio_inv, 1)
  
  # Le secteur le plus contraint détermine la production effective
  facteur_inv <- apply(ratio_inv, 2, min)
  x_a <- x_opt * facteur_inv
  
  # ── Étape 5 : Distribution proportionnelle (rationing) ──────────────────────
  # Si la production effective ne suffit pas à satisfaire toute la demande,
  # chaque client reçoit une fraction proportionnelle x_a/D de sa commande.
  ratio_rationing <- ifelse(D_t > 0, x_a / D_t, 1)
  ratio_rationing <- pmin(ratio_rationing, 1)
  
  # Commandes effectivement livrées (multiplication ligne par ligne)
  # diag(ratio) %*% O multiplie chaque ligne f de O par ratio[f]
  O_received <- diag(ratio_rationing) %*% O_t
  
  # Demande finale effectivement satisfaite et pertes correspondantes
  Y_received <- Y * ratio_rationing
  Y_loss     <- Y - Y_received
  
  # ── Étape 6 : Mise à jour des inventaires ───────────────────────────────────
  # S(t+1) = S(t) + livraisons_reçues - inputs_consommés
  # Livraisons par secteur fournisseur (agrégation des colonnes de O_received)
  livraisons <- matrix(0, nrow = N_SECTEURS, ncol = p,
                       dimnames = dimnames(S_t))
  for (s in seq_len(N_SECTEURS)) {
    idx_prod_s <- idx_par_secteur[[s]]
    livraisons[s, ] <- colSums(O_received[idx_prod_s, , drop = FALSE])
  }
  
  # Inputs consommés pour produire x_a
  consommation <- A_sectoriel *
    matrix(x_a, nrow = N_SECTEURS, ncol = p, byrow = TRUE)
  
  S_t <- pmax(0, S_t + livraisons - consommation)
  
  # ── Étape 7 : Mise à jour des commandes O(t+1) ──────────────────────────────
  # Cible d'inventaire : n_j × A_sect × x_opt (Hallegatte Eq. 3)
  S_target <- n_j_matrix * A_sectoriel *
    matrix(x_opt, nrow = N_SECTEURS, ncol = p, byrow = TRUE)
  
  # Écart à combler (positif uniquement)
  S_gap <- pmax(0, S_target - S_t)
  
  # Commandes totales par (secteur fournisseur, industrie cliente) :
  #   O_total[s, f] = (1/τ_s) × S_gap[s, f] + consommation[s, f]
  # (Hallegatte Eq. 4 : la première composante reconstitue les stocks,
  # la seconde renouvelle la consommation courante)
  O_total_sect <- (1 / tau_s_par_f[idx_par_secteur[[1]]]) * 0  # init
  # On construit O_total_sect ligne par ligne (par secteur fournisseur)
  O_total_sect <- matrix(0, nrow = N_SECTEURS, ncol = p,
                         dimnames = dimnames(S_t))
  for (s in seq_len(N_SECTEURS)) {
    O_total_sect[s, ] <- (1 / ARIO_TAU_S[s]) * S_gap[s, ] + consommation[s, ]
  }
  
  # Distribution des commandes entre fournisseurs spatiaux selon les parts
  # initiales Z_share. On utilise la variante Hallegatte 2013 (parts fixes,
  # pas de substitution inter-régionale automatique).
  for (s in seq_len(N_SECTEURS)) {
    idx_prod_s <- idx_par_secteur[[s]]
    Z_block <- Z[idx_prod_s, , drop = FALSE]
    sommes_col <- colSums(Z_block)
    
    # Parts initiales de chaque fournisseur dans chaque client
    parts_block <- sweep(Z_block, 2,
                         ifelse(sommes_col > 0, sommes_col, 1),
                         FUN = "/")
    parts_block[, sommes_col == 0] <- 0
    
    # Distribution
    O_t[idx_prod_s, ] <- sweep(parts_block, 2, O_total_sect[s, ], FUN = "*")
  }
  
  # ── Étape 8 : Mise à jour de α (surproduction, Eq. 12 de Hallegatte) ────────
  # Indicateur de rareté : ζ = (D - x_a) / D
  zeta_t <- ifelse(D_t > 0, pmax(0, (D_t - x_a) / D_t), 0)
  
  # Si rareté > 0 : α augmente vers α_max
  # Sinon         : α retourne vers α_base
  alpha_t <- ifelse(
    zeta_t > 0,
    alpha_t + (ARIO_ALPHA_MAX  - alpha_t) * zeta_t * (ARIO_DT / ARIO_TAU_ALPHA),
    alpha_t + (ARIO_ALPHA_BASE - alpha_t)          * (ARIO_DT / ARIO_TAU_ALPHA)
  )
  
  # ── Étape 9 : Récupération exponentielle de Δ ───────────────────────────────
  # Pendant la perturbation (t ≤ DUREE_JOURS) : Δ constant.
  # Après : décroissance exponentielle vers 0 avec τ_recup.
  if (t > DUREE_JOURS) {
    Delta_t <- Delta_f * exp(-(t - DUREE_JOURS) / ARIO_TAU_RECUP)
  }
  
  # ── Enregistrement de la trajectoire ────────────────────────────────────────
  for (s in seq_len(N_SECTEURS)) {
    production_par_jour_secteur[t, s] <- sum(x_a[idx_par_secteur[[s]]])
    demande_non_satisfaite[t, s]      <- sum(Y_loss[idx_par_secteur[[s]]])
    alpha_par_jour[t, s]               <- mean(alpha_t[idx_par_secteur[[s]]])
  }
  for (P in seq_len(n_provinces)) {
    production_par_jour_province[t, P] <- sum(x_a[idx_par_province[[P]]])
  }
  delta_par_jour[t] <- mean(Delta_t)
  
  pb_ario$tick()
}

cat("✓ Simulation terminée\n\n")


# ==============================================================================
# X.5 : Agrégation des résultats et calcul des pertes indirectes
# ==============================================================================

cat("── X.5 : Calcul des pertes indirectes ────────────────────────────────\n\n")

# ── Production de référence (équilibre constant sur l'horizon) ────────────────
# Au jour t = 0, chaque industrie produit x_0[f]. Sur l'horizon complet,
# la production cumulée de référence est ARIO_HORIZON_JOURS × x_0.
prod_ref_secteur <- numeric(N_SECTEURS)
prod_ref_province <- numeric(n_provinces)
names(prod_ref_secteur)  <- SECTEURS
names(prod_ref_province) <- noms_provinces

for (s in seq_len(N_SECTEURS)) {
  prod_ref_secteur[s] <- sum(x_0[idx_par_secteur[[s]]])
}
for (P in seq_len(n_provinces)) {
  prod_ref_province[P] <- sum(x_0[idx_par_province[[P]]])
}

prod_ref_secteur_tot  <- prod_ref_secteur  * ARIO_HORIZON_JOURS
prod_ref_province_tot <- prod_ref_province * ARIO_HORIZON_JOURS

# Production effective cumulée
prod_eff_secteur_tot  <- colSums(production_par_jour_secteur)
prod_eff_province_tot <- colSums(production_par_jour_province)

# ── Pertes indirectes par secteur ─────────────────────────────────────────────
pertes_secteur <- prod_ref_secteur_tot - prod_eff_secteur_tot
pertes_pct_secteur <- ifelse(prod_ref_secteur_tot > 0,
                             pertes_secteur / prod_ref_secteur_tot * 100, 0)

tableau_pertes_secteur <- tibble(
  Secteur                  = SECTEURS,
  Production_ref_musd      = round(prod_ref_secteur_tot, 2),
  Production_eff_musd      = round(prod_eff_secteur_tot, 2),
  Perte_indirecte_musd     = round(pertes_secteur, 2),
  Perte_pct                = round(pertes_pct_secteur, 2)
) %>%
  arrange(desc(Perte_indirecte_musd))

cat("Pertes indirectes par secteur :\n")
print(tableau_pertes_secteur)
cat("\n")

# ── Pertes indirectes par province ────────────────────────────────────────────
pertes_province <- prod_ref_province_tot - prod_eff_province_tot
pertes_pct_province <- ifelse(prod_ref_province_tot > 0,
                              pertes_province / prod_ref_province_tot * 100, 0)

tableau_pertes_province <- tibble(
  Province              = noms_provinces,
  Choc_capacite_pct     = round(Delta_P * 100, 2),
  Production_ref_musd   = round(prod_ref_province_tot, 2),
  Production_eff_musd   = round(prod_eff_province_tot, 2),
  Perte_indirecte_musd  = round(pertes_province, 2),
  Perte_pct             = round(pertes_pct_province, 2)
) %>%
  arrange(desc(Perte_indirecte_musd))

cat("Pertes indirectes par province :\n")
print(tableau_pertes_province)
cat("\n")

# ── Synthèse globale ──────────────────────────────────────────────────────────
perte_indirecte_totale <- sum(pertes_secteur)
demande_perdue_totale  <- sum(demande_non_satisfaite)
surcouts_directs_usd   <- sum(od_compare$surcout_absolu_usd, na.rm = TRUE)

# Multiplicateur indirect : ratio entre pertes indirectes et surcoûts directs
# (les surcoûts directs sont en USD, les pertes indirectes en M USD)
multiplicateur <- if (surcouts_directs_usd > 0) {
  (perte_indirecte_totale * 1e6) / surcouts_directs_usd
} else NA

cat("==========================================================\n")
cat("  SYNTHÈSE — PARTIE X (ARIO-inventory)\n")
cat("==========================================================\n\n")
cat("Scénario             :", NOM_SCENARIO, "\n")
cat("Description          :", DESCRIPTION_SCENARIO, "\n")
cat("Durée perturbation   :", DUREE_JOURS, "jours\n")
cat("Horizon simulation   :", ARIO_HORIZON_JOURS, "jours\n")
cat("ψ (hétérogénéité)    :", ARIO_PSI, "\n\n")
cat("Pertes économiques :\n")
cat("  Surcoûts directs (Partie IX)  :",
    round(surcouts_directs_usd / 1e6, 2), "M USD\n")
cat("  Pertes indirectes (production):",
    round(perte_indirecte_totale, 2), "M USD\n")
cat("  Demande finale non satisfaite :",
    round(demande_perdue_totale, 2), "M USD\n")
if (!is.na(multiplicateur)) {
  cat("  Multiplicateur indirect/direct :",
      round(multiplicateur, 2),
      "(ratio pertes indirectes / surcoûts directs)\n")
}
cat("\n")

# ── Exports CSV via DuckDB ────────────────────────────────────────────────────
duck_write(tableau_pertes_secteur,
           paste0("ario_pertes_secteurs_", NOM_SCENARIO))
duck_write(tableau_pertes_province,
           paste0("ario_pertes_provinces_", NOM_SCENARIO))

# Trajectoires complètes (un fichier par dimension d'agrégation)
trajectoire_secteurs_df <- production_par_jour_secteur %>%
  as.data.frame() %>%
  mutate(jour = seq_len(ARIO_HORIZON_JOURS)) %>%
  select(jour, everything())

trajectoire_provinces_df <- production_par_jour_province %>%
  as.data.frame() %>%
  mutate(jour = seq_len(ARIO_HORIZON_JOURS)) %>%
  select(jour, everything())

duck_write(trajectoire_secteurs_df,
           paste0("ario_trajectoire_secteurs_", NOM_SCENARIO))
duck_write(trajectoire_provinces_df,
           paste0("ario_trajectoire_provinces_", NOM_SCENARIO))

# Exports vers CSV
fichiers_a_exporter <- c(
  paste0("ario_pertes_secteurs_",        NOM_SCENARIO),
  paste0("ario_pertes_provinces_",       NOM_SCENARIO),
  paste0("ario_trajectoire_secteurs_",   NOM_SCENARIO),
  paste0("ario_trajectoire_provinces_",  NOM_SCENARIO)
)

for (nom_table in fichiers_a_exporter) {
  dbExecute(con, paste0(
    "COPY (SELECT * FROM ", nom_table, ") TO '",
    file.path(DIR_EXPORTS, paste0(nom_table, ".csv")),
    "' (FORMAT CSV, HEADER)"
  ))
}

cat("\n✓ Exports CSV terminés (", length(fichiers_a_exporter), "fichiers)\n\n")


cat("==========================================================\n")
cat("  PARTIE X TERMINÉE\n")
cat("==========================================================\n\n")

saveRDS(
  list(
    tableau_pertes_secteur   = tableau_pertes_secteur,
    tableau_pertes_province  = tableau_pertes_province,
    trajectoire_secteurs_df  = trajectoire_secteurs_df,
    trajectoire_provinces_df = trajectoire_provinces_df,
    perte_indirecte_totale   = perte_indirecte_totale,
    multiplicateur           = multiplicateur,
    NOM_SCENARIO             = NOM_SCENARIO,
    prod_ref_secteur         = prod_ref_secteur,   
    prod_ref_province        = prod_ref_province, 
    noms_provinces           = noms_provinces,    
    Z                        = Z,                  
    industries_idx           = industries_idx,     
    provinces_ario           = provinces_ario,    
    date_creation            = Sys.time()
  ),
  PERSIST_ARIO
)
cat("✓ persist_ario.rds\n")