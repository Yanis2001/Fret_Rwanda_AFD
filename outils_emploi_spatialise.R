# =============================================================================
# outils_emploi_spatialise.R
#
# OBJECTIF
#   Remplacer la ventilation PROPORTIONNELLE de l'emploi par district × secteur
#   (data/raw/rwa_emploi_district_secteur_2022.csv) par une répartition
#   SPATIALISÉE réaliste, sans modifier les deux marges :
#     • total d'emploi de chaque district (somme de la ligne)  → INCHANGÉ
#     • total national de chaque secteur   (somme de la colonne)→ INCHANGÉ
#
# POURQUOI
#   Le fichier d'origine attribue à chaque district la MÊME structure
#   sectorielle nationale (part de chaque secteur identique partout, coef. de
#   variation = 0). Conséquence : aucune zone n'est spécialisée, la production
#   minière est « saupoudrée » au prorata de la population et dépasse partout la
#   faible demande domestique de minerai → aucune zone importatrice nette → le
#   modèle gravitaire doublement contraint (Furness, 03_transport.R) ne peut pas
#   équilibrer le secteur Mines (94,6 % d'erreur sur les origines).
#
# MÉTHODE (3 étapes)
#   1. Quotients de localisation (LQ) : on multiplie la matrice proportionnelle
#      d'origine par un facteur LQ[type_district, secteur] qui encode la
#      géographie économique réelle (mines dans la ceinture 3T, services/
#      industrie/commerce à Kigali et dans les villes, agriculture en rural).
#   2. Raking / IPF (Iterative Proportional Fitting) : on rééquilibre la matrice
#      LQ pour qu'elle retrouve EXACTEMENT les marges d'origine (lignes = totaux
#      districts, colonnes = totaux nationaux). L'IPF préserve la structure
#      relative encodée par les LQ tout en imposant les deux marges.
#   3. Arrondi entier CONTRÔLÉ : on convertit en effectifs entiers en gardant
#      les deux marges exactes (méthode du plus grand reste par ligne, puis
#      échanges intra-ligne pour rétablir les totaux de colonnes).
#
# ENTRÉE  : data/raw/rwa_emploi_district_secteur_2022_source_nationale.csv
#           (archive de la ventilation nationale d'origine — non modifiée)
# SORTIE  : data/raw/rwa_emploi_district_secteur_2022.csv (fichier lu par le pipeline)
#
# RÉ-EXÉCUTION : Rscript outils_emploi_spatialise.R
#   Le script repart toujours de l'archive « _source_nationale », il est donc
#   idempotent (relancer ne re-spatialise pas une matrice déjà spatialisée).
# =============================================================================

set.seed(2022)  # reproductibilité de l'arrondi contrôlé (départ déterministe)

chemin_archive <- "data/raw/rwa_emploi_district_secteur_2022_source_nationale.csv"
chemin_sortie  <- "data/raw/rwa_emploi_district_secteur_2022.csv"

stopifnot(file.exists(chemin_archive))
df  <- read.csv(chemin_archive, check.names = FALSE, stringsAsFactors = FALSE)

# Colonnes d'emploi sectoriel (ordre du fichier — à préserver à l'écriture)
secteurs_csv <- grep("^Emploi_", names(df), value = TRUE)

# Matrice de base (districts × secteurs) = ventilation proportionnelle d'origine
M_base <- as.matrix(df[, secteurs_csv])
rownames(M_base) <- df$District

# Marges à préserver (cibles exactes)
cible_lignes   <- rowSums(M_base)   # total d'emploi par district  (INCHANGÉ)
cible_colonnes <- colSums(M_base)   # total national par secteur   (INCHANGÉ)

# ── Étape 1 : quotients de localisation ──────────────────────────────────────
# Classification des 30 districts en 5 types selon leur profil économique réel.
#   Kigali   : Kigali City (3 districts) — tertiaire/industrie, pas de mines
#   VilleSec : pôles urbains régionaux (Huye, Musanze, Rubavu, Rusizi, Muhanga)
#   Minier   : ceinture 3T (étain/tantale/tungstène) — Ouest, Nord, Sud miniers
#   Est      : plaines agro-pastorales de l'Est — agriculture/élevage, peu de mines
#   Agricole : autres districts ruraux (Sud/Nord) à dominante agricole
type_district <- c(
  Nyarugenge = "Kigali",   Gasabo   = "Kigali",   Kicukiro  = "Kigali",
  Huye       = "VilleSec", Musanze  = "VilleSec", Rubavu    = "VilleSec",
  Rusizi     = "VilleSec", Muhanga  = "VilleSec",
  Rutsiro    = "Minier",   Ngororero= "Minier",   Karongi   = "Minier",
  Nyamasheke = "Minier",   Nyabihu  = "Minier",   Rulindo   = "Minier",
  Gakenke    = "Minier",   Burera   = "Minier",   Nyaruguru = "Minier",
  Nyamagabe  = "Minier",   Kamonyi  = "Minier",
  Nyagatare  = "Est",      Gatsibo  = "Est",      Kayonza   = "Est",
  Kirehe     = "Est",      Ngoma    = "Est",      Bugesera  = "Est",
  Rwamagana  = "Est",
  Nyanza     = "Agricole", Gisagara = "Agricole", Ruhango   = "Agricole",
  Gicumbi    = "Agricole"
)
stopifnot(all(df$District %in% names(type_district)))  # couverture des 30 districts

# Quotients de localisation LQ[type, secteur] (1 = moyenne nationale).
# > 1 : secteur sur-représenté dans ce type de district ; < 1 : sous-représenté.
# Les valeurs sont des a priori d'expert ; le raking (étape 2) les ramène ensuite
# aux marges exactes — seule la STRUCTURE relative entre districts est conservée.
# Colonnes dans l'ordre : Agriculture, Mines, Industrie, Construction,
#                         Commerce, Transport, Services.
LQ <- rbind(
  Kigali   = c(0.25, 0.05, 2.8, 2.5, 2.5, 2.2, 3.0),  # capitale : tertiaire/industrie, ~0 mine
  VilleSec = c(0.70, 1.20, 1.5, 1.4, 1.5, 1.4, 1.6),  # villes : services + un peu de mines
  Minier   = c(1.05, 5.50, 0.7, 0.7, 0.7, 0.7, 0.6),  # ceinture 3T : forte concentration minière
  Est      = c(1.30, 0.10, 0.6, 0.7, 0.8, 0.9, 0.6),  # plaines agro-pastorales : ~0 mine
  Agricole = c(1.25, 0.30, 0.6, 0.7, 0.7, 0.7, 0.6)   # rural agricole : peu de mines
)
colnames(LQ) <- secteurs_csv

# Application des LQ : M0[d,s] = base[d,s] × LQ[type(d), s]
LQ_par_district <- LQ[type_district[df$District], , drop = FALSE]
M0 <- M_base * LQ_par_district

# ── Étape 2 : raking IPF (impose les deux marges exactes) ─────────────────────
# On alterne mise à l'échelle des lignes (vers cible_lignes) et des colonnes
# (vers cible_colonnes) jusqu'à ce que les deux marges soient atteintes.
# Convergence garantie : marges compatibles (même total) et matrice positive.
M <- M0
for (iter in seq_len(1000)) {
  M <- M * (cible_lignes / rowSums(M))                 # cale les totaux districts
  M <- sweep(M, 2, cible_colonnes / colSums(M), "*")   # cale les totaux secteurs
  err <- max(abs(rowSums(M) - cible_lignes) / cible_lignes)
  if (err < 1e-12) break
}
cat("Raking IPF : convergé en", iter, "itérations (err marges <", format(err, digits = 2), ")\n")

# ── Étape 3 : arrondi entier contrôlé (préserve les deux marges) ─────────────
# 3a. Arrondi par ligne via la méthode du plus grand reste : la somme de chaque
#     ligne reste EXACTEMENT égale au total du district (entier).
M_int <- matrix(0L, nrow(M), ncol(M), dimnames = dimnames(M))
for (d in seq_len(nrow(M))) {
  plancher <- floor(M[d, ])
  manque   <- as.integer(round(cible_lignes[d] - sum(plancher)))  # unités à replacer
  if (manque > 0) {
    restes <- M[d, ] - plancher
    gagnants <- order(restes, decreasing = TRUE)[seq_len(manque)] # plus grands restes
    plancher[gagnants] <- plancher[gagnants] + 1
  }
  M_int[d, ] <- as.integer(plancher)
}

# 3b. Correction des colonnes : après 3a les totaux de secteurs peuvent dévier de
#     quelques unités (somme des écarts = 0). On résorbe par des échanges +1/-1
#     SUR LA MÊME LIGNE (un secteur en excès cède une unité à un secteur en
#     déficit), ce qui laisse les totaux de districts intacts.
ecart_col <- colSums(M_int) - cible_colonnes
n_echanges <- 0L
while (any(ecart_col != 0)) {
  s_plus  <- which.max(ecart_col)   # secteur en excès (à réduire)
  s_moins <- which.min(ecart_col)   # secteur en déficit (à augmenter)
  # Ligne où prélever : celle qui a le plus d'effectifs dans s_plus (impact relatif
  # minimal), à condition de rester >= 0 après le -1.
  candidates <- which(M_int[, s_plus] >= 1)
  d <- candidates[which.max(M_int[candidates, s_plus])]
  M_int[d, s_plus]  <- M_int[d, s_plus]  - 1L
  M_int[d, s_moins] <- M_int[d, s_moins] + 1L
  ecart_col[s_plus]  <- ecart_col[s_plus]  - 1
  ecart_col[s_moins] <- ecart_col[s_moins] + 1
  n_echanges <- n_echanges + 1L
}
cat("Arrondi contrôlé :", n_echanges, "échanges intra-ligne pour caler les colonnes\n")

# ── Vérifications strictes des marges (entiers) ──────────────────────────────
stopifnot(
  all(rowSums(M_int) == as.integer(round(cible_lignes))),     # totaux districts exacts
  all(colSums(M_int) == as.integer(round(cible_colonnes))),   # totaux secteurs exacts
  all(M_int >= 0)                                             # pas d'effectif négatif
)

# ── Écriture du CSV (mêmes colonnes/ordre que l'original) ─────────────────────
df_out <- df
df_out[, secteurs_csv] <- M_int
write.csv(df_out, chemin_sortie, row.names = FALSE)
cat("✓ Fichier spatialisé écrit :", chemin_sortie, "\n\n")

# ── Diagnostic : la spécialisation spatiale est maintenant non nulle ──────────
tot_lignes <- rowSums(M_int)
cat("Part de chaque secteur dans l'emploi du district — min / max / CV (%) :\n")
for (s in secteurs_csv) {
  p <- 100 * M_int[, s] / tot_lignes
  cat(sprintf("  %-22s %6.2f  %6.2f   CV=%5.1f\n", s, min(p), max(p),
              100 * sd(p) / mean(p)))
}
cat("\nEmploi minier : top 5 et bottom 5 districts (effectifs) :\n")
o <- order(M_int[, "Emploi_Mines"], decreasing = TRUE)
print(data.frame(District = df$District[o],
                 Mines    = M_int[o, "Emploi_Mines"])[c(1:5, 26:30), ], row.names = FALSE)
