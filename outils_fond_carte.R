################################################################################
# outils_fond_carte.R
# RÔLE : Définit la fonction fond_carte() réutilisée par tous les scripts de
#        visualisation. Doit être sourcé APRÈS avoir chargé les objets géo
#        (rwanda_provinces, rwanda_national, bbox_carto, lacs_raw/ok, parcs_raw/ok)
#        dans l'environnement courant.
# USAGE : source("outils_fond_carte.R")
# APPELÉ PAR : 01_reseau.R, 02_couts.R, 03_transport.R, 04_vulnerabilite.R,
#              05_ario.R, viz_reseau.R, viz_fret.R, viz_vulnerabilite.R, viz_ario.R
################################################################################

# Vérifie que les objets nécessaires sont présents
.objets_requis <- c("rwanda_provinces", "rwanda_national", "bbox_carto",
                    "lacs_ok", "parcs_ok")
.manquants <- .objets_requis[!sapply(.objets_requis, exists, envir = .GlobalEnv)]
if (length(.manquants) > 0) {
  stop("outils_fond_carte.R : objets manquants dans l'environnement : ",
       paste(.manquants, collapse = ", "),
       "\n  → Charger persist_geodata.rds avant de sourcer ce fichier.")
}
rm(.objets_requis, .manquants)

fond_carte <- function() {
  
  carte <- tm_shape(rwanda_provinces, bbox = bbox_carto) +
    tm_polygons(
      fill = "#F5F5F0",
      col  = "#AAAAAA",
      lwd  = 0.8,
      fill.legend = tm_legend(show = FALSE)
    ) +
    tm_shape(rwanda_national) +
    tm_borders(col = "#222222", lwd = 2.5)
  
  if (parcs_ok && !is.null(parcs_raw)) carte <- carte +
      tm_shape(parcs_raw) +
      tm_polygons(
        fill        = "#A8D5A2",
        col         = "#5A9E52",
        lwd         = 1.2,
        fill_alpha  = 0.45,
        fill.legend = tm_legend(show = FALSE)
      )
  
  if (lacs_ok && !is.null(lacs_raw)) carte <- carte +
      tm_shape(lacs_raw) +
      tm_polygons(
        fill        = "#A8C8E8",
        col         = "#7AAAC8",
        lwd         = 0.5,
        fill.legend = tm_legend(show = FALSE)
      )
  
  carte
}

cat("✓ outils_fond_carte.R : fond_carte() disponible\n")