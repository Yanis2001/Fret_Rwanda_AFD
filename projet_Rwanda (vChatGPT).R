################################################################################
# Projet Rwanda
################################################################################

routes_rwanda <- prepare_routes("rwanda-260315.osm.pbf")

reseau_rwanda <- build_clean_network(routes_rwanda)

aretes_df <- run_block3(reseau_rwanda, dem_rwanda, con)