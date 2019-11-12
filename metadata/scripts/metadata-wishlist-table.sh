#!/bin/bash

#This script outputs a table of the metadata that we'd like to have

sIODir=$1
sJavaDir=$2

cd $sIODir

sqlite3 formatted_metadata/metadata_formatted.db "select * from tbl1 where variable in ('forward_primer',  'reverse_primer', 'target_gene', 'primer_suite', 'run', 'illumina_index', 'sequencing_platform', 'water_age', 'tot_depth_water_col', 'temperature', 'sulfate', 'sodium', 'sequencing_platform', 'samp_store_temp', 'samp_store_dur', 'sample_type', 'sample_name', 'salinity', 'rock_type', 'rock_age', 'project_abstract', 'pressure', 'ph', 'material_secondary', 'longitude', 'lithology', 'lat_lon', 'latitude', 'habitat_description', 'geo_loc_name_marine', 'geo_loc_name_continental', 'geo_loc_name', 'feature_secondary', 'env_package', 'env_material', 'env_feature', 'env_biome', 'elevation', 'domain', 'dna_region', 'dna_quantitation', 'dna_extraction_meth', 'depth_subterrestrial', 'depth_subseafloor', 'depth_in_core', 'depth', 'conductivity', 'collection_date', 'chloride', 'borehole_depth', 'biome_secondary', 'blank_or_control');" | tail -n+2 > formatted_metadata/metadata_wishlist_table.csv
java -cp $sJavaDir/Utilities.jar edu.ucsf.FlatFileToPivotTable.FlatFileToPivotTableLauncher \
	--sValueHeader=VALUE \
	--rgsExpandHeaders=VARIABLE \
	--sDataPath=$sIODir/formatted_metadata/metadata_wishlist_table.csv \
	--sOutputPath=$sIODir/formatted_metadata/metadata_wishlist_table.csv
sed -i -e "s|unknown|null|g" -e "s|undefined|null|g" -e "s|VARIABLE=||g" -e "s|\,\,|\,null\,|g" -e "s|None|null|g" formatted_metadata/metadata_wishlist_table.csv

