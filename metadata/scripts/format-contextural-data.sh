#!/bin/bash

#This script formats CoDL contextural data downloaded from VAMPS2

sIODir=/home/jladau/Desktop/current-projects/codl-contextural-data
sJavaDir=/home/jladau/Documents/Research/Java

cd $sIODir

#resolving multiple fields problems
rm -f temp.28.db
sqlite3.2 temp.28.db ".import $sIODir/raw-data/dco_all_metadata_2019-10-25.csv tbl1"
sqlite3.2 temp.28.db "select distinct VARIABLE, VALUE from tbl1 where VALUE in ('aquifer', 'borehole', 'cave', 'enrichment', 'geological fracture', 'geyser', 'hydrothermal vent', 'lake', 'mine', 'ocean_trench', 'reservoir', 'seep', 'spring', 'volcano', 'well') order by VARIABLE;" > temp.29.csv
sqlite3.2 temp.28.db "select distinct VARIABLE, VALUE from tbl1 where VALUE in ('terrestrial biome', 'marine biome') order by VARIABLE;" > temp.30.csv
sqlite3.2 temp.28.db "select distinct VARIABLE, VALUE from tbl1 where VALUE in ('biofilm', 'fluid', 'microbial mat material', 'mud', 'oil', 'rock', 'sand', 'sediment', 'soil', 'water') order by VARIABLE;" > temp.31.csv

#copying raw contextural data
cp $sIODir/raw-data/dco_all_metadata_2019-10-25.csv temp.2.csv

#correcting vamps enrichment and blank/control fields
sed -i "s|sample_type\,enrichment|enrichment\,TRUE|g" temp.2.csv
sed -i "s|sample_type\,environmental\ sample|enrichment\,FALSE|g" temp.2.csv
sed -i "s|sample_type\,control|blank_or_control\,TRUE|g" temp.2.csv

#correcting vamps env_biome field
sed -i "s|envo_biome|env_biome|g" temp.2.csv

#correcting vamps env_feature field
sed -i "s|access_point_type|env_feature|g" temp.2.csv
sed -i "s|feature_secondary|env_feature|g" temp.2.csv
sed -i "s|sample_type|env_feature|g" temp.2.csv

#correcting vamps env_material field
sed -i "s|env_matter|env_material|g" temp.2.csv
sed -i "s|env_package|env_material|g" temp.2.csv
sed -i "s|environmental_packag|env_material|g" temp.2.csv
sed -i "s|envo_material|env_material|g" temp.2.csv
sed -i "s|lithology|env_material|g" temp.2.csv
sed -i "s|rock_type|env_material|g" temp.2.csv

#removing quotations
sed -i "s|\"||g" temp.2.csv

#flattening interpolated values file
lstColumnsToFlatten=`head --lines=1 raw-data/metadata_minimal_pivot_selected_2017-09-20.csv | sed "s|PROJECT_ID\,SAMPLE_ID\,||g" | sed "s|\r||g"#`
java -cp $sJavaDir/Utilities.jar edu.ucsf.PivotTableToFlatFile.PivotTableToFlatFileLauncher \
	--sDataPath=$sIODir/raw-data/metadata_minimal_pivot_selected_2017-09-20.csv \
	--lstColumnsToFlatten=$lstColumnsToFlatten \
	--sOutputPath=$sIODir/temp.3.csv
sed -i "s|FLAT_VAR_KEY\,FLAT_VAR_VALUE|VARIABLE\,VALUE|g" temp.3.csv

#appending source and concatenating
sed -i "1,1 s|^|SOURCE\,|g" temp.2.csv
sed -i "2,$ s|^|vamps\,|g" temp.2.csv
sed -i "1,1 s|^|SOURCE\,|g" temp.3.csv
sed -i "2,$ s|^|codl\,|g" temp.3.csv
cp temp.2.csv temp.4.csv
tail -n+2 temp.3.csv >> temp.4.csv

#standardizing null values
sed -i -e "s|unknown|null|g" -e "s|undefined|null|g" -e "s|\,\,|\,null\,|g" -e "s|None|null|g" temp.4.csv

#standardizing ph names
sed -i "s|pH|ph|g" temp.4.csv

#removing null values
grep -v "\,null$" temp.4.csv | sponge temp.4.csv

#loading flat files to databases
rm -f temp.5.db
sqlite3.2 temp.5.db ".import $sIODir/temp.4.csv tbl1"

#loading data
sqlite3.2 temp.5.db "select SOURCE, PROJECT_ID, SAMPLE_ID, VARIABLE, VALUE from tbl1 where 
VARIABLE='latitude' or 
VARIABLE='longitude' or 
VARIABLE='enrichment' or
VARIABLE='env_biome' or
VARIABLE='env_feature' or
VARIABLE='env_material' or
VARIABLE='ph' or
VARIABLE='temperature' or
VARIABLE='blank_or_control' or
VARIABLE='include';" > temp.7.csv
java -cp $sJavaDir/Utilities.jar edu.ucsf.FlatFileToPivotTable.FlatFileToPivotTableLauncher \
	--sValueHeader=VALUE \
	--rgsExpandHeaders='SOURCE,VARIABLE' \
	--sDataPath=$sIODir/temp.7.csv \
	--sOutputPath=$sIODir/temp.8.csv
sed -i -e "1,1 s|SOURCE=||g" -e "1,1 s|VARIABLE=||g" -e "1,1 s|\;|_|g" temp.8.csv

rm -f temp.9.db
sqlite3.2 temp.9.db ".import $sIODir/temp.8.csv tbl1"
sqlite3.2 temp.9.db "select PROJECT_ID, SAMPLE_ID, 
(case when not(codl_latitude='null') then codl_latitude else vamps_latitude end) as LATITUDE, 
(case when not(codl_longitude='null') then codl_longitude else vamps_longitude end) as LONGITUDE,
(case when not(codl_env_biome='null') then codl_env_biome else vamps_env_biome end) as ENV_BIOME,
(case when not(codl_env_feature='null') then codl_env_feature else vamps_env_feature end) as ENV_FEATURE,
(case when not(codl_env_material='null') then codl_env_material else vamps_env_material end) as ENV_MATERIAL,
(case when not(codl_ph='null') then codl_ph else vamps_ph end) as PH,
(case when not(codl_temperature='null') then codl_temperature else vamps_temperature end) as TEMPERATURE,
(case when not(codl_blank_or_control='null') then codl_blank_or_control else 
(case when not(vamps_enrichment='null') then 'FALSE' else vamps_blank_or_control end) 
end) as BLANK_OR_CONTROL,
(case when not(codl_enrichment='null') then codl_enrichment else vamps_enrichment end) as ENRICHMENT,
codl_include as INCLUDE from tbl1;" > temp.10.csv

#compiling list of unique project/sample/contextural data combinations
rm -f temp.15.db
sqlite3.2 temp.15.db ".import $sIODir/temp.10.csv tbl1"
sqlite3.2 temp.15.db "select SAMPLE_ID as SAMPLE_ID_UNIQUE, PROJECT_ID as PROJECT_ID_UNIQUE, PROJECT_ID, SAMPLE_ID from tbl1 where not(PROJECT_ID='DCO_RESQ');" > temp.16.csv
sed -i "1,1 s|SAMPLE_ID_UNIQUE|SAMPLE_ID_UNIQUE\-\-SAMPLE_ID_UNIQUE|1" temp.16.csv
sed "s|\-\-|\;|1" temp.16.csv | cut -d\; -f2- > temp.17.csv

sqlite3.2 temp.15.db "select SAMPLE_ID as TEMP, PROJECT_ID as PROJECT_ID_OLD, SAMPLE_ID as SAMPLE_ID_OLD from tbl1 where PROJECT_ID='DCO_RESQ';" | tail -n+2 > temp.18.csv
sed "s|\-\-|\;|1" temp.18.csv | cut -d\; -f2- | sed "s|_|\,|2" | sed "1,1 s|^|PROJECT_ID_UNIQUE\,SAMPLE_ID_UNIQUE\,PROJECT_ID\,SAMPLE_ID\n|g" > temp.19.csv
java -cp $sJavaDir/Utilities.jar edu.ucsf.ReorderColumns.ReorderColumnsLauncher \
	--sDataPath=$sIODir/temp.19.csv \
	--rgsNewHeaderOrdering='SAMPLE_ID_UNIQUE,PROJECT_ID_UNIQUE,PROJECT_ID,SAMPLE_ID' \
	--sOutputPath=$sIODir/temp.19.csv
tail -n+2 temp.19.csv >> temp.17.csv

#joining unique names to contextural data database
joiner 'SAMPLE_ID,PROJECT_ID' temp.10.csv temp.17.csv | cut -d\, -f3- > temp.21.csv

#flattening contextural data database and filling in unique records
java -cp $sJavaDir/Utilities.jar edu.ucsf.PivotTableToFlatFile.PivotTableToFlatFileLauncher \
	--sDataPath=$sIODir/temp.21.csv \
	--lstColumnsToFlatten='LATITUDE,LONGITUDE,ENV_BIOME,ENV_FEATURE,ENV_MATERIAL,PH,TEMPERATURE,ENRICHMENT,BLANK_OR_CONTROL,INCLUDE' \
	--sOutputPath=$sIODir/temp.22.csv
grep -v "\,null" temp.22.csv | sponge temp.22.csv
java -cp $sJavaDir/Utilities.jar edu.ucsf.FlatFileToPivotTable.FlatFileToPivotTableLauncher \
	--sValueHeader=FLAT_VAR_VALUE \
	--rgsExpandHeaders=FLAT_VAR_KEY \
	--sDataPath=$sIODir/temp.22.csv \
	--sOutputPath=$sIODir/temp.23.csv
sed -i "1,1 s|FLAT_VAR_KEY\=||g" temp.23.csv

#joining filled in records
joiner 'PROJECT_ID_UNIQUE,SAMPLE_ID_UNIQUE' temp.17.csv temp.23.csv | sed "s|\,NA|\,null|g" > temp.24.csv

#compiling map from otu table sample ids to contextural data ids
head --lines=1 $sIODir/raw-data/Bv4v5_All.txt | sed "s|\ |\n|g" | sed "s|___|~__|g" | sed "s|__|\,|g" | cut -d\, -f2-3 | sed "s|\,|\-\-|g" > temp.25.csv
paste -d\, <(head --lines=1 $sIODir/raw-data/Bv4v5_All.txt | sed "s|\ |\n|g" | grep 'DCO_') <(cat temp.25.csv | grep 'DCO_') | sed "1,1 s|^|SAMPLE_ID_OTU_TABLE\,SAMPLE_ID\n|g" | sed "s|~|_|g" > temp.26.csv
head --lines=1 $sIODir/raw-data/Av4v5_All.txt | sed "s|\ |\n|g" | sed "s|___|~__|g" | sed "s|__|\,|g" | cut -d\, -f2-3 | sed "s|\,|\-\-|g" > temp.25.csv
paste -d\, <(head --lines=1 $sIODir/raw-data/Av4v5_All.txt | sed "s|\ |\n|g" | grep 'DCO_') <(cat temp.25.csv | grep 'DCO_') | sed "1,1 s|^|SAMPLE_ID_OTU_TABLE\,SAMPLE_ID\n|g" | sed "s|~|_|g" | tail -n+2 >> temp.26.csv

#joining contextural data
joiner 'SAMPLE_ID' temp.26.csv temp.24.csv > temp.27.csv
sed -i "s|\,NA|\,null|g" temp.27.csv

#replacing old values with new value labels
rm -f temp.32.db
sqlite3.2 temp.32.db ".import $sIODir/temp.27.csv tbl1"
sqlite3.2 temp.32.db "select distinct BLANK_OR_CONTROL from tbl1;" | tail -n+2 | sed "1,1 s|^|VARIABLE\,VALUE\n|g" | sed "2,$ s|^|blank_or_control\,|g" > temp.33.csv
sqlite3.2 temp.32.db "select distinct ENRICHMENT from tbl1;" | sed "2,$ s|^|enrichment\,|g" | tail -n+2 >> temp.33.csv
sqlite3.2 temp.32.db "select distinct ENV_BIOME from tbl1;" | sed "2,$ s|^|env_biome\,|g" | tail -n+2 >> temp.33.csv
sqlite3.2 temp.32.db "select distinct ENV_FEATURE from tbl1;" | sed "2,$ s|^|env_feature\,|g" | tail -n+2 >> temp.33.csv
sqlite3.2 temp.32.db "select distinct ENV_MATERIAL from tbl1;" | sed "2,$ s|^|env_material\,|g" | tail -n+2 >> temp.33.csv
#TODO manually create value-correction-map.csv from temp.33.csv here

cut -d\, -f2-3 raw-data/value-correction-map.csv | sed "s|VALUE_CODL_VAMPS\,VALUE_CORRECT|OLD_STRING\,NEW_STRING|g" > temp.13.csv

sed -i -e "s|^|\"|g" -e "s|\,|\"\,\"|g" -e "s|$|\"|g" temp.13.csv
sed -i -e "s|^|\"|g" -e "s|\,|\"\,\"|g" -e "s|$|\"|g" temp.27.csv


java -cp $sJavaDir/Utilities.jar edu.ucsf.ReplaceStringsUsingFile.ReplaceStringsUsingFileLauncher \
	--sReplacementMapPath=$sIODir/temp.13.csv \
	--sOutputPath=$sIODir/temp.34.csv \
	--sInputPath=$sIODir/temp.27.csv \
	--bReplaceFirstOnly=false
sed -i "s|\,NA|\,null|g" temp.34.csv
sed -i "s|\"||g" temp.34.csv

#updating sample and project id headers
java -cp $sJavaDir/Utilities.jar edu.ucsf.CutByHeader.CutByHeaderLauncher \
	--sDataPath=$sIODir/temp.34.csv \
	--rgsHeadersToInclude='SAMPLE_ID_OTU_TABLE,SAMPLE_ID_UNIQUE,PROJECT_ID_UNIQUE,BLANK_OR_CONTROL,ENRICHMENT,ENV_BIOME,ENV_FEATURE,ENV_MATERIAL,LATITUDE,LONGITUDE,PH,TEMPERATURE,INCLUDE' \
	--sOutputPath=$sIODir/temp.34.csv
sed -i -e "s|SAMPLE_ID_UNIQUE|SAMPLE_ID|g" -e "s|PROJECT_ID_UNIQUE|PROJECT_ID|g" temp.34.csv
java -cp $sJavaDir/Utilities.jar edu.ucsf.ReorderColumns.ReorderColumnsLauncher \
	--sDataPath=$sIODir/temp.34.csv \
	--rgsNewHeaderOrdering='SAMPLE_ID_OTU_TABLE,PROJECT_ID,SAMPLE_ID,BLANK_OR_CONTROL,ENRICHMENT,ENV_BIOME,ENV_FEATURE,ENV_MATERIAL,LATITUDE,LONGITUDE,PH,TEMPERATURE,INCLUDE' \
	--sOutputPath=$sIODir/temp.34.csv

#updating include field
rm -f temp.35.db
sqlite3.2 temp.35.db ".import $sIODir/temp.34.csv tbl1"
sqlite3.2 temp.35.db "select *, (case when not(INCLUDE='null') then INCLUDE else 
(case when BLANK_OR_CONTROL='FALSE' and ENRICHMENT='FALSE' then 'TRUE' else 
(case when BLANK_OR_CONTROL='null' or ENRICHMENT='null' then 'null' else 'FALSE' end) end) end) as INCLUDE_2 from tbl1;" > temp.36.csv
sed -i "s|GCCAAT_TGACT_1__DCO_BMS_Bv4v5__Extraction_blank__20190109\,null\,null\,null\,null\,null\,null\,null\,null\,null\,null\,null\,null\,null|GCCAAT_TGACT_1__DCO_BMS_Bv4v5__Extraction_blank__20190109\,DCO_BMS\,null\,TRUE\,null\,null\,null\,null\,null\,null\,null\,null\,null\,FALSE|g" temp.36.csv

#converting output to database
rm -f temp.37.db
sqlite3.2 temp.37.db ".import $sIODir/temp.36.csv tbl1"

#outputting results
sqlite3.2 temp.37.db "select distinct PROJECT_ID, SAMPLE_ID, INCLUDE_2 as INCLUDE, BLANK_OR_CONTROL, ENRICHMENT, ENV_BIOME, ENV_FEATURE, ENV_MATERIAL, LATITUDE, LONGITUDE, PH, TEMPERATURE from tbl1 order by PROJECT_ID, SAMPLE_ID;" > formatted-contextural-data.csv

sqlite3.2 temp.37.db "select distinct PROJECT_ID, SAMPLE_ID, INCLUDE_2 as INCLUDE, BLANK_OR_CONTROL, ENRICHMENT, ENV_BIOME, ENV_FEATURE, ENV_MATERIAL, LATITUDE, LONGITUDE, PH, TEMPERATURE from tbl1 where not(INCLUDE_2='null') and (BLANK_OR_CONTROL='null') and not(ENRICHMENT='null') and not(ENV_BIOME='null') and not(ENV_FEATURE='null') and not(ENV_MATERIAL='null') and not(LATITUDE='null') and not(LONGITUDE='null') order by PROJECT_ID, SAMPLE_ID;" > formatted-contextural-data-complete-samples.csv

sqlite3.2 temp.37.db "select distinct PROJECT_ID, SAMPLE_ID, INCLUDE_2 as INCLUDE, BLANK_OR_CONTROL, ENRICHMENT, ENV_BIOME, ENV_FEATURE, ENV_MATERIAL, LATITUDE, LONGITUDE, PH, TEMPERATURE from tbl1 where (INCLUDE_2='TRUE' or INCLUDE_2='null') and (BLANK_OR_CONTROL='null' or ENRICHMENT='null' or ENV_BIOME='null' or ENV_FEATURE='null' or ENV_MATERIAL='null' or LATITUDE='null' or LONGITUDE='null') and not(PROJECT_ID='DCO_BRZ') order by PROJECT_ID, SAMPLE_ID;" > formatted-contextural-data-incomplete-samples-needed.csv

sqlite3.2 temp.37.db "select distinct PROJECT_ID from tbl1 where (INCLUDE_2='TRUE' or INCLUDE_2='null') and (BLANK_OR_CONTROL='null' or ENRICHMENT='null' or ENV_BIOME='null' or ENV_FEATURE='null' or ENV_MATERIAL='null' or LATITUDE='null' or LONGITUDE='null') and not(PROJECT_ID='DCO_BRZ') order by PROJECT_ID, SAMPLE_ID;" | grep -v 'null' > incomplete-project-list-needed.csv

sqlite3.2 temp.37.db "select distinct SAMPLE_ID_OTU_TABLE, PROJECT_ID, SAMPLE_ID from tbl1;" | grep -v 'null' > otu-table-sample-id-map.csv

#TODO include and flag records that have been added this weekend
#TODO update list of records that are null
#TODO forward the whole thing
#TODO flag interpolated field values


#cleaning up
#rm -f temp.*.*
