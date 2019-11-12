#!/bin/bash

#This script formats CoDL metadata downloaded from VAMPS2

sIODir=$1
sJavaDir=$2

cd $sIODir

#decompressing raw metadata
#cp $sIODir/raw_metadata/dco_all_metadata_2017-10-11.csv.gz temp.1.csv.gz
#cp $sIODir/raw_metadata/dco_all_metadata_2017-11-14.csv.gz temp.1.csv.gz
#cp $sIODir/raw_metadata/dco_all_metadata_2017-11-29.csv.gz temp.1.csv.gz
#cp $sIODir/raw_metadata/dco_all_metadata_2017-12-04.csv.gz temp.1.csv.gz
cp $sIODir/raw_metadata/dco_all_metadata_2017-12-15.csv.gz temp.1.csv.gz
gunzip -r temp.1.csv.gz
mv temp.1.csv temp.2.csv

#flattening interpolated values file
lstColumnsToFlatten=`head --lines=1 raw_supplementary_metadata/metadata_minimal_pivot_selected_2017-09-20.csv | sed "s|PROJECT_ID\,SAMPLE_ID\,||g" | sed "s|\r||g"#`
java -cp $sJavaDir/Utilities.jar edu.ucsf.PivotTableToFlatFile.PivotTableToFlatFileLauncher \
	--sDataPath=$sIODir/raw_supplementary_metadata/metadata_minimal_pivot_selected_2017-09-20.csv \
	--lstColumnsToFlatten=$lstColumnsToFlatten \
	--sOutputPath=$sIODir/temp.3.csv
sed -i "s|FLAT_VAR_KEY\,FLAT_VAR_VALUE|VARIABLE\,VALUE|g" temp.3.csv

#appending blank_or_control and enrichment field
grep 'blank_or_control' temp.3.csv | sed "s|blank_or_control|blank_or_control\,null|g" >> temp.2.csv
grep 'enrichment' temp.3.csv | sed "s|enrichment|enrichment\,null|g" >> temp.2.csv
grep 'include' temp.3.csv | sed "s|include|include\,null|g" >> temp.2.csv

#standardizing null values
sed -i -e "s|unknown|null|g" -e "s|undefined|null|g" -e "s|\,\,|\,null\,|g" -e "s|None|null|g" temp.3.csv
sed -i -e "s|unknown|null|g" -e "s|undefined|null|g" -e "s|\,\,|\,null\,|g" -e "s|None|null|g" temp.2.csv

#joining interpolated values
java -cp $sJavaDir/Utilities.jar edu.ucsf.JoinTables.JoinTablesLauncher \
	--sBaseDataPath=$sIODir/temp.2.csv \
	--sAppendDataPath=$sIODir/temp.3.csv \
	--rgsBaseKeys='PROJECT_ID,SAMPLE_ID,VARIABLE' \
	--rgsAppendKeys='PROJECT_ID,SAMPLE_ID,VARIABLE' \
	--rgsAppendValues='VALUE' \
	--sOutputPath=$sIODir/temp.1.csv
sed -i -e "s|VALUE|VALUE_0|" -e "s|\,\-9999$|\,null|g" temp.1.csv

#adding interpolated values (priority is given to values input by the investigators)
rm -f temp.4.db
sqlite3 temp.4.db ".import $sIODir/temp.1.csv tbl1"
echo 'PROJECT_ID,SAMPLE_ID,VARIABLE,VALUE' > temp.5.csv
sqlite3 temp.4.db "select PROJECT_ID, SAMPLE_ID, VARIABLE, VALUE_0 from tbl1 where not(VALUE_0='null') or VALUE='null';" | tail -n+3 >> temp.5.csv
i1=`wc -l temp.5.csv | cut -d' ' -f1`
sqlite3 temp.4.db "select PROJECT_ID, SAMPLE_ID, VARIABLE, VALUE from tbl1 where VALUE_0='null' and not(VALUE='null');" | tail -n+3 >> temp.5.csv
i2=`wc -l temp.5.csv | cut -d' ' -f1`

echo $(($i2-$i1))'  interpolated records added'

#adding geoconductivity geohydroconnectivity data
sed "s|\r|\n|g" raw_supplementary_metadata/geohydroconnectivity_metadata_rick_colwell.csv | cut -d\, -f1,2,14 | tail -n+2 | sed "s|\,|\,geohydroconnectivity_raw\,|2" >> temp.5.csv
sed "s|\r|\n|g" raw_supplementary_metadata/geohydroconnectivity_metadata_rick_colwell.csv | cut -d\, -f1,2,14 | tail -n+2 | sed -e "s|1$|high|g" -e "s|2$|high|g" -e "s|3$|high|g" -e "s|4$|low|g" -e "s|5$|low|g" | sed "s|\,|\,geohydroconnectivity\,|2" >> temp.5.csv

mv temp.5.csv formatted_metadata/metadata_formatted.csv

#importing formatted metadata to database
rm -f formatted_metadata/metadata_formatted.db
sqlite3 formatted_metadata/metadata_formatted.db ".import $sIODir/formatted_metadata/metadata_formatted.csv tbl1"

#cleaning up
rm -f *.txt
rm -f metadata-*.csv
rm -f temp.*
