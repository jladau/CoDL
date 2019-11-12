#!/bin/bash

#This script outputs a table of the metadata that are useful that are reasonably minimal

sIODir=$1
sJavaDir=$2
sBIOMPath=$3
sUtilitiesDir=$4

cd $sIODir

#outputting and appending sequence counts
java -cp $sJavaDir/Utilities.jar  edu.ucsf.BIOM.PrintReadCounts.PrintReadCountsLauncher \
	--sDataPath=$sBIOMPath \
	--sOutputPath=$sIODir/temp.0.csv \
	--bNormalize=false
bash $sUtilitiesDir/ExtractMetadataBiom.sh $sBIOMPath $sIODir/temp.1.db
sqlite3 temp.1.db "select * from tbl1;" | tail -n+2 > temp.2.csv
java -cp $sJavaDir/Utilities.jar edu.ucsf.JoinTables.JoinTablesLauncher \
	--sBaseDataPath=$sIODir/temp.2.csv \
	--sAppendDataPath=$sIODir/temp.0.csv \
	--rgsBaseKeys='SAMPLE_ID' \
	--rgsAppendKeys='SAMPLE_ID' \
	--rgsAppendValues='NUMBER_READS' \
	--sOutputPath=$sIODir/temp.3.csv

#appending substrate
echo 'ENV_MATERIAL,SUBSTRATE' > temp.4.csv
echo 'mud,sediment_or_surface' >> temp.4.csv
echo 'sediment,sediment_or_surface' >> temp.4.csv
echo 'rock,rock' >> temp.4.csv
echo 'fluid,fluid' >> temp.4.csv
echo 'water,fluid' >> temp.4.csv
java -cp $sJavaDir/Utilities.jar edu.ucsf.JoinTables.JoinTablesLauncher \
	--sBaseDataPath=$sIODir/temp.3.csv \
	--sAppendDataPath=$sIODir/temp.4.csv \
	--rgsBaseKeys='ENV_MATERIAL' \
	--rgsAppendKeys='ENV_MATERIAL' \
	--rgsAppendValues='SUBSTRATE' \
	--sOutputPath=temp.5.csv

#turning into database
rm -f temp.6.db
sqlite3 temp.6.db ".import $sIODir/temp.5.csv tbl1"
sqlite3 temp.6.db "select sample_id, latitude, longitude from tbl1;" | tail -n+2 > all-samples.csv
sqlite3 temp.6.db "select sample_id, latitude, longitude from tbl1 where not(temperature='null');" | tail -n+2 > temperature-samples.csv
sqlite3 temp.6.db "select sample_id, latitude, longitude from tbl1 where not(ph='null');" | tail -n+2 > ph-samples.csv
sqlite3 temp.6.db "select sample_id, latitude, longitude from tbl1 where not(temperature='null') and substrate='sediment_or_surface';" | tail -n+2 > temperature-sediment_or_surface-samples.csv
sqlite3 temp.6.db "select sample_id, latitude, longitude from tbl1 where not(temperature='null') and substrate='rock';" | tail -n+2 > temperature-rock-samples.csv
sqlite3 temp.6.db "select sample_id, latitude, longitude from tbl1 where not(temperature='null') and substrate='fluid';" | tail -n+2 > temperature-fluid-samples.csv
mv temp.6.db mapping_data.db

#cleaning up
mkdir -p mapping_data
rm temp.*
mv *.csv mapping_data/
mv mapping_data.db mapping_data/


