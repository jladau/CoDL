#!/bin/bash

#This script tallies the metadata that has been entered for each project

sIODir=$1
sJavaDir=$2

cd $sIODir

#outputting overall counts
iSamples=`sqlite3 formatted_metadata/metadata_formatted.db "select distinct sample_id from tbl1;" | tail -n+3 | wc -l`
sqlite3 formatted_metadata/metadata_formatted.db "select VARIABLE, cast(count(variable) as real)/cast($iSamples as real) as FRACTION_SAMPLES_WITH_VALUES from tbl1 where not(value='null') and not(value='unknown') and not(value='undefinded') and not(value='') and not(value='None') group by VARIABLE order by FRACTION_SAMPLES_WITH_VALUES desc;" | tail -n+2 > formatted_metadata/metadata_counts_overall.csv

#outputting counts by project
sqlite3 formatted_metadata/metadata_formatted.db "select PROJECT_ID, VARIABLE, count(variable) as COUNT_SAMPLES_WITH_VALUES from tbl1 where not(value='null') and not(value='unknown') and not(value='undefinded') and not(value='') and not(value='None') group by PROJECT_ID, VARIABLE order by PROJECT_ID, VARIABLE;" | tail -n+2 | sed "s|\r||g" > temp.1.csv

sqlite3 formatted_metadata/metadata_formatted.db "select PROJECT_ID, count(SAMPLE_ID) as COUNT_SAMPLES from (select distinct PROJECT_ID, SAMPLE_ID from tbl1) group by PROJECT_ID;" | tail -n+2 | sed "s|\r||g" > temp.2.csv
sqlite3 formatted_metadata/metadata_formatted.db "select PROJECT_ID, VARIABLE from tbl1;" | tail -n+2 | sed "s|\r||g" > temp.7.csv

java -cp $sJavaDir/Utilities.jar edu.ucsf.JoinTables.JoinTablesLauncher \
	--sBaseDataPath=$sIODir/temp.7.csv \
	--sAppendDataPath=$sIODir/temp.2.csv \
	--rgsBaseKeys='PROJECT_ID' \
	--rgsAppendKeys='PROJECT_ID' \
	--rgsAppendValues='COUNT_SAMPLES' \
	--sOutputPath=$sIODir/temp.8.csv
java -cp $sJavaDir/Utilities.jar edu.ucsf.JoinTables.JoinTablesLauncher \
	--sBaseDataPath=$sIODir/temp.8.csv \
	--sAppendDataPath=$sIODir/temp.1.csv \
	--rgsBaseKeys='PROJECT_ID,VARIABLE' \
	--rgsAppendKeys='PROJECT_ID,VARIABLE' \
	--rgsAppendValues='COUNT_SAMPLES_WITH_VALUES' \
	--sOutputPath=$sIODir/temp.3.csv
sed -i "s|\-9999|0|g" temp.3.csv
rm -f temp.4.db
sqlite3 temp.4.db ".import $sIODir/temp.3.csv tbl1"
sqlite3 temp.4.db "select *, cast(COUNT_SAMPLES_WITH_VALUES as real)/cast(COUNT_SAMPLES as real) as FRACTION_SAMPLES_WITH_VALUES from tbl1;" | tail -n+2 > temp.5.csv
cut -d\, -f1-3,5 temp.5.csv > temp.6.csv
java -cp $sJavaDir/Utilities.jar edu.ucsf.FlatFileToPivotTable.FlatFileToPivotTableLauncher \
	--sValueHeader=FRACTION_SAMPLES_WITH_VALUES \
	--rgsExpandHeaders=VARIABLE \
	--sDataPath=$sIODir/temp.6.csv \
	--sOutputPath=$sIODir/temp.6.csv
sed -i -e "s|VARIABLE=||g" -e "s|null|0|g" temp.6.csv
paste -d\, <(cut -d\, -f2 temp.6.csv) <(cut -d\, -f1 temp.6.csv) <(cut -d\, -f3- temp.6.csv) > formatted_metadata/metadata_counts_by_project.csv

#cleaning up
rm temp.*.csv
rm temp.*.db
