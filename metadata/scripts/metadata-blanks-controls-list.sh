#!/bin/bash

#This script outputs a table of the metadata that we'd like to have

sIODir=$1
sJavaDir=$2

cd $sIODir

sqlite3 formatted_metadata/metadata_formatted.db "select * from tbl1 where variable in ('blank_or_control',  'enrichment', 'include');" | tail -n+2 > formatted_metadata/metadata-blanks-controls-list.csv
java -cp $sJavaDir/Utilities.jar edu.ucsf.FlatFileToPivotTable.FlatFileToPivotTableLauncher \
	--sValueHeader=VALUE \
	--rgsExpandHeaders=VARIABLE \
	--sDataPath=$sIODir/formatted_metadata/metadata-blanks-controls-list.csv \
	--sOutputPath=$sIODir/formatted_metadata/metadata-blanks-controls-list.csv
sed -i -e "s|unknown|null|g" -e "s|undefined|null|g" -e "s|VARIABLE=||g" -e "s|\,\,|\,null\,|g" -e "s|None|null|g" formatted_metadata/metadata-blanks-controls-list.csv

