#!/bin/bash

#This script tallies the metadata that has been entered for each project

sIODir=$1
sJavaDir=$2

cd $sIODir/formatted_metadata

mkdir -p additional_formatted_metadata
mkdir temp
mv metadata_minimal_table* temp/
mv *.csv additional_formatted_metadata/
mv *.db additional_formatted_metadata/
mv temp/*.csv .
rmdir temp
