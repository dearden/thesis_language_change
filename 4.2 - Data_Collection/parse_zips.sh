#!/bin/bash

$ZIP_DIR='*INSERT_DIRECTORY*/Zips'
$DUMP_DIR='*INSERT_DIRECTORY*/Dump'
$TIDY_DIR='*INSERT_DIRECTORY*/Tidy'
$TEMP_DIR='*INSERT_DIRECTORY*/Processed'
$JSON_DIR='*INSERT_DIRECTORY*/Final'

$DB_PATH='*INSERT_DIRECTORY*/commons-update.db'

$START='2015-05-07'
$END='2019-12-12'

echo 'Beginning extraction.'

echo $ZIP_DIR
echo $DUMP_DIR
echo $TIDY_DIR

python3 filter_files.py "$ZIP_DIR" "$DUMP_DIR"

echo 'All files extracted.'

python3 tidy_files.py "$DUMP_DIR" "$TIDY_DIR"

echo 'All files tidied.'

python3 process_xml.py "$TIDY_DIR" "$TEMP_DIR"

echo 'All files processed'

python3 remove_duplicates.py "$TEMP_DIR" "$JSON_DIR"

echo 'All Duplicates Removed'

python3 delete_outdated.py "$JSON_DIR" "$START" "$END"

echo 'Deleted files out of time range'

python3 make_db.py "$DB_PATH", "$JSON_DIR"

echo 'Created database.'
