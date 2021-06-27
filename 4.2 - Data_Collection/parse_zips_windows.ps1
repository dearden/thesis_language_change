$ZIP_DIR = '*INSERT_DIRECTORY*/Zips'
$DUMP_DIR = '*INSERT_DIRECTORY*/Dump'
$TIDY_DIR = '*INSERT_DIRECTORY*/Tidy'
$TEMP_DIR = '*INSERT_DIRECTORY*/Processed'
$JSON_DIR = '*INSERT_DIRECTORY*/Final'

$DB_PATH = '*INSERT_DIRECTORY*/commons-update.db'

$START = '2015-05-07'
$END = '2019-12-12'

echo 'Beginning extraction.'

echo $ZIP_DIR
echo $DUMP_DIR
echo $TIDY_DIR

python filter_files.py "$ZIP_DIR" "$DUMP_DIR"

echo 'All files extracted.'

python tidy_files.py "$DUMP_DIR" "$TIDY_DIR"

echo 'All files tidied.'

python process_xml.py "$TIDY_DIR" "$TEMP_DIR"

echo 'All files processed'

python remove_duplicates.py "$TEMP_DIR" "$JSON_DIR"

echo 'All Duplicates Removed'

python delete_outdated.py "$JSON_DIR" "$START" "$END"

echo 'Deleted files out of time range'

python make_db.py "$DB_PATH", "$JSON_DIR"

echo 'Created database.'
