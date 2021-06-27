# Data Collection

The code used to create the Hansard database.
They should all work - though might break if any of the APIs they use have changed.

Two scripts that run everything to make the database:
- parse_zips.sh
- parse_zips_windows.ps1

Python scripts for doing most of the processing:

Included in the parse scripts:

- download_zips.py -> downloads all parliamentary resources between two given dates.

- filter_files.py -> extracts all of the xml for commons hansard.

- tidy_files.py -> removes a lot of redundant directories from output of filter_files.py.

- remove_duplicates.py -> removes duplicate debates.

- delete_outdated.py -> removes debates outside of time range.

- make_db.py -> makes the database.

Separate:

- add_display_names.py -> adds a display name for each MP
- add_stances.py -> adds stances on selected issues.
- sample-for-testing.py -> samples some debates to manually check data integrity.
- update_db.py -> updates the database.
