import os
import re
import json
import sys
from shutil import copyfile
from datetime import datetime


def get_all_files(dir_path):
    for subdir, dirs, files in os.walk(dir_path):
        for filename in files:
            yield os.path.join(subdir, filename).replace("\\", "/")
            # yield "{0}/{1}".format(subdir, filename)


if __name__ == "__main__":
    # Get the directories from the input parameters.
    if len(sys.argv) > 1:
        deb_dir = sys.argv[1]
        out_dir = sys.argv[2]
    else:
        deb_dir = input("Please enter the debates directory:\n") # processed_commons
        out_dir = input("Enter output directory:\n") # debates-final

    # Create the output directory if need be.
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)

    all_files = get_all_files(deb_dir)
    debs = [f for f in all_files]
    files_to_keep = [deb for deb in debs]

    for debate_file in debs:
        for comparison_file in debs:
            # Skip if it's just the same file.
            if debate_file == comparison_file:
                continue

            # Use a regex to match the file name and identify relevant groups.
            deb_match = re.fullmatch(r"(.*\/)?(\d+)\-CHAN(\d+)\.xml\.json", debate_file)
            com_match = re.fullmatch(r"(.*\/)?(\d+)\-CHAN(\d+)\.xml\.json", comparison_file)

            # Get the number of this hansard edition.
            deb_num = deb_match.group(3)
            com_num = com_match.group(3)

            # Get the unique ID part of the file.
            deb_id = deb_match.group(2)
            com_id = com_match.group(2)

            # Checks if these two debates have the same number.
            if deb_num == com_num:
                with open(debate_file) as debfile:
                    debate = json.load(debfile)

                with open(comparison_file) as comfile:
                    comparison = json.load(comfile)

                # Checks if they have the same date. If they do, then one is a new version.
                if list(debate.values())[0]['date'] == list(comparison.values())[0]['date']:
                    # If the current file is more recent, get rid of the comparison.
                    if int(deb_id) > int(com_id):
                        if comparison_file in files_to_keep:
                            files_to_keep.remove(comparison_file)

                else:
                    pass

    # Go through the files we want to keep.
    for f in files_to_keep:
        # Find the date of the current file
        with open(f) as debfile:
            debate = json.load(debfile)
        # Format the date to remove spaces
        date = list(debate.values())[0]['date'].strip()
        date = datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')

        # Get the actual file name bit and make the output file name.
        file_match = re.fullmatch(r"(.*\/)?(\d+)\-(CHAN\d+)\.xml\.json", f)
        ou_fp = os.path.join(out_dir, "{1}-{0}.json".format(file_match.group(3), date))
        # Copy the file across.
        copyfile(f, ou_fp)
    pass
