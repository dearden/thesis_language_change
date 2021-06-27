import os
import sys
import regex as re
from shutil import copyfile

if len(sys.argv) > 1:
    in_dir = sys.argv[1]
    out_dir = sys.argv[2]
else:
    in_dir = input("Enter Dump Directory: ")
    out_dir = input("Enter Tidy Directory")

directory_re = re.compile(r'.*[\\\/](\d\d\d\d\-\d\d\-\d\d).*?[\\\/](\d+)[\\\/].*'
)
# iterate through all files and call the find_all_xmls function.
for subdir, dirs, files in os.walk(in_dir):
    for filename in files:
        curr_fp = os.path.join(subdir, filename).replace("\\", "/")
        print("Reading in {}".format(curr_fp))

        # Match the fp to the regex
        m = directory_re.match(subdir)

        # If the file does not match the expected format, skip it
        if not m:
            print("Bad file name: ", curr_fp)
            continue

        # Add the date to the out directory
        out_file_dir = "{0}/{1}".format(out_dir, m.group(1))

        # Make the directory if it doesn't exist
        if not os.path.isdir(out_file_dir):
            os.makedirs(out_file_dir)

        # Add the ID number to the file name to allow for multiple versions of same file.
        new_filename = "{0}-{1}".format(m.group(2), filename)
        out_fp = os.path.join(out_file_dir, new_filename)

        # Check if the file already exists. If not, save it.
        if not os.path.isfile(out_fp):
            copyfile(curr_fp, out_fp)
            print("Writing to {}".format(out_fp))
        else:
            print("There was a problem. There was already a {}.".format(filename))

        print(out_fp)
