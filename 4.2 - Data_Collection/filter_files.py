import os
import zipfile
import re
import sys
from io import BytesIO


# Recursively finds all the Commons XML files in a zipped folder.
def findXML(curr_archive, filename):    
    # For each file in the zip archive
    for file_info in curr_archive.infolist():
        # Open current file.
        with curr_archive.open(file_info) as curr_file:
            # If it's a zip, open that up and search that for XMLs.
            if ".zip" in curr_file.name:
                #new_filename = "{0}/{1}".format(filename, curr_file.name.split(".")[0])
                # We have to read in bytes because that is what ZipFile wants to take.
                curr_file_bytes = BytesIO(curr_file.read())
                with zipfile.ZipFile(curr_file_bytes, "r") as next_archive:
                    findXML(next_archive, filename)
            # If the file is a Commons xml file, then extract it.
            elif re.fullmatch(r'CHAN\d+\.xml', curr_file.name):
                # Work out file name.
                new_filename = "{0}/{1}".format(filename, curr_file.name)
                # Extract to the new file.
                curr_archive.extract(file_info, new_filename)
                print("Extracting {}".format(curr_file.name))
                print("Writing to {}".format(new_filename))

# Function that kicks off all the lovely recursion.
def find_all_xmls(zip_dir, filename):
    with zipfile.ZipFile(zip_dir, "r") as curr_archive:
        findXML(curr_archive, filename)

# Main method - gets all those pesky xmls and writes them to files.
if __name__ == "__main__":
    # the directories we are working with.
    if len(sys.argv) > 1:
        in_dir = sys.argv[1]
        out_dir = sys.argv[2]
    else:
        in_dir = input("Enter Zip Directory: ")
        out_dir = input("Enter Dump Directory")

    # iterate through all files and call the find_all_xmls function.
    for subdir, dirs, files in os.walk(in_dir):
        for filename in files:
            curr_fp = os.path.join(subdir, filename).replace("\\", "/")
            out_fp = os.path.join(out_dir, re.match(r'.*[\\\/](\d\d\d\d\-\d\d\-\d\d)[\\\/].*', curr_fp).group(1), filename.split(".")[0])
            if not os.path.isdir(out_fp):
                os.makedirs(out_fp)

            find_all_xmls(curr_fp, out_fp)
