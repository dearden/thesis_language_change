import os
import re
import sys
from datetime import datetime

in_dir = sys.argv[1]

start_date = sys.argv[2]
start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
end_date = sys.argv[3]
end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

for subdir, dirs, files in os.walk(in_dir):
    for filename in files:
        curr_file = os.path.join(subdir, filename)

        curr_date = re.match(r"(\d+\-\d+\-\d+)\-.*", filename).group(1)
        curr_datetime = datetime.strptime(curr_date, '%Y-%m-%d')

        if curr_datetime > end_datetime or curr_datetime < start_datetime:
            print("{0} - {1}".format(curr_date, curr_datetime))
            os.remove(curr_file)