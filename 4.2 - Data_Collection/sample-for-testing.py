import os
import re
import random

def get_all_files(dir_path):
    for subdir, dirs, files in os.walk(dir_path):
        for filename in files:
            yield filename

def get_files_from_year(all_files, year):
    for f in all_files:
        m_year = re.match(r"(\d+)\-.*", f).group(1)
        if year == m_year:
            yield f

def get_files_from_month(all_files, month):
    for f in all_files:
        m_month = re.match(r"\d+\-(\d+)\-.*", f).group(1)
        if month == m_month:
            yield f

if __name__ == "__main__":
    seed = 10
    random.seed(seed)
    sample = []

    debates_dir = input("Enter debates directory:\n") # debates final

    all_files = [f for f in get_all_files(debates_dir)]
    for y in ["2015", "2016", "2017", "2018", "2019"]:
        y_files = [f for f in get_files_from_year(all_files, y)]
        for m in ["01", "04", "07", "10"]:
            m_files = [f for f in get_files_from_month(y_files, m)]
            if len(m_files):
                sample.append(random.choice(m_files))

    for f in sample:
        print(f)
