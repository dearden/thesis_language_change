import pandas as pd
import os
import sys
from datetime import datetime
import numpy as np
import sqlite3
import nltk
import regex as re
import spacy
import json

from helper_functions import pos_tokenise, get_contribution_windows, split_corpus, check_dir, make_tok_chunks, get_chunks
from mp_sampling import multi_mp_splits_with_limit, multi_mp_splits, get_end_of_windows
from run_CE_experiments import get_groups_toks_and_contribs

with open("../project-config.json") as config_file:
    project_config = json.load(config_file)

DB_FP = project_config["DB_FP"]
MP_Group_FP = project_config["GROUPS_FP"]
with open(project_config["SPEAKER_FILE"]) as speaker_file:
    speaker_list = json.load(speaker_file)


if __name__ == "__main__":
    out_fp = input("Enter a filepath for tokens:\n")

    conn = sqlite3.connect(DB_FP)
    curs = conn.cursor()

    # Gets all the contributions and creates a nice dataframe
    all_contributions = pd.read_sql_query("SELECT uid, body FROM contributions;", con=conn)
    all_contributions.columns = ['uid', 'text']
    all_contributions.set_index("uid", inplace=True)
    all_contributions = all_contributions["text"]

    
    for i, text in all_contributions.items():
        with open(out_fp, "a") as out_file:
            out = [int(i), pos_tokenise(text)]
            out_file.write(json.dumps(out) + "\n")