# This script is for running the analysis that comes after the cross-entropy stuff.

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


with open("../project-config.json") as config_file:
    project_config = json.load(config_file)

DB_FP = project_config["DB_FP"]
MP_Group_FP = project_config["GROUPS_FP"]
with open(project_config["SPEAKER_FILE"]) as speaker_file:
    speaker_list = json.load(speaker_file)

from helper_functions import clean_text, spacy_tokenise, get_contribution_windows, get_keywords_from_tokens, split_corpus

sql_get_all_posts ="""
SELECT c.uid, m.name, m.PimsId, p.party, d.date, c.body, c.topic, c.section, s.tmay_deal, s.benn_act, s.ref_stance, s.constituency_leave, c.usas_file
FROM contributions as c
INNER JOIN members as m
ON m.PimsId = c.member
INNER JOIN debates as d
ON d.uid = c.debate
INNER JOIN member_party as p
ON p.PimsId = m.PimsId
INNER JOIN member_stances as s
ON s.PimsId = m.PimsId
WHERE (d.date BETWEEN date("2015-05-01") AND date("2019-12-11"))
AND (((d.date BETWEEN p.start AND p.end) AND NOT (p.end IS NULL))
OR ((d.date >= p.start) AND (p.end IS NULL)));""".strip()

convert_to_date = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")


def check_dir(dir_name):
    """
    Checks if a directory exists. Makes it if it doesn't.
    """
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


def tokenise(text):
    """
    Turns given text into tokens.
    """
    cleaned = clean_text(text)
    cleaned = re.sub(r"(\p{P})\p{P}*", r"\1 ", cleaned)
    tokens = spacy_tokenise(cleaned)
    return tokens


def get_groups_toks_and_contribs(queries, gnames, all_contributions, all_toks, token_limit=60):
    """
    Gets the provided groups and tokens given pandas queries.
    """
    out_contribs = dict()
    out_toks = dict()

    for query, gname in zip(queries, gnames):
        # Get contributions for each group.
        curr = all_contributions.query(query)

        # Get tokens for each group.
        curr_toks = all_toks[all_toks.index.isin(curr.index)]

        # Only keep tokens for contributions with >60 posts.
        curr_toks = curr_toks[curr_toks.apply(len) >= token_limit].apply(lambda x: x[:token_limit])

        # Get rid of contributions with <= 60 posts.
        curr = curr[curr.index.isin(curr_toks.index)]

        # Set output
        out_contribs[gname] = curr
        out_toks[gname] = curr_toks

    # Create combined list of contributions
    combined = pd.concat(list(out_contribs.values()), axis=0)

    return out_contribs, out_toks, combined


if __name__ == "__main__":
    # out_dir = input("Enter output directory:")
    out_dir = "C:/Users/Eddie/Documents/Datasets/Hansard Output/Keywords/Thesis"

    check_dir(out_dir)

    token_limit = 60
    queries = ["party == 'Conservative'", "party == 'Labour'"]
    gnames = ["Conservative", "Labour"]

    conn = sqlite3.connect(DB_FP)
    curs = conn.cursor()

    # Gets all the contributions and creates a nice dataframe
    all_contributions = pd.read_sql_query(sql_get_all_posts, conn)
    all_contributions.columns = ['uid', 'name', 'PimsId', 'party', 'date', 'text', 'topic', 'section', 'tmay_deal', 'benn_act', 'ref_stance', 'constituency_leave', 'usas_file']
    all_contributions.set_index("uid", inplace=True)
    all_contributions['date'] = all_contributions['date'].apply(convert_to_date)

    all_contributions = all_contributions.query("PimsId not in @speaker_list")
    all_contributions.sort_values("date", inplace=True)

    # Tokenise the contributions
    all_toks =  all_contributions["text"].apply(tokenise)

    # Only keep ones with >60 toks (and only keep first 60)
    all_toks = all_toks[all_toks.apply(len) >= token_limit]
    all_contributions = all_contributions.loc[all_toks.index]

    # Get the EU and Non-EU mentions
    eu_mentions, non_eu_mentions = split_corpus(all_contributions, "eu")

    # Get tokens of EU-mentions
    eu_toks = all_toks.loc[eu_mentions.index]

    # Get tokens of Non-EU-mentions
    non_eu_toks = all_toks.loc[non_eu_mentions.index]

    # Get the non-EU Conservative and Labour groups
    non_eu_group_contribs, non_eu_group_toks, non_eu_combined = get_groups_toks_and_contribs(queries, gnames, non_eu_mentions, all_toks, token_limit)
    non_eu_combined_toks = all_toks.loc[non_eu_combined.index]

    # Get the non-EU Conservative and Labour groups
    eu_group_contribs, eu_group_toks, eu_combined = get_groups_toks_and_contribs(queries, gnames, eu_mentions, all_toks, token_limit)
    eu_combined_toks = all_toks.loc[eu_combined.index]


    ################################################################################################################################
    #   KW of groups against Leavers after 2018/03/15
    ################################################################################################################################

    window_start = convert_to_date("2018-03-15 00:00:00")

    # Get contributions from the window
    after_point = eu_mentions.query("date >= @window_start")

    # Get the groups
    r_con_remainers = after_point.query("constituency_leave < 50 and ref_stance == 'remain'")
    l_con_remainers = after_point.query("constituency_leave > 50 and ref_stance == 'remain'")
    leavers = after_point.query("ref_stance == 'leave'")

    kw = dict()

    # Calculate the keywords
    kw["r_con"] = get_keywords_from_tokens(all_toks.loc[r_con_remainers.index], all_toks.loc[leavers.index]).to_dict()

    # Calculate the keywords
    kw["l_con"] = get_keywords_from_tokens(all_toks.loc[l_con_remainers.index], all_toks.loc[leavers.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "rise_kw_leavers.json"), "w") as out_file:
        json.dump(kw, out_file)


    ################################################################################################################################
    #   EU KW of each group after 2018/03/15
    ################################################################################################################################

    neu_after_point = non_eu_mentions.query("date >= @window_start")

    kw = dict()

    # Calculate the keywords
    kw["r_con"] = get_keywords_from_tokens(all_toks.loc[r_con_remainers.index], all_toks.loc[neu_after_point.index]).to_dict()

    # Calculate the keywords
    kw["l_con"] = get_keywords_from_tokens(all_toks.loc[l_con_remainers.index], all_toks.loc[neu_after_point.index]).to_dict()

    # Calculate the keywords
    kw["leavers"] = get_keywords_from_tokens(all_toks.loc[leavers.index], all_toks.loc[neu_after_point.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "rise_kw_eu.json"), "w") as out_file:
        json.dump(kw, out_file)


    ################################################################################################################################
    #   KW of each group after 2018/03/15 to before
    ################################################################################################################################

    eu_before = eu_mentions.query("date < @window_start")

    kw = dict()

    # Calculate the keywords
    kw["r_con"] = get_keywords_from_tokens(all_toks.loc[r_con_remainers.index], all_toks.loc[eu_before.index]).to_dict()

    # Calculate the keywords
    kw["l_con"] = get_keywords_from_tokens(all_toks.loc[l_con_remainers.index], all_toks.loc[eu_before.index]).to_dict()

    # Calculate the keywords
    kw["leavers"] = get_keywords_from_tokens(all_toks.loc[leavers.index], all_toks.loc[eu_before.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "rise_kw_before.json"), "w") as out_file:
        json.dump(kw, out_file)


    ################################################################################################################################
    #   EU KW of Labour and Conservatives in dip (2018/10/30 -> 2019/04/24) 
    ################################################################################################################################

    window_start = convert_to_date("2018-10-30 00:00:00")
    window_end = convert_to_date("2019-04-24 00:00:00")

    # Get contributions from the window
    eu_in_window = eu_mentions.query("date >= @window_start and date <= @window_end")
    neu_in_window = non_eu_mentions.query("date >= @window_start and date <= @window_end")

    # Get the groups
    con = eu_in_window.query("party == 'Conservative'")
    lab = eu_in_window.query("party == 'Labour'")

    kw = dict()

    # Calculate the keywords
    kw["con"] = get_keywords_from_tokens(all_toks.loc[con.index], all_toks.loc[neu_in_window.index]).to_dict()

    # Calculate the keywords
    kw["lab"] = get_keywords_from_tokens(all_toks.loc[lab.index], all_toks.loc[neu_in_window.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "dip_eu_kw.json"), "w") as out_file:
        json.dump(kw, out_file)

    
    ################################################################################################################################
    #   KW of Labour and Conservatives in dip compared to before (2018/10/30 -> 2019/04/24) 
    ################################################################################################################################

    eu_before_window = eu_mentions.query("date < @window_start")

    kw = dict()

    # Calculate the keywords
    kw["con"] = get_keywords_from_tokens(all_toks.loc[con.index], all_toks.loc[eu_before_window.index]).to_dict()

    # Calculate the keywords
    kw["lab"] = get_keywords_from_tokens(all_toks.loc[lab.index], all_toks.loc[eu_before_window.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "dip_bef_kw.json"), "w") as out_file:
        json.dump(kw, out_file)