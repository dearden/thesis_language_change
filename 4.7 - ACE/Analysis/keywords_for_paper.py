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
    out_dir = "C:/Users/Eddie/Documents/Datasets/Hansard Output/Keywords/15K"

    window_start = convert_to_date("2019-04-01 00:00:00")
    window_end = convert_to_date("2019-09-03 00:00:00")

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
    #   KW of Remain-Constituency Remainers against Leavers at given window
    ################################################################################################################################

    # Get contributions from the window
    in_window = eu_mentions.query("date >= @window_start and date <= @window_end")

    # Get the groups
    r_con_remainers = in_window.query("constituency_leave < 50 and ref_stance == 'remain'")
    leavers = in_window.query("ref_stance == 'leave'")

    # Calculate the keywords
    kw = get_keywords_from_tokens(all_toks.loc[r_con_remainers.index], all_toks.loc[leavers.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "kw_against_leavers.json"), "w") as out_file:
        json.dump(kw, out_file)

    ################################################################################################################################
    #   KW of Remain-Constituency Remainers against Leave-Constituency Remainers at given window
    ################################################################################################################################

    # Get contributions from the window
    in_window = eu_mentions.query("date >= @window_start and date <= @window_end")

    # Get the groups
    r_con_remainers = in_window.query("constituency_leave < 50 and ref_stance == 'remain'")
    l_con_remainers = in_window.query("constituency_leave > 50 and ref_stance == 'remain'")

    # Calculate the keywords
    kw = get_keywords_from_tokens(all_toks.loc[r_con_remainers.index], all_toks.loc[l_con_remainers.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "kw_against_lcon_remainers.json"), "w") as out_file:
        json.dump(kw, out_file)

    ################################################################################################################################
    #   KW against all other contributions (not in the window)
    ################################################################################################################################

    # Get contributions from the window
    in_window = eu_mentions.query("date >= @window_start and date <= @window_end")

    # Get the groups
    r_con_remainers = in_window.query("constituency_leave < 50 and ref_stance == 'remain'")
    others = eu_mentions.query("uid not in @in_window.index")

    # Calculate the keywords
    kw = get_keywords_from_tokens(all_toks.loc[r_con_remainers.index], all_toks.loc[others.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "kw_everything_else.json"), "w") as out_file:
        json.dump(kw, out_file)

    ################################################################################################################################
    #   KW against global self
    ################################################################################################################################

    # Get contributions from the window
    in_window = eu_mentions.query("date >= @window_start and date <= @window_end")

    # Get the groups
    r_con_remainers = in_window.query("constituency_leave < 50 and ref_stance == 'remain'")
    itself = eu_mentions.query("constituency_leave < 50 and ref_stance == 'remain'")

    # Calculate the keywords
    kw = get_keywords_from_tokens(all_toks.loc[r_con_remainers.index], all_toks.loc[itself.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "kw_vs_global_self.json"), "w") as out_file:
        json.dump(kw, out_file)

    ################################################################################################################################
    #   EU KW for each group (against non-eu)
    ################################################################################################################################

    # Get contributions from the window
    in_window = eu_mentions.query("date >= @window_start and date <= @window_end")

    # Get the groups
    r_con_remainers = in_window.query("constituency_leave < 50 and ref_stance == 'remain'")
    l_con_remainers = in_window.query("constituency_leave > 50 and ref_stance == 'remain'")
    leavers = in_window.query("ref_stance == 'leave'")
    non_eu = non_eu_mentions.query("date >= @window_start and date <= @window_end")

    # Calculate the keywords
    kw = dict()
    kw["r_con_remainers"] = get_keywords_from_tokens(all_toks.loc[r_con_remainers.index], all_toks.loc[non_eu.index]).to_dict()
    kw["l_con_remainers"] = get_keywords_from_tokens(all_toks.loc[l_con_remainers.index], all_toks.loc[non_eu.index]).to_dict()
    kw["leavers"] = get_keywords_from_tokens(all_toks.loc[leavers.index], all_toks.loc[non_eu.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "kw_against_non_eu.json"), "w") as out_file:
        json.dump(kw, out_file)


    ################################################################################################################################
    #   KW of local and global group against Non-EU, Globally
    ################################################################################################################################

    # Get contributions from the window
    in_window = eu_mentions.query("date >= @window_start and date <= @window_end")

    # Get the groups
    r_con_remainers_loc = in_window.query("constituency_leave < 50 and ref_stance == 'remain'")
    r_con_remainers_glo = all_contributions.query("constituency_leave < 50 and ref_stance == 'remain'")

    l_con_remainers = in_window.query("constituency_leave > 50 and ref_stance == 'remain'")
    leavers = in_window.query("ref_stance == 'leave'")

    # Calculate the keywords
    kw = dict()
    kw["local"] = get_keywords_from_tokens(all_toks.loc[r_con_remainers_loc.index], non_eu_toks).to_dict()
    kw["global"] = get_keywords_from_tokens(all_toks.loc[r_con_remainers_glo.index], non_eu_toks).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "kw_against_non_eu_glob.json"), "w") as out_file:
        json.dump(kw, out_file)


    ################################################################################################################################
    #   Keywords of Leave/Remain in and around the dip of 2018
    ################################################################################################################################

    window_1_start = convert_to_date("2017-11-15 00:00:00")
    window_1_end = convert_to_date("2018-01-09 00:00:00")

    window_2_start = convert_to_date("2018-01-09 00:00:00")
    window_2_end = convert_to_date("2018-05-07 00:00:00")

    # Get contributions from the window
    in_1st_window = eu_mentions.query("date >= @window_1_start and date <= @window_1_end")
    in_2nd_window = eu_mentions.query("date >= @window_2_start and date <= @window_2_end")

    remain1 = in_1st_window.query("ref_stance == 'remain'")
    leave1 = in_1st_window.query("ref_stance == 'leave'")

    remain2 = in_2nd_window.query("ref_stance == 'remain'")
    leave2 = in_2nd_window.query("ref_stance == 'leave'")

    not_in_dip = eu_mentions.query("date < @window_1_start")

    # Calculate the keywords
    kw_dip = dict()
    kw_dip["remain"] = get_keywords_from_tokens(all_toks.loc[remain1.index], all_toks.loc[not_in_dip.index]).to_dict()
    kw_dip["leave"] = get_keywords_from_tokens(all_toks.loc[leave1.index], all_toks.loc[not_in_dip.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "2018_ref_dip_kw_v_before.json"), "w") as out_file:
        json.dump(kw_dip, out_file)

    # Calculate the keywords
    kw_dip = dict()
    kw_dip["remain"] = get_keywords_from_tokens(all_toks.loc[remain2.index], all_toks.loc[remain1.index]).to_dict()
    kw_dip["leave"] = get_keywords_from_tokens(all_toks.loc[leave2.index], all_toks.loc[leave1.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "2018_ref_dip_kw_change.json"), "w") as out_file:
        json.dump(kw_dip, out_file)


    ################################################################################################################################
    #   Keywords of Labour in their 2016 spike
    ################################################################################################################################

    window_start = convert_to_date("2016-02-03 00:00:00")
    window_end = convert_to_date("2016-06-29 00:00:00")

    # Get contributions from the window
    in_window = all_contributions.query("date >= @window_start and date <= @window_end")

    lab = in_window.query("party == 'Labour'")

    not_in_window = all_contributions.query("date < @window_start or date > @window_end")

    # Calculate the keywords
    kw = get_keywords_from_tokens(all_toks.loc[lab.index], all_toks.loc[not_in_window.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "labour_kw_peak.json"), "w") as out_file:
        json.dump(kw, out_file)

    ################################################################################################################################
    #   EU Keywords of Lab/Con in their unpredictability dip of 2019
    ################################################################################################################################

    window_1_start = convert_to_date("2019-02-20 00:00:00")
    window_1_end = convert_to_date("2019-04-08 00:00:00")

    window_2_start = convert_to_date("2019-04-08 00:00:00")
    window_2_end = convert_to_date("2019-10-07 00:00:00")

    # Get contributions from the window
    in_1st_window = eu_mentions.query("date >= @window_1_start and date <= @window_1_end")
    in_2nd_window = eu_mentions.query("date >= @window_2_start and date <= @window_2_end")

    lab1 = in_1st_window.query("party == 'Labour'")
    con1 = in_1st_window.query("party == 'Conservative'")

    lab2 = in_2nd_window.query("party == 'Labour'")
    con2 = in_2nd_window.query("party == 'Conservative'")

    not_in_dip = eu_mentions.query("date < @window_1_start")

    # Calculate the keywords
    kw_dip = dict()
    kw_dip["lab"] = get_keywords_from_tokens(all_toks.loc[lab1.index], all_toks.loc[not_in_dip.index]).to_dict()
    kw_dip["con"] = get_keywords_from_tokens(all_toks.loc[con1.index], all_toks.loc[not_in_dip.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "2019_party_dip_kw_v_before.json"), "w") as out_file:
        json.dump(kw_dip, out_file)

    # Calculate the keywords
    kw_dip = dict()
    kw_dip["lab"] = get_keywords_from_tokens(all_toks.loc[lab2.index], all_toks.loc[lab1.index]).to_dict()
    kw_dip["con"] = get_keywords_from_tokens(all_toks.loc[con2.index], all_toks.loc[con1.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "2019_party_dip_kw_change.json"), "w") as out_file:
        json.dump(kw_dip, out_file)


    ################################################################################################################################
    #   Keywords of Leave/Remain in their 2018 change against previous windows.
    ################################################################################################################################

    window_start = convert_to_date("2018-01-09 00:00:00")
    window_end = convert_to_date("2018-05-17 00:00:00")

    # Get contributions from the window
    in_window = eu_mentions.query("date >= @window_start and date <= @window_end")

    leave = in_window.query("ref_stance == 'leave'")
    remain = in_window.query("ref_stance == 'remain'")

    before = eu_mentions.query("date < @window_start")

    # Calculate the keywords
    kw = dict()
    kw["leave"] = get_keywords_from_tokens(all_toks.loc[leave.index], all_toks.loc[before.index]).to_dict()
    kw["remain"] = get_keywords_from_tokens(all_toks.loc[remain.index], all_toks.loc[before.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "kw_ref_2018_v_before.json"), "w") as out_file:
        json.dump(kw, out_file)

    ################################################################################################################################
    #   KW of Leave/Remain in window of 2018 change
    ################################################################################################################################

    window_start = convert_to_date("2018-01-09 00:00:00")
    window_end = convert_to_date("2018-05-17 00:00:00")

    # Get contributions from the window
    in_window = all_contributions.query("date >= @window_start and date <= @window_end")

    leave = in_window.query("ref_stance == 'leave'")
    remain = in_window.query("ref_stance == 'remain'")

    # Calculate the keywords
    kw = dict()
    kw["leave"] = get_keywords_from_tokens(all_toks.loc[leave.index], all_toks.loc[remain.index]).to_dict()
    kw["remain"] = get_keywords_from_tokens(all_toks.loc[remain.index], all_toks.loc[leave.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "kw_lea_rem_2018.json"), "w") as out_file:
        json.dump(kw, out_file)


    ################################################################################################################################
    #   KW of Leave/Remain before and after their 2018 change
    ################################################################################################################################

    window_start = convert_to_date("2018-01-09 00:00:00")
    window_end = convert_to_date("2018-05-17 00:00:00")

    before = all_contributions.query("date < @window_start")

    leave = before.query("ref_stance == 'leave'")
    remain = before.query("ref_stance == 'remain'")

    # Calculate the keywords
    bef_kw = dict()
    bef_kw["leave"] = get_keywords_from_tokens(all_toks.loc[leave.index], all_toks.loc[remain.index]).to_dict()
    bef_kw["remain"] = get_keywords_from_tokens(all_toks.loc[remain.index], all_toks.loc[leave.index]).to_dict()

    after = eu_mentions.query("date < @window_start")

    leave = after.query("ref_stance == 'leave'")
    remain = after.query("ref_stance == 'remain'")

    # Calculate the keywords
    aft_kw = dict()
    aft_kw["leave"] = get_keywords_from_tokens(all_toks.loc[leave.index], all_toks.loc[remain.index]).to_dict()
    aft_kw["remain"] = get_keywords_from_tokens(all_toks.loc[remain.index], all_toks.loc[leave.index]).to_dict()

    # Save it to a file
    with open(os.path.join(out_dir, "kw_bef_aft_2018.json"), "w") as out_file:
        json.dump({"before": bef_kw, "after": aft_kw}, out_file)
