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

from helper_functions import tokenise, get_contribution_windows, split_corpus, check_dir
from mp_sampling import multi_mp_splits_with_limit, multi_mp_splits, get_end_of_windows

with open("../project-config.json") as config_file:
    project_config = json.load(config_file)

DB_FP = project_config["DB_FP"]
MP_Group_FP = project_config["GROUPS_FP"]
with open(project_config["SPEAKER_FILE"]) as speaker_file:
    speaker_list = json.load(speaker_file)

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


def get_groups_toks_and_contribs(queries, gnames, all_contributions, all_toks, token_limit=60, param_list=None):
    """
    Gets the provided groups and tokens given pandas queries.
    """
    out_contribs = dict()
    out_toks = dict()

    if param_list is None:
        param_list = [[] for g in gnames]

    for query, gname, params in zip(queries, gnames, param_list):
        # Get contributions for each group.
        curr = all_contributions.query(query)

        # Get tokens for each group.
        curr_toks = all_toks[all_toks.index.isin(curr.index)]

        # Only keep tokens for contributions with >60 posts.
        if token_limit is not None:
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
    config_fp = input("Enter config file location:")
    out_dir = input("Enter output directory:")

    check_dir(out_dir)

    # Read in the parameter iterations.
    with open(config_fp) as config_file:
        config_info = json.load(config_file)

    token_limit = config_info["token_limit"]
    queries = config_info["queries"]
    gnames = config_info["gnames"]

    conn = sqlite3.connect(DB_FP)
    curs = conn.cursor()

    # Gets all the contributions and creates a nice dataframe
    all_contributions = pd.read_sql_query(sql_get_all_posts, conn)
    all_contributions.columns = ['uid', 'name', 'PimsId', 'party', 'date', 'text', 'topic', 'section', 'tmay_deal', 'benn_act', 'ref_stance', 'constituency_leave', 'usas_file']
    all_contributions.set_index("uid", inplace=True)
    convert_to_date = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
    all_contributions['date'] = all_contributions['date'].apply(convert_to_date)

    all_contributions = all_contributions.query("PimsId not in @speaker_list")
    all_contributions.sort_values("date", inplace=True)

    # Tokenise the contributions
    all_toks =  all_contributions["text"].apply(tokenise)

    # Get the EU and Non-EU mentions
    eu_mentions, non_eu_mentions = split_corpus(all_contributions, "eu")

    # Get tokens of EU-mentions and keep only contributions with at least 60 tokens.
    eu_toks = all_toks.loc[eu_mentions.index]
    eu_toks = eu_toks[eu_toks.apply(len) >= token_limit]
    eu_mentions = eu_mentions.loc[eu_toks.index]

    # Get tokens of Non-EU-mentions and keep only contributions with at least 60 tokens.
    non_eu_toks = all_toks.loc[non_eu_mentions.index]
    non_eu_toks = non_eu_toks[non_eu_toks.apply(len) >= token_limit]
    non_eu_mentions = non_eu_mentions.loc[non_eu_toks.index]

    # Get the Conservative and Labour groups for all contributions
    group_contribs, group_toks, combined = get_groups_toks_and_contribs(queries, gnames, all_contributions, all_toks, token_limit)

    # Get the Conservative and Labour groups for Non-EU Mentions
    non_eu_group_contribs, non_eu_group_toks, non_eu_combined = get_groups_toks_and_contribs(queries, gnames, non_eu_mentions, all_toks, token_limit)

    # Get the Conservative and Labour groups for EU Mentions
    eu_group_contribs, eu_group_toks, eu_combined = get_groups_toks_and_contribs(queries, gnames, eu_mentions, all_toks, token_limit)

    # Loop through all the parameter iterations and create results for each.
    for param_combo in config_info["params"]:
        # If using EU data, use the EU contribs
        if param_combo["data"] == "eu":
            curr_contribs = eu_group_contribs
            curr_toks = eu_group_toks

            curr_ref = non_eu_combined # formerly non_eu_mentions
            curr_ref_toks = non_eu_toks.loc[non_eu_combined.index] # formerly non_eu_toks

        # Otherwise, if using full, use all contribs
        elif param_combo["data"] == "full":
            curr_contribs = group_contribs
            curr_toks = group_toks

            curr_ref = combined
            curr_ref_toks = all_toks[all_toks.index.isin(combined)]

        # If it's not something know, just move on to the next parameter.
        else:
            print("Unrecognised data parameter.")
            continue

        if param_combo["contrib_limit"]:
            comparisons, meta = multi_mp_splits_with_limit(gnames,
                                                            list(curr_contribs.values()),
                                                            list(curr_toks.values()),
                                                            curr_ref, curr_ref_toks,
                                                            window_func=get_contribution_windows,
                                                            window_size=param_combo["win_size"], window_step=param_combo["win_step"],
                                                            n_runs=param_combo["n_runs"], balanced_groups=param_combo["balanced"],
                                                            comp_method=param_combo["comp_method"], n_words_per_contribution=token_limit,
                                                            n_contribs_per_mp=param_combo["contribs_per_mp"])

        else:
            comparisons, meta = multi_mp_splits(gnames,
                                                list(curr_contribs.values()),
                                                list(curr_toks.values()),
                                                curr_ref, curr_ref_toks,
                                                window_func=get_contribution_windows,
                                                window_size=param_combo["win_size"], window_step=param_combo["win_step"],
                                                n_runs=param_combo["n_runs"], balanced_groups=param_combo["balanced"],
                                                comp_method=param_combo["comp_method"], n_words_per_contribution=token_limit)

        end_of_windows = get_end_of_windows(pd.concat(list(curr_contribs.values()) + [curr_ref], axis=0),
                                                        get_contribution_windows, param_combo["win_size"], param_combo["win_step"])
        end_of_windows = [datetime.strftime(d, "%Y-%m-%d") for d in end_of_windows]

        if param_combo["comp_method"] == "CE_Fluct":
            comparisons_dict = [{gsnap: {datetime.strftime(w, "%Y-%m-%d"): run[gsnap][w].to_dict() for w in run[gsnap]} for gsnap in run} for run in comparisons]
        elif param_combo["comp_method"] == "KLD_Fluct":
            comparisons_dict = [{gsnap: {datetime.strftime(w, "%Y-%m-%d"): kld for w, kld in run[gsnap].items()} for gsnap in run} for run in comparisons]
        elif param_combo["comp_method"] == "KLD":
            comparisons_dict = [{gsnap: {gtest: {datetime.strftime(w, "%Y-%m-%d"): kld for w, kld in run[gsnap][gtest].items()} for gtest in run[gsnap]} for gsnap in run} for run in comparisons]
        else:
            comparisons_dict = [{gsnap: {gtest: {datetime.strftime(w, "%Y-%m-%d"): run[gsnap][gtest][w].to_dict() for w in run[gsnap][gtest]} for gtest in run[gsnap]} for gsnap in run} for run in comparisons]

        meta_dict = [{metaVal: {gname: {datetime.strftime(w, "%Y-%m-%d"): run[metaVal][gname][w] for w in run[metaVal][gname]} for gname in run[metaVal]} for metaVal in run} for run in meta]

        param_combo["token_limit"] = token_limit
        param_combo["queries"] = queries
        param_combo["gnames"] = gnames
        out_dict = {"params": param_combo, "comparisons": comparisons_dict, "meta": meta_dict, "end_of_windows": end_of_windows}

        out_fp = "{0}_{1}_{2}_{3}_{4}_lim_{5}_{6}_runs.json".format(param_combo["data"], param_combo["comp_method"],
                                                        param_combo["win_size"], param_combo["win_step"],
                                                        "w{}".format(param_combo["contribs_per_mp"]) if param_combo["contrib_limit"] else "n",
                                                        "balanced" if param_combo["balanced"] else "unbalanced", param_combo["n_runs"])

        with open(os.path.join(out_dir, out_fp), "w") as out_file:
            json.dump(out_dict, out_file)

        print("Written file: ", out_fp)
