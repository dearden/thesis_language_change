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

from helper_functions import tokenise, get_contribution_windows, split_corpus, check_dir, make_tok_chunks, get_chunks
from mp_sampling import multi_mp_splits_with_limit, multi_mp_splits, get_end_of_windows
from run_CE_experiments import get_groups_toks_and_contribs

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


def single_CE_run(gnames, curr_contribs, curr_toks, curr_ref, curr_ref_toks,
                    win_size, win_step, n_runs, balanced_groups, w_limit,
                    token_limit, n_contribs_per_mp, out_fp):

    if w_limit:
        # For doing with a limit per MP
        comparisons, meta = multi_mp_splits_with_limit(gnames,
                                                        list(curr_contribs.values()),
                                                        list(curr_toks.values()),
                                                        curr_ref, curr_ref_toks,
                                                        window_func=get_contribution_windows,
                                                        window_size=win_size, window_step=win_step,
                                                        n_runs=n_runs, balanced_groups=balanced_groups,
                                                        comp_method="CE", n_words_per_contribution=token_limit,
                                                        n_contribs_per_mp=n_contribs_per_mp)
    else:
        comparisons, meta = multi_mp_splits(gnames,
                                            list(curr_contribs.values()),
                                            list(curr_toks.values()),
                                            curr_ref, curr_ref_toks,
                                            window_func=get_contribution_windows,
                                            window_size=win_size, window_step=win_step,
                                            n_runs=n_runs, balanced_groups=balanced_groups,
                                            comp_method="CE", n_words_per_contribution=token_limit)

    end_of_windows = get_end_of_windows(pd.concat(list(curr_contribs.values()) + [curr_ref], axis=0),
                                                    get_contribution_windows, win_size, win_step)
    end_of_windows = [datetime.strftime(d, "%Y-%m-%d") for d in end_of_windows]

    comparisons_dict = [{gsnap: {gtest: {datetime.strftime(w, "%Y-%m-%d"): run[gsnap][gtest][w].to_dict() for w in run[gsnap][gtest]} for gtest in run[gsnap]} for gsnap in run} for run in comparisons]

    meta_dict = [{metaVal: {gname: {datetime.strftime(w, "%Y-%m-%d"): run[metaVal][gname][w] for w in run[metaVal][gname]} for gname in run[metaVal]} for metaVal in run} for run in meta]

    param_combo = {"win_type": "contributions", "win_size": win_size, "win_step": win_step,
                    "n_runs": n_runs, "balanced": balanced_groups, "comp_method": "CE",
                    "contrib_limit": w_limit, "token_limit": token_limit, "queries": queries, "gnames": gnames}

    out_dict = {"params": param_combo, "comparisons": comparisons_dict, "meta": meta_dict, "end_of_windows": end_of_windows}

    with open(out_fp, "w") as out_file:
        json.dump(out_dict, out_file)

    print("Written file: ", out_fp)


if __name__ == "__main__":
    startTime = datetime.now()

    out_dir = input("Enter a directory to put results in:\n")

    check_dir(out_dir)

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

    # Default parameters: 60k for all, 12k for EU.

    # Setting the hyperparameters for all runs. Could adjust for each RQ if I wanted
    win_size = 12000    # Number of contributions in each window
    win_step = 12000    # Number of contribution each window moves on by.
    n_runs = 50          # Number of runs. (50 takes quite a long time)
    balanced=True       # Whether or not to balance the samples (same number of members in each group)
    w_limit = True      # Whether or not to limit the number of contributions from each member
    contrib_limit = 60  # Max number of contributions per member.
    curr_contributions = eu_mentions.drop("text", axis=1)  # the contributions to use (either "all_contributions" or "eu_mentions")

    # Keep only the tokens in the current contributions
    all_toks = all_toks.loc[curr_contributions.index]

    # Convert to chunks
    chunk_size = 60
    all_toks =  make_tok_chunks(all_toks, chunk_size) # Makes the chunks (with a new index)
    idx_map = all_toks["idx"]
    # Gets contributions for chunks (and reindexes)
    curr_contributions = curr_contributions.loc[idx_map.loc[idx_map.isin(curr_contributions.index)]].set_index(all_toks.index) 
    all_toks = all_toks["chunk"]

    print("Data ready: ", datetime.now() - startTime)
    startTime = datetime.now()

    ############################################################################################################
    # Remainers vs Leavers :How do remainers from remain and leave constituencies change compared to leavers.
    ############################################################################################################

    leavers = curr_contributions.query("ref_stance == 'leave'")
    remainers = curr_contributions.query("ref_stance == 'remain'")

    token_limit = None
    queries = ["constituency_leave < 50 and ref_stance == 'remain'", "constituency_leave > 50 and ref_stance == 'remain'", "ref_stance == 'leave'"]
    gnames = ["Remain_Constituency_Remainers", "Leave_Constituency_Remainers", "Leavers"]

    group_contribs, group_toks, combined = get_groups_toks_and_contribs(queries, gnames, curr_contributions, all_toks, token_limit)

    curr_contribs = group_contribs
    curr_toks = group_toks

    out_fp = os.path.join(out_dir, "remain_constituency.json")

    single_CE_run(gnames, curr_contribs, curr_toks, None, None,
                    win_size, win_step, n_runs, balanced, w_limit,
                    token_limit, contrib_limit, out_fp)

    print("RQ1 Done: ", datetime.now() - startTime)
    startTime = datetime.now()


    #####################################################################################################################
    # Referendum Stance : How do remainers and leavers change compared to each other, and how unpredictable are they?
    #####################################################################################################################

    token_limit = None
    queries = ["ref_stance == 'remain'", "ref_stance == 'leave'"]
    gnames = ["remain", "leave"]

    group_contribs, group_toks, combined = get_groups_toks_and_contribs(queries, gnames, curr_contributions, all_toks, token_limit)

    curr_contribs = group_contribs
    curr_toks = group_toks
    
    out_fp = os.path.join(out_dir, "remain_leave.json")

    single_CE_run(gnames, curr_contribs, curr_toks, None, None,
                    win_size, win_step, n_runs, balanced, w_limit,
                    token_limit, contrib_limit, out_fp)

    print("RQ2 Done: ", datetime.now() - startTime)
    startTime = datetime.now()


    ###########################################################################################################################################
    # Party Comparison : How do Conservative and Labour change compared to each other across the whole corpus, and how unpredictable are they?
    ###########################################################################################################################################

    token_limit = None
    queries = ["party == 'Conservative'", "party == 'Labour'"]
    gnames = ["Conservative", "Labour"]

    group_contribs, group_toks, combined = get_groups_toks_and_contribs(queries, gnames, curr_contributions, all_toks, token_limit)

    curr_contribs = group_contribs
    curr_toks = group_toks

    out_fp = os.path.join(out_dir, "conservative_labour.json")

    single_CE_run(gnames, curr_contribs, curr_toks, None, None,
                    win_size, win_step, n_runs, balanced, w_limit,
                    token_limit, contrib_limit, out_fp)

    print("RQ3 Done: ", datetime.now() - startTime)
    startTime = datetime.now()
