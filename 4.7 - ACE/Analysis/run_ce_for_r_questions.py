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
from run_CE_experiments import get_groups_toks_and_contribs

with open("../project-config.json") as config_file:
    project_config = json.load(config_file)

import sys
sys.path.insert(1, "../../")
from settings import DB_FP

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

    with open(os.path.join(out_dir, out_fp), "w") as out_file:
        json.dump(out_dict, out_file)

    print("Written file: ", out_fp)


if __name__ == "__main__":
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

    # Setting the hyperparameters for all runs. Could adjust for each RQ if I wanted
    win_size = 15000
    win_step = 15000
    n_runs = 5
    balanced=True
    w_limit = True
    contrib_limit = 20
    curr_contributions = all_contributions

    #############################################################################################
    # RQ 1: How do leave and remain conservatives change compared to Conservatives in general
    #############################################################################################

    conservative = curr_contributions.query("party == 'Conservative'")

    token_limit = 60
    queries = ["ref_stance == 'remain'", "ref_stance == 'leave'"]
    gnames = ["remain", "leave"]

    group_contribs, group_toks, combined = get_groups_toks_and_contribs(queries, gnames, conservative, all_toks, token_limit)

    curr_contribs = group_contribs
    curr_toks = group_toks

    curr_ref = conservative
    curr_ref_toks = all_toks.loc[curr_ref.index]

    single_CE_run(gnames, curr_contribs, curr_toks, curr_ref, curr_ref_toks,
                    win_size, win_step, n_runs, balanced, w_limit,
                    token_limit, contrib_limit, "leave_remain_conservative.json")

    #################################################################################################################
    # RQ 2: How do conservatives from leave and remain constituencies change compared to Conservatives in general
    #################################################################################################################

    conservative = curr_contributions.query("party == 'Conservative'")

    token_limit = 60
    queries = ["constituency_leave < 50", "constituency_leave > 50"]
    gnames = ["remain_constituency", "leave_constituency"]

    group_contribs, group_toks, combined = get_groups_toks_and_contribs(queries, gnames, conservative, all_toks, token_limit)

    curr_contribs = group_contribs
    curr_toks = group_toks

    curr_ref = conservative
    curr_ref_toks = all_toks.loc[curr_ref.index]

    single_CE_run(gnames, curr_contribs, curr_toks, curr_ref, curr_ref_toks,
                    win_size, win_step, n_runs, balanced, w_limit,
                    token_limit, contrib_limit, "leave_remain_constituency_conservative.json")

    #############################################################################################
    # RQ 3:How do remainers from remain and leave constituencies change compared to leavers.
    #############################################################################################

    leavers = curr_contributions.query("ref_stance == 'leave'")
    remainers = curr_contributions.query("ref_stance == 'remain'")

    token_limit = 60
    queries = ["constituency_leave < 50 and ref_stance == 'remain'", "constituency_leave > 50 and ref_stance == 'remain'", "ref_stance == 'leave'"]
    gnames = ["Remain_Constituency_Remainers", "Leave_Constituency_Remainers", "Leavers"]

    group_contribs, group_toks, combined = get_groups_toks_and_contribs(queries, gnames, curr_contributions, all_toks, token_limit)

    curr_contribs = group_contribs
    curr_toks = group_toks

    single_CE_run(gnames, curr_contribs, curr_toks, None, None,
                    win_size, win_step, n_runs, balanced, w_limit,
                    token_limit, contrib_limit, "remain_constituency.json")


    #######################################################################################################
    # RQ 4: How do remainers and leavers change compared to each other, and how unpredictable are they?
    #######################################################################################################

    token_limit = 60
    queries = ["ref_stance == 'remain'", "ref_stance == 'leave'"]
    gnames = ["remain", "leave"]

    group_contribs, group_toks, combined = get_groups_toks_and_contribs(queries, gnames, curr_contributions, all_toks, token_limit)

    curr_contribs = group_contribs
    curr_toks = group_toks

    single_CE_run(gnames, curr_contribs, curr_toks, None, None,
                    win_size, win_step, n_runs, balanced, w_limit,
                    token_limit, contrib_limit, "remain_leave.json")


    #############################################################################################################################
    # RQ 5: How do Conservative and Labour change compared to each other across the whole corpus, and how unpredictable are they?
    #############################################################################################################################

    token_limit = 60
    queries = ["party == 'Conservative'", "party == 'Labour'"]
    gnames = ["Conservative", "Labour"]

    group_contribs, group_toks, combined = get_groups_toks_and_contribs(queries, gnames, curr_contributions, all_toks, token_limit)

    curr_contribs = group_contribs
    curr_toks = group_toks

    single_CE_run(gnames, curr_contribs, curr_toks, None, None,
                    win_size, win_step, n_runs, balanced, w_limit,
                    token_limit, contrib_limit, "conservative_labour.json")

    ##############################################################################################################################
    # RQ 6: How do the ERG and Rebel Alliance vary compared to parliament at large
    ##############################################################################################################################

    conservative = curr_contributions.query("party == 'Conservative'")

    # Get the Rebel Alliance
    mp_df = pd.read_csv(os.path.join(MP_Group_FP, "lost-whip.csv"), index_col=0)
    lost_whip_mps = mp_df.index

    # Get the ERG
    mp_df = pd.read_csv(os.path.join(MP_Group_FP, "Vote_Leave_Board_and_Committee.csv"), index_col=0)
    vote_leave_mps = mp_df.index

    other_mps = curr_contributions.query("PimsId not in @lost_whip_mps and PimsId not in @vote_leave_mps")["PimsId"].unique()

    token_limit = 60
    queries = ["PimsId in @params", "PimsId in @params", "PimsId in @params"]
    gnames = ["lost_whip", "vote_leave", "other"]
    param_list = [lost_whip_mps, vote_leave_mps, other_mps]

    group_contribs, group_toks, combined = get_groups_toks_and_contribs(queries, gnames, curr_contributions, all_toks, token_limit, param_list)

    curr_contribs = group_contribs
    curr_toks = group_toks

    curr_ref = curr_contributions
    curr_ref_toks = all_toks.loc[curr_ref.index]

    single_CE_run(gnames, curr_contribs, curr_toks, curr_ref, curr_ref_toks,
                    win_size, win_step, n_runs, balanced, w_limit,
                    token_limit, contrib_limit, "lost_whip_vote_leave.json")
