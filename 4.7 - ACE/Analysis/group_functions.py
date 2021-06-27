import pandas as pd
import os
import json


with open("../project-config.json") as config_file:
    project_config = json.load(config_file)

MP_Group_FP = project_config["GROUPS_FP"]

leave_or_remain = lambda x: "remain" if x<50 else "leave"

def get_parties(contributions):
        for party in ["Conservative", "Labour"]:
            yield party, contributions[contributions["party"] == party]

def get_brexit_stance(contributions):
    for stance in ["leave", "remain"]:
        yield stance, contributions[contributions["ref_stance"] == stance]

def get_brexit_stance_combos(contributions):
    for stance, group_contribs in contributions.groupby([contributions.constituency_leave.apply(leave_or_remain), contributions["ref_stance"]]):
        if stance[0] == "unknown" or stance[1] == "unknown":
            continue

        yield "con-{0}-mp-{1}".format(stance[0], stance[1]), group_contribs

def get_ref_tmay_combos(contributions):
    for stance, group_contribs in contributions.groupby([contributions["ref_stance"], contributions["tmay_deal"]]):
        if stance[0] != "unknown":
            yield "ref-{0}-tmay-{1}".format(stance[0], stance[1]), group_contribs

    yield "ref-remain-tmay-aye-benn-aye", contributions.query("ref_stance == 'remain' & tmay_deal == 'aye' & benn_act == 'aye'")
    yield "ref-leave-tmay-no-benn-no", contributions.query("ref_stance == 'leave' & tmay_deal == 'no' & benn_act == 'no'")

def get_custom_mp_groups(contributions):
    # Go through each file in the MP Group Directory
    for fname in os.listdir(MP_Group_FP):
        # Only process if it's a CSV
        if fname.endswith(".csv"):
            gname = fname[:-4]
            # Read in the data and yield contributions by MPs in this set.
            mp_df = pd.read_csv(os.path.join(MP_Group_FP, fname), index_col=0)
            group_ids = mp_df.index
            yield gname, contributions.query("PimsId in @group_ids")

def get_all_contributions(contributions):
    yield "all", contributions


def get_group_function(group_type):
    # Set the group function. Can be any function that yields a group and the contributions from that group.
    # Currently you have to pick from pre-set options. This will be improved later.
    if group_type == "party":
        group_function = get_parties
    elif group_type == "brexit_stance":
        group_function = get_brexit_stance
    elif group_type == "brexit_stance_mp_and_constituency":
        group_function = get_brexit_stance_combos
    elif group_type == "mp_groups":
        group_function = get_custom_mp_groups
    elif group_type == "ref_tmay":
        group_function = get_ref_tmay_combos
    else:
        print("No grouping specified. Running for entire corpus")
        group_function = get_all_contributions

    return group_function
