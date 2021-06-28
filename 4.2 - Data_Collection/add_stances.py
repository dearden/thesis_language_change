import pandas as pd
import sqlite3
from sqlite3 import Error as SQLError
from datetime import datetime
import re
import csv
import os
import json

from fuzzywuzzy import fuzz

import sys
sys.path.insert(1, "../")
from settings import DB_FP, CORPUS_META

sql_get_members ="""
SELECT c.PimsId, m.name, c.constituency
FROM members as m
INNER JOIN member_constituency as c
ON c.PimsId = m.PimsId
WHERE (((? BETWEEN c.start AND c.end) AND NOT (c.end IS NULL))
OR ((? >= c.start) AND (c.end IS NULL)));""".strip()

sql_get_most_recent_constituency = """
SELECT c.PimsId, m.name, c.constituency, max(c.start)
FROM members as m
INNER JOIN member_constituency as c
ON c.PimsId = m.PimsId
GROUP BY c.PimsId;""".strip()


def create_connection(filename):
    """ create a database connection to a database that resides
        in the memory
    """
    connection = None
    try:
        connection = sqlite3.connect(filename)
        print(sqlite3.version)
    except SQLError as e:
        print(e)
        if connection:
            connection.close()
            print("Error occurred creating connection")
            connection = None

    return connection


# This is the old function I was using for processing ref stances of mps
# It attempts to match the names of the mps.
# I have since realised it's probably a better idea to simple do it on consituency.
def process_ref_using_names(mp_ref_fp, conn):
    members = pd.read_sql_query("SELECT * FROM members", conn)

    mp_ref = pd.read_csv(mp_ref_fp, header=0)
    mp_ref = mp_ref.loc[:, ["Title", "First_Name", "Surname", "Constituency", "Stance"]]

    pimsIds = []
    for i, mp in mp_ref.iterrows():
        loose_reg_name = ".* {0}".format(mp.Surname)
        tight_reg_name = "(({0}|Mr|Ms|Dr|Lord|Mrs|Sir|Baroness) )?{1} {2}".format(mp.Title, mp.First_Name, mp.Surname)

        loose_found = members[members['name'].str.match(loose_reg_name)]

        if len(loose_found) == 1:
            print("{0} - {1}".format(loose_reg_name, loose_found.iloc[0]['name']))
            curr_pims_id = loose_found.iloc[0]['PimsId']
        else:
            tight_found = members[members['name'].str.match(tight_reg_name)]
            if len(tight_found) == 1:
                print("{0} - {1}".format(tight_reg_name, tight_found.iloc[0]['name']))
                curr_pims_id = tight_found.iloc[0]['PimsId']
            else:
                for ci, curr in loose_found.iterrows():
                    found_constituency = loose_found[loose_found["curr_constituency"]==mp['Constituency']]
                    if len(found_constituency) == 1:
                        print("{0} - {1}".format(tight_reg_name, found_constituency['name'].iloc[0]))
                        curr_pims_id = found_constituency.iloc[0]['PimsId']
                    else:
                        print("{0} - {1}".format(tight_reg_name, loose_found))
                        curr_pims_id = None
        pimsIds.append(curr_pims_id)

    mp_ref['PimsId'] = pimsIds
    mp_ref.to_csv("MP_Stance_Thing.csv")


def process_ref(fp, conn):
    stances = pd.read_csv(fp, header=0)
    stances = stances.loc[:, ["Title", "First_Name", "Surname", "Constituency", "Stance"]]

    stances["Name"] = stances.apply(lambda x: "{0} {1}".format(x["First_Name"], x["Surname"]), axis=1)
    stances = stances.loc[:, ["Name", "Constituency", "Stance"]]

    date = "2016-06-23"
    members = pd.read_sql_query(sql_get_members, conn, params=(date, date))
    member_stance = get_vote_per_mp(stances, members, field_name="Stance")

    return member_stance


def process_ref_constituency(fp, conn):
    votes = pd.read_csv(fp, header=0)

    votes["Name"] = [""]*len(votes)
    votes = votes.loc[:, ["Name", "Constituency", "LEAVE FIGURE TO USE"]]
    votes.columns = ["Name", "Constituency", "Vote"]

    date = "2016-06-23"
    members = pd.read_sql_query(sql_get_most_recent_constituency, conn)
    member_stance = get_constituency_vote_per_mp(votes, members)

    return member_stance


def process_vote(fp, mp_db_conn):
    with open(fp) as vote_file:
        file_lines = [line.strip() for line in vote_file]

    div_num = file_lines[0].split(": ")[1].split(",")[0].strip()
    vo_name = file_lines[3].split(",")[0].strip()
    vo_date = file_lines[1].split(": ")[1].split(",")[0].strip()
    vo_date = datetime.strptime(vo_date, '%d/%m/%Y')
    vo_ayes = file_lines[5].split(": ")[1].split(",")[0].strip()
    vo_noes = file_lines[6].split(": ")[1].split(",")[0].strip()

    meta = {"div_num": div_num, "vote_name": vo_name, "vote_date": vo_date, "ayes": vo_ayes, "noes": vo_noes}

    votes = pd.read_csv(fp, skiprows=9, header=0)

    readable_date = vo_date.strftime("%Y-%m-%d")
    members = pd.read_sql_query(sql_get_members, conn, params=(readable_date, readable_date))

    votes = votes.loc[:, ["Member", "Constituency", "Vote"]]
    votes.columns = ["Name", "Constituency", "Vote"]
    member_vote = get_vote_per_mp(votes, members)

    return meta, member_vote


def get_pims_from_constituency(constituency, mp_name, members):
    if constituency in members['constituency'].values:
        return members.set_index("constituency").loc[constituency, 'PimsId']
    else:
        compare_constituency = lambda x: fuzz.ratio(x.lower(), constituency.lower())
        compare_name = lambda x: fuzz.ratio(x.lower(), mp_name.lower())

        members['closeness'] = members["constituency"].apply(compare_constituency)
        most_similar = members.loc[members['closeness'].idxmax()]

        if compare_constituency(most_similar['constituency']) > 95 and mp_name=="":
            return most_similar['PimsId']
        elif compare_constituency(most_similar['constituency']) > 85 and compare_name(most_similar['name']) > 50:
            return most_similar['PimsId']
        else:
            return None



def get_vote_per_mp(votes, members, field_name="Vote"):
    votes['PimsId'] = votes.loc[:, ["Constituency", "Name"]].apply(lambda x: get_pims_from_constituency(x[0], x[1], members), axis=1)
    out = votes.dropna()
    out.set_index("PimsId", inplace=True)
    return out[field_name]


def get_constituency_vote(constituency, constituencies):
    if constituency in constituencies['Constituency'].values:
        return constituencies.set_index("Constituency").loc[constituency, 'Vote']
    else:
        compare_constituency = lambda x: fuzz.ratio(x.lower(), constituency.lower())

        constituencies['closeness'] = constituencies["Constituency"].apply(compare_constituency)
        most_similar = constituencies.loc[constituencies['closeness'].idxmax()]

        if compare_constituency(most_similar['Constituency']) > 85:
            return most_similar['Vote']
        else:
            return None


def get_constituency_vote_per_mp(constituencies, members):
    members['vote'] = members["constituency"].apply(lambda x: get_constituency_vote(x, constituencies))
    out = members.dropna(subset=["vote"])
    out.set_index("PimsId", inplace=True)
    return out["vote"]


def clean_vote_text(vote):
    if "Aye" in vote:
        return "aye"
    elif "No" in vote:
        return "no"
    else:
        return "did-not-vote"


def clean_ref_text(vote):
    if vote.strip() == "Remain":
        return "remain"
    elif vote.strip() == "Leave":
        return "leave"
    else:
        return "unknown"


def create_stance_table(data, conn):
    sql_create_member_stance = """
    CREATE TABLE IF NOT EXISTS member_stances (
        PimsId integer NOT NULL,
        tmay_deal text,
        benn_act text,
        ref_stance text,
        constituency_leave integer,
        PRIMARY KEY (PimsId),
        FOREIGN KEY (PimsId) REFERENCES members (PimsId)
    );"""

    curs = conn.cursor()
    curs.execute(sql_create_member_stance)

    for i, row in data.iterrows():
        command = '''INSERT INTO member_stances(PimsId, tmay_deal, benn_act, ref_stance, constituency_leave)
                VALUES(?, ?, ?, ?, ?);'''
        curr_entry = (i, row['tmay_deal'],
                        row['benn_act'], row['ref_stance'],
                        row['constituency_ref_stance'])
        curs.execute(command, curr_entry)


if __name__ == "__main__":
    out_dir = input("Enter Output Directory:\n")

    mp_ref_fp = os.path.join(CORPUS_META, "MPReferendumStance(Politics Home).csv")

    conn = create_connection(DB_FP)
    curs = conn.cursor()

    tmay_fp = os.path.join(CORPUS_META, "tmay-deal.csv")
    tmay_meta, tmay = process_vote(tmay_fp, conn)
    tmay = tmay.apply(clean_vote_text)

    benn_fp = os.path.join(CORPUS_META, "benn-anti-no-deal.csv")
    benn_meta, benn = process_vote(benn_fp, conn)
    benn = benn.apply(clean_vote_text)

    mp_ref = process_ref(mp_ref_fp, conn)
    mp_ref = mp_ref.apply(clean_ref_text)

    con_ref_fp = os.path.join(CORPUS_META, "eureferendum_constituency.csv")
    con_ref = process_ref_constituency(con_ref_fp, conn)
    con_ref = con_ref.apply(lambda x: float(x.strip("%")))

    mp_stances = pd.concat([tmay, benn, mp_ref, con_ref], axis=1)
    mp_stances.columns = ["tmay_deal", "benn_act", "ref_stance", "constituency_ref_stance"]

    mp_stances.to_csv(os.path.join(out_dir, "test_merged.csv"))

    create_stance_table(mp_stances, conn)

    conn.commit()
    conn.close()
