import sys
import os
import re
import json

import sqlite3
from sqlite3 import Error as SQLError
from datetime import datetime
from lxml import etree


# All the SQL code was taken from:
# https://www.sqlitetutorial.net/sqlite-python/create-tables/


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


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except SQLError as e:
        print(e)


def get_all_debates(dir_fp):
    # Loop through all files and yield one file name at a time.
    for subdir, dirs, files in os.walk(dir_fp):
        for filename in files:
            curr_fp = os.path.join(subdir, filename)
            with open(curr_fp) as curr_file:
                curr_debate = json.load(curr_file)
            yield curr_debate

def get_all_members(debates):
    all_members = list()
    for debate in debates:
        for contribution in debate.values():
            curr_member = contribution['member']
            curr_member = {"pims":curr_member['member_id'], "mnis":curr_member['member_mnis'], "xid": curr_member['member_xid']}
            if curr_member not in all_members:
                all_members.append(curr_member)
    return all_members

def get_full_mp_info(curr_member, members_xml):
    num_chillens = len(members_xml)
    for member in members_xml.iterfind("{*}Member"):
        if curr_member['pims'] == member.attrib['Pims_Id'] or curr_member['mnis'] == member.attrib['Member_Id'] or curr_member['xid'] == member.attrib['Clerks_Id']:
            out = dict()

            # Get all the info about this user
            # Have to do an if statement here because sometimes PimsId from the API is not real.
            if member.attrib['Pims_Id'] == "":
                out['PimsId'] = curr_member['pims']
            else:
                out["PimsId"] = member.attrib['Pims_Id']

            out["ClerksId"] = member.attrib['Clerks_Id']
            out["MnisId"] = member.attrib['Member_Id']
            out["name"] = member.find("{*}DisplayAs").text
            out["curr_party"] = member.find("{*}Party").text
            out["curr_constituency"] = member.find("{*}MemberFrom").text
            out["member_since"] = member.find("{*}HouseStartDate").text
            out["member_until"] = member.find("{*}HouseEndDate").text

            # Loop through all of the parties the MP has stood for
            parties = []
            parties_xml = member.find("{*}Parties")
            # Add an entry for each one.
            for party in parties_xml.iterfind("{*}Party"):
                curr_party = dict()
                curr_party['start'] = party.find("{*}StartDate").text
                curr_party['end'] = party.find("{*}EndDate").text
                curr_party['name'] = party.find("{*}Name").text
                parties.append(curr_party)
            out['parties'] = parties

            # Loop through all of the constituencies the MP has represented.
            constituencies = []
            constituencies_xml = member.find("{*}Constituencies")
            # Add an entry for each one.
            for constituency in constituencies_xml.iterfind("{*}Constituency"):
                curr_constituency = dict()
                curr_constituency['start'] = constituency.find("{*}StartDate").text
                curr_constituency['end'] = constituency.find("{*}EndDate").text
                curr_constituency['name'] = constituency.find("{*}Name").text
                constituencies.append(curr_constituency)
            out['constituencies'] = constituencies

            print("Collected data for {}".format(out["name"]))

            return out

def get_info_for_commons(members):
    # create element tree object
    all_members_tree = etree.parse("http://data.parliament.uk/membersdataplatform/services/mnis/members/query/House=Commons|Membership=all/Parties|Constituencies")

    # get root element
    all_members_root = all_members_tree.getroot()

    # loop through each member and find their information
    for member in members:
        if member['mnis'] is not None:
            curr_member_tree = etree.parse("http://data.parliament.uk/membersdataplatform/services/mnis/members/query/House=Commons|id={}|Membership=all/Parties|Constituencies".format(member['mnis']))
            curr_member_root = curr_member_tree.getroot()
            yield get_full_mp_info(member, curr_member_root)
        else:
            yield get_full_mp_info(member, all_members_root)
    pass


def add_members(debates, connection):
    members = get_info_for_commons(get_all_members(debates))
    cursor = connection.cursor()

    # # Below is for testing if the thing is working properly.
    # all_members = [m for m in members]

    # xxx = dict()
    # for x in all_members:
    #     if x is None:
    #         continue
    #     if x['MnisId'] not in xxx:
    #         xxx[x['MnisId']] = [x]
    #     else:
    #         xxx[x['MnisId']].append(x)

    already_added = []
    all_members = []
    for member in members:
        if member is None:
            continue
        if member["PimsId"] == "":
            continue
        if member['PimsId'] in already_added:
            continue

        all_members.append(member)
        already_added.append(member['PimsId'])

        # Add the member
        command = '''INSERT INTO members(PimsId, MnisId, ClerksId, name, curr_party, curr_constituency, member_since, member_until)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?);'''
        curr_entry = (member['PimsId'], member['MnisId'], member['ClerksId'],
                        member['name'], member['curr_party'], member['curr_constituency'],
                        member['member_since'], member['member_until'])
        cursor.execute(command, curr_entry)

        # Add the member's parties
        for party in member['parties']:
            command = '''INSERT INTO member_party(PimsId, start, end, party)
                            VALUES(?, ?, ?, ?);'''
            curr_entry = (member['PimsId'], party['start'], party['end'], party['name'])
            cursor.execute(command, curr_entry)

        # Add the member's constituencies
        for constituency in member['constituencies']:
            command = '''INSERT INTO member_constituency(PimsId, start, end, constituency)
                            VALUES(?, ?, ?, ?);'''
            curr_entry = (member['PimsId'], constituency['start'], constituency['end'], constituency['name'])
            cursor.execute(command, curr_entry)
    return all_members


# def add_members(debates, connection):
#     members = dict()
#     pims_mnis_map = dict()
#     for debate in debates:
#         for contribution in debate.values():
#             curr_member = contribution['member']

#             # current
#             if curr_member['member_id'] == "-1":
#                 print("Found the prick")

#             # If this member is already in the dictionary add count to the relevant name.
#             if curr_member['member_id'] in members:
#                 # If this name has been seen before, count it.
#                 if curr_member['member_name'] in members[curr_member['member_id']]:
#                     members[curr_member['member_id']][curr_member['member_name']] += 1
#                 # If not, initialise a new counter.
#                 else:
#                     members[curr_member['member_id']][curr_member['member_name']] = 1
#             # If this member hasn't been seen, initialise a new counter.
#             else:
#                 members[curr_member['member_id']] = dict()
#                 members[curr_member['member_id']][curr_member['member_name']] = 1

#     cursor = connection.cursor()
#     for member in members:
#         curr_member = members[member]
#         curr_counts = list(curr_member.values())
#         curr_names = list(curr_member.keys())

#         best_name = None
#         title_regex = r'(mr|mrs|sir|lady|dr|dame|lord|baroness)\W+(.*)'
#         preferable_name = r'(\w+\W\w+(\W\w+)*)'
#         for name in curr_names:
#             if re.match(r'the .*', name.lower()):
#                 print("{} is not a valid name".format(name))
#                 continue

#             if "speaker" in name or "deputy" in name.lower():
#                 print("{} is not a valid name".format(name))
#                 continue

#             t = re.match(title_regex, name.lower())
#             if t is not None:
#                 m = re.fullmatch(preferable_name, t.group(2).lower())
#             else:
#                 m = re.fullmatch(preferable_name, name.lower())
#             if m is not None:
#                 best_name = m.group(1)

#         if best_name is None:
#             max_index = curr_counts.index(max(curr_counts))
#             most_common_name = curr_names[max_index]
#             best_name = most_common_name.lower()

#         print("{1}, {0}".format(best_name, member))
#         command = '''INSERT INTO members(uid, name, party, constituency)
#                     VALUES(?, ?, ?, ?);'''
#         curr_entry = (int(member), best_name, None, None)
#         cursor.execute(command, curr_entry)
#         print(cursor.lastrowid)


def add_debates(debates, connection):
    cursor = connection.cursor()
    for debate in debates:
        opener = list(debate.values())[0]
        curr_datetime = datetime.strptime(opener['date'], '%d %B %Y')
        curr_hansard = re.match(r".*CHAN(\d+)", opener['hansard_file']).group(1)
        curr_uid = opener['hansard_file']

        command = '''INSERT INTO debates(uid, date, hansardNum, file)
                    VALUES(?, ?, ?, ?);'''
        curr_entry = (curr_uid, curr_datetime, int(curr_hansard), opener['hansard_file'])
        cursor.execute(command, curr_entry)
        print(cursor.lastrowid)


def add_contributions(debates, members, connection):
    cursor = connection.cursor()
    for debate in debates:
        for contribution in debate.values():
            if contribution['uid'] is None:
                continue

            # By default, assume the listed PimsId is correct.
            member_id = contribution["member"]['member_id']
            # If it isn't, we'll need to find the right one.
            if member_id == "-1" or member_id is None or member_id == "":
                # find the PimsId based on their Mnis ID instead
                for m in members:
                    if m['MnisId'] == contribution['member']['member_mnis']:
                        member_id = m['PimsId']
                        break
                    elif m['ClerksId'] == contribution['member']['member_xid']:
                        member_id = m['PimsId']
                        break

            if member_id == "-1" or member_id == "" or member_id is None:
                continue


            isQuestion = contribution['type'] == "Question"

            if 'question' in contribution:
                if contribution['question'] is not None:
                    referringTo = contribution['question']['uid']
            else:
                referringTo = None

            if 'department' in contribution:
                department = contribution['department']
            else:
                department = None

            if 'topic' in contribution:
                topic = contribution['topic']
            else:
                topic = None

            curr_deb_id = contribution['hansard_file']

            try:
                command = '''INSERT INTO contributions(uid, member, debate, body, isQuestion, referringTo, topic, section, contType, sectionTag, department)
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'''
                curr_entry = (int(contribution['uid']), int(member_id),
                                curr_deb_id, contribution['text'], isQuestion, referringTo,
                                topic, contribution['section'], contribution['contribution_type'],
                                contribution['section_tag'], department)
                cursor.execute(command, curr_entry)
                print(cursor.lastrowid)
            except SQLError as e:
                print("Something went wrong adding contribution {}".format(contribution['uid']))
                print(e)
            except Exception as e:
                print("Something went wrong adding contribution {}".format(contribution['uid']))
                print(e)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        filename = input("Enter DB FP: ")
        json_dir = input("Enter JSON Dir: ")
    else:
        filename = sys.argv[1]
        json_dir = sys.argv[2]

    connection = create_connection(filename)

    sql_create_contributions = """
    CREATE TABLE IF NOT EXISTS contributions (
        uid integer PRIMARY KEY,
        member integer NOT NULL,
        debate integer NOT NULL,
        body text NOT NULL,
        isQuestion integer NOT NULL,
        referringTo integer,
        topic text,
        section text,
        contType text,
        sectionTag text,
        department text,
        FOREIGN KEY (member) REFERENCES members (PimsId)
        FOREIGN KEY (debate) REFERENCES debates (uid)
    );"""

    sql_create_debates = """
    CREATE TABLE IF NOT EXISTS debates (
        uid text PRIMARY KEY,
        date text,
        hansardNum text,
        file text
    );"""

    sql_create_members = """
    CREATE TABLE IF NOT EXISTS members (
        PimsId integer PRIMARY KEY,
        MnisId integer,
        ClerksId integer,
        name text,
        curr_party text,
        curr_constituency text,
        member_since text,
        member_until text
    );"""

    sql_create_member_party = """
    CREATE TABLE IF NOT EXISTS member_party (
        PimsId integer NOT NULL,
        start text NOT NULL,
        end text,
        party text NOT NULL,
        PRIMARY KEY (PimsId, start),
        FOREIGN KEY (PimsId) REFERENCES members (PimsId)
    );"""

    sql_create_member_constituency = """
    CREATE TABLE IF NOT EXISTS member_constituency (
        PimsId integer NOT NULL,
        start text NOT NULL,
        end text,
        constituency text NOT NULL,
        PRIMARY KEY (PimsId, start),
        FOREIGN KEY (PimsId) REFERENCES members (PimsId)
    );"""

    if connection is not None:
        # Create Debates table
        create_table(connection, sql_create_debates)
        print("Created debates table.")

        # Create Members table
        create_table(connection, sql_create_members)
        print("Created members table.")

        # Create Contribution table
        create_table(connection, sql_create_contributions)
        print("Created contributions table.")

        # Create Contribution table
        create_table(connection, sql_create_member_party)
        print("Created contributions table.")

        # Create Contribution table
        create_table(connection, sql_create_member_constituency)
        print("Created contributions table.")
    else:
        print("Cannot connect to Database.")


    all_debates = get_all_debates(json_dir)
    members = add_members(all_debates, connection)

    all_debates = get_all_debates(json_dir)
    add_debates(all_debates, connection)

    all_debates = get_all_debates(json_dir)
    add_contributions(all_debates, members, connection)

    connection.commit()
    connection.close()
