# This script is for adding all historical roles each member has held.

import sys
import sqlite3
import pandas as pd
import json
from sqlite3 import Error as SQLError
from make_db import create_connection, create_table
from lxml import etree

sys.path.insert(1, "../")
from settings import DB_FP as db_fp

def get_posts_from_xml(curr_member, members_xml):
    posts = []

    for member in members_xml.iterfind("{*}Member"):
        if str(curr_member['PimsId']) == member.attrib['Pims_Id'] or str(curr_member['MnisId']) == member.attrib['Member_Id'] or str(curr_member['ClerksId']) == member.attrib['Clerks_Id']:
            # First get all of the Government Posts for the current member
            posts_xml = member.find("{*}GovernmentPosts")
            # Add an entry for each one.
            for post in posts_xml.iterfind("{*}GovernmentPost"):
                curr_post = dict()
                curr_post["uid"] = post.attrib["Id"]
                curr_post['PimsId'] = curr_member["PimsId"]
                curr_post['start'] = post.find("{*}StartDate").text
                curr_post['end'] = post.find("{*}EndDate").text
                curr_post['post'] = post.find("{*}Name").text
                posts.append(curr_post)

            # Then get all of the Opposition Posts for the current member
            posts_xml = member.find("{*}OppositionPosts")
            # Add an entry for each one.
            for post in posts_xml.iterfind("{*}OppositionPost"):
                curr_post = dict()
                curr_post["uid"] = post.attrib["Id"]
                curr_post['PimsId'] = curr_member["PimsId"]
                curr_post['start'] = post.find("{*}StartDate").text
                curr_post['end'] = post.find("{*}EndDate").text
                curr_post['post'] = post.find("{*}Name").text
                posts.append(curr_post)

            # Then get all of the Parliamentary Posts for the current member
            posts_xml = member.find("{*}ParliamentaryPosts")
            # Add an entry for each one.
            for post in posts_xml.iterfind("{*}ParliamentaryPosts"):
                curr_post = dict()
                curr_post["uid"] = post.attrib["Id"]
                curr_post['PimsId'] = curr_member["PimsId"]
                curr_post['start'] = post.find("{*}StartDate").text
                curr_post['end'] = post.find("{*}EndDate").text
                curr_post['post'] = post.find("{*}Name").text
                posts.append(curr_post)

            print("Added roles for ", curr_member["display_name"])

            break

    return posts


def get_member_posts(member):
    all_members_root = "http://data.parliament.uk/membersdataplatform/services/mnis/members/query/House=Commons|Membership=all/GovernmentPosts|OppositionPosts|ParliamentaryPosts/"

    if member['MnisId'] is not None:
        curr_member_tree = etree.parse("http://data.parliament.uk/membersdataplatform/services/mnis/members/query/House=Commons|id={}|Membership=all/GovernmentPosts|OppositionPosts|ParliamentaryPosts".format(member['MnisId']))
        curr_member_root = curr_member_tree.getroot()
        return get_posts_from_xml(member, curr_member_root)
    else:
        return get_posts_from_xml(member, all_members_root)


def add_member_posts(posts, cursor):
    # Add the member's parties
    for member_posts in posts:
        for post in member_posts:
            command = '''INSERT INTO member_post(uid, PimsId, start, end, post)
                            VALUES(?, ?, ?, ?, ?);'''
            curr_entry = (post['uid'], post['PimsId'], post['start'], post['end'], post['post'])
            cursor.execute(command, curr_entry)

if __name__ == "__main__":
    # Make connection to the database.
    conn = create_connection(db_fp)
    curs = conn.cursor()

    # Get the members data
    members = pd.read_sql_query("SELECT * FROM members;", conn)

    # Get the member posts
    member_posts = members.apply(get_member_posts, axis=1)

    # SQL for making the table of posts
    sql_create_member_post = """
    CREATE TABLE IF NOT EXISTS member_post (
        uid integer NOT NULL,
        PimsId integer NOT NULL,
        start text NOT NULL,
        end text,
        post text NOT NULL,
        PRIMARY KEY (uid, PimsId, start),
        FOREIGN KEY (PimsId) REFERENCES members (PimsId)
    );"""

    # Create the table
    create_table(conn, sql_create_member_post)

    # Add the posts
    add_member_posts(member_posts, curs)

    # Commit the changes
    conn.commit()
    conn.close()
