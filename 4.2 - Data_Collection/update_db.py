# This code is used to update an existing database of Hansard Contributions with a new database.
# The original DB will be kept, with new contributions and members, etc, being added from the new database.
import sys
import sqlite3
import pandas as pd

from sqlite3 import Error as SQLError
from make_db import create_connection

sql_get_contributions = "SELECT * FROM contributions;"
sql_get_debates = "SELECT * FROM debates;"


def update_debates_table(cursor, debates):
    for uid, row in debates.iterrows():
        command = '''
                    INSERT INTO debates(uid, date, hansardNum, file)
                    VALUES(?, ?, ?, ?);
                    '''
        curr_entry = (uid, row.date, row.hansardNum, row.file)
        cursor.execute(command, curr_entry)
        print(cursor.lastrowid)

def update_contributions_table(cursor, contributions):
    for uid, row in contributions.iterrows():
        command = '''
                    INSERT INTO contributions(uid, member, debate, body, isQuestion, referringTo, topic, section, contType, sectionTag, department)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    '''

        curr_entry = (uid, row.member, row.debate, row.body, row.isQuestion, row.referringTo, row.topic, row.section, row.contType, row.sectionTag, row.department)
        cursor.execute(command, curr_entry)
        print(cursor.lastrowid)


if __name__ == "__main__":
    old_db_fp = input("Enter FP of Old Database: ")
    new_db_fp = input("Enter FP of New Database: ")

    # Make connection to the two databases
    # First the old database that we want to update.
    conn_old = create_connection(old_db_fp)
    curs = conn_old.cursor()

    # Now the new database.
    conn_update = create_connection(new_db_fp)

    # Read in Old DB
    # Gets all the contributions and creates a nice dataframe
    old_contributions = pd.read_sql_query(sql_get_contributions, conn_old).set_index("uid")

    # Gets the old debates
    old_debates = pd.read_sql_query(sql_get_debates, conn_old).set_index("uid")

    # Read in New DB
    # Contributions
    update_contributions = pd.read_sql_query(sql_get_contributions, conn_update).set_index("uid")
    # Debates
    update_debates = pd.read_sql_query(sql_get_debates, conn_update).set_index("uid")

    # Identify new contributions
    new_contributions = update_contributions.query("uid not in @old_contributions.index")

    # Identify new debates
    new_debates = update_debates.query("uid not in @old_debates.index")

    # Check if there's new members (hopefully not)
    new_members = update_contributions.query("member not in @old_contributions.member").member.unique()

    # Update the contributions table
    print("------------------------------------")
    print("Updating contributions")
    print("------------------------------------")
    update_contributions_table(curs, new_contributions)

    # Update the debates table
    print("------------------------------------")
    print("Updating debates")
    print("------------------------------------")
    update_debates_table(curs, new_debates)

    # Commit the changes
    conn_old.commit()
    conn_old.close()
