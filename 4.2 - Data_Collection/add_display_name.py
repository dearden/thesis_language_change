import sqlite3
import os
import sys
import re


if __name__ == "__main__":
    if len(sys.argv) < 2:
        db_fp = input("Please enter DB location: ")
    else:
        db_fp = sys.argv[1]

    conn = sqlite3.connect(db_fp)
    curs = conn.cursor() 

    addDisplayNameColumn = "ALTER TABLE members ADD COLUMN display_name text"

    try:
        curs.execute(addDisplayNameColumn)
    except sqlite3.OperationalError as e:
        print(e)
        print("Probably just already have the columns.")

    sql_get_members = "SELECT PimsId, name FROM members"

    curs.execute(sql_get_members)
    id_list = curs.fetchall()

    reg_remove_title = r'(([m|M]rs?|[m|M]s|[d|D]ame|[s|S]ir|[b|B]aron(ess)?|[l|L]ord|[l|L]ady|[d|D]r) )?(.*)'
    reg_at_least_two_names = r'\S+ \S+.*'

    for cont in id_list:
        id = cont[0]
        full_name = cont[1]
        name = None

        # Filter off the titles by matching this regex and getting the 4th group.
        m = re.match(reg_remove_title, full_name)
        if m:
            name = m.group(4)

            # Flag up names which don't have two names 
            # (e.g. if "Lord Pickles" was reduced to "Pickles")
            m = re.match(reg_at_least_two_names, name)
            if m is None:
                print("{0} - Dodgy name: {1}".format(id, full_name))

            with open("dodgy_names.txt", "a") as name_file:
                name_file.write("\n{0} - Dodgy name: {1}".format(id, full_name))


        vals = (name, id)
        curs.execute("UPDATE members SET display_name=? WHERE PimsId=?", vals)

    conn.commit()
    conn.close()

