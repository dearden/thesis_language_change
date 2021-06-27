import os
import feedparser
import requests
import wget

from datetime import timedelta, date
from dateutil import rrule


def get_atom_resources(url):
    resources = feedparser.parse(url)
    for entry in resources['entries']:
        yield entry

# Dump all of the stuff that produced an exception.
def add_to_discard_pile(url):
    with open("zip_discard_pile.txt", "a") as discard_pile:
        discard_pile.write("{}\n".format(url))

# Downloads the file at the given url to the given directory.
def download_zip(url, out_dir):
    try:
        wget.download(url, "{0}/{1}".format(out_dir, url.split("/")[-1]))
    except Exception as e:
        print(e)
        add_to_discard_pile(url)

def download_zips(resources, out_dir):
    # Loop through all of the resources in the feed
    for entry in resources:
        # Find the file extension (which will tell us the type of resource)
        extension = entry['link'].split(".")[-1].strip()

        # Record the resource url.
        r_url = entry['link']

        # Catch the zip files.
        if extension == "zip":
            print("\n\n---------------------------")
            print(entry['title'])
            print("---------------------------\n")

            download_zip(r_url, out_dir)

# Given the two given dates, gives a formatted beginning and end.
def get_month_boundaries(t1, t2):
    # Get the formatted strings for beginning and end.
    start_first = t1.strftime("%Y-%m-%d")
    end_first = (t2 - timedelta(days=1)).strftime("%Y-%m-%d")
    # Yield them.
    return (start_first, end_first)


# Get a time range comprising entire months from beginning to end.
def get_times(start, end):
    # Get the start of the next month after the start time.
    start_of_full_months = date(year=start.year, 
                                month=(start.month % 12) + 1, 
                                day=1)

    # Get all intermediary times (i.e. the whole months)
    all_times = [dt for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_of_full_months, until=end)]
    
    # Yield the remainder of the first month. (beginning/end)
    yield get_month_boundaries(start, all_times[0])

    # Loop through the rest of the dates and yield the beginning/end pair.
    for i in range(len(all_times)-1):
        yield get_month_boundaries(all_times[i], all_times[i+1])

    # Return the beginning/end for the final month.
    yield get_month_boundaries(all_times[-1], end + timedelta(days=1))

if __name__ == "__main__":
    # Originally go 2015/05/07 -> 2019/09/10
    start = date(year=2019, month=6, day=1)
    end = date(year=2020, month=1, day=1)

    out_dir = input("Enter Directory: ")
    
    for t in get_times(start, end):
        url = "http://api.data.parliament.uk/resources/files/feed?skip=0&take=all&fromdate={0}&todate={1}".format(t[0], t[1])
        
        # curr_dir = "hansard_zips/{}".format(t[0])
        curr_dir = "{0}/{1}".format(out_dir, t[0])
        if not os.path.isdir(curr_dir):
            os.makedirs(curr_dir)

        resources = get_atom_resources(url)
        download_zips(resources, curr_dir)