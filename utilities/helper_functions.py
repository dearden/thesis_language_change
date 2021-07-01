import sys
import os

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
from datetime import date, timedelta
import regex as re
import json
import time
import unicodedata
import spacy
import itertools
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from collections import Counter

sys.path.insert(1, "../")
from settings import DB_FP

nlp = spacy.load('en_core_web_sm')

main_dir = "../"
MP_Group_FP = "../mp_groups"

# regex for identifying EU/brexit mentionseu_mentions = all_contributions.loc[all_contribut
eu_regex = r'\b(EU|[Ee]uropean [Uu]nion|[Bb]rexit)\b'

leave_or_remain = lambda x: "remain" if x<50 else "leave"

round_down_month = lambda x: x.replace(day=1)

convert_to_bool = lambda x: True if x=="True" else False

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



def keywords_filter(contributions, kw_mention_regex=None, kw_section_regex=None):
    # Below, we define lambdas to filter
    # Returns true if a text matches the eu regex.
    check_if_eu_mention = lambda x: True if re.search(kw_mention_regex, x.lower()) is not None else False
    check_if_eu_section = lambda x: True if re.search(kw_section_regex, x.lower()) is not None else False

    eu_contribs = pd.DataFrame(columns=contributions.columns)
    if kw_mention_regex is not None:
        # Gets the subset of the dataframe filtered with the provided function.
        eu_contribs = contributions[contributions['text'].apply(check_if_eu_mention)]

    eu_sections = pd.DataFrame(columns=contributions.columns)
    if kw_section_regex is not None:
        # Gets all contributions with a section.
        sections_not_none =  contributions[contributions["section"].apply(lambda x: x is not None)]
        # Gets subset of dataframe where section matches EU regex.
        eu_sections = sections_not_none[sections_not_none['section'].apply(check_if_eu_section)]

    eu_contribs_sections = eu_contribs.combine_first(eu_sections)

    return eu_contribs_sections


# Splits corpus into subset and reference based on the type given
def split_corpus(all_contributions, filter_type,
                    mention_wordlist_fp=os.path.join(main_dir, "resources", "eu_mention_search_terms.json"),
                    section_wordlist_fp=os.path.join(main_dir, "resources", "eu_section_search_terms.json")):
    # Load in all the EU search terms for searching text for mentions.
    with open(mention_wordlist_fp) as eu_term_file:
        eu_term_list = json.load(eu_term_file)

    # regex for identifying EU/brexit mentions
    eu_mention_regex = "({})".format("|".join(eu_term_list))

    # Load in all the EU search terms for finding sections to do with Europe.
    with open(section_wordlist_fp) as eu_term_file:
        eu_term_list = json.load(eu_term_file)

    # regex for identifying EU/brexit sections
    eu_section_regex = "({})".format("|".join(eu_term_list))

    # Gets the subset of the dataframe filtered with the desired function. (default is EU mentions)
    if callable(filter_type):
        subset = all_contributions.apply(filter_type)
    elif filter_type == "eu":
        subset = keywords_filter(all_contributions, kw_mention_regex=eu_mention_regex, kw_section_regex=eu_section_regex)
    elif filter_type == "eu_mentions":
        subset = keywords_filter(all_contributions, kw_mention_regex=eu_mention_regex)
    elif filter_type == "eu_sections":
        subset = keywords_filter(all_contributions, kw_section_regex=eu_section_regex)
    elif filter_type == "no_filter":
        subset = all_contributions
    else:
        subset = keywords_filter(all_contributions, kw_mention_regex=eu_mention_regex, kw_section_regex=eu_section_regex)

    # Gets the Reference Corpus (everything not in subset)
    ref_corpus = all_contributions[~all_contributions.index.isin(subset.index)]

    return subset, ref_corpus


def log_ratio(x, y):
    ratio = x / y
    return np.log2(ratio)


def get_log_ratios(counts1, counts2):
    out = []
    lenCorp1 = sum(counts1.values())
    lenCorp2 = sum(counts2.values())

    for word in set(list(counts1) + list(counts2)):
        get_value = lambda w, d: d[w] if w in d else 0
        c1 = get_value(word, counts1)
        c2 = get_value(word, counts2)

        # If either of the corpuses have no text, set lr to None.
        if lenCorp1 == 0 or lenCorp2 == 0:
            lr = None
        else:
            lr = log_ratio((c1+0.5)/lenCorp1, (c2+0.5)/lenCorp2)

        out.append((word, c1, c2, lr, lenCorp1, lenCorp2))
    return out


def count_tokens(texts):
    count = Counter()
    for text in texts:
        count.update(text)
    return count


def get_keywords_from_tokens(tokens, comparison, lr_threshold=1, count_threshold=10):
    tok_counts = dict(count_tokens(tokens.values))
    comp_counts = dict(count_tokens(comparison.values))

    # calculate the log ratios for the two count dictionaries.
    # This will create a dataframe  with the log-ratio for each word in the corpus w.r.t the comparison.
    log_ratios = pd.DataFrame(get_log_ratios(tok_counts, comp_counts),
                            columns=['word', 'count', 'comp_count', 'log-ratio', 'len', 'comp_len'])

    # Make the words the index.
    log_ratios.set_index("word", inplace=True)

    # If we have set a log-ratio threshold, only keep the words with an LR above that threshold.
    if lr_threshold is not None:
        out_kw = log_ratios.loc[log_ratios['count'] > count_threshold].loc[log_ratios['log-ratio'] > lr_threshold].sort_values("log-ratio", ascending=False)["log-ratio"].apply(float)
    # Otherwise, just keep all of it.
    else:
        out_kw = log_ratios

    return out_kw


# Creates a generator which gives the beginning of each window in given range and step.
def dayrange(d_start, d_end, window=0, step=1):
    for n in range(0, int(((d_end - timedelta(days=window)) - d_start).days), step):
        yield d_start + timedelta(n)

# Get time windows beginning at the start and rolling a specified amount of time forward each window. (e.g. a number of days)
def get_time_windows(contributions, window_size, step):
    # lambda for checking a date is within two bounds
    check_in_range = lambda x, beg, end: beg <= x.date() <= end

    # Sort the values initially otherwise it won't work.
    contributions = contributions.sort_values("date", ascending=True)
    start_date = contributions.iloc[0].date
    end_date = contributions.iloc[-1].date

    for curr_day in dayrange(start_date, end_date, window=window_size, step=step):
        # create window
        win_beg = curr_day
        win_end = curr_day + timedelta(days=window_size-1)
        # get all posts for which time stamp is in window
        contributions_in_window = contributions[contributions['date'].apply(check_in_range, args=(win_beg, win_end))]
        yield win_beg.strftime("%Y/%m/%d"), contributions_in_window


# Get rolling windows which move forward a number of contributions each time.
def get_contribution_windows(contributions, window_size, step):
    # Sort the values initially because otherwise it won't work.
    contributions = contributions.sort_values("date", ascending=True)
    # Go through the contributions with the step specified and return the specified size window.
    for i in range(0, len(contributions) - window_size, step):
        curr_window = contributions.iloc[i:i+window_size]
        curr_name = contributions.iloc[i].date.strftime("%Y/%m/%d")
        yield curr_name, curr_window


def clean_text(text):
    text = text.lower()
    text = text.strip()

    text = re.sub(r"\d+(\.\d+)*", "NUMBER", text)

    text = text.replace("\n", " ")

    text = re.sub(r"([\.\,\!\?\:\;]+)(?=\w)", r"\1 ", text)

    text = unicodedata.normalize("NFKD", text)

    text = re.sub(r"[ ]+", r" ", text)
    return text


def spacy_tokenise(text):
    doc = nlp.tokenizer(text)
    return [tok.text for tok in doc]


def add_key_dates(ax, key_dates):
    if key_dates is not None:
        for date, event in key_dates.iterrows():
            ax.axvline(x=date, color="gray", alpha=event.transparency, zorder=1)


def plot_timeline(ax, dates, names, fontsize=16):
    # Choose some nice levels
    levels = np.tile([-3, 3, -2, 2, -1, 1],
                    int(np.ceil(len(dates)/6)))[:len(dates)]

    ax.vlines(dates, 0, levels, color="tab:red")  # The vertical stems.
    ax.plot(dates, np.zeros_like(dates), "-o",
            color="k", markerfacecolor="w", markersize=14)  # Baseline and markers on it.

    # annotate lines
    for d, l, r in zip(dates, levels, names):
        ax.annotate(r, xy=(d, l),
                    xytext=(0, np.sign(l)*3), textcoords="offset points",
                    horizontalalignment="center",
                    verticalalignment="bottom" if l > 0 else "top", fontsize=fontsize)

    # format xaxis with 4 month intervals
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right", fontsize=fontsize)

    # remove y axis and spines
    ax.yaxis.set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    ax.set_ylim(-5, 5)

    ax.margins(y=0.1)
    ax.grid()

def check_dir(dir_name):
    """
    Checks if a directory exists. Makes it if it doesn't.
    """
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


def tokenise(text):
    """
    Turns given text into tokens.
    """
    cleaned = clean_text(text)
    cleaned = re.sub(r"(\p{P})\p{P}*", r"\1 ", cleaned)
    tokens = spacy_tokenise(cleaned)
    return tokens


def pos_tokenise(text):
    """
    Turns given text into tokens and PoS tags.
    """
    cleaned = clean_text(text)
    cleaned = re.sub(r"(\p{P})\p{P}*", r"\1 ", cleaned)
    
    doc = nlp(cleaned)
    return [{"tok": tok.text, "lemma":tok.lemma_, "pos": tok.pos_} for tok in doc]

merge_lists = lambda x: list(itertools.chain.from_iterable(x))


def get_chunks(idx, tokens, chunk_size):
    for i in range(0, len(tokens)-chunk_size, chunk_size):
        yield idx, tokens[i:i+chunk_size]
        

def make_tok_chunks(tokens, chunk_size):
    chunks = [[[idx, chunk] for idx, chunk in get_chunks(idx, curr_toks, chunk_size)] for idx, curr_toks in tokens.items()]
    chunks = merge_lists(chunks)
    chunks = pd.DataFrame(chunks, columns=["idx", "chunk"])
    return chunks


def get_static_kws(all_contributions, all_counts, tokenised, group_type="party", filter_type="eu"):
    # Get the reference corpus
    subset, ref_corpus = split_corpus(all_contributions, filter_type)

    print("FINDING KEYWORDS")

    # Get the function which gives us our groups (if this wasn't already given)
    if callable(group_type):
        group_function = group_type
    else:
        group_function = get_group_function(group_type)

    kw_dic = dict()
    check_frequency_threshold = lambda x: True if all_counts[x] > 10 else False


    # First pass through to find the keywords.
    for group, contributions in group_function(all_contributions):
        # Create the current group name.
        curr_group_name = group

        contribs = subset[subset.index.isin(contributions.index)]
        comparis = ref_corpus[ref_corpus.index.isin(contributions.index)]

        # Sorts the current contributions by date.
        contribs = contribs.sort_values("date", ascending=True)
        comparis = comparis.sort_values("date", ascending=True)

        # Get the tokens.
        contrib_toks = tokenised.loc[contribs.index]
        compari_toks = tokenised.loc[comparis.index]

        print("=======================================")
        print("STARTED PROCESSING GROUP {}".format(curr_group_name))
        print("=======================================")

        keywords = get_keywords_from_tokens(contrib_toks, compari_toks, lr_threshold=1)
        keywords = keywords[keywords.index.to_series().apply(check_frequency_threshold)]
            
        kw_dic[curr_group_name] = keywords

    return kw_dic