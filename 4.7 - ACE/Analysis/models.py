import pandas as pd
import os
import sys
import numpy as np

from math import log
from nltk import bigrams, trigrams
from collections import Counter, defaultdict
from scipy.stats import entropy
#from scipy.special import softmax, rel_entr
from datetime import datetime, timedelta

from helper_functions import get_time_windows, get_contribution_windows


class BigramModel:
    def __init__(self, contrib_toks, smoothing=1):
        # Create a placeholder for model
        self.counts = defaultdict(lambda: defaultdict(lambda: 0))
        self.total_counts = 0
        self.all_bigrams = set()
        self.smoothing = smoothing

        # Count frequency of co-occurance
        for contribution in contrib_toks:
            for w1, w2 in bigrams(contribution, pad_right=True, pad_left=True):
                self.counts[w1][w2] += 1
                self.total_counts += 1
                self.all_bigrams.add((w1, w2))

    def get_score(self, word, context):
        if self.counts[context][word] > 0:
            word_count = self.counts[context][word]
            context_count = sum(self.counts[context].values())
            return (word_count + self.smoothing) / (context_count + self.smoothing)
        else:
            unigram_count = sum(self.counts[word].values())
            return (unigram_count + self.smoothing) / (self.total_counts + self.smoothing) * 0.4

    def get_cross_entropy(self, toks):
        curr_bigrams = list(bigrams(toks, pad_right=True, pad_left=True))
        q = np.array([self.get_score(w2, w1) for w1, w2 in curr_bigrams])
        return -1 * np.mean(np.log2(q))

    def get_kl_divergence(self, p_model):
        all_bigrams = self.all_bigrams.union(p_model.all_bigrams)
        p = [p_model.get_score(w2, w1) for w1, w2 in all_bigrams]
        q = [self.get_score(w2, w1) for w1, w2 in all_bigrams]
        h = entropy(p, q)
        return h


class SnapshotModels:
    def __init__(self, contributions, toks, window_size, window_step, win_func, printout=False, model=BigramModel, smoothing=1):
        self.snapshots = dict()
        self.windows = dict()
        self.window_size = window_size
        self.window_step = window_step

        # Make the snapshot models at each window
        for window, win_contributions in win_func(contributions, window_size, window_step):
            if printout:
                print(window)
            window = datetime.strptime(window, "%Y/%m/%d")
            # Get the current window for the snapshot model
            curr_window = toks[toks.index.isin(win_contributions.index)]
#             curr_window = curr_window.apply(lambda x: x[:60])

            # Create the bigram model
            win_model = model(curr_window, smoothing=smoothing)
            self.snapshots[window] = win_model
            self.windows[window] = win_contributions.index

    def get_snapshot(self, date):
        check_in_range = lambda x, beg, end: beg <= x.date() < end
        snapshots = pd.Series(self.snapshots)

        for i, snap_date in zip(range(len(snapshots)-1), snapshots.index[:-1]):
            next_window = snapshots.index[i+1]
            if check_in_range(date, snap_date, next_window):
                yield snap_date, snapshots[snap_date]

        if date.date() > snapshots.index[-1]:
            yield snapshots.index[-1], snapshots.iloc[-1]

    def get_previous(self, date):
        check_in_range = lambda x, beg, end: beg <= x.date() < end
        snapshots = pd.Series(self.snapshots)

        prev_date = None

        for i, snap_date in zip(range(len(snapshots)-1), snapshots.index[:-1]):
            next_window = snapshots.index[i+1]

            # If it's the first window, there can be no previous.
            if prev_date is None:
                yield prev_date, None
            elif check_in_range(date, snap_date, next_window):
                yield prev_date, snapshots[prev_date]

            prev_date = snap_date

        if date.date() > snapshots.index[-1] and len(snapshots) > 1:
            yield snapshots.index[-2], snapshots.iloc[-2]

    def calculate_cross_entropies(self, toks, dates, limit=None):
        comparisons = []
        for (i, curr_tokens), date in zip(toks.items(), dates):
            # Loop through possible models (could be multiple in case of overlapping windows)
            for window_date, model in self.get_snapshot(date):
                if model is not None:
                    # calculate the cross-entropy
                    val = model.get_cross_entropy(curr_tokens[:limit])
                    comparisons.append((date, window_date, i, val))
                else:
                    pass

        entropy_df = pd.DataFrame(comparisons, columns=["date", "window", "uid", "cross-entropy"])
        return entropy_df

    # Function to find the cross entropy of posts from each window according to the model of the previous window.
    def calculate_ce_fluctuation(self, toks, dates, limit=None):
        # Loop through each contribution
        comparisons = []
        for (i, curr_tokens), date in zip(toks.items(), dates):
            # Loop through possible previous models (could be multiple in case of overlapping windows)
            for window_date, model in self.get_previous(date):
                if model is not None:
                    # Get the cross-entropy
                    val = model.get_cross_entropy(curr_tokens[:limit])
                    comparisons.append((date, window_date, i, val))
                else:
                    pass

        entropy_df = pd.DataFrame(comparisons, columns=["date", "window", "uid", "cross-entropy"])
        return entropy_df

    # Given a set of snapshot models, gets the KL Divergence of those models to these.
    # Both sets of snapshots must have the same index.
    def calculate_kld(self, comp):
        kl_divergence = {i1: model1.get_kl_divergence(model2) for (i1, model1), (i2, model2) in zip(self.snapshots.items(), comp.snapshots.items())}
        kl_divergence = pd.Series(kl_divergence)
        return kl_divergence

    # Function to find KL of each month according to the previous.
    def calculate_kl_fluctuation(self):
        kl_divergence = dict()
        all_windows = list(self.snapshots.keys())
        for i in range(1, len(all_windows)):
            curr = self.snapshots[all_windows[i]]
            prev = self.snapshots[all_windows[i-1]]

            kl_divergence[all_windows[i]] = prev.get_kl_divergence(curr)

        kl_divergence = pd.Series(kl_divergence)
        return kl_divergence


# Class for Snapshots if one specifies posts at each window - necessary for sampling
class SnapshotModelsPreset(SnapshotModels):
    """
    Had to modify the SnapshotModels class to take an input of contributions already split into windows.
    This means that one can sample the windows as you like rather than letting SnapshotModels use everything.
    """
    def __init__(self, contribution_windows, toks, printout=False, model=BigramModel, smoothing=1):
        self.snapshots = dict()
        self.windows = dict()

        # Make the snapshot models at each window
        for window in contribution_windows.index:
            # Get the current window for the snapshot model
            curr_toks = toks[contribution_windows[window].index]

            # Create the bigram model
            win_model = model(curr_toks, smoothing=smoothing)
            self.snapshots[window] = win_model
            self.windows[window] = contribution_windows[window].index


    def calculate_cross_entropies_set_windows(self, contribution_windows, toks, limit=None):
        """
        Method for calculating the cross entropy of a given set of contributions, which has already been split into windows.
        :param contribution_windows: This is a series of dataframes (one for each window). These must be the same as the snapshot windows.
        :returns: a dictionary of the cross-entropies for each window.
        """
        # Initialise output
        cross_entropies = dict()

        # Loop through each window in the data
        for window, curr_contributions in contribution_windows.items():
            # Get the current tokenised contributions for the window
            curr_toks = toks[curr_contributions.index]

            # Calculate cross-entropy for the current lot of tokens
            cross_entropies[window] = curr_toks.apply(lambda x: self.snapshots[window].get_cross_entropy(x[:limit]))

        return cross_entropies

    def calculate_ce_fluctuation_set_windows(self, contribution_windows, toks, limit=None):
        """
        Method for calculating the cross entropy of a given set of contributions, which has already been split into windows.
        :param contribution_windows: This is a series of dataframes (one for each window). These must be the same as the snapshot windows.
        :returns: a dictionary of the cross-entropies for each window.
        """
        # Initialise output
        cross_entropies = dict()

        all_windows = list(contribution_windows.keys())

        # Loop through each window in the data, as well as the next window
        for curr_window, next_window in zip(all_windows[:-1], all_windows[1:]):
            # Get the contributions for the next window
            next_contributions = contribution_windows[next_window]

            # Get the current tokenised contributions for the window
            next_toks = toks[next_contributions.index]

            # Calculate cross-entropy for the current lot of tokens
            cross_entropies[next_window] = next_toks.apply(lambda x: self.snapshots[curr_window].get_cross_entropy(x[:limit]))

        return cross_entropies
