import pandas as pd
import os
from datetime import datetime
import numpy as np
import sys
from models import SnapshotModels, BigramModel, SnapshotModelsPreset
from helper_functions import get_time_windows


def get_samples_and_test_set(gnames, all_groups_contribs, all_groups_toks,
                             window_func=get_time_windows, window_size=360, window_step=90,
                             snap_samples_per_user=10, snap_num_users=100, test_sample_size=1000, replace=False):
    """
    Method for sampling a set of contributions to train snapshot models on for each group in each time window.
    This is a sample of 'snap_num_users' users with 'snap_samples_per_user' posts each ('snapshot sample').
    Also creates a sample for calculating the cross entropy of.
    This second sample is a sample of 'test_sample_size' posts by users not in the snapshot sample ('test sample').
    """
    # Create a matrix of all contributions from different groups
    combined = pd.concat(all_groups_contribs, axis=0)
    # Initialise snapshot samples dict
    samples = {gname: dict() for gname in gnames}
    # Inititalise the test samples.
    test_contribs = {gname: [] for gname in gnames}

    # Loop through the time windows.
    for window, window_contribs in window_func(combined, window_size, window_step):
        # Convert the window to datetime
        window = datetime.strptime(window, "%Y/%m/%d")
        # Loop through the given groups
        for curr_gname, curr_group_contribs, curr_group_toks in zip(gnames, all_groups_contribs, all_groups_toks):
            # Get the posts for current group in current window
            group_window_contribs = curr_group_contribs[curr_group_contribs.index.isin(window_contribs.index)]
#             print(curr_gname, window)
#             print(group_window_contribs.shape[0], "contributions.")

            # Get the number of posts per user
            user_post_counts = group_window_contribs.groupby("PimsId").size()
#             print(user_post_counts[user_post_counts >= snap_samples_per_user].shape[0], "users with over 10 posts.")
#             print("----------------------")

#             import pdb; pdb.set_trace()
#             print(window, "\t", len(user_post_counts[user_post_counts >= snap_samples_per_user]))
            # Get a sample of users to train snapshot.
            user_sample = user_post_counts[user_post_counts >= snap_samples_per_user].sample(snap_num_users, replace=replace).index

            # Get the contribs in sample1
            contribs_in_sample = group_window_contribs.loc[group_window_contribs["PimsId"].isin(user_sample)]
            # Sample 10 contributions from each of these.
            curr_sample = contribs_in_sample.groupby("PimsId").apply(lambda x: x.sample(snap_samples_per_user, replace=replace)).reset_index(level="PimsId", drop=True)
            # Sort the contributions
            samples[curr_gname][window] = curr_sample.sort_values("date", ascending=True)

            # Get only the contributions that are not in the sample
            curr_non_sample = group_window_contribs.loc[~group_window_contribs.PimsId.isin(user_sample)].sample(test_sample_size, replace=replace)
            test_contribs[curr_gname].append(curr_non_sample)

    # Make samples a dictionary with a pandas series for each group's samples per window.
    samples = {gname: pd.Series(samples[gname]) for gname in gnames}

    # Make test_contribs a dictionary with a dataframe of all test contributions
    test_contribs = {gname: pd.concat(test_contribs[gname], axis=0).sort_values("date", ascending=True) for gname in gnames}

    return samples, test_contribs


def calculate_CE_per_group(samples, test_contribs, test_toks, group_names, all_groups_toks, n_words_per_contrib):
    """
    Calculates the Cross-Entropy between each combination of groups.

    :param samples: contributions for training snapshot models.
    :param test_contribs: contributions for comparing to snapshots.
    :param test_toks: tokens of test contributions.
    :param group_names: all the names of groups.
    :param all_groups_toks: all tokens, an entry for each group, in the same order as group names.
    :param n_words_per_contrib: the number of words to use from each contribution.

    :returns: The Cross-Entropies of each group against the snapshot model of each group.
    """
    # Initialises the dictionary of cross entropies per group for this run.
    all_groups_CE = {gname: {} for gname in group_names}

    # Loop through each group to train a snapshot model and compare the contributions of all groups to that snapshot.
    for i in range(len(group_names)):
        # Create snapshot model using the snapshot samples for the current group.
        snaps = SnapshotModelsPreset(samples[group_names[i]], all_groups_toks[i].apply(lambda x: x[:n_words_per_contrib]))
        # Loop through each group.
        for gname in group_names:
            # Calculate the cross entropy for each of the test samples
            all_groups_CE[group_names[i]][gname] = snaps.calculate_cross_entropies(test_toks[gname].apply(lambda x: x[:n_words_per_contrib]), test_contribs[gname].date)

    return all_groups_CE


def calculate_KLD_per_group(samples, test_contribs, test_toks, group_names, all_groups_toks, n_words_per_contrib):
    """
    Calculates the KL-Divergence between each combination of groups.

    :param samples: contributions for training snapshot models.
    :param test_contribs: contributions for comparing to snapshots.
    :param test_toks: tokens of test contributions.
    :param group_names: all the names of groups.
    :param all_groups_toks: all tokens, an entry for each group, in the same order as group names.
    :param n_words_per_contrib: the number of words to use from each contribution.

    :returns: The KL-Divergence of each group against the snapshot model of each group.
    """
    all_snapshots = {}
    # Loop through each group and train a snapshot model
    for i in range(len(group_names)):
        # Create snapshot model using the snapshot samples for the current group.
        all_snapshots[group_names[i]] = SnapshotModelsPreset(samples[group_names[i]], all_groups_toks[i].apply(lambda x: x[:n_words_per_contrib]))

    # Initialises the dictionary of cross entropies per group for this run.
    all_groups_KLD = {gname: {} for gname in group_names}

    # Loop through each group to train a snapshot model and compare the contributions of all groups to that snapshot.
    for i in range(len(group_names)):
        # Loop through each group.
        for gname in group_names:
            # Calculate the KLD between each snapshot.
            all_groups_KLD[group_names[i]][gname] = all_snapshots[group_names[i]].calculate_kld(all_snapshots[gname])

    return all_groups_KLD


def calculate_CE_fluct_per_group(samples, test_contribs, test_toks, group_names, all_groups_toks, n_words_per_contrib):
    """
    Calculates the Cross-Entropy Fluctuation of each group.

    :param samples: contributions for training snapshot models.
    :param test_contribs: contributions for comparing to snapshots.
    :param test_toks: tokens of test contributions.
    :param group_names: all the names of groups.
    :param all_groups_toks: all tokens, an entry for each group, in the same order as group names.
    :param n_words_per_contrib: the number of words to use from each contribution.

    :returns: The Cross-Entropy fluctuation of each group.
    """
    # Initialises the dictionary of cross entropies per group for this run.
    all_groups_CE = {}

    # Loop through each group to train a snapshot model and compare the contributions of all groups to that snapshot.
    for i in range(len(group_names)):
        # Create snapshot model using the snapshot samples for the current group.
        snaps = SnapshotModelsPreset(samples[group_names[i]], all_groups_toks[i].apply(lambda x: x[:n_words_per_contrib]))
        all_groups_CE[group_names[i]] = snaps.calculate_ce_fluctuation(test_toks[group_names[i]].apply(lambda x: x[:n_words_per_contrib]), test_contribs[group_names[i]].date)

    return all_groups_CE

def calculate_KLD_fluct_per_group(samples, test_contribs, test_toks, group_names, all_groups_toks, n_words_per_contrib):
    """
    Calculates the KL-Divergence Fluctuation of each group.

    :param samples: contributions for training snapshot models.
    :param test_contribs: contributions for comparing to snapshots.
    :param test_toks: tokens of test contributions.
    :param group_names: all the names of groups.
    :param all_groups_toks: all tokens, an entry for each group, in the same order as group names.
    :param n_words_per_contrib: the number of words to use from each contribution.

    :returns: The KL-Divergence fluctuation of each group.
    """
    # Initialises the dictionary of cross entropies per group for this run.
    all_groups_KLD = {gname: {} for gname in group_names}

    # Loop through each group and train a snapshot model
    for i in range(len(group_names)):
        # Create snapshot model using the snapshot samples for the current group.
        snap = SnapshotModelsPreset(samples[group_names[i]], all_groups_toks[i].apply(lambda x: x[:n_words_per_contrib]))
        all_groups_KLD[group_names[i]] = snap.calculate_kl_fluctuation()

    return all_groups_KLD


def multiple_run_sampling(group_names, all_groups_contribs, all_groups_toks, n_runs=5,
                          snap_samples_per_user=10, snap_num_users=100,
                          test_sample_size=1000, replace=False, n_words_per_contrib=60,
                          win_func=get_time_windows, win_size=360, win_step=90,
                          comparison_method="CE"):
    """Runs the sampling multiple times and calculates a comparison at each run.

    Performs multiple runs of sampling and calculation of comparison metric.
    For each run, creates a "snapshot" sample of 1000 contributions to train a snapshot model on.
    Also creates a "test" sample of 1000 contributions for which to calculate a comparison, e.g. cross-entropy or KL-Divergence.
    Then calculates the cross entropy of each contribution in the test sample w.r.t the snapshot models.
    It does this calculation for every combination of groups.

    :param group_names: a list of the groups in the given data.
    :param all_group_contribs: a list of Dataframes of contributions - one for each group.
    :param all_groups_toks: a list that matches "all_group_contribs" but consists of a pandas Series of
                            tokenised contributions - one for each group.
    :param n_runs: the number of times to resample and recalculated cross entropy.
    :param comparison_func: the method used to compare the groups - must be "CE", "KLD", "CE_Fluct", or "KLD_Fluct".

    :returns: a list of dictionaries, one for each run. Each dictionary has another dictionary for each
                group which contains cross entropy results dataframes. One can find the information of a
                run as follows: "runs[group1][group2]" would contain a DataFrame with the cross entropies
                of test samples from group2 according to the snapshot model trained on group1.
    """

    comparison_methods = {
                            "CE": calculate_CE_per_group,
                            "KLD": calculate_KLD_per_group,
                            "CE_Fluct": calculate_CE_fluct_per_group,
                            "KLD_Fluct": calculate_KLD_fluct_per_group
                         }

    # Initialise empty output array.
    all_runs = []

    # Loop for each run.
    for run_num in range(n_runs):
        # Sample snapshot and test samples for each group.
        samples, test_contribs = get_samples_and_test_set(group_names,
                                                          all_groups_contribs,
                                                          all_groups_toks,
                                                          snap_samples_per_user=snap_samples_per_user,
                                                          snap_num_users=snap_num_users,
                                                          test_sample_size=test_sample_size,
                                                          replace=replace,
                                                          window_func=win_func,
                                                          window_size=win_size,
                                                          window_step=win_step)

        # Get all of the test tokens
        test_toks = {group_names[i]: all_groups_toks[i][test_contribs[group_names[i]].index] for i in range(len(group_names))}

        # Calculate the comparison values for all groups
        all_groups_comparison = comparison_methods[comparison_method](samples, test_contribs, test_toks, group_names, all_groups_toks, n_words_per_contrib)

        # Add dictionary for current run to output list.
        all_runs.append(all_groups_comparison)

    return all_runs


def get_CE_means_per_run(all_runs, snapshot_group, test_group):
    means = []
    stds = []

    for run in all_runs:
        curr_cross_entropies = run[snapshot_group][test_group]
        cross_entropy_means = curr_cross_entropies.groupby("window")["cross-entropy"].mean()
        cross_entropy_stds = curr_cross_entropies.groupby("window")["cross-entropy"].std()
        means.append(cross_entropy_means)
        stds.append(cross_entropy_stds)

    means = pd.concat(means, axis=1)
    means.columns = range(len(all_runs))
    stds = pd.concat(stds, axis=1)
    stds.columns = range(len(all_runs))
    return means, stds


def get_KLD_per_run(all_runs, snapshot_group, test_group):
    KLDs = []

    for run in all_runs:
        KLDs.append(run[snapshot_group][test_group])

    KLDs = pd.concat(KLDs, axis=1)
    KLDs.columns = range(len(all_runs))
    return KLDs


def get_mean_per_window_of_runs(window_means_all_runs):
    mean_per_window = window_means_all_runs.mean(axis=1)
    std_per_window = window_means_all_runs.std(axis=1)

    return mean_per_window, std_per_window
