import pandas as pd
import matplotlib.pyplot as plt
import os
import sys
from datetime import datetime
import numpy as np

from helper_functions import get_time_windows, get_contribution_windows, split_corpus, add_key_dates
from models import BigramModel, SnapshotModels, SnapshotModelsPreset
from sampling_over_time import get_samples_and_test_set, multiple_run_sampling
from sampling_over_time import get_CE_means_per_run, get_KLD_per_run, get_mean_per_window_of_runs


def calculate_CE_per_group(samples, test_contribs, group_names, all_groups_toks, n_words_per_contrib):
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
        for j, gname in enumerate(group_names):
            # Calculate the cross entropy for each of the test samples
            all_groups_CE[group_names[i]][gname] = snaps.calculate_cross_entropies_set_windows(test_contribs[gname], all_groups_toks[j], limit=n_words_per_contrib)

    return all_groups_CE


def calculate_CE_fluct_per_group(samples, test_contribs, group_names, all_groups_toks, n_words_per_contrib):
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
    for i, gname in enumerate(group_names):
        # Create snapshot model using the snapshot samples for the current group.
        snaps = SnapshotModelsPreset(samples[gname], all_groups_toks[i].apply(lambda x: x[:n_words_per_contrib]))
        all_groups_CE[gname] = snaps.calculate_ce_fluctuation_set_windows(test_contribs[gname], all_groups_toks[i].apply(lambda x: x[:n_words_per_contrib]), 60)

    return all_groups_CE


def calculate_KLD_per_group(samples, test_contribs, group_names, all_groups_toks, n_words_per_contrib):
    """
    Calculates the KL-Divergence between each combination of groups.

    :param samples: contributions for training snapshot models.
    :param test_contribs: contributions for comparing to snapshots.
    :param group_names: all the names of groups.
    :param all_groups_toks: all tokens, an entry for each group, in the same order as group names.
    :param n_words_per_contrib: the number of words to use from each contribution.

    :returns: The KL-Divergence of each group against the snapshot model of each group.
    """
    test_models = {}
    # Loop through each group and train models for the test data
    for i, gname in enumerate(group_names):
        # Create snapshot model using the test samples for the current group.
        test_models[gname] = SnapshotModelsPreset(test_contribs[gname], all_groups_toks[i].apply(lambda x: x[:n_words_per_contrib]))

    # Initialises the dictionary of cross entropies per group for this run.
    all_groups_KLD = {gname: {} for gname in group_names}

    # Loop through each group to train a snapshot model and compare the contributions of all groups to that snapshot.
    for i, snap_gname in enumerate(group_names):
        # Create snapshot model using the snapshot samples for the current group.
        snaps = SnapshotModelsPreset(samples[snap_gname], all_groups_toks[i].apply(lambda x: x[:n_words_per_contrib]))
        # Loop through each group.
        for test_gname in group_names:
            # Calculate the KLD of each test snapshot against the current snapshot
            all_groups_KLD[snap_gname][test_gname] = snaps.calculate_kld(test_models[test_gname])

    return all_groups_KLD


def calculate_KLD_fluct_per_group(samples, test_contribs, group_names, all_groups_toks, n_words_per_contrib):
    """
    Calculates the KLD Fluctuation of each group.

    :param samples: contributions for training snapshot models.
    :param test_contribs: contributions for comparing to snapshots.
    :param test_toks: tokens of test contributions.
    :param group_names: all the names of groups.
    :param all_groups_toks: all tokens, an entry for each group, in the same order as group names.
    :param n_words_per_contrib: the number of words to use from each contribution.

    :returns: The KLD fluctuation of each group.
    """
    # Initialises the dictionary of cross entropies per group for this run.
    all_groups_KLD = {}

    # Loop through each group to train a snapshot model and compare the contributions of all groups to that snapshot.
    for i, gname in enumerate(group_names):
        # Get the samples from the current window to train the test model.
        test_model_samples = test_contribs[gname].iloc[1:]

        # Get the samples for the previous window to train the snapshots.
        snap_model_samples = samples[gname].iloc[:-1]
        snap_model_samples.index = test_model_samples.index

        # Create snapshot model using the snapshot samples for the current group and previous window.
        snap_model = SnapshotModelsPreset(snap_model_samples, all_groups_toks[i].apply(lambda x: x[:n_words_per_contrib]))

        # Create a test model using the test samples for the current group and window.
        test_model = SnapshotModelsPreset(test_model_samples, all_groups_toks[i].apply(lambda x: x[:n_words_per_contrib]))

        # Calculate the KLD
        all_groups_KLD[gname] = snap_model.calculate_kld(test_model)

    return all_groups_KLD


def get_contribs_per_window(contributions, subset, window_func, window_size, window_step):
    """
    Gets the contributions per window for the given contributions.
    """
    windows = dict()
    for window, window_contribs in window_func(contributions, window_size, window_step):
        window = datetime.strptime(window, "%Y/%m/%d")
        win_subset = subset[subset.index.isin(window_contribs.index)]
        windows[window] = win_subset

    return pd.Series(windows)


def split_MPs(mp_list, percentage=0.5, random_state=None):
    """
    Randomly splits MPs into Snapshot and Test samples based on the percentage.
    :param mp_list: Numpy array of MPs.
    :percentage: Percentage of MPs to make Snapshot samples (e.g. 0.6 -> 60% snapshot, 40% test.)
    """
    if random_state is not None:
        random_state.shuffle(mp_list)
    else:
        np.random.shuffle(mp_list)

    snap_MPs = mp_list[:int((len(mp_list)+1)*percentage)]
    test_MPs = mp_list[int((len(mp_list)+1)*percentage):]

    return snap_MPs, test_MPs


def get_snap_and_test_no_limit(all_contribs, contributions, reference_contribs, win_func, win_size, win_step, percentage_split=0.6, random_state=None):
    """
    Splits the given contributions into snapshot and testing samples.
    """
    # Get MPs for each group
    mps = contributions.PimsId.unique()

    # Split MPs into snapshot and train
    mps_snap, mps_test = split_MPs(mps, percentage_split, random_state=random_state)

    # Get the snap and test samples
    snap = contributions[contributions["PimsId"].isin(mps_snap)]
    test = contributions[contributions["PimsId"].isin(mps_test)]

    if reference_contribs is not None:
        # Get the snap and test samples for the reference
        ref_snap = reference_contribs[reference_contribs["PimsId"].isin(mps_snap)]
        ref_test = reference_contribs[reference_contribs["PimsId"].isin(mps_test)]
    else:
        ref_snap, ref_test = None, None

    return snap, test, ref_snap, ref_test


def get_snap_and_test_no_limit_bal(all_contribs, gnames, group_contribs, reference_contribs, win_func, win_size, win_step, percentage_split=0.6, random_state=None):
    """
    Splits the given contributions into snapshot and testing samples for each group, ensuring that all groups have the same number of users.
    """
    mps_snap = dict()
    mps_test = dict()

    # Loop through each group to get the MPs
    for i, g in enumerate(gnames):
        # Get MPs for each group
        mps = group_contribs[i].PimsId.unique()

        # Split MPs into snapshot and train
        mps_snap[g], mps_test[g] = split_MPs(mps, percentage_split, random_state=random_state)

    # Get the minimum number of MPs for snap and test
    min_snap = min([len(mps_snap[g]) for g in mps_snap])
    min_test = min([len(mps_test[g]) for g in mps_test])

    # Balance the number of MPs
    mps_snap = {g: mps_snap[g][:min_snap] for g in gnames}
    mps_test = {g: mps_test[g][:min_test] for g in gnames}

    # Initialise the samples
    snap, test, ref_snap, ref_test = dict(), dict(), dict(), dict()

    # Loop again through each group to create the samples
    for i, g in enumerate(gnames):
        # Get the snap and test samples
        snap[g] = group_contribs[i][group_contribs[i]["PimsId"].isin(mps_snap[g])]
        test[g] = group_contribs[i][group_contribs[i]["PimsId"].isin(mps_test[g])]

        if reference_contribs is not None:
            # Get the snap and test samples for the reference
            ref_snap[g] = reference_contribs[reference_contribs["PimsId"].isin(mps_snap[g])]
            ref_test[g] = reference_contribs[reference_contribs["PimsId"].isin(mps_test[g])]
        else:
            ref_snap, ref_test = None, None

    return snap, test, ref_snap, ref_test


def multi_mp_splits(gnames, group_contributions, toks, reference, ref_toks, window_func, window_size, window_step,
                        n_runs=5, comp_method="CE", n_words_per_contribution=60, balanced_groups=False, random_state=None):
    """
    Over n runs, splits MPs into Test and Snapshot samples and calculates the cross-entropy of the test samples according to the snapshot samples for each group.
    """
    comparison_methods = {
                            "CE": calculate_CE_per_group,
                            "CE_Fluct": calculate_CE_fluct_per_group,
                            "KLD": calculate_KLD_per_group,
                            "KLD_Fluct": calculate_KLD_fluct_per_group
                        }

    comp_method = comparison_methods[comp_method]

    # Create a matrix of all contributions from different groups
    combined = pd.concat(group_contributions, axis=0)

    # Combine the combined groups with the reference if the reference is not None
    if reference is not None:
        # combined = pd.concat([combined, reference], axis=0)
        combined = combined.combine_first(reference)

        # Add the reference to the tokens and group names
        gnames_w_ref = gnames + ["Reference"]
        toks.append(ref_toks)
    else:
        gnames_w_ref = gnames

    all_runs = []
    meta = []
    for run_num in range(n_runs):
        # Get the samples for each group.
        snap_samples, test_samples, ref_snap, ref_test = dict(), dict(), dict(), dict()

        if balanced_groups:
            snap_samples, test_samples, \
            ref_snap, ref_test = get_snap_and_test_no_limit_bal(combined, gnames,
                                                                group_contributions,
                                                                reference,
                                                                window_func,
                                                                window_size, window_step, random_state=random_state)
            # Split the contributions into windows
            snap_samples = {gname: get_contribs_per_window(combined, snap_samples[gname], window_func, window_size, window_step) for gname in gnames}
            test_samples = {gname: get_contribs_per_window(combined, test_samples[gname], window_func, window_size, window_step) for gname in gnames}

        else:
            # Loop through each group to split into snapshot and testing samples
            for i, gname in enumerate(gnames):
                snap_samples[gname], test_samples[gname], \
                ref_snap[gname], ref_test[gname] = get_snap_and_test_no_limit(combined,
                                                                            group_contributions[i],
                                                                            reference,
                                                                            window_func,
                                                                            window_size, window_step, random_state=random_state)
                # Split the contributions into windows
                snap_samples[gname] = get_contribs_per_window(combined, snap_samples[gname], window_func, window_size, window_step)
                test_samples[gname] = get_contribs_per_window(combined, test_samples[gname], window_func, window_size, window_step)

        if reference is not None:
            # Sort out the reference samples
            ref_snap = pd.concat(list(ref_snap.values()))
            ref_snap = get_contribs_per_window(combined, ref_snap, window_func, window_size, window_step)

            ref_test = pd.concat(list(ref_test.values()))
            ref_test = get_contribs_per_window(combined, ref_test, window_func, window_size, window_step)

            snap_samples["Reference"] = ref_snap
            test_samples["Reference"] = ref_test

        # Calculate the comparison values for all groups
        all_groups_comparison = comp_method(snap_samples, test_samples, gnames_w_ref, toks, n_words_per_contribution)

        all_runs.append(all_groups_comparison)

        # Do the meta stuff
        # Curr num posts
        curr_num_snap_posts = {g: {w: len(snap_samples[g][w]) for w in snap_samples[g].index} for g in gnames_w_ref}
        curr_num_test_posts = {g: {w: len(test_samples[g][w]) for w in test_samples[g].index} for g in gnames_w_ref}

        # Num users
        curr_num_snap_users = {g: {w: len(snap_samples[g][w].PimsId.unique()) for w in snap_samples[g].index} for g in gnames_w_ref}
        curr_num_test_users = {g: {w: len(test_samples[g][w].PimsId.unique()) for w in test_samples[g].index} for g in gnames_w_ref}

        # Put in meta
        meta.append({"SnapPosts": curr_num_snap_posts, "TestPosts":curr_num_test_posts,
                     "SnapUsers": curr_num_snap_users, "TestUsers":curr_num_test_users})

    return all_runs, meta


def get_ce_mean_and_std(cross_entropies, group_snap, group_test):
    """
    Gets the mean and std of the average cross-entropy per run for each window.
    """

    # Get the mean of each window across all runs.
    means = get_CE_means_fixed_windows(cross_entropies, group_snap, group_test).mean(axis=1)

    # Get the stds of the CE across all runs.
    stds = get_CE_means_fixed_windows(cross_entropies, group_snap, group_test).std(axis=1)

    return means, stds


def get_CE_means_fixed_windows(all_runs, group1, group2):
    """
    Gets the means for each window and each run.
    Creates a matrix with a column per run and a row per window.
    """
    means = []

    for i, curr_run in enumerate(all_runs):
        run_means = {window: curr_run[group1][group2][window].mean() for window in curr_run[group1][group2]}
        means.append(run_means)

    means = pd.DataFrame(means).T
    return means


def sample_n_contribs_per_MP(all_contribs, group_contributions, n_contribs_per_mp, window_func, window_size, window_step, random_state=None):
    """
    Given a set of contributions, sample n contributions per MP.
    """
    sampled_contributions = dict()

    # Loop through the time windows.
    for window, window_contribs in window_func(all_contribs, window_size, window_step):
        # Convert the window to datetime
        window = datetime.strptime(window, "%Y/%m/%d")

        sampled_contributions[window] = []

        curr_contribs = group_contributions[group_contributions.index.isin(window_contribs.index)]

        for MP, ids in curr_contribs.groupby("PimsId").groups.items():
            MP_contribs = curr_contribs.loc[ids]
            if len(ids) >= n_contribs_per_mp:
                sampled_contributions[window].append(MP_contribs.sample(n_contribs_per_mp, random_state=random_state))
            else:
                sampled_contributions[window].append(MP_contribs)

#         import pdb; pdb.set_trace()
        sampled_contributions[window] = pd.concat(sampled_contributions[window], axis=0).sort_values("date", ascending=True)

    sampled_contributions = pd.Series(sampled_contributions)
    return sampled_contributions


def get_samples_by_these_MPs(all_contribs, contribs, mps_snap, mps_test, contrib_limit, win_func, win_size, win_step, random_state=None):
    """
    Gets the samples for the given snapshot and test MPs.
    """

    # Get the Labour and Conservative split samples
    snap = contribs[contribs["PimsId"].isin(mps_snap)]
    test = contribs[contribs["PimsId"].isin(mps_test)]

    # Get sample for labour and conservative
    snap_sampled = sample_n_contribs_per_MP(all_contribs, snap, contrib_limit, win_func, win_size, win_step, random_state=random_state)
    test_sampled = sample_n_contribs_per_MP(all_contribs, test, contrib_limit, win_func, win_size, win_step, random_state=random_state)

    return snap_sampled, test_sampled


def get_snap_and_test_with_limit(all_contribs, group_contribs, reference_contribs, contrib_limit, win_func, win_size, win_step, percentage_split=0.6, random_state=None):
    """
    Splits the given contributions into snapshot and test samples, with a limit of contributions per mp.
    """
    # Get MPs for each group
    mps = group_contribs.PimsId.unique()

    # Split MPs into snapshot and train
    mps_snap, mps_test = split_MPs(mps, percentage_split, random_state=random_state)

    # Get the snap and test samples
    snap, test = get_samples_by_these_MPs(all_contribs, group_contribs, mps_snap, mps_test, contrib_limit, win_func, win_size, win_step, random_state=random_state)

    if reference_contribs is not None:
        # Get the snap and test samples for the reference
        ref_snap, ref_test = get_samples_by_these_MPs(all_contribs, reference_contribs, mps_snap, mps_test, contrib_limit, win_func, win_size, win_step, random_state=random_state)
    else:
        ref_snap, ref_test = None, None

    return snap, test, ref_snap, ref_test


# Merges a list of samples.
def merge_samples(samples):
    """
    Merges a list of samples. For example, could make multiple groups into a single sample.
    """
    merged = {w: pd.concat([samp[w] for samp in samples]) for w in samples[0].index}
    return pd.Series(merged)


def get_snap_and_test_with_limit_bal(all_contribs, gnames, group_contribs, reference_contribs, contrib_limit, win_func, win_size, win_step, percentage_split=0.6, random_state=None):
    """
    Splits the given contributions into snapshot and test samples, with a limit of contributions per mp.

    Also ensures that all groups have the same number of MPs in Snap and Test samples.
    """
    mps_snap = dict()
    mps_test = dict()

    # Loop through each group to get the MPs
    for i, g in enumerate(gnames):
        # Get MPs for each group
        mps = group_contribs[i].PimsId.unique()

        # Split MPs into snapshot and train
        mps_snap[g], mps_test[g] = split_MPs(mps, percentage_split, random_state=random_state)

    # Get the minimum number of MPs for snap and test
    min_snap = min([len(mps_snap[g]) for g in mps_snap])
    min_test = min([len(mps_test[g]) for g in mps_test])

    # Balance the number of MPs
    mps_snap = {g: mps_snap[g][:min_snap] for g in gnames}
    mps_test = {g: mps_test[g][:min_test] for g in gnames}

    # Initialise the samples
    snap, test, ref_snap, ref_test = dict(), dict(), dict(), dict()

    # Loop again through each group to create the samples
    for i, g in enumerate(gnames):
        # Get the snap and test samples
        snap[g], test[g] = get_samples_by_these_MPs(all_contribs, group_contribs[i], mps_snap[g], mps_test[g], contrib_limit, win_func, win_size, win_step, random_state=random_state)

        if reference_contribs is not None:
            # Get the snap and test samples for the reference
            ref_snap[g], ref_test[g] = get_samples_by_these_MPs(all_contribs, reference_contribs, mps_snap[g], mps_test[g], contrib_limit, win_func, win_size, win_step, random_state=random_state)
        else:
            ref_snap, ref_test = None, None

    return snap, test, ref_snap, ref_test


def multi_mp_splits_with_limit(gnames, group_contributions, toks, reference, ref_toks,
                               window_func, window_size, window_step,
                               n_contribs_per_mp=5, n_runs=5,
                               comp_method="CE", n_words_per_contribution=60, balanced_groups=False, random_state=None):
    """
    Over n runs, splits MPs into Test and Snapshot samples and calculates the cross-entropy of the test samples according to the snapshot samples for each group.

    Imposes a limit of a specified number of contributions per MP for each sample.
    """
    comparison_methods = {
                            "CE": calculate_CE_per_group,
                            "CE_Fluct": calculate_CE_fluct_per_group,
                            "KLD": calculate_KLD_per_group,
                            "KLD_Fluct": calculate_KLD_fluct_per_group
                         }

    comp_method = comparison_methods[comp_method]

    # Create a matrix of all contributions from different groups
    combined = pd.concat(group_contributions, axis=0)

    # Combine the combined groups with the reference if the reference is not None
    if reference is not None:
        # combined = pd.concat([combined, reference], axis=0)
        combined = combined.combine_first(reference)

        # Add the reference to the tokens and group names
        gnames_w_ref = gnames + ["Reference"]
        toks.append(ref_toks)
    else:
        gnames_w_ref = gnames

    all_runs = []
    meta = []
    for run_num in range(n_runs):
        # Initialise the samples
        snap_samples, test_samples, ref_snap, ref_test = dict(), dict(), dict(), dict()

        # Get the samples for each group, either with or without balanced groups
        if balanced_groups:
            # Gets snapshot and test samples with same number of MPs for each group
            snap_samples, test_samples, \
            ref_snap, ref_test = get_snap_and_test_with_limit_bal(combined,
                                                                  gnames,
                                                                  group_contributions,
                                                                  reference,
                                                                  n_contribs_per_mp,
                                                                  window_func,
                                                                  window_size, window_step, random_state=random_state)
        else:
            # Loop through each group to split into snapshot and testing samples
            for i, gname in enumerate(gnames):
                snap_samples[gname], test_samples[gname], \
                ref_snap[gname], ref_test[gname] = get_snap_and_test_with_limit(combined,
                                                                              group_contributions[i],
                                                                              reference,
                                                                              n_contribs_per_mp,
                                                                              window_func,
                                                                              window_size, window_step, random_state=random_state)

        if reference is not None:
            # Create the reference snapshot and test samples
            snap_samples["Reference"] = merge_samples(list(ref_snap.values()))
            test_samples["Reference"] = merge_samples(list(ref_test.values()))

        # Calculate the comparison values for all groups
        all_groups_comparison = comp_method(snap_samples, test_samples, gnames_w_ref, toks, n_words_per_contribution)

        all_runs.append(all_groups_comparison)

        # Do the meta stuff
        # Curr num posts
        curr_num_snap_posts = {g: {w: len(snap_samples[g][w]) for w in snap_samples[g].index} for g in gnames_w_ref}
        curr_num_test_posts = {g: {w: len(test_samples[g][w]) for w in test_samples[g].index} for g in gnames_w_ref}

        # Num users
        curr_num_snap_users = {g: {w: len(snap_samples[g][w].PimsId.unique()) for w in snap_samples[g].index} for g in gnames_w_ref}
        curr_num_test_users = {g: {w: len(test_samples[g][w].PimsId.unique()) for w in test_samples[g].index} for g in gnames_w_ref}

        # Put in meta
        meta.append({"SnapPosts": curr_num_snap_posts, "TestPosts":curr_num_test_posts,
                     "SnapUsers": curr_num_snap_users, "TestUsers":curr_num_test_users})

    return all_runs, meta


def plot_group_similarity_across_runs_simple(means, stds, ax=None, colour="black", label="", point_alpha=0.75, fill_alpha=0.4, key_dates=None):
    """
    Plot means and standard deviations over time with a simple line plot.
    """
    ax.plot(means, color=colour, label=label)
    ax.plot(means, color=colour, marker=".", linewidth=0, alpha=point_alpha, zorder=2)
    ax.fill_between(means.index,
                     means.values + stds.values,
                     means.values - stds.values,
                     color=colour, alpha=fill_alpha)#, step="post")

    if label is not None:
        ax.legend()
    # plt.set_xticks(rotation=90)
    ax.tick_params(axis='x', rotation=90)

    if key_dates is not None:
        add_key_dates(ax, key_dates)


def plot_group_similarity_across_runs_stepped(means, stds, ax=None, colour="black", label="", line_style="-", line_alpha=1, point_alpha=0.75, fill_alpha=0.4, key_dates=None, end_dates=None):
    """
    Plot means and standard deviations over time with a stepped line plot.

    Only really works for non-overlapping windows.
    """
    # Make copies so we don't change the original by mistake
    n_means = means.copy()
    n_stds = stds.copy()

    if end_dates is not None:
        n_means[end_dates[-1]] = n_means.iloc[-1]
        n_stds[end_dates[-1]] = n_stds.iloc[-1]

    ax.plot(n_means, drawstyle='steps-post', linestyle=line_style, color=colour, label=label, alpha=line_alpha)
#     ax.plot(means, linestyle='--', color=colour, label="{0} to {1}".format(group_test, group_snap), alpha=0.5)
    ax.plot(n_means, color=colour, marker=".", linewidth=0, alpha=point_alpha, zorder=2)
    ax.fill_between(n_means.index,
                     n_means.values + n_stds.values,
                     n_means.values - n_stds.values,
                     color=colour, alpha=fill_alpha, step="post")

    if label != "":
        ax.legend()

    ax.tick_params(axis='x', rotation=90)

    if key_dates is not None:
        add_key_dates(ax, key_dates)


def get_beginning_and_ends_of_windows(contribs, win_func, win_size, win_step):
    """
    Given contributions and window parameters, get the beginning and end of each window.
    """
    for window, curr_contribs in win_func(contribs, win_size, win_step):
        yield (curr_contribs.iloc[0].date, curr_contribs.iloc[-1].date)


def make_ribbon_plot(contribs, means, ax, win_func, win_size, win_step, stds=None, colour=None, label=None, main_line_alpha=1, other_line_alpha=0.2, fill_alpha=0.2, key_dates=None):
    """
    Makes a "ribbon" plot of cross-entropy/KLD means over time.

    This plot makes windows clear unlike a standard line plot.

    The trade-off is that it is difficult to convey standard deviation.
    """
    beg_and_end = list(get_beginning_and_ends_of_windows(contribs, win_func, win_size, win_step))
    end_timestamps = [curr[1] for curr in beg_and_end]

    end_means = means.copy()
    end_means.index = end_timestamps

    # Plots the line for the beginning and end of each window
    ax.plot(means, color=colour, alpha=main_line_alpha, zorder=4)
    ax.plot(end_means, color=colour, alpha=other_line_alpha, zorder=3)

    # Plots the STD if you want
    if stds is not None:
        # Horizontal line for STD.
        ax.hlines(means.values + stds.values, means.index, end_means.index, color=colour, alpha=0.3, zorder=3, linestyle="--")
        ax.hlines(means.values - stds.values, means.index, end_means.index, color=colour, alpha=0.3, zorder=3, linestyle="--")

    # Plots the horizontal lines between beginnings and ends.
    ax.hlines(means.values, means.index, end_means.index, color=colour, alpha=other_line_alpha, zorder=3)

    if key_dates is not None:
        add_key_dates(ax, key_dates)

    # Keeps track of the previous beginning and end for the loop to come.
    prev_beg = (means.index[0], means.values[0])
    prev_end = (end_means.index[0], end_means.values[0])

    # Loop through each beginning and end and draw a polygon between the four points of previous beginning, previous end, current end, and current beginning.
    for curr_beg, curr_end in zip(means.iloc[1:].items(), end_means.iloc[1:].items()):
        ax.fill([prev_beg[0], prev_end[0], curr_end[0], curr_beg[0]],
                [prev_beg[1], prev_end[1], curr_end[1], curr_beg[1]],
                color=colour, alpha=fill_alpha, zorder=5)

        prev_beg = curr_beg
        prev_end = curr_end


def get_meta_value_over_time(value_name, meta):
    """
    Gets the value of a meta value over time.
    """
    meta_values = dict()
    for group in meta[0][value_name]:
        meta_values[group] = pd.concat([pd.Series(meta[i][value_name][group]) for i in range(len(meta))], axis=1)

    return meta_values


def plot_meta_value_avg_and_std(group_dfs, ax, colours):
    """
    Plots the mean and standard deviation of a meta value over time.
    """
    for gname in group_dfs:
        curr_df = group_dfs[gname]
        ax.plot(curr_df.mean(axis=1), color=colours[gname])
        ax.fill_between(curr_df.index,
                        curr_df.mean(axis=1).values + curr_df.std(axis=1).values,
                        curr_df.mean(axis=1).values - curr_df.std(axis=1).values,
                        color=colours[gname], alpha=0.2)


def get_end_of_windows(contributions, window_func, window_size, window_step):
    end_dates = []
    for window, contribs in window_func(contributions, window_size, window_step):
        end_dates.append(contribs.sort_values("date", ascending=True).iloc[-1]["date"])

    return end_dates
