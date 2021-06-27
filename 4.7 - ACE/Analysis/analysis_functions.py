import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime
import numpy as np
import json
from scipy.stats import ttest_ind, ttest_rel, mannwhitneyu


def calc_significance_between_averaged_runs(curr_group, other_group):
    curr_window = curr_group.name
    other_group = other_group.loc[curr_window, :]

    stat, p = ttest_rel(curr_group.values, other_group.values)
    return p


def compare_comparisons(comparisons1, comparisons2):
    # return ttest_ind(comparisons1.values, comparisons2.values)[1]
    return mannwhitneyu(comparisons1.values, comparisons2.values)[1]


def plot_hlines(hlines_to_plot, ax, colour):
    ax.hlines([x[0] for x in hlines_to_plot],
              [x[1] for x in hlines_to_plot],
              [x[2] for x in hlines_to_plot],
              color=colour, zorder=5, linewidth=3)


def plot_vlines(vlines_to_plot, ax, colour):
    ax.vlines([x[0] for x in vlines_to_plot],
              [x[1] for x in vlines_to_plot],
              [x[2] for x in vlines_to_plot],
              color=colour, zorder=5, linewidth=3)


def plot_events_in_hlines(hlines_to_plot, key_dates, ax):
    for date, event in key_dates.iterrows():
        display_event = False
        # Want to check if the key date is in one of the key windows
        for beg, end in [(x[1], x[2]) for x in hlines_to_plot]:
            if date < end and date > beg:
                display_event = True

        if display_event:
            ax.axvline(x=date, color="gray", alpha=event.transparency, zorder=1)


def highlight_possible_key_windows(dist_from_mean, ends, ax, colour, key_dates):
    """
    Highlights windows where the distance from the mean is more than the standard deviation
    """
    # Add the end date to the distances so the graph includes the final window
    dist_from_mean[ends[-1]] = dist_from_mean.iloc[-1]

    # Plots
    ax.plot(dist_from_mean, color=colour, drawstyle='steps-post', alpha=0.3)
    ax.axhline(y=dist_from_mean.std(), color=colour, linestyle="--", alpha=0.6)
    ax.axhline(y=-dist_from_mean.std(), color=colour, linestyle="--", alpha=0.6)

    hlines_to_plot = []
    for i in range(len(dist_from_mean)-1):
        if abs(dist_from_mean.iloc[i]) > dist_from_mean.std():
            hlines_to_plot.append([dist_from_mean.iloc[i], dist_from_mean.index[i], ends[i]])

    plot_hlines(hlines_to_plot, ax, colour)

    if key_dates is not None:
        plot_events_in_hlines(hlines_to_plot, key_dates, ax)


def get_significant_windows(group_runs, comp_runs, ends, sig_level=0.05):
    means = group_runs.mean(axis=1)

    sig = group_runs.apply(calc_significance_between_averaged_runs, args=[comp_runs], axis=1)

    hlines_to_plot = []
    for i in range(len(sig)):
        if abs(sig.iloc[i]) < sig_level:
            hlines_to_plot.append([means.iloc[i], sig.index[i], ends[i]])

    return hlines_to_plot


def highlight_significant_windows(group_runs, group_name, comp_runs, comp_name, ends, ax, colour, key_dates=None, comp_col="Grey", sig_level=0.05):

    means = group_runs.mean(axis=1)
    comp_means = comp_runs.mean(axis=1)

    # Add the final end date to the means so graph shows whole window
    means[ends[-1]] = means.iloc[-1]
    comp_means[ends[-1]] = comp_means.iloc[-1]

    ax.plot(means, color=colour, drawstyle='steps-post', alpha=0.3)
    ax.plot(comp_means, color=comp_col, drawstyle='steps-post', alpha=1)

    hlines_to_plot = get_significant_windows(group_runs, comp_runs, ends, sig_level=sig_level)
    plot_hlines(hlines_to_plot, ax, colour)

    if key_dates is not None:
        plot_events_in_hlines(hlines_to_plot, key_dates, ax)


def get_significant_changes(group_runs, sig_level=0.05):
    runs1 = group_runs.iloc[1:]
    runs2 = group_runs.iloc[:-1].set_index(runs1.index)

    means = group_runs.mean(axis=1)
    means1 = means.iloc[1:]
    means2 = means.iloc[:-1]

    sig = runs1.apply(calc_significance_between_averaged_runs, args=[runs2], axis=1)

    vlines_to_plot = []
    for i in range(len(sig)):
        if abs(sig.iloc[i]) < sig_level:
            vlines_to_plot.append([runs1.index[i], means1.iloc[i], means2.iloc[i]])

    return vlines_to_plot


def highlight_significant_changes(group_runs, ends, ax, colour, line_alpha=0.3, sig_level=0.05):
    means = group_runs.mean(axis=1)
    ax.plot(means, color=colour, drawstyle='steps-post', linestyle='--', alpha=line_alpha)

    lines_to_plot = get_significant_changes(group_runs, sig_level=sig_level)
    plot_vlines(lines_to_plot, ax, colour)



def highlight_significant_windows_multi_ttest(group_comparisons, group_name, comp_comparisons, comp_name, ends, ax, colour, key_dates, sig_level=0.05):

    group_runs = group_comparisons.apply(np.vectorize(lambda x: x.mean()))
    comp_runs = comp_comparisons.apply(np.vectorize(lambda x: x.mean()))

    means = group_runs.mean(axis=1)
    comp_means = comp_runs.mean(axis=1)

    # Add the final end date to the means so graph shows whole window
    means[ends[-1]] = means.iloc[-1]
    comp_means[ends[-1]] = comp_means.iloc[-1]

    ax.plot(means, color=colour, drawstyle='steps-post', alpha=0.3)
    ax.plot(comp_means, color="Grey", drawstyle='steps-post', alpha=1)

    sig = pd.DataFrame(np.vectorize(compare_comparisons)(group_comparisons, comp_comparisons), index=group_comparisons.index)
    num_significant = sig.apply(np.vectorize(lambda x: x < sig_level)).apply(lambda x: len(x[x]), axis=1)
    sig_check = num_significant >= 0.8 * sig.shape[1]
#     sig_check = sig.apply(np.vectorize(lambda x: x < sig_level)).all(axis=1)

    hlines_to_plot = []
    for i in range(len(sig_check)):
        if sig_check.iloc[i]:
            hlines_to_plot.append([means.iloc[i], sig_check.index[i], ends[i]])

    plot_hlines(hlines_to_plot, ax, colour)

    if key_dates is not None:
        plot_events_in_hlines(hlines_to_plot, key_dates, ax)
