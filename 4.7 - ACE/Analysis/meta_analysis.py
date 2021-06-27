import os
import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns


def get_top_bottom_counts(df, percentile):
    quantile_value = df.quantile(percentile)
    
    top = df.loc[df > quantile_value]
    bottom = df.loc[df <= quantile_value]
    
    return quantile_value, top, bottom


def fix_ax(ax, title, xlabel, ylabel):
    ax.set_title(title, fontsize=14)
    ax.set_xlabel(xlabel, fontsize=14)
    ax.set_ylabel(ylabel, fontsize=14)
    plt.setp(ax.get_xticklabels(), fontsize=14)
    plt.setp(ax.get_yticklabels(), fontsize=14)
    ax.ticklabel_format(style='plain')


def individual_dist(data, ax, plot_name, stat_name):
    sns.distplot(data, hist=False, kde=True, ax=ax,
                 kde_kws={'linewidth': 2, 'shade': True})
    fix_ax(ax, plot_name, stat_name, "Density")


def individual_hist(data, ax, plot_name, stat_name):
    data.plot.hist(grid=True, bins=30, rwidth=0.9, ax=ax)
    fix_ax(ax, plot_name, stat_name, "Frequency")


def top_bottom_stats(df, percentile, stat_name=""):
    quantile_value, top, bottom = get_top_bottom_counts(df, percentile)
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))

    individual_dist(df[df <= quantile_value], 
                    axes[0,0], 
                    "Density Plot {0} for posts <= {1}%".format(stat_name, 100*percentile), 
                    stat_name)

    individual_dist(df[df > quantile_value], 
                    axes[0,1], 
                    "Density Plot {0} for posts > {1}%".format(stat_name, 100*percentile), 
                    stat_name)

    individual_hist(df[df <= quantile_value], 
                    axes[1,0], 
                    "Histogram {0} for posts <= {1}%".format(stat_name, 100*percentile), 
                    stat_name)

    individual_hist(df[df > quantile_value], 
                    axes[1,1], 
                    "Histogram {0} for posts > {1}%".format(stat_name, 100*percentile), 
                    stat_name)

    return fig
    
    
def get_users_in_percentile(df, all_contributions, percentile):
    quantile_value = df.quantile(percentile)
    
    top = df.loc[df > quantile_value]
    bottom = df.loc[df <= quantile_value]
    
    top_contribs = all_contributions.loc[top.index]
    bot_contribs = all_contributions.loc[bottom.index]
    
    return len(top_contribs["PimsId"].unique()), len(bot_contribs["PimsId"].unique())


def check_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        
def make_histogram(df, fp, xlabel=""):
    fig, ax = plt.subplots(figsize=(8,5))
    df.plot.hist(grid=True, bins=30, rwidth=0.9, ax=ax)
    
    fix_ax(ax, "", xlabel, "Frequency")
    plt.tight_layout()
    fig.savefig(fp)
    
def make_quant_dist(df, name, quantile, output, fp):
    # Get number of posts above and below percentile
    quantile_value, top, bottom = get_top_bottom_counts(df, quantile)
    output.append("{0} in top {1}% - {2}".format(top.sum(), 100-quantile*100, name))
    output.append("{0} in bottom {1}% - {2}".format(bottom.sum(), quantile*100, name))
    
    fig = top_bottom_stats(df, quantile, name)
    plt.tight_layout()
    fig.savefig(fp)

def get_meta(out_dir, contributions, tokens, quantiles):
    check_dir(out_dir)
    
    output_log = []
    output_log.append("Num Posts: {}".format(len(contributions)))
    output_log.append("Num Users: {}".format(len(contributions["PimsId"].unique())))

    post_lengths = tokens.apply(len)
    
    output_log.append("Median words / post: {}".format(post_lengths.median()))
    output_log.append("Mean words / post: {}".format(post_lengths.mean()))
    output_log.append("Num words: {}".format(post_lengths.sum()))

    contribs_per_user = contributions.groupby("PimsId").size()
    
    output_log.append("Num Users w/ >50 Posts: {}".format(len(contribs_per_user[contribs_per_user > 50])))
    output_log.append("Mean posts per user: {}".format(contribs_per_user.mean()))
    output_log.append("Median posts per user: {}".format(contribs_per_user.median()))

    words_per_user = post_lengths.groupby(contributions["PimsId"]).sum()
    
    output_log.append("Median words / user: {}".format(words_per_user.median()))
    output_log.append("Mean words / user: {}".format(words_per_user.mean()))
    output_log.append("Num words: {}".format(words_per_user.sum()))

    # Post Length distributions
    make_histogram(post_lengths, os.path.join(out_dir, "post_lengths_hist.pdf"), "Post Length")
    # Contribs per user distributions
    make_histogram(contribs_per_user, os.path.join(out_dir, "user_contribs_hist.pdf"), "Contributions per MP")
    # Words per user distribution
    make_histogram(words_per_user, os.path.join(out_dir, "user_words_hist.pdf"), "Words per MP")

    
    # Do the stuff that depends on quantiles
    for quantile in quantiles:
        # Get the number of users above and below quantile in terms of num posts. 
        # (Same user can be in both. Measure of spread, not distribution of user posts.)
        users_in_top, users_in_bottom = get_users_in_percentile(post_lengths, contributions, quantile)
        output_log.append("{0} users in top {1}% of contribs".format(users_in_top, 100-quantile*100))
        output_log.append("{0} users in bottom {1}% of contribs".format(users_in_bottom, quantile*100))
        
        # Post length distribution
        make_quant_dist(post_lengths, "Post Lengths", quantile, output_log,
                        os.path.join(out_dir, "post_lengths_density_{}_quantile.pdf".format(int(quantile*100))))
        
        # Contribs per user distribution
        make_quant_dist(contribs_per_user, "Contribs Per User", quantile, output_log,
                        os.path.join(out_dir, "user_contribs_density_{}_quantile.pdf".format(int(quantile*100))))
        
        # Words per user distribution
        make_quant_dist(words_per_user, "Words Per User", quantile, output_log,
                        os.path.join(out_dir, "user_words_density_{}_quantile.pdf".format(int(quantile*100))))
        
    # Write to file
    with open(os.path.join(out_dir, "meta.txt"), "w") as meta_file:
        meta_file.write("\n".join(output_log))
        
    # Close all the plots
    plt.close("all")