# Analysis

The code used to perform our method.

We have included some jupyter notebooks that produce our results:
- Analysis_Section.ipynb -> Produces the graphs for the paper.
- Stability_Analysis.ipynb -> Produces graphs showing stability of method across hyperparameters.

These are the scripts used by these notebooks (and some that are used separately):
- analysis_functions.py -> Functions from the analysis section, e.g. finding key areas.
- group_functions.py -> Functions to split contributions into groups.
- helper_functions.py -> Utility functions for performing tasks, e.g. tokenisation.
- keywords_for_paper.py -> Script to get keywords featured in paper.
- meta_analysis.py -> Script to perform meta analysis.
- models.py -> Contains the Snapshot model classes.
- mp_sampling.py -> Methods for create snapshot and testing samples.
- paper_ACE.py -> Code to produce the results from the paper.
- run_CE_experiments.py -> Code to produce results for experimenting with hyperparameters.
- run_ce_for_r_questions.py -> Code to produce results for Analysis section - extended with a few extra analyses.
- sampling_over_time.py -> Methods for calculating cross-entropy over multiple runs.
