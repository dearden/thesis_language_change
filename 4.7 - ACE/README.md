# cross-entropy-language-change
A repo containing code for plotting language change over time with cross-entropy.

## Setup Instructions
- Create a python virtualenv in the main directory and install the requirements.
- Might need to set up NLTK and Spacy.
- Unzip the Database and put it's location in the config file.
- Unzip the Results, for both all and EU contributions.
- Unzip the results from the hyperparameter experiments if you want to run the notebook.

## Specific Config Instructions
- Put the follow filepaths into the config file.
  - MAIN_DIR -> the directory containing everything.
  - DB_FP -> FP of the database.
- Do a search and replace to change "\*MAIN_DIR\*" to the FP you entered before.

This is a brief description of all the config attributes.
- **DB_FP**: Database
- **GROUPS_FP**: CSVs containing groups of users.
- **SPEAKER_FILE**: List of Speakers (of the house of commons) to remove.
- **DATES_FP**: CSV of key dates during referendum.
- **CORPUS_METADATA_DIR**: Directory containing meta information used in corpus creation.
- **RESULTS_DIR_ALL**: Directory containing jsons for results over all contributions.
- **RESULTS_DIR_EU**: Directory containing jsons for results over EU contributions.
- **GRAPHS_DIR**: Directory containing graphs.
- **KW_DIR**: Directory containing keyword lists.
- **EXPERIMENT_RESULTS**: Directory containing graphs from stability analysis.

## Running the Data Collection
See the readme in the Data Collection folder.

## Running the Analysis
See the readme in the Analysis folder for more info.

- Complete the setup as described above.
- Run the file "paper_ACE.py" to produce the ACE results from the paper.
  - Within this file, hyperparameters can be specified.
  - This code takes quite a while to run with the default parameters.
- Run the Jupyter Notebook: Analysis_Section.ipynb
  - This will produce all the graphs from the paper.
  - Make sure you've unzipped the results and filled the config file out with their locations.
- Run the Jupyter Notebook: Paper Stability_Analysis.ipynb
  - This produces loads of graphs that show the stability of the method.
  - You'll need the results in the folder "hyperparam_experiments", or run the run_CE_experiments.py file to get your own results.
  - There is a provided config json which feeds the scripts hyperparameter combinations to test.
