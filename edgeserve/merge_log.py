import pandas as pd
import argparse


class MergeLog:
    """
    MergeLog class is used to merge a log file and a prune file.
    """
    def __init__(self, log_file):
        self.log_file = log_file
        self.prune_file = log_file + '.prune'

    def merge(self):
        log_df = pd.read_csv(self.log_file, dtype=str)

        if '.wal' in self.log_file:
            prune_df = pd.read_csv(self.prune_file, header=None, names=log_df.columns, dtype=str)
            merged_df = pd.merge(log_df, prune_df, indicator=True, how='outer')
            clean_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns='_merge')
        elif '.orl' in self.log_file:
            with open(self.prune_file, 'r') as f:
                prune_lines = f.read().splitlines()
            clean_df = log_df[~log_df['msg_in_uuid'].isin(prune_lines)]

        # Save the clean dataframe to a new CSV file
        clean_df.to_csv(self.log_file + '.merged', index=False, na_rep='None')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Merge Log')
    parser.add_argument('--log_file', type=str,
                        help='Path to log file. The prune file should be named as log_file.prune')
    args = parser.parse_args()

    merge_log = MergeLog(args.log_file)
    merge_log.merge()
