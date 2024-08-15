from edgeserve.compute import Compute
from edgeserve.data_source import AudioSource

import numpy as np
import pickle
import pytest


@pytest.fixture()
def raw_data():
    small_time_end = 0
    large_time_end = 0
    small_cur_text = ''
    large_cur_text = ''
    aggregated_text = ''

    def aggregate(small, large):
        # Should be straightforward for data-triggered joins
        # Input: (small_time_beg, small_time_end, small_cur_text), (large_time_beg, large_time_end, large_cur_text)
        from Levenshtein import distance as levenshtein_distance
        nonlocal small_time_end, large_time_end, small_cur_text, large_cur_text, aggregated_text
        latest_text = ''
        if small:
            small_time_end = small[1]
            small_cur_text += small[2]
            latest_text = small_cur_text
        if large:
            large_time_end = large[1]
            large_cur_text += large[2]
            latest_text = large_cur_text

        print('Small: ', small)
        print('Large: ', large)

        # def generate_suggestion(previous, current):
        #     """
        #     Generate the best suggestion for merging the current transcription into the previous one.
        #
        #     Args:
        #     previous (str): The previous transcription.
        #     current (str): The current transcription.
        #
        #     Returns:
        #     str: The merged transcription.
        #     """
        #     distance_list = distances(previous, current)
        #
        #     # Find the suffix with the minimum distance
        #     min_distance_suffix = min(distance_list, key=lambda x: x[1])
        #     cutting_index = min_distance_suffix[0]
        #
        #     current_tokens = current.split(' ')
        #     suggestion = ' '.join(current_tokens[cutting_index:])
        #
        #     return previous + ' ' + suggestion

        def find_best_match(previous, current):
            previous_tokens = previous.split()
            current_tokens = current.split()

            min_distance = float('inf')
            best_match_index = 0
            best_match_length = 0

            for i in range(len(previous_tokens)):
                for j in range(i, len(previous_tokens)):
                    dist = levenshtein_distance(' '.join(previous_tokens[i:j + 1]), ' '.join(current_tokens))
                    if dist < min_distance:
                        min_distance = dist
                        best_match_index = i
                        best_match_length = j - i + 1

            return best_match_index, best_match_length

        def merge_texts(previous, current):
            previous_tokens = previous.split()
            current_tokens = current.split()

            best_match_index, best_match_length = find_best_match(previous, current)

            merged_text = ' '.join(previous_tokens[:best_match_index] + current_tokens)

            return merged_text

        aggregated_text = merge_texts(aggregated_text, latest_text)

        return aggregated_text

    return {'node': 'pulsar://localhost:6650',
            'gate-aggr-in': lambda x: pickle.loads(x),
            'task-aggr': aggregate}


def test_pipeline(raw_data):
    with Compute(raw_data['task-aggr'], raw_data['node'], topic_in='audio-aggr', gate_in=raw_data['gate-aggr-in'],
                 gate_out=lambda x: x.encode('utf-8'), worker_id='aggregator', log_path='/tmp/edgeserve/logs',
                 log_filename='aggregator', is_log_verbose=True, max_time_diff_ms=10 ** 10,
                 single_input=True) as aggregator:
        while True:
            try:
                print('Aggregator Output:', next(aggregator))
            except StopIteration:
                break
