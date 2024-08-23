#!/bin/bash

# Start a new tmux session named "audio-logfilter"
tmux new-session -d -s audio-logfilter

# Define your Python scripts
scripts=(
    "pytest -s test_audio_small.py" \
    "pytest -s test_audio_large.py" \
    "pytest -s test_audio_aggregate.py" \
    "pytest -s test_audio_log_filter.py" \
    "python3 ../edgeserve/log_propagate.py --log-path /tmp/edgeserve/logs --log-prefix small --outgoing-op aggregator" \
    "python3 ../edgeserve/log_propagate.py --log-path /tmp/edgeserve/logs --log-prefix large --outgoing-op aggregator" \
    "pytest -s test_audio_source.py"
)

# Path to your virtual environment
venv_path="~/venv/bin/activate"

# Create a new tmux window for each script
for i in "${!scripts[@]}"; do
    if [ $i -eq 0 ]; then
        # Run the first script in the first window
        tmux send-keys -t audio-logfilter "source $venv_path && ${scripts[$i]}" C-m
    else
        # Split the window for each additional script and run it
        tmux split-window -h -t audio-logfilter
        tmux send-keys -t audio-logfilter "source $venv_path && ${scripts[$i]}" C-m
    fi
    # Select the next pane (you can customize this as per your layout)
    tmux select-layout -t audio-logfilter tiled
done

# Attach to the tmux session
tmux attach-session -t audio-logfilter
