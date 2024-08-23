#!/bin/bash

# Start a new tmux session named "multimodal-multimodal-logfilter"
tmux new-session -d -s multimodal-logfilter

# Define your Python scripts
scripts=(
    "pytest -s test_multimodal_aggr.py" \
    "pytest -s test_multimodal_log_filter.py" \
    "python3 ../edgeserve/log_propagate.py --log-path /tmp/edgeserve/logs --log-prefix multimodal-aggr --outgoing-op dst" \
    "pytest -s test_multimodal_text.py" \
    "pytest -s test_multimodal_image.py"
)

# Path to your virtual environment
venv_path="~/venv/bin/activate"

# Create a new tmux window for each script
for i in "${!scripts[@]}"; do
    if [ $i -eq 0 ]; then
        # Run the first script in the first window
        tmux send-keys -t multimodal-logfilter "source $venv_path && ${scripts[$i]}" C-m
    else
        # Split the window for each additional script and run it
        tmux split-window -h -t multimodal-logfilter
        tmux send-keys -t multimodal-logfilter "source $venv_path && ${scripts[$i]}" C-m
    fi
    # Select the next pane (you can customize this as per your layout)
    tmux select-layout -t multimodal-logfilter tiled
done

# Attach to the tmux session
tmux attach-session -t multimodal-logfilter
