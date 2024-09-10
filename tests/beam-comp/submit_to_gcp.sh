python3 beam_audio_small.py \
    --runner DirectRunner \
    --project apache-beam-baseline \
    --region us-east5 \
    --input_topic "projects/apache-beam-baseline/topics/audio-small-chunks" \
    --output_topic "projects/apache-beam-baseline/topics/text-small-chunks" \
    --streaming \
    --extra_package extra_pkg_for_beam/dist/whisper_streaming-0.1.tar.gz