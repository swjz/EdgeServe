import pickle
import apache_beam as beam
from apache_beam.coders.coders import PickleCoder
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec

from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub

from whisper_streaming.whisper_online import FasterWhisperASR, OnlineASRProcessor


class MyStreamingOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input_topic', type=str, help='Input Pub/Sub topic')
        parser.add_value_provider_argument('--output_topic', type=str, help='Output Pub/Sub topic')


class ASRStatefulDoFn(beam.DoFn):
    state_spec = ReadModifyWriteStateSpec('asr_buffer', PickleCoder())

    def setup(self):
        print("Setting up ASR processor")
        from whisper_streaming.whisper_online import FasterWhisperASR, OnlineASRProcessor, HypothesisBuffer
        self.asr = OnlineASRProcessor(FasterWhisperASR('en', 'tiny'))
        self.asr.transcript_buffer = HypothesisBuffer(logfile=None)  # sys.stderr cannot be pickled

    def process(self, element, buffer_state=beam.DoFn.StateParam(state_spec)):
        import numpy as np
        import pickle
        dummy_key, new_audio_chunk_bytes = element  # Extract dummy_key and audio chunk from the tuple
        buffer_content = buffer_state.read()
        if buffer_content is not None:
            self.asr.audio_buffer, self.asr.buffer_time_offset, self.asr.transcript_buffer, self.asr.commited = buffer_content
        new_audio_chunk = np.frombuffer(new_audio_chunk_bytes, dtype=np.float32)
        self.asr.audio_buffer = np.append(self.asr.audio_buffer, new_audio_chunk)  # equiv to asr.insert_audio_chunk()
        partial_result = self.asr.process_iter()
        buffer_state.write((self.asr.audio_buffer, self.asr.buffer_time_offset, self.asr.transcript_buffer, self.asr.commited))
        yield pickle.dumps(partial_result)


def run():
    pipeline_options = PipelineOptions()

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'apache-beam-baseline'
    google_cloud_options.job_name = 'beam-audio-small'
    google_cloud_options.staging_location = 'gs://beam-audio-small/staging'
    google_cloud_options.temp_location = 'gs://beam-audio-small/temp'
    pipeline_options.view_as(StandardOptions).streaming = True

    custom_options = pipeline_options.view_as(MyStreamingOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        beam.coders.registry.register_coder(OnlineASRProcessor, PickleCoder)

        # Read messages from a Pub/Sub topic
        audio_chunks = p | 'ReadFromPubSub' >> ReadFromPubSub(topic=custom_options.input_topic.get())

        keyed_audio_chunks = audio_chunks | 'AddKey' >> beam.Map(lambda x: ('dummy_key', x))

        # Process the messages (e.g., transform to upper case)
        transformed = keyed_audio_chunks | 'TransformMessages' >> beam.ParDo(ASRStatefulDoFn())

        # Write the transformed messages to another Pub/Sub topic
        transformed | 'WriteToPubSub' >> WriteToPubSub(topic=custom_options.output_topic.get())


if __name__ == '__main__':
    run()
