from setuptools import setup, find_packages

setup(
    name='whisper_streaming',
    version='0.1',
    description='whisper_streaming package for Apache Beam',
    packages=find_packages(),
    install_requires=[
        'soundfile',
        'numpy',
        'librosa',
        'faster_whisper',
    ],
)
