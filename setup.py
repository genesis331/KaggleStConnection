from setuptools import setup
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='kaggle-st-connection',
    version='1.3.2',
    py_modules=['KaggleAPIConnection'],
    install_requires=['kaggle', 'streamlit', 'pandas'],
    author='Cheah Zixu',
    description='st.connection implementation for Kaggle Public API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/genesis331/KaggleStConnection',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
