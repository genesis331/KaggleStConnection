from setuptools import setup

setup(
    name='kaggle-st-connection',
    version='1.3.0',
    py_modules=['KaggleAPIConnection'],
    install_requires=['kaggle', 'streamlit', 'pandas'],
    author='Cheah Zixu',
    description='st.experimental_connection implementation for Kaggle Public API',
    long_description='st.experimental_connection implementation for Kaggle Public API',
    url='https://github.com/genesis331/KaggleStConnection',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
