from setuptools import setup, find_packages


setup(
    name="hadoop_pip",
    version="1.0.0",
    description="Tool to schedule SPARK PIP jobs",
    packages=find_packages(),
    author="Charlie Hoffman",
    license="MIT",
    install_requires=[
        "awscli<=1.17.0,>=1.16.176",
    ],
    scripts=[
        "hadoop_pip/run_pip.py",
    ],
)
