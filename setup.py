from setuptools import setup, find_packages


setup(
    name="hadoop_pip",
    version="1.0.2",
    description="Tool to schedule SPARK PIP jobs",
    packages=find_packages(),
    author="Charlie Hoffman",
    license="MIT",
    install_requires=[
        "boto3~=1.9.225",
        "click~=7.0"
    ],
    scripts=[
        "hadoop_pip/run_pip.py",
    ],
)
