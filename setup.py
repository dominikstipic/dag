from setuptools import find_packages, setup

setup(
    name="dag",
    packages=find_packages(exclude=["dag_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
