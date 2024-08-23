from setuptools import find_packages, setup

setup(
    name="raccoon_dashboard",
    packages=find_packages(exclude=["raccoon_dashboard_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
