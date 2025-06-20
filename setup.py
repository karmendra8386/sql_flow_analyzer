from setuptools import setup, find_packages

setup(
    name="sql_flow_analyzer",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "click>=8.0.0",
        "rich>=10.0.0",
        "sqlparse>=0.4.0",
    ],
    entry_points={
        "console_scripts": [
            "sql-flow-analyzer=sql_analyzer.__main__:cli",
        ],
    },
) 