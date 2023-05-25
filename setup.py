from setuptools import setup,find_packages,find_namespace_packages
from mb_sql.version import version

setup(
    name="mb_sql",
    version=version,
    description="SQL functions package",
    author=["Malav Bateriwala"],
    packages=find_packages() + find_namespace_packages(include=["mb.*"]),
    #packages=find_packages(),
    scripts=[],
    install_requires=[
        "numpy",
        "mb_pandas",
        "sqlalchemy",
        "pandas"],
    python_requires='>=3.8',)