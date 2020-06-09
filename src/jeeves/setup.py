from setuptools import setup, find_packages

setup(
    name="Jeeves",
    version="0.0.1",
    packages=find_packages(),
    description="Command-line tool for use in iCentris GCP machine-learning project",
    author="Noah Goodrich",
    include_package_data=True,
    install_requires=[
        'click',
        'pyconfighelper',
        'google-cloud-secret-manager',
        'future'
    ],
    entry_points={
        'console_scripts': ['jeeves=jeeves.cli:cli'],
    }
)
