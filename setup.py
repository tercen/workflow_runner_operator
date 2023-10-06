from setuptools import setup, find_packages

setup(
    name='template_runner',
    version='0.0.1',
    packages=find_packages(exclude=['workflow_files']),
    install_requires=[
        'tercen_python_client @ git+https://github.com/tercen/tercen_python_client@0.7.16'
    ],
)