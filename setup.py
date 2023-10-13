from setuptools import setup, find_packages



setup(
    name='workflow_runner',
    version='0.0.1',
    packages=find_packages(exclude=['workflow_files']),
    install_requires=[
        'tercen_python_client @ git+https://github.com/tercen/tercen_python_client@0.7.16'
    ],
    entry_points={
    "console_scripts": [
      "template_test = workflow_runner.entry:exec"
    ]
    }
)

