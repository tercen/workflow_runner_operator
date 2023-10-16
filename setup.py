from setuptools import setup #, find_packages



setup(
    name='workflow_runner',
    version='0.0.1',
    #packages=find_packages(exclude=['workflow_files']),
    packages=["workflow_runner"],
    package_dir={'workflow_runner': '.' },
    install_requires=[
        'tercen_python_client @ git+https://github.com/tercen/tercen_python_client@0.7.16'
    ],
    entry_points={
    "console_scripts": [
      "template_test = entry:exec"
    ]
    }
)

