from setuptools import setup #, find_packages



setup(
    name='workflow_runner_operator',
    version='0.1.1',
    #packages=find_packages(exclude=['workflow_files']),
    packages=["workflow_funcs", "workflow_runner"],
    package_dir={'workflow_funcs': 'workflow_funcs', "workflow_runner":'.' },
    install_requires=[
        'tercen_python_client @ git+https://github.com/tercen/tercen_python_client@0.11.1'
    ],
    entry_points={
    "console_scripts": [
      "template_test = entry:exec"
    ]
    }
)

