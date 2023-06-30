
# Template Python Operator

Cell-wise mean calculated implemented in Python.



## Helpful Commands

### Create a virtual environment

```bash
python3 -m venv -p PATH_TO_PYTHON venv
source venv/bin/activate
```

### Install Tercen Python Client

```bash
python3 -m pip install --force git+https://github.com/tercen/tercen_python_client@0.1.7
```

### Wheel

Though not strictly mandatory, many packages require it.

```bash
python3 -m pip install wheel
```

### Generating Requirements.txt 

```bash
python3 -m tercen.util.requirements . > requirements.txt
```


### VSCode Launch

To run and debug the code, `VSCode` requires a launch.json file, which will be automatically created.
If the generated file does not run properly within the virtual environment, use the one below:

```JSON
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Current File",
            "type": "python",
            "python": "PATH TO VENV PYTHON",
            "request": "launch",
            "program": "${file}",
            "console": "integratedTerminal",
            "justMyCode": true,
            "env": { "PYTHONPATH": "${workspaceRoot}"}
            
        }
    ]
}
```

    