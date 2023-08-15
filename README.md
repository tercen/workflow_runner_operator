
# Workflow Runner

Repo responsible for cloning and running workflows from a CLI. 

Through the configuration file, the user can change operators and their versions, input files, as well as set tolerance ranges for output comparisons.

Finally, if the comparison is successful, the updated version of the workflow may replace the older reference workflow.

### Configuration file

Below is an example configuration JSON file with all available fields.

```JSON
{
    "workflowId":"57cffa7f7a5cfd889a797ae40005206d",
    "updateOnSuccess":"False",
    "outputArtifact":"run_all_report.json",
    "toleranceType":"absolute",
    "tolerance":0.02,
    "verbose":"True",
    "tableStepFiles":[{
        "stepId":"7f856005-341a-4543-a6f1-01d2c242aa45",
        "fileId":"57cffa7f7a5cfd889a797ae40005388d"
    }],
    "operators":[{
        "stepId":"b47e85ae-76c7-469d-80f5-f19a56fb7469",
        "operatorURL":"https://github.com/tercen/mean_operator",
        "version":"1.2.0"
    }]
}
```

```workflowId``` refers to the reference workflow. Must have been previously run.
```updateOnSuccess``` Should the reference workflow be replaced by the new one in case of successful run.
```toleranceType``` Relative, Absolute or Equality. How column values should be compared
```tolerance``` The tolerance value. Ignored in case of Equality type
```verbose``` Print debug messages
```tableStepFiles``` List of objects containing the ```stepId``` of a TableStep and a ```fileId``` of the associated file. If not specified, use the same file as the reference workflow.
```operators``` List of objects containing the ```stepId``` of a DataStep where the operator will be updated by the one defined in ```operatorURL``` and ```version```

    