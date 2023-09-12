import os 
import sys, getopt, glob, shutil, subprocess
import json
import polars as pl

from zipfile import ZipFile

import tempfile, string, random

sys.path.append('./')
sys.path.append('../../')

from util import msg, which
from workflow_setup_client import create_test_workflow, update_table_relations
from workflow_compare_client import diff_workflow
from workflow_stats import stats_workflow

from tercen.client.factory import TercenClient
from tercen.client import context as tercen

from tercen.model.base import RunWorkflowTask, InitState, DoneState, Workflow, TableSchema


def run_workflow(workflow, project, client):
    # RUN the CLONED workflow 
    runTask = RunWorkflowTask()
    runTask.state = InitState()
    runTask.workflowId = workflow.id
    runTask.workflowRev = workflow.rev
    runTask.owner = project.acl.owner
    runTask.projectId = project.id

    runTask = client.taskService.create(obj=runTask)
    client.taskService.runTask(taskId=runTask.id)
    runTask = client.taskService.waitDone(taskId=runTask.id)

def parse_args(argv):
    params = {}
    opts, args = getopt.getopt(argv,"",
                               ["templateInfo=", "templateWkfVersion=", 
                                "templateRepo=", "templateWkfPath=",
                                "serviceUri=", "projectId=",
                                "user=", "passw=", "authToken=", "dataset=", "datasetMap="])
    
    # python3 runner.py --templateRepo=tercen/workflow_runner --templateWkfPath=workflow_files/reference_workflow.zip --templateWkfVersion=a442105f74371285c49572148deb024436176ef8
    # wget -o /tmp/some_workflow.zip https://github.com/tercen/tercen_python_client/raw/0.7.11/setup.py

    # If this is passed, use this to get the workflow
    templateInfo = ''
    workflowVersion = ''
    serviceUri = 'http://127.0.0.1'
    servicePort = '5400'
    templateRepo = ''
    templateWkfPath = ''
    projectId = ''
    user = 'test'
    passw = 'test'
    authToken = ''
    confFilePath = ''
    dataset = 'Crabs Data.csv'
    datasetMap = {}
    
    for opt, arg in opts:
        if opt == '-h':
            print('runner.py ARGS')
            sys.exit()

        if opt == '--templateInfo':
            templateInfo = arg
        
        if opt == '--templateWkfVersion':
            workflowVersion = arg
        
        if opt == '--templateRepo':
            templateRepo = arg


        if opt == '--templateWkfPath':
            templateWkfPath = arg

        if opt == '--projectId':
            projectId = arg

        if opt == '--serviceUri':
            serviceUri = arg

        if opt == '--servicePort':
            servicePort = arg

        if opt == '--workflowId':
            workflowId = arg

        if opt == '--user':
            user = arg
        
        if opt == '--passw':
            passw = arg
        
        if opt == '--authToken':
            authToken = arg

        if opt == '--confFilePath':
            confFilePath = arg

    with open(templateInfo) as f:
        templateInfo = json.load(f)

    params["templateInfo"] = templateInfo
    serviceUri = '{}:{}'.format(serviceUri, servicePort)

    client = TercenClient(serviceUri)
    client.userService.connect(user, passw)

    params["client"] = client
    params["user"] = user
    params["projectId"] = projectId
    params["confFilePath"] = confFilePath
    
    # #FIXME 
    # # Methods in the client's base.py are missing the response parse
    # # Se library calls like the one below are not working.
    # # This must be changed in future version, both in the client and here
    # # print(client.documentService.getTercenDatasetLibrary(0,100) )
    # #
    # # Also, this methods returns a TableSchema without id
    # from tercen.http.HttpClientService import HttpClientService, URI, encodeTSON, decodeTSON, MultiPart, MultiPartMixTransformer, URI
    # uri = URI.create("api/v1/d" + "/" + "getTercenDatasetLibrary")
    # p = {}
    # p["offset"] = 0
    # p["limit"] = 100
    # response = client.httpClient.post(
    #     client.tercenURI.resolve(uri).toString(), None, encodeTSON(p))
    
    # schemas = [TableSchema.createFromJson(sch) for sch in decodeTSON(response)]
    # idx = which([sch.name == dataset for sch in schemas])
    
    # schema = schemas[idx]
    # #print(schema.id) # MISSING
    # # #END OF hard code for the client


    return params


if __name__ == '__main__':
    
    #python3 template_tester.py --templateInfo=workflow_files/template_map.json --projectId=2aa4e5e69e49703961f2af4c5e000dd1

    absPath = os.path.dirname(os.path.abspath(__file__))
    
    params = parse_args(sys.argv[1:])


    # Add to params
    verbose = True

    resultList = []

    msg( "Starting Workflow Runner.", verbose )
    msg( "Testing template {}.".format(params["templateInfo"]["templateName"]), verbose )
    for wkfParams in params["templateInfo"]["workflows"]:
        if wkfParams["version"] == 'latest':
            version = 'main'
            # version = subprocess.check_output(['git', '-c',  "versionsort.suffix=-", "ls-remote", "--sort=v:refname",
            #                             "https://github.com/tercen/tercen"])
            # version = version.splitlines()[-1].decode("utf-8")
            # version = version.split('\t')[0].strip()
            
        else:
            version = wkfParams["version"]
        
        #git -c 'versionsort.suffix=-' ls-remote --tags --sort='v:refname' https://github.com/tercen/tercen | tail --lines=1 | cut --delimiter='       ' --fields=1

        #-O ./data/some_workflow.zip
        #https://github.com/tercen/workflow_runner/blob/a442105f74371285c49572148deb024436176ef8/workflow_files/reference_workflow.zip
        gitCmd = 'https://github.com/{}/raw/{}/{}'.format(wkfParams["repo"], version,wkfParams["goldenStandard"])

        # tmpDir = "{}/{}".format(tempfile.gettempdir(), ''.join(random.choices(string.ascii_uppercase + string.digits, k=12)))
        tmpDir = "data"

        zipFilePath = "{}/{}".format(tmpDir, wkfParams["goldenStandard"].split("/")[-1])

        #os.mkdir(tmpDir)

        subprocess.call(['wget', '-O', zipFilePath, gitCmd])
        subprocess.run(["unzip", '-d', tmpDir, '-o', zipFilePath])

        zip  = ZipFile(zipFilePath)
        currentZipFolder = zip.namelist()[0]

        workflowInfo = {"verbose":True, "toleranceType":"relative","tolerance":0.001,"operators":[], 
                "tableStepFiles":[{"stepId":"", "filename":""}]}

        if hasattr(workflowInfo, "verbose"):
            verbose = bool(workflowInfo["verbose"])
        else:
            verbose = False

        
        client = params["client"]
        msg( "Testing workflow {}/{}".format(wkfParams["repo"], wkfParams["goldenStandard"]), verbose )

        project = client.projectService.get(params["projectId"])


        zip  = ZipFile(zipFilePath)
        currentZipFolder = zip.namelist()[0]
    

    

        with open( "{}/{}/workflow.json".format(tmpDir, currentZipFolder) ) as wf:
            wkfJson = json.load(wf)
            wkf = Workflow.createFromJson( wkfJson )
            params["workflow"] = wkf

        wkf.projectId = project.id
        wkf.acl = project.acl

        workflows = create_test_workflow(client, wkf, workflowInfo, verbose=verbose)
        workflow = workflows[0]
        refWorkflow = workflows[1]

        params["referenceSchemaPath"] = "{}/{}/data/".format(tmpDir, currentZipFolder)

        update_table_relations(client, refWorkflow, workflow, wkfParams, params["user"], verbose=verbose)

        msg("Running all steps", workflowInfo["verbose"])


        run_workflow(workflow, project, client)
        msg("Finished", workflowInfo["verbose"])

        # Retrieve the updated, ran workflow
        workflow = client.workflowService.get(workflow.id)

        resultDict = diff_workflow(client, workflow, refWorkflow, params["referenceSchemaPath"], workflowInfo["tolerance"],
                                workflowInfo["toleranceType"], workflowInfo["verbose"])

        resultList.append(resultDict)

        client.workflowService.delete(workflow.id, workflow.rev)

        


    
    print(resultList)

    # Remove tmp files and zip file
    fileList = glob.glob("{}/*".format(tmpDir), recursive=False)
    for f in fileList:
        if os.path.isdir(f):
            shutil.rmtree(f)
        else:
            os.unlink(f)
    


    #stats =  stats_workflow(ctx, workflow, refWorkflow, verbose=False)
    


    #print(stats)
    
    # if workflowInfo["updateOnSuccess"] == "True" and len(resultDict) == 0:
    #     print("Updating reference workflow")
    #     TODO Maybe
    #     ctx.context.client.workflowService.delete(workflow.id, workflow.rev)
    #     refWorkflow = update_operators(refWorkflow, operatorList, ctx)
    #     for stp in refWorkflow.steps[1:]:
    #         TODO Update based on step type
    #         stp.state.taskState = InitState()
        

    #     ctx.context.client.workflowService.update(refWorkflow)
    #     run_workflow(refWorkflow, project, ctx)
    