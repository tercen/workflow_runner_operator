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

from tercen.model.base import RunWorkflowTask, InitState,  Workflow, Project


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
                               ["templateInfo=", 
                                "templateVersion=", "templateRepo=", "templatePath=",
                                "gsVersion=", "gsRepo=", "gsPath=",
                                "serviceUri=", "projectId=",
                                "user=", "passw=", "authToken=", "dataset=", "datasetMap=", "verbose",
                                "tolerance=", "toleranceType=",
                                "filename=", "filemap="])
    
# workflowInfo = {"verbose":True, "toleranceType":"relative","tolerance":0.001,"operators":[], 
                # "tableStepFiles":[{"stepId":"", "filename":""}]}

#python3 template_tester.py  --templateRepo=tercen/workflow_lib_repo --templateVersion=latest --templatePath=template_mean_crabs_2.zip  --gsRepo=tercen/workflow_lib_repo --gsVersion=latest --gsPath=golden_standard_mean_crabs_2.zip 

    serviceUri = 'http://127.0.0.1'
    servicePort = '5400'
    templateRepo = 'tercen/workflow_lib_repo'
    templateVersion = 'main'
    templatePath = 'template_mean_crabs_2.zip'
    

    gsRepo = 'tercen/workflow_lib_repo'
    gsVersion = 'main'
    gsPath = 'golden_standard_mean_crabs_2.zip'
    

    projectId = ''
    user = 'test'
    passw = 'test'
    authToken = ''
    confFilePath = ''
    verbose = False
    
    tolerance = 0.001
    toleranceType="relative"

    # TODO Add the file mapping parse for multiple table steps situation
    filename=None
    filemap=None 

    
    for opt, arg in opts:
        if opt == '-h':
            print('runner.py ARGS')
            sys.exit()

        if opt == '--templateInfo':
            templateInfo = arg
        
        if opt == '--templateVersion':
            templateVersion = arg
            if templateVersion == "latest":
                templateVersion = "main"
        
        if opt == '--templateRepo':
            templateRepo = arg


        if opt == '--templatePath':
            templatePath = arg

        if opt == '--gsVersion':
            gsVersion = arg
            if gsVersion == "latest":
                gsVersion = "main"
        
        if opt == '--gsRepo':
            gsRepo = arg


        if opt == '--gsPath':
            gsPath = arg

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

        if opt == '--tolerance':
            tolerance = float(arg)

        if opt == '--toleranceType':
            toleranceType = arg

        if opt == '--verbose':
            verbose = True

        if opt == '--filename':
            filename = arg
        if opt == '--filemap':
            filemap = arg
            
            

    serviceUri = '{}:{}'.format(serviceUri, servicePort)

    client = TercenClient(serviceUri)
    client.userService.connect(user, passw)

    params["client"] = client
    params["user"] = user
    params["projectId"] = projectId
    params["confFilePath"] = confFilePath
    params["verbose"] = verbose
    params["tolerance"] = tolerance
    params["toleranceType"] = toleranceType

    # python3 template_tester.py  --templateRepo=tercen/workflow_lib_repo --templateVersion=latest --templatePath=template_mean_crabs_2.zip --gsRepo=tercen/workflow_lib_repo --gsVersion=latest --gsPath=golden_standard_mean_crabs_2.zip --projectId=2aa4e5e69e49703961f2af4c5e000dd1

    
    params["templateVersion"] = templateVersion
    params["templateRepo"] = templateRepo
    params["templatePath"] = templatePath

    params["gsVersion"] = gsVersion
    params["gsRepo"] = gsRepo
    params["gsPath"] = gsPath
    
    params["filename"] = filename
    
    if filemap != None and os.path.exists(filemap):
        with open(filemap) as f:
            params["filemap"] = json.load(f)
        
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
    #python3 template_tester.py  --templateRepo=tercen/workflow_lib_repo --templateVersion=latest --templatePath=template_mean_crabs_2.zip --gsRepo=tercen/workflow_lib_repo --gsVersion=latest --gsPath=golden_standard_mean_crabs_2.zip --projectId=2aa4e5e69e49703961f2af4c5e000dd1 --filename="Crabs DataB.csv"

    absPath = os.path.dirname(os.path.abspath(__file__))
    
    params = parse_args(sys.argv[1:])
    client = params["client"]
    
    # Download template workflow
    gitCmd = 'https://github.com/{}/raw/{}/{}'.format(params["templateRepo"], params["templateVersion"],params["templatePath"])
    # tmpDir = "{}/{}".format(tempfile.gettempdir(), ''.join(random.choices(string.ascii_uppercase + string.digits, k=12)))
    tmpDir = "data"

    zipFilePath = "{}/{}".format(tmpDir, params["templatePath"].split("/")[-1])

    #os.mkdir(tmpDir)

    subprocess.call(['wget', '-O', zipFilePath, gitCmd])
    subprocess.run(["unzip", '-qq', '-d', tmpDir, '-o', zipFilePath])

    zip  = ZipFile(zipFilePath)
    currentZipFolder = zip.namelist()[0]


    # Create temp project to run tests

    project = Project()
    project.name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
    project.name = 'template_test_' + project.name
    project.acl.owner = params['user']
    project = client.projectService.create(project)
    params["projectId"] = project.id

    project = client.projectService.get(params["projectId"])


    with open( "{}/{}/workflow.json".format(tmpDir, currentZipFolder) ) as wf:
        wkfJson = json.load(wf)
        wkf = Workflow.createFromJson( wkfJson )
        wkf.projectId = project.id
        wkf.acl = project.acl
        #params["templateWorkflow"] = wkf


    # FIXME Take DEFAULT values as parameters
    verbose = params["verbose"]

    resultList = []

    msg( "Starting Workflow Runner.", verbose )
    msg( "Testing template {}/{}.".format(params["templateRepo"], params["templatePath"]), verbose )

    workflows = create_test_workflow(client, wkf, params, verbose=verbose)
    workflow = workflows[0]
    refWorkflow = workflows[1]


    
    if "filemap" in params and params["filemap"] != None:
        filemap = params["filemap"]
    elif "filename" in params and params["filename"] != None:
        filemap = params["filename"]
    else:
        filemap = None
        

    try:
        update_table_relations(client, refWorkflow, workflow, filemap, params["user"], verbose=verbose)
        
    except FileNotFoundError as e:
        print(e)
        workflow.steps = []
        client.workflowService.update(workflow)
        client.workflowService.delete(workflow.id, workflow.rev)
        sys.exit(1)

    msg("Running all steps", verbose)


    run_workflow(workflow, project, client)
    msg("Finished", verbose)

    # Retrieve the updated, ran workflow
    workflow = client.workflowService.get(workflow.id)

            # #git -c 'versionsort.suffix=-' ls-remote --tags --sort='v:refname' https://github.com/tercen/tercen | tail --lines=1 | cut --delimiter='       ' --fields=1

    # #-O ./data/some_workflow.zip
    # #https://github.com/tercen/workflow_runner/blob/a442105f74371285c49572148deb024436176ef8/workflow_files/reference_workflow.zip
    gitCmd = 'https://github.com/{}/raw/{}/{}'.format(params["gsRepo"], params["gsVersion"],params["gsPath"])

    # # tmpDir = "{}/{}".format(tempfile.gettempdir(), ''.join(random.choices(string.ascii_uppercase + string.digits, k=12)))
    tmpDir = "data"

    zipFilePath = "{}/{}".format(tmpDir, params["gsPath"].split("/")[-1])

    # #os.mkdir(tmpDir)
    subprocess.call(['wget', '-O', zipFilePath, gitCmd])
    subprocess.run(["unzip", '-qq', '-d', tmpDir, '-o', zipFilePath])

    zip  = ZipFile(zipFilePath)
    currentZipFolder = zip.namelist()[0]


    with open( "{}/{}/workflow.json".format(tmpDir, currentZipFolder) ) as wf:
        wkfJson = json.load(wf)
        gsWkf = Workflow.createFromJson( wkfJson )
        gsWkf.projectId = project.id
        gsWkf.acl = project.acl

    
    # msg( "Testing workflow {}/{}".format(wkfParams["repo"], wkfParams["goldenStandard"]), verbose )


    # zip  = ZipFile(zipFilePath)
    # currentZipFolder = zip.namelist()[0]
    params["referenceSchemaPath"] = "{}/{}/data/".format(tmpDir, currentZipFolder)



    # wkf.projectId = project.id
    # wkf.acl = project.acl

    try:
        resultDict = diff_workflow(client, workflow, gsWkf, params["referenceSchemaPath"], params["tolerance"],
                                params["toleranceType"], verbose)

        resultList.append(resultDict)
    finally:
        client.workflowService.delete(workflow.id, workflow.rev)

        


    
    print(resultList)

    # Remove tmp files and zip file
    fileList = glob.glob("{}/*".format(tmpDir), recursive=False)
    for f in fileList:
        if os.path.isdir(f):
            shutil.rmtree(f)
        else:
            os.unlink(f)

    client.teamService.delete(project.id, project.rev)
    


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
    