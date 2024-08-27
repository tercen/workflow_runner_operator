import string, random, traceback, hashlib,\
        json, sys, getopt, os,  base64
from pathlib import Path

import polars as pl


import workflow_funcs.workflow_setup as workflow_setup, \
    workflow_funcs.workflow_compare as workflow_compare, \
        workflow_funcs.util as util

from tercen.client.factory import TercenClient

from tercen.model.impl import InitState,  Workflow, Project, GitProjectTask, FileDocument
from tercen.client import context as ctx


def parse_args(argv):
    params = {}
    opts, args = getopt.getopt(argv,"",
                               ["templateRepo=", "gitToken=", "tag=", "branch=",
                                "update_operator", "quiet", "report", "opMem=",
                                "templateFolder=", "hidden_columns", \
                                "serviceUri=", "user=", "passw=", "token=",
                                 "tolerance=", "toleranceType=", "taskId=" ]
                                )
    templateRepo ="tercen/kumo_data_prep_operator" 

    # If running locally or creating new operator, memory might no be set
    # This parameter sets the memory for ALL operators
    params["opMem"] = "5000000000" 

    params["user"] = 'test'
    params["passw"] = 'test'
    params["token"] = None
    params["verbose"] = True
    params["tag"] = ''
    params["branch"] = 'main'
    params["update_operator"] = False
    params["report"] = False
    params["tolerance"] = 0.001
    params["toleranceType"] = "relative"
    params["hidden_columns"] = False

    params["templateFolder"] = "workflow_tests" #None

    params["taskId"] = None

    params["serviceUri"] = "http://127.0.0.1:5400"
    params["client"] = None

    gitToken = None

    for opt, arg in opts:
        if opt == '-h':
            print('runner.py ARGS')
            sys.exit()

        if opt == '--templateRepo':
            templateRepo = arg

        if opt == '--gitToken':
            gitToken = arg

        if opt == '--serviceUri':
            if arg is None or arg == "":
                params["serviceUri"] = "http://127.0.0.1:5400"
            else:
                params["serviceUri"] = arg

        if opt == '--opMem':
            params["opMem"] = arg

        if opt == '--templateFolder':
            params["templateFolder"] = arg

        if opt == '--user':
            util.msg("[INFO] Running wokflow as USER {}".format(arg), verbose=True)
            if arg is None or arg == "":
                params["user"] = "test"
            else:
                params["user"] = arg
        
        if opt == '--passw':
            if arg is None or arg == "":
                params["passw"] = "test"
            else:
                params["passw"] = arg
        
        if opt == '--token':
            params["token"] = arg

        if opt == '--tolerance':
            params["tolerance"] = float(arg)

        if opt == '--toleranceType':
            params["toleranceType"] = arg

        if opt == '--tag':
            params["tag"] = arg

        if opt == '--branch':
            params["branch"] = arg

        if opt == '--quiet':
            params["verbose"] = False
        
        if opt == '--hidden_columns':
            params["hidden_columns"] = True

        if opt == '--taskId':
            params["taskId"] = arg

        if opt == '--update_operator':
            params["update_operator"] = True
            
        if opt == '--report':
            params["report"] = True
    
   
    templateRepo = "https://github.com/" + templateRepo

    params["templateRepo"] = templateRepo
        
    if gitToken == None and "GITHUB_TOKEN" in os.environ:
        gitToken = os.environ["GITHUB_TOKEN"]

    params["gitToken"] = gitToken

    return params


def run_with_params(params, mode="cli"):
    resultList = {}
    try:
        if params["client"] == None:
            client = TercenClient(params["serviceUri"])
            if params["token"] == None:
                client.userService.connect(params["user"], params["passw"])
            else:
                tercenCtx = ctx.TercenContext(params["token"])
                client = tercenCtx.context.client

        else:
            client = params["client"] # Running as operator

        # Create temp project on which to run tests
        # if mode == "cli":
        project = Project()
        project.name = 'WorkflowRunner_' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
        project.acl.owner = params['user']
        project = client.projectService.create(project)
        params["projectId"] = project.id
        # else:
            # project = client.projectService.get(params["projectId"])

        # Clone the template project from git into the temp project
        importTask = GitProjectTask()
        importTask.owner = params['user']
        importTask.state = InitState()


        util.msg( "Template Params", params["verbose"] )
        util.msg( "Template: {}".format(params["templateRepo"]), params["verbose"] )
        util.msg( "TAG: {}".format(params["tag"]), params["verbose"] )

        importTask.addMeta("PROJECT_ID", project.id)
        importTask.addMeta("PROJECT_REV", project.rev)
        importTask.addMeta("GIT_ACTION", "reset/pull")
        importTask.addMeta("GIT_PAT", params["gitToken"])
        importTask.addMeta("GIT_URL", params["templateRepo"])
        importTask.addMeta("GIT_BRANCH",params["branch"])
        importTask.addMeta("GIT_MESSAGE", "")
        if params["tag"] != "":
            importTask.addMeta("GIT_TAG", params["tag"])

        importTask = client.taskService.create(importTask)
        client.taskService.runTask(importTask.id)
        importTask = client.taskService.waitDone(importTask.id)
        

        objs = client.persistentService.getDependentObjects(project.id)
        workflowList = util.filter_by_type(objs, Workflow)
        # fileList is retrieved to search for configuration files, if any
        fileList = util.filter_by_type(objs, FileDocument)
        verbose = params["verbose"]
        
        util.msg( "Searching for workflows in {}/{}".format(params["templateRepo"], params["templateFolder"] ), verbose )

        statusList=[]
        allPass = True
        foundTests = False
        for w in workflowList:
            wkfName = w.name
            folderId = w.folderId

            if util.is_golden_standard(wkfName):
                continue
            if folderId != "":
                folder = client.folderService.get(folderId)
                folder = folder.name
            else:
                folder = ""

            if params["templateFolder"] != None:
                if folder != params["templateFolder"]:
                    continue
            util.msg( "Found {}/{}".format(folder, wkfName ), verbose )
            # Clear possible previous configurations
            params["config"] = None

            for f in fileList:
                if Path(f.name).stem == wkfName:
                    configFile = f
                    params["config"] = util.parse_config_file(configFile, client)

                    util.msg( "Config file detected for workflow", verbose )
                    break
            

            gsList = util.filter_by_golden_standard( workflowList, wkfName)

            wkf = w
            gsWkf = None
            for w2 in gsList:
                gsWkf = w2
                util.msg( "Testing template {} against {}.".format(wkfName, gsWkf.name ), verbose )
                foundTests = True
                workflowRun = workflow_setup.setup_workflow(client, wkf, gsWkf=gsWkf, params=params)


                resultDict = []
                util.msg("Running all steps", verbose)
                util.run_workflow(workflowRun, project, client)
                util.msg("Done", verbose)

                # Retrieve the updated, ran workflow
                workflowRun = client.workflowService.get(workflowRun.id)

                util.msg("Comparing Results", verbose)
                resultDict = workflow_compare.diff_workflow(client, workflowRun, gsWkf,  params["tolerance"],
                                        params["toleranceType"], params["hidden_columns"], verbose)


                if resultDict != None and resultDict != []:
                    if params["report"] == True:
                        resultDict = {w2.name: resultDict[0]}
                        resultList = {**resultList, **resultDict}

                        util.msg("{} and {} comparison FAILED".format(\
                            wkfName, gsWkf.name), verbose)
                        statusList.append({\
                            "workflow":wkfName,\
                            "goldenStandard":gsWkf.name,\
                            "status":0,
                            "result":resultDict})
                        allPass = False
                    else:

                        if isinstance(resultDict, dict) or isinstance(resultDict, list):
                            with open('test_results.json', 'w', encoding='utf-8') as f:
                                json.dump(fixDictTypes(resultDict), f, ensure_ascii=False, indent=4)
                        else:
                            with open('test_results.json', "w") as f:
                                for line in resultDict:
                                    f.write(" ".join(line) + "\n") 
                            

                        raise Exception("Comparison between {} and {} failed.".format(\
                            wkfName, gsWkf.name))
                    
                else:
                    util.msg("{} and {} comparison was SUCCESSFUL".format(\
                        wkfName, gsWkf.name), verbose)
                    statusList.append({\
                            "workflow":wkfName,\
                            "goldenStandard":gsWkf.name,\
                            "status":1,
                            "result":{}})
            
                util.msg("Finished Run", verbose)
        if foundTests == False:
            raise Exception("No tests were found in folder {}.".format(params["templateFolder"]))
                            
        if allPass == True: 
            with open('test_results.json', 'w', encoding='utf-8') as f:
                json.dump({"Status":"Success"}, f, ensure_ascii=False, indent=4)
        else:
            if isinstance(resultList, dict):
                with open('test_results.json', 'w', encoding='utf-8') as f:
                    json.dump(resultList, f, ensure_ascii=False, indent=4)
            else:
                with open('test_results.json', "w") as f:
                    for line in resultList:
                        f.write(" ".join(line) + "\n") 
                            
            raise Exception("At least one workflow comparison failed. Check the generated reported.")
    except Exception as e:
        util.msg("Workflow runner failed with error: ", True)
        util.msg(traceback.format_exc(), True)

        raise e
        
    # finally:
        # if project != None and client != None:
            # client.workflowService.delete(project.id, project.rev)

    if mode == "operator":
        return statusList

import numpy as np
from typing import cast

def fixDictTypes(dictObj):

    if isinstance(dictObj, list):
        
        # for o in dictObj:
        for i in range(len(dictObj)):
            o = dictObj[i]
            if isinstance(o, dict) or isinstance(o, list):
                o = fixDictTypes(o)
            elif( type(o) == np.int32 or type(o) == np.int64 ):
                o = int(o)
            elif( type(o) == np.float64 or type(o) == np.float32):
                o = float(o)
            dictObj[i] = o
                
                
            # elif(isinstance(v, np.ndarray)):
            #     print("{} is ndarray".format(k))
            #     if( v.dtype == np.int32 ):
            #         print("{}".format(k))
            #         v = float(v.tolist())
            #     else:
            #         v = v.tolist()
            #     o[k] = v
                    
    else:
        for k, v in dictObj.items():
            if isinstance(v, dict)  or isinstance(v, list):
                dictObj[k] = fixDictTypes(v)
            elif(isinstance(v, np.ndarray)):
                
                if( v.dtype == np.int32 ):
                    v = float(v.tolist())
                else:
                    v = v.tolist()
                dictObj[k]=v
                
    return dictObj
            

def tercenBool(value):
    if value == "true":
        return True
    else:
        return False 

def run(argv):
    params = parse_args(argv)
    #http://127.0.0.1:5400/test/w/ac44dd4f14f28b0884cf7c9d600027f1/ds/1ba15e7c-6c3e-4521-81f2-d19fa58a57b9
    # params["taskId"] = "someId"
    
    if params["taskId"] != None:
        # tercenCtx = ctx.TercenContext(workflowId="ac44dd4f14f28b0884cf7c9d600027f1",\
                                    #   stepId="1ba15e7c-6c3e-4521-81f2-d19fa58a57b9")
        tercenCtx = ctx.TercenContext()
        params["client"] = tercenCtx.context.client
        task = tercenCtx.client.taskService.get(params["taskId"])
        params["user"] = task.owner
        # wkf = tercenCtx.client.workflowService.get(tercenCtx.get_workflow_id())
        # params["projectId"] = wkf.projectId

        reportMode = tercenCtx.operator_property('Report Mode', typeFn=tercenBool, default=False)
        opMem = tercenCtx.operator_property('Memory', typeFn=int, default=-1)
        gitToken = tercenCtx.operator_property('Github Token', typeFn=str, default="")
        tolerance = tercenCtx.operator_property('Tolerance', typeFn=float, default=0.001)
        toleranceType = tercenCtx.operator_property('Tolerance Type', typeFn=str, default="relative")


        if gitToken == "":
            gitToken = os.getenv("GITHUB_TOKEN")
        
        # Repo -> Branch -> Version
        df = tercenCtx.cselect()
        
        nRepos = df.shape[0]
        outDf = pl.DataFrame()
        outDf2 = pl.DataFrame()

        for i in range(0, nRepos):
            params["templateRepo"] = df[i,"repository"]
            params["branch"] = df[i,"branch"]
            params["tag"] = df[i,"version"]
            params["gitToken"] = gitToken
            params["report"] = reportMode
            params["tolerance"] = tolerance
            params["toleranceType"] = toleranceType.lower()
            # opMem = 500000000
            if opMem > 0:
                params["opMem"] = str(opMem)

            statusList = run_with_params(params, mode="operator")
            
            for st in statusList:
                output_str = []
                outBytes = json.dumps(st["result"]).encode("utf-8")
                output_str.append(base64.b64encode(outBytes))
                
                checksum = hashlib.md5(outBytes).hexdigest()
                if outDf.is_empty():
                    outDf = pl.DataFrame({".ci": i,\
                                        "workflow": st["workflow"],\
                                        "golden_standard": st["goldenStandard"],\
                                        "status": st["status"]})
                   
                else:
                    outDf =  pl.concat([outDf,\
                                         pl.DataFrame({".ci": i,\
                                        "workflow": st["workflow"],\
                                        "golden_standard": st["goldenStandard"],\
                                        "status": st["status"]})\
                                        ])
                    
                # Only saves reports for comparisons which went wrong
                if st["status"] == 0:
                    if outDf.is_empty():
                        outDf2 = pl.DataFrame({".ci": i,\
                                "checksum":checksum,\
                                "filename":"{}.json".format(st["goldenStandard"]), \
                                "mimetype":"application/json",\
                                ".content":output_str})
                    else:
                        outDf2 = pl.concat([outDf2,\
                                pl.DataFrame({".ci": i,\
                                            "checksum":checksum,\
                                            "filename":"{}.json".format(st["goldenStandard"]), \
                                            "mimetype":"application/json",\
                                            ".content":output_str})\
                                ])
                        
        dfArray = []
        if not outDf.is_empty():
            outDf = outDf.with_columns(pl.col('.ci').cast(pl.Int32))\
                        .with_columns(pl.col('status').cast(pl.Float64))
            outDf = tercenCtx.add_namespace(outDf) 
            dfArray.append(outDf)
            
        if not outDf2.is_empty():
            outDf2 = outDf2.with_columns(pl.col('.ci').cast(pl.Int32))\
                        .with_columns(pl.col('.content').cast(pl.Utf8))
            outDf2 = tercenCtx.add_namespace(outDf2) 
            dfArray.append(outDf2)
        
        if len(dfArray) > 0:
            tercenCtx.save(dfArray)
        
    else:
        run_with_params(params)
    

if __name__ == '__main__':
    run(sys.argv[1:])

