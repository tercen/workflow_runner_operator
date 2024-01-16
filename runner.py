import string, random, traceback, hashlib,\
        json, sys, getopt, os, zlib, base64

import polars as pl


import workflow_funcs.workflow_setup as workflow_setup, \
    workflow_funcs.workflow_compare as workflow_compare, \
        workflow_funcs.util as util

import tercen.util.helper_functions as tercenUtil

from tercen.client.factory import TercenClient

from tercen.model.impl import InitState,  Workflow, Project, GitProjectTask
from tercen.client import context as ctx


def parse_args(argv):
    params = {}
    opts, args = getopt.getopt(argv,"",
                               ["templateRepo=", "gitToken=", "tag=", "branch=",
                                "update_operator", "quiet", "report", "opMem=",
                                "serviceUri=", "user=", "passw=", "token=",
                                 "tolerance=", "toleranceType=", "taskId=" ]
                                )

    
    

    #docker run -t --net=host tercen/workflow_runner:latest --templateRepo=tercen/git_project_test  --gitToken=ddd serviceUri=http://127.0.0.1:5400 --opMem="500000000"
    # FIXME DEBUG
    templateRepo = "" #"tercen/git_project_test" 

    # If running locally or creating new operator, memory might no be set
    # This parameter sets the memory for ALL operators
    params["opMem"] = None #"500000000" #None

    params["user"] = 'test'
    params["passw"] = 'test'
    params["token"] = ''
    gitToken = None
    params["verbose"] = True
    params["tag"] = ''
    params["branch"] = 'main'

    params["update_operator"] = False
    params["report"] = False
    
    params["tolerance"] = 0.001
    params["toleranceType"] = "relative"

    params["taskId"] = None

    params["serviceUri"] = "http://127.0.0.1:5400"
    params["client"] = None

    for opt, arg in opts:
        if opt == '-h':
            print('runner.py ARGS')
            sys.exit()

        if opt == '--templateRepo':
            templateRepo = arg

        if opt == '--gitToken':
            gitToken = arg

        if opt == '--serviceUri':
            params["serviceUri"] = arg

        if opt == '--opMem':
            params["opMem"] = arg

        if opt == '--user':
            params["user"] = arg
        
        if opt == '--passw':
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

    # python3 template_tester.py  --templateRepo=tercen/workflow_lib_repo --templateVersion=latest --templatePath=template_mean_crabs_2.zip --gsRepo=tercen/workflow_lib_repo --gsVersion=latest --gsPath=golden_standard_mean_crabs_2.zip --projectId=2aa4e5e69e49703961f2af4c5e000dd1
    return params


def run_with_params(params, mode="cli"):
    resultList = []
    try:
        if params["client"] == None:
            client = TercenClient(params["serviceUri"])
            if params["token"] == None:
                client.userService.connect(params["user"], params["passw"])
            else:
                tercenCtx = ctx.TercenContext(params["token"])
                ctx = tercenCtx.context.client

        else:
            client = params["client"] # Running as operator

        # client = params["client"]
        # Create temp project to run tests
        project = Project()
        project.name = 'template_test_' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
        project.acl.owner = params['user']
        project = client.projectService.create(project)
        params["projectId"] = project.id

        # Clone the template project from git
        importTask = GitProjectTask()
        importTask.owner = params['user']
        importTask.state = InitState()

        importTask.addMeta("PROJECT_ID", project.id)
        importTask.addMeta("PROJECT_REV", project.rev)
        importTask.addMeta("GIT_ACTION", "reset/pull")
        importTask.addMeta("GIT_PAT", params["gitToken"])
        importTask.addMeta("GIT_URL", params["templateRepo"])
        
        importTask.addMeta("GIT_BRANCH",params["branch"])
        importTask.addMeta("GIT_MESSAGE", "")
        importTask.addMeta("GIT_TAG", params["tag"])


        importTask = client.taskService.create(importTask)
        client.taskService.runTask(importTask.id)
        importTask = client.taskService.waitDone(importTask.id)
        
        objs = client.persistentService.getDependentObjects(project.id)
        workflowList = util.filter_by_type(objs, Workflow)


        verbose = params["verbose"]
        
        statusList=[]
        
        for w in workflowList:
            
            wkfName = w.name

            # FIXME DEBUG
            #if not wkfName.startswith("Complex"):
            # if wkfName != "WizardWkf":
            #    continue
                
            
            nameParts = wkfName.split("_")
            if not (nameParts[-1].startswith("gs") and len(nameParts) > 1):
                wkf = w
                gsWkf = None
                for w2 in workflowList:
                    nameParts = w2.name.split("_")
                    if w2.name == (wkfName + "_" + nameParts[-1]):
                        gsWkf = w2

                        
                        util.msg( "Testing template {} against {}.".format(wkfName, gsWkf.name ), verbose )
                        
                        workflowRun = workflow_setup.setup_workflow(client, wkf, gsWkf=gsWkf, params=params)
                    

                        util.msg("Running all steps", verbose)
                        util.run_workflow(workflowRun, project, client)
                        util.msg("Done", verbose)

                        # Retrieve the updated, ran workflow
                        workflowRun = client.workflowService.get(workflowRun.id)

                        util.msg("Comparing Results", verbose)
                        resultDict = workflow_compare.diff_workflow(client, workflowRun, gsWkf,  params["tolerance"],
                                                params["toleranceType"], verbose)


                        if resultDict != None and resultDict != []:
                            if params["report"] == True:
                                resultList.append({w2.name: resultDict[0]})   
                                util.msg("{} and {} comparison FAILED".format(\
                                    wkfName, gsWkf.name), verbose)
                                statusList.append({\
                                    "workflow":wkfName,\
                                    "goldenStandard":gsWkf.name,\
                                    "status":0,
                                    "result":resultDict})
                            else:
                                with open('test_results.json', 'w', encoding='utf-8') as f:
                                    json.dump(resultDict, f, ensure_ascii=False, indent=4)

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
                            
                            
        with open('test_results.json', 'w', encoding='utf-8') as f:
                json.dump({"Status":"Success"}, f, ensure_ascii=False, indent=4)
    except Exception as e:
        util.msg("Workflow runner failed with error: ", True)
        util.msg(traceback.format_exc(), True)

        raise e
        
    finally:
        if project != None and client != None:
            client.workflowService.delete(project.id, project.rev)

    if mode == "operator":
        return statusList


def run(argv):
    params = parse_args(argv)
    #http://127.0.0.1:5400/test/w/ac44dd4f14f28b0884cf7c9d600027f1/ds/1ba15e7c-6c3e-4521-81f2-d19fa58a57b9
    # params["taskId"] = "someId"
    
    if params["taskId"] != None:
        # TODO Run as operator
        # tercenCtx = ctx.TercenContext(workflowId="ac44dd4f14f28b0884cf7c9d600027f1",\
                                    #   stepId="1ba15e7c-6c3e-4521-81f2-d19fa58a57b9")
        tercenCtx = ctx.TercenContext()
        params["client"] = tercenCtx.context.client
  

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

            templateRepo = "https://github.com/" + df[i,0]
            params["templateRepo"] = templateRepo
            params["branch"] = df[i,1]
            params["tag"] = df[i,2]
            params["gitToken"] = gitToken
            #FIXME Read this from parameters
            params["report"] = True
            params["tolerance"] = tolerance
            params["toleranceType"] = toleranceType.lower()
            # opMem = 500000000
            if opMem > 0:
                params["opMem"] = str(opMem)

            statusList = run_with_params(params, mode="operator")
            
            for st in statusList:

                

                output_str = []
                # outBytes = zlib.compress(json.dumps(st["result"]).encode("utf-8"))
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
                    
        outDf = outDf.with_columns(pl.col('.ci').cast(pl.Int32))\
                     .with_columns(pl.col('status').cast(pl.Float64))
        outDf2 = outDf2.with_columns(pl.col('.ci').cast(pl.Int32))\
                       .with_columns(pl.col('.content').cast(pl.Utf8))

        outDf = tercenCtx.add_namespace(outDf) 
        outDf2 = tercenCtx.add_namespace(outDf2) 
        tercenCtx.save([outDf, outDf2])
        
    else:
        run_with_params(params)
    




if __name__ == '__main__':
    #absPath = os.path.dirname(os.path.abspath(__file__))
    run(sys.argv[1:])
