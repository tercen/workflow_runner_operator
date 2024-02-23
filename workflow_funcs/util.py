from datetime import datetime
import re
from tercen.model.impl import RunWorkflowTask, InitState, Pair

def msg( message, verbose=False):
    if verbose == True or verbose == "True":
        print("[{}] {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), message))


def which(arr):
    trueIdx = []
    for idx, val in enumerate(arr):
        if val == True:
            trueIdx.append(idx)

    if len(trueIdx) == 1:
        trueIdx = trueIdx[0]
    return trueIdx


def filter_by_type( objList, cls, parent=False ):
    typeList = []
    for o in objList:
        if (parent == True and issubclass(o, cls)) or \
            isinstance(o, cls ):
            typeList.append(o)

    return typeList


def is_golden_standard( name, baseName=None ):
    if baseName == None:
        return re.search("[A-Za-z0-9]+_gs[A-Za-z0-9]+$", name) != None
    else:
        return re.search("{}_gs[A-Za-z0-9]+$".format(baseName), name) != None

def filter_by_golden_standard( objList, wkfName):
    gsList = []
    for o in objList:
        if is_golden_standard(o.name, baseName=wkfName):
            gsList.append(o)

    return gsList

def filter_by_field_value( objList, fieldName, fieldValue ):
    outList = []
    
    for o in objList:
        
        if fieldName in list(o.__dict__.keys()) and o.__dict__[fieldName] == fieldValue:
            outList.append(o)


    return outList


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
