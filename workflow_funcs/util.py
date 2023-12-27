from datetime import datetime

from tercen.model.impl import RunWorkflowTask, InitState

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


# def numpy_to_list(obj):
#     if isinstance(obj, np.ndarray):
#         obj = obj.tolist()

#         for i in range(0, len(obj)):
#             obj[i] = numpy_to_list(obj[i])

#         return obj
#     elif isinstance(obj, list):
#         for i in range(0, len(obj)):
#             obj[i] = numpy_to_list(obj[i])

#         return obj
#     elif isinstance(obj, dict):
#         for key in obj:
#             obj[key] = numpy_to_list(obj[key])
        
#         return obj
#     elif isinstance(obj, pl.Series):
#         obj = obj.to_list()

#         for i in range(0, len(obj)):
#             obj[i] = numpy_to_list(obj[i])

#         return obj
#     else:
#         return obj

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
