import sys

sys.path.append('./')
sys.path.append('../../')

from .util import msg, which


from tercen.model.base import *




def stats_workflow(ctx, workflow, refWorkflow, verbose=False):
    
    stats = {}
    totalTime = 0.0
    totalRefTime = 0.0
    ran = True
    refRan = True
    stats["WorkflowSuccessful"] = ran
    stats["ReferenceWorkflowSuccessful"] = refRan
    stats["WorkflowDuration"] = totalTime
    stats["ReferenceWorkflowDuration"] = totalRefTime
    stats["Steps"] = []
    for i in range(0, len(workflow.steps)):
        stp = workflow.steps[i]
        refStp = refWorkflow.steps[i]
        stepStats = {}

        stepStats["Name"] = stp.name
        
        ran = ran and isinstance(stp.state.taskState, DoneState)
        refRan = refRan and isinstance(refStp.state.taskState, DoneState)

        stepStats["Successful"] = ran
        stepStats["ReferenceSuccessful"] = refRan

        
        
        if len(stp.state.taskId) > 0:
            tsk = ctx.context.client.taskService.get(stp.state.taskId)
            totalTime += tsk.duration

            stepStats["Duration"] = tsk.duration

        if len(refStp.state.taskId) > 0:
            tsk = ctx.context.client.taskService.get(refStp.state.taskId)
            totalRefTime += tsk.duration

            stepStats["ReferenceDuration"] = tsk.duration
        

        stats["Steps"].append(stepStats)
    stats["WorkflowSuccessful"] = ran
    stats["ReferenceWorkflowSuccessful"] = refRan

    stats["WorkflowDuration"] = totalTime
    stats["ReferenceWorkflowDuration"] = totalRefTime

        

    return stats

