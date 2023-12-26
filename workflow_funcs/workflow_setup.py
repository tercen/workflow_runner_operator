import copy
from datetime import datetime

import workflow_funcs.util as util


from tercen.model.impl import *
from tercen.client import context as tercen

import numpy as np

from datetime import datetime

from pytson import encodeTSON



def get_installed_operator(client,  opName, opUrl, opVersion, params, verbose=False):
    installedOperators = client.documentService.findOperatorByOwnerLastModifiedDate(params['user'], '')
    opTag = '{}@{}@{}'.format(opName, opUrl, opVersion)
    comp = [opTag ==  '{}@{}@{}'.format(iop.name, iop.url.uri, iop.version) for iop in installedOperators]

    if opTag == '@@' or opUrl == '':
        return None
    
    if not np.any(comp):
        # install the operator
        util.msg("Installing {}:{} from {}".format(opName, opVersion, opUrl), verbose)
        installTask = CreateGitOperatorTask()
        installTask.state = InitState()
        installTask.url.uri = opUrl
        installTask.version = opVersion
        installTask.gitToken = params["gitToken"]
        
        installTask.testRequired = True
        installTask.isDeleted = False
        installTask.owner = params["user"]

        installTask = client.taskService.create(installTask)
        client.taskService.runTask(installTask.id)
        installTask = client.taskService.waitDone(installTask.id)

        if isinstance(installTask.state, DoneState):
            operator = client.operatorService.get(installTask.operatorId)
        else:
            raise RuntimeError("Installation of operator " + opTag + " failed. USER: " + params["user"]) 
    else:
        idx = util.which(comp)
        if isinstance(idx, list):
            idx = idx[0]

        operator = installedOperators[idx]

    return operator
    
def update_operators( workflow, client, params):
    
    operatorLib = client.documentService.getTercenOperatorLibrary(0, 300)


    for stpIdx in range(0, len(workflow.steps)):
        if hasattr(workflow.steps[stpIdx].model, "operatorSettings"):
            opName = workflow.steps[stpIdx].model.operatorSettings.operatorRef.name
            opVersion = workflow.steps[stpIdx].model.operatorSettings.operatorRef.version

            # Might have multiple versions, get latest
            opIdx = util.which([op.name == opName for op in operatorLib])

            if opIdx != []:
                if len(opIdx) > 1:
                    #TODO if there are multiple versions, find the latest
                    pass

                libOp = operatorLib[opIdx]
                if libOp.version > opVersion:
                    util.msg("Updating {} operator from version {} to version {}".format(\
                        opName, libOp.version, opVersion), params["verbose"])
                    
                    operator = get_installed_operator(client,  \
                                                      libOp.name, \
                                                      libOp.url.uri, libOp.version, params)
                    
                    workflow.steps[stpIdx].model.operatorSettings.operatorRef.operatorId = operator.id
                    workflow.steps[stpIdx].model.operatorSettings.operatorRef.name = operator.name
                    workflow.steps[stpIdx].model.operatorSettings.operatorRef.url = operator.url
                    workflow.steps[stpIdx].model.operatorSettings.operatorRef.version = operator.version
    
    return workflow


def setup_workflow(client, templateWkf, gsWkf, params):
    # Copy is wanted in the case of multiple golden standards being tested
    util.msg("Copying workflow", params["verbose"])


    workflow = copy.deepcopy(templateWkf)
    workflow.name = "{}_{}".format(templateWkf.name, datetime.now().strftime("%Y%m%d_%H%M%S"))
    workflow.id = ''

    if params["update_operator"] == True:
        util.msg("Checking for updated operator versions", params["verbose"])
        workflow = update_operators( workflow, client, params)
    

    #FIXME If nothing changes, the cached version of the computedRelation is used
    # Not usually a problem, but then we cannot delete the new workflow if needed
    # because it indicates a dependency to the reference workflow
    for stp in workflow.steps:
        if hasattr(stp, "computedRelation"):
            #TODO Check if this factor actually exists
            # Operators (like downsample) might not have it
            # NOTE Adding a filter to avoid caches will break wizard steps
            # Some new strategy here is needed
            stp.model.axis.xyAxis[0].yAxis.axisExtent.y = \
                stp.model.axis.xyAxis[0].yAxis.axisExtent.y+0.1           

        
        stp.state.taskState = InitState()

    
    workflow = client.workflowService.create(workflow)


    update_table_relations(client, workflow, gsWkf, verbose=params["verbose"] )
    update_wizard_factors(client, workflow, gsWkf, verbose=params["verbose"] )

    workflow.addMeta("RUN_WIZARD_STEP", "true")

    client.workflowService.update(workflow)

    return workflow

# Separate function for legibility
def update_table_relations(client, workflow, gsWorkflow, verbose=False):
    util.msg("Setting up table step references in new workflow.", verbose)

    for gsStp in gsWorkflow.steps:
        if isinstance(gsStp, TableStep):
            # Number of steps might have changed
            for i in range(0, len(workflow.steps)):
                stp = workflow.steps[i]
                # NOTE Assumes the golden standard was a clone of an execution of the template
                # This assumption MIGHT NOT hold
                if stp.id == gsStp.id:
                    stp.model = copy.deepcopy(gsStp.model)
                    stp.state.taskState = DoneState()
                    stp.name = gsStp.name
                    

def update_wizard_factors(client, workflow, gsWorkflow, verbose=False):
    util.msg("Updating Wizard factors.", verbose)

    for gsStp in gsWorkflow.steps:
        if isinstance(gsStp, WizardStep):
            # Number of steps might have changed
            for i in range(0, len(workflow.steps)):
                stp = workflow.steps[i]
                # NOTE Assumes the golden standard was a clone of an execution of the template
                # This assumption MIGHT NOT hold
                if stp.id == gsStp.id:
                    stp.model = copy.deepcopy(gsStp.model)
                    


    

    