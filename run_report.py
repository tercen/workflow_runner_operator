import os 
import sys
import json
sys.path.append('./')
sys.path.append('../../')


from tercen.model.base import *
from tercen.client import context as tercen

import numpy as np
from fpdf import FPDF


def add_step_result(pdf, label, value):
    pdf.cell(40, 10, label + ':')
    pdf.set_font('Arial', '', 12)
    pdf.cell(10, 10, value)
    pdf.set_font('Arial', 'B', 12)
    pdf.ln()

    return pdf

if __name__ == '__main__':
    print( "Running Workflow tests")

    absPath = os.path.dirname(os.path.abspath(__file__))
    
    conf_path = os.path.join(absPath, 'env.conf')
    json_path = os.path.join(absPath, 'workflow_files/run_all.json')


    username = 'test'
    passw = 'test'
    conf = {}

    with open(conf_path) as f:
        for line in f:
            if len(line.strip()) > 0:
                (key, val) = line.split(sep="=")
                conf[str(key)] = str(val).strip()
    serviceUri = ''.join([conf["SERVICE_URL"], ":", conf["SERVICE_PORT"]])

    with open(json_path) as f:
        workflowInfo = json.load(f) 

    ctx = tercen.TercenContext(
        username=username,
        password=passw,
        serviceUri=serviceUri,
        workflowId=workflowInfo["workflowId"])
    
    workflow = ctx.context.client.workflowService.get(workflowInfo["workflowId"])    

    taskTime = {}

    stp = workflow.steps[0].state.taskState
    isSuccess = np.all([isinstance(stp.state.taskState, DoneState) for stp in workflow.steps])

    for i in range(1, len(workflow.steps)):
        tsk = ctx.context.client.taskService.get(workflow.steps[i].state.taskId)
        taskTime[workflow.steps[i].name] = tsk.duration

    pdf = FPDF('P', 'mm', 'A4')
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 10, workflow.name)
    pdf.ln()
    pdf.set_font('Arial', 'B', 12)
    if isSuccess:
        pdf.cell(0, 10, 'Workflow ran successfully in {:.02f} minutes'.format( np.sum(list(taskTime.values()))/60 ))
    else:
        pdf.cell(0, 10, 'Workflow failed')

    pdf.ln(h=20)
    pdf.cell(0, 10, "Steps Summary")
    pdf.ln(h=15)



    for stp in workflow.steps:

        pdf = add_step_result(pdf, "Step name", stp.name)
        if isinstance(stp.state.taskState, DoneState):
            pdf = add_step_result(pdf, "Run status", "Successful")
        else:
            pdf = add_step_result(pdf, "Run status", "Failed")

        

        
        if(isinstance(stp, TableStep)):
            pdf = add_step_result(pdf, "Columns", ''.join(stp.model.relation.inNames))
        else:
            if hasattr(stp.computedRelation, 'joinOperators'):
                #FIXME There are no values here
                #TODO Multiple output tables
                #TODO Join, Gather Steps
                schema = ctx.context.client.tableSchemaService.get(
                    stp.computedRelation.joinOperators[0].rightRelation.relation.mainRelation.id
                )


                colString = ''
                for i in range(0, len(schema.columns)):
                    col = schema.columns[i]
                    colString +=  '' + col.name + ' [' + col.type + ']'
                    if i < (len(schema.columns)-1):
                        colString +=  ', '
                    
                pdf = add_step_result(pdf, "Columns", colString)
            else:
                pdf = add_step_result(pdf, "Columns", "No operator set (no output columns)")

        pdf.ln(h=15)


    pdf.output('test_output.pdf', 'F')
    