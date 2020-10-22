# IMPORTING PACKAGES
import json
from washevents_comparison import *

# ORCHRASTRATION FUNCTION
def verification(event, payload):

    try:
        # READING THE INPUTS FROM THE JSON FILE
        with open('config.json') as input_file:
            input_params = json.load(input_file)

        if input_params['module'] == 'mvp1':
            mvp1_verification_summary = mvp1_washevents_comparison(input_params['input'], input_params['output'])
        else:
            mvp2_verification_summary = mvp2_washevents_comparison(input_params['input'], input_params['output'], input_params['parameters'], input_params['separate_store'], input_params['difference_calculation_parameters'])

    except Exception as identifier:
        if input_params['module'] == 'mvp1':
            print ("Failed to verifiy the results of MVP1 verification due to : {}".format(identifier))
        else:
            print ("Failed to verifiy the results of MVP2 verification due to : {}".format(identifier))
        raise identifier
    
    else:
        if input_params['module'] == 'mvp1':
            print ('************** MVP1 WASHEVENTS VERIFICATION COMPLETED SUCCESSFULLY **************')
        else:
            print ('************** MVP2 CALCULATIONS VERIFICATION COMPLETED SUCCESSFULLY **************')        
        return True