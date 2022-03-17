#Usage: python <this script>
#Input: None
#Output: A list of subjects and task-run directories readable by HCP's TaskfMRIAnalysis/TaskfMRIAnalysis.sh
#Details: This merely traverses the data in which we download the staged multirunfix data and inserts the correct
# information if these directories exist for a specific user.
#

from os import walk, listdir, path
from re import match
import pandas as pd
task='GUESSING'
#task='CARIT'
hcpdir='/ncf/hcp/data/HCD-tfMRI-MultiRunFix'
print('Getting {} dirs'.format(task))
id_dirs=[adir for adir in listdir(hcpdir) if match(r"HCD[0-9]{7}_V1_MR", adir)]
def get_task_dirs(id_dir, basedir, task):
    dirpostfix='MNINonLinear/Results'
    full_id_dir='/'.join([hcpdir,id_dir, dirpostfix])
    if path.isdir(full_id_dir):
        task_dirs='@'.join([task_dir for task_dir in listdir(full_id_dir) if match(r"tfMRI_" + task + "_(AP|PA)", task_dir)])
    else:
        print("Warning, directory does not exist: " + full_id_dir)
        task_dirs=''
    return(task_dirs)
print('Getting CARIT dirs for each ID dir')
task_dirs=[get_task_dirs(id_dir=id_dir, basedir=hcpdir, task=task) for id_dir in id_dirs]

print('Saving results...')
input_df=pd.DataFrame(data = {'id' : id_dirs, 'task' : task_dirs, 'l2' : "tfMRI_" + task})
input_df[input_df.task != ''].to_csv('{}_TaskAnalysis_input.txt'.format(task), sep = ' ', header = False, index = False)