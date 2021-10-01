from os import walk, listdir, path
from re import match
import pandas as pd
hcpdir='/ncf/hcp/data/HCD-tfMRI-MultiRunFix'
print('Getting ID dirs')
id_dirs=[adir for adir in listdir(hcpdir) if match(r"HCD[0-9]{7}_V1_MR", adir)]
def get_CARIT_dirs(id_dir, basedir):
    dirpostfix='MNINonLinear/Results'
    full_id_dir='/'.join([hcpdir,id_dir, dirpostfix])
    if path.isdir(full_id_dir):
        carit_dirs='@'.join([carit for carit in listdir(full_id_dir) if match(r"tfMRI_CARIT", carit)])
    else:
        print("Warning, directory does not exist: " + full_id_dir)
        carit_dirs=''
    return(carit_dirs)
print('Getting CARIT dirs for each ID dir')
carit_dirs=[get_CARIT_dirs(id_dir=id_dir, basedir=hcpdir) for id_dir in id_dirs]
print('Saving results...')
input_df=pd.DataFrame(data = {'id' : id_dirs, 'task' : carit_dirs, 'l2' : "tfMRI_CARIT"})
input_df[input_df.task != ''].to_csv('TaskAnalysis_input.txt', sep = ' ', header = False, index = False)
