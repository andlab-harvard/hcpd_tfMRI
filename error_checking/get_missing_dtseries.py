from os import walk, listdir, path
import re 
import pandas as pd
import numpy as np
hcpdir='/ncf/hcp/data/HCD-tfMRI-MultiRunFix'
print('Getting ID dirs')
id_dirs=[adir for adir in listdir(hcpdir) if re.match(r"HCD[0-9]{7}_V1_MR", adir)]

def check_dtseries(id_dir, basedir):
    print('Checking for ' + id_dir)
    pattern = re.compile('tfMRI_.*_Atlas_hp0_clean.dtseries.nii')
    dirpostfix = 'MNINonLinear/Results'
    full_id_dir = '/'.join([hcpdir,id_dir, dirpostfix])
    if path.isdir(full_id_dir):
        task_dirs = [task for task in listdir(full_id_dir) if re.match(r"tfMRI_(CARIT|GUESSING)_(AP|PA)", task)]
        dtseries_fns = []
        dtseries_len = []
        for task_dir in task_dirs:
            dtseries_fn = [nii for nii in listdir('/'.join([full_id_dir, task_dir])) if re.match(pattern, nii)]
            dtseries_len.append(len(dtseries_fn))
            dtseries_fns.append(dtseries_fn)
        rdf = pd.DataFrame({'Subject' : id_dir, 'task' : task_dirs, 'dtseries' : dtseries_fns, 'N' : dtseries_len})
    else:
        print("Warning, directory does not exist: " + full_id_dir)
        rdf = pd.DataFrame({'Subject' : id_dir, 'task' : None, 'dtseries' : None, 'N' : np.nan}, index=[0])
    return(rdf)
print('Checking dtseries for each CARIT and GUESSING task in each ID dir')
dtseries_df_list = [check_dtseries(id_dir=id_dir, basedir=hcpdir) for id_dir in id_dirs]

dtseries_df = pd.concat(dtseries_df_list)
print('Exporting list of missing DTSeries...')
dtseries_df[dtseries_df.N == 0].to_csv('missing_dtseries.txt', sep = ' ', header = True, index = False)
print('Exporting list of subjects missing DTSeries...')
missing_subs = dtseries_df[dtseries_df.N == 0].loc[:, 'Subject'].drop_duplicates().str.replace('_.*_.*$', '')
missing_subs.to_csv('missing_dtseries_subs.txt', sep = ' ', header = True, index = False)
