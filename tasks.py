from invoke import task, Collection
from prompt_toolkit import prompt
from prompt_toolkit.validation import Validator
import os
import re
import shutil
import logging
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def setup_logging(log_file: str = None, log_level: str ='INFO'):
    logger = logging.getLogger(__name__)
    if logger.hasHandlers(): 
        logger.handlers = []

    # Set the logging level based on the input argument
    logging_level = getattr(logging, log_level)
    logger.setLevel(logging_level)

    # Set the log message format to include the function name
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
    if log_file:
        log_file_handler = logging.FileHandler(log_file)
        log_file_handler.setFormatter(log_formatter)
        logger.addHandler(log_file_handler)
    else:
        # Otherwise, create a console handler and attach the formatter
        log_console_handler = logging.StreamHandler()
        log_console_handler.setFormatter(log_formatter)
        logger.addHandler(log_console_handler)
    
    return logger


def is_yn(text: str) -> bool:
    """
    Check if the given text is 'y', 'n', 'yes', or 'no'.

    Args:
        text (str): The text to check.

    Returns:
        bool: True if the text is 'y', 'n', 'yes', or 'no', otherwise False.
    """
    return text.lower() in ['y', 'n', 'yes', 'no']

# Define a validator for yes/no input based on the is_yn function
yn_validator = Validator.from_callable(
    is_yn,
    error_message='Input must be "y" or "n".',
    move_cursor_to_end=True)

def remove_task_files(targetdir: str, task: str, hcdsession: str = None, logger = None):
    """
    Remove specific task files in a given directory.

    Args:
        targetdir (str): The directory where files should be searched.
        task (list): The tasks to target for file removal.
        hcdsession (list): The HCD session to terget for file removal.

    This function identifies and removes directories and files corresponding to
    the tasks specified in the targettask tuple.
    
    Example usage:
        targetdir = "/ncf/hcp/data/HCD-tfMRI-MultiRunFix/"
        task = "CARIT"
        hcdsession = "HCD1389665_V1_MR"
        remove_task_files(targetdir, targettask, hcdsession)
    """
    
    # List all directories in targetdir that start with "HCD"
    hcd_dirs = [d for d in os.listdir(targetdir) if os.path.isdir(os.path.join(targetdir, d)) and d.startswith("HCD")]

    if hcdsession is not None:
        hcd_dirs = [d for d in hcd_dirs if d == hcdsession]
    # Iterate over the directories found (only the first one in this example)
    
    for thisdir in hcd_dirs:
        # Extract the base name of the directory
        HCDID = os.path.basename(thisdir)
        logger.info(f"Cleaning {task} for {HCDID}")
        # Define the path to the results directory
        taskdirloc = os.path.join(targetdir, thisdir, "MNINonLinear", "Results")
        
        # Iterate over the specified target tasks

        # Find subdirectories in taskdirloc that match the task pattern
        try:
            task_sub_dirs = [
                d
                for d in os.listdir(taskdirloc)
                if os.path.isdir(os.path.join(taskdirloc, d))
                and re.match(r"tfMRI_{}_.*(AP|PA)$".format(task), d)
            ]
        except Exception as e:
            logger.exception("An exception occurred: %s", e)
            
        # Iterate over the task subdirectories found
        for sub_dir in task_sub_dirs:
            sub_dir_path = os.path.join(taskdirloc, sub_dir)
            if os.path.isdir(sub_dir_path):
                # Identify directories matching the task pattern with the .feat extension
                featdirs = [
                    os.path.join(sub_dir_path, d)
                    for d in os.listdir(sub_dir_path)
                    if os.path.isdir(os.path.join(sub_dir_path, d))
                    and re.match(r"^tfMRI_{}.*.feat$".format(task), d)
                ]
                # Remove (or print) the identified directories
                for adir in featdirs:
                    try:
                        shutil.rmtree(adir)  # Uncomment to actually remove
                        logger.debug(f"Removed directory: {adir}")
                    except Exception as e:
                        logger.exception("An exception occurred: %s", e)

                # Identify files matching the task pattern with the .fsf extension
                fsffiles = [
                    os.path.join(sub_dir_path, f)
                    for f in os.listdir(sub_dir_path)
                    if os.path.isfile(os.path.join(sub_dir_path, f))
                    and re.match(r"^tfMRI_{}.*.fsf$".format(task), f)
                ]
                # Remove (or print) the identified files
                for afsf in fsffiles:
                    try:
                        os.remove(afsf)  # Uncomment to actually remove
                        logger.debug(f"Removed file: {afsf}")
                    except Exception as e:
                        logger.exception("An exception occurred: %s", e)

@task
def clean(c, task: str, targetdir: str = "/ncf/hcp/data/HCD-tfMRI-MultiRunFix/", hcdsession: str = None):
    """
    Invoke task to clean files and directories.

    Args:
        c: Invoke context object.
        task: Specify the task to clean.
        hcdsession (optional): Specify specific sessions to clean.

    If no task is provided, the function prompts for confirmation before cleaning.
    
    Example usage:
        invoke clean --targetdir /ncf/hcp/data/HCD-tfMRI-MultiRunFix/ --targettask CARIT --hcdsession HCD1389665_V1_MR
    """
    logger = setup_logging(log_file = c.log_file, log_level = c.log_level)
    hcdsession_text = hcdsession
    if not hcdsession_text:
        hcdsession_text = "All"
    logger.info(f"Running clean for task: {task}, targetdir: {targetdir}, hcdsession: {hcdsession_text}")
    remove_task_files(targetdir=targetdir, task=task, hcdsession=hcdsession, logger = logger)

@task
def build_first(c, task: str, parcellated=False, test=False, max_jobs: int = 200):
    """
    Build and schedule first-level task-based fMRI analysis jobs.

    Parameters:
        c (object): The context object for the task.
        task (str): The name of the task. Valid options are "CARIT-PREPOT", "CARIT-PREVCOND", or "GUESSING".
        parcellated (bool, optional): If True, run first-level models on parcellated data. Default is False.
        test (bool, optional): If True, runs a test only submitting job for one participant. Default is False.
        max_jobs (int, optional): Change the number of concurrent first-level model fits. Default is 200.
    Raises:
        ValueError: If the 'task' parameter is not one of the valid options.

    Returns:
        None
    """
    logger = setup_logging(log_file = c.log_file, log_level = c.log_level)

    # Check if the 'task' parameter is valid
    VALID_TASKS = ["CARIT-PREPOT", "CARIT-PREVCOND", "GUESSING"]
    if task not in VALID_TASKS:
        raise ValueError(f"Task is misspecified: Please provide a valid task. Valid tasks are {VALID_TASKS}")

    # Extract the short version of the task name, either "CARIT" or "GUESSING"
    short_task = re.match(r"^(CARIT|GUESSING).*", task)[1]

    # Prepare the 'parcellated' flag for the command
    if parcellated:
        parcellated_flag = "parcellated"
    else:
        parcellated_flag = ""

    # Generate a list of input files for each run of the task
    id_list_files = [ 
        f"first_level/{task}-l1-list_{i}run.txt" 
        for i in range(1, 3) 
    ]

    # Loop through each id_list_file and create the corresponding command
    for id_list_file in id_list_files:
        run_me = "sbatch"

        # Get the number of lines in the id_list_file
        with open(id_list_file, "r") as f:
            num_lines = sum(1 for _ in f)

        # If in 'test' mode, set the number of lines to 0 to avoid job submission
        if test:
            num_lines = 0

        array_job_range = "-".join([ str(i) for i in [0, num_lines-1] if i >= 0 ])
        # Add the job submission command with appropriate arguments to the list
        run_me += f" --array={array_job_range}%{max_jobs} sbatch_TaskfMRIAnalysis.bash {id_list_file} {short_task} {parcellated_flag}"
        
        try:
            logger.info(f"Running {run_me}")
            result = c.run(run_me)
            logger.info(result)
        except Exception as e:
            logger.exception(f"Failed to run sbatch job: {e}")

def extract_parcellated_array(c, task, logger, id_list_file):
    # Extract the short version of the task name, either "CARIT" or "GUESSING"
    short_task = re.match(r"^(CARIT|GUESSING).*", task)[1]
    # Generate a list of input files for each run of the task
    logger.debug(f"SLURM is {os.getenv('SLURM_ARRAY_TASK_ID')}")
    ARRAYID = int(os.getenv('SLURM_ARRAY_TASK_ID'))
    if ARRAYID is None:
        raise ValueError("Cannot get SLURM_ARRAY_TASK_ID")
    else:
        id_list = pd.read_table(id_list_file, sep=" ", header=None, names=["pid", "hcp_task", "fsf"])
        pid = id_list.pid[ARRAYID]
        hcp_tasks = id_list.hcp_task[ARRAYID].split("@")
        
        logger.info(f"Extracting data for {pid}: {hcp_tasks}")
        
        for hcp_task in hcp_tasks:
            d = re.match(".*_(AP|PA)$", hcp_task)[1]
            l1path = c.l1dir.format(studyfolder = c.studyfolder,
                                    pid = "HCD0001305_V1_MR",
                                    task = f"tfMRI_{short_task}_{d}")
            parcellated_stats_dir = os.path.join(
                l1path,
                f"tfMRI_{task.replace('-', '_')}_{d}_hp200_s4_level1_hp0_clean_ColeAnticevic.feat",
                "ParcellatedStats"
            )

            logger.debug(f"parcellated_stats_dir is {parcellated_stats_dir}")
            cope_files = [
                f for f 
                in os.listdir(parcellated_stats_dir) 
                if re.match("cope\d{1,2}.ptseries.nii", f)
            ]
            for cope in cope_files:
                text_file = os.path.join(parcellated_stats_dir, re.sub(r"\.nii$", ".txt", cope))
                cope_file = os.path.join(parcellated_stats_dir, cope)
                cmd = f"wb_command -cifti-convert -to-text {cope_file} {text_file}"
                logger.debug(f"command is {cmd}")
                wb_command = c.run(cmd)
                logger.debug(f"wb_command: {wb_command}")
            
@task
def extract_parcellated(c, task: str, test=False, max_jobs: int = 200, array_job=False, id_list_file=None):
    logger = setup_logging(log_file = c.log_file, log_level = c.log_level)
    VALID_TASKS = ["CARIT-PREPOT", "CARIT-PREVCOND", "GUESSING"]
    if task not in VALID_TASKS:
        raise ValueError(f"Task is misspecified: Please provide a valid task. Valid tasks are {VALID_TASKS}")

    if array_job:
        try:
            extract_parcellated_array(c, task=task, logger=logger, id_list_file=id_list_file)
        except Exception as e:
            logger.exception(f"Could not run array job: {e}")
    else:
        id_list_files = [ 
            f"first_level/{task}-l1-list_{i}run.txt" 
            for i in range(1, 3) 
        ]
        for id_list_file in id_list_files:
            id_list = pd.read_table(id_list_file, sep=" ", header=None, names=["pid", "task", "fsf"])
            nrow = id_list.shape[0]
            if test:
                nrow = 2
            # Batch processing logic using sbatch
            sbatch_template = f"""
{c.sbatch_header}
. PYTHON_MODULES.txt
. workbench-1.3.2.txt
mamba activate hcpl
invoke extract-parcellated --task {task} --array-job --id-list-file "{id_list_file}"
EOF
"""
            cmd = f"sbatch --array=0-{nrow - 1}%{max_jobs} <<EOF {sbatch_template}"
            logger.debug(f"Command:\n\n{cmd}")
            sbatch_result = c.run(cmd)
            logger.info(f"{sbatch_result.stdout}\n{sbatch_result.stderr}")
    
    
ns = Collection(clean, build_first, extract_parcellated)
ns.configure({'log_level': "INFO", 'log_file': "invoke.log"})  
