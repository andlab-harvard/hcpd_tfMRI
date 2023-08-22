from invoke import task, Collection
from prompt_toolkit import prompt
from prompt_toolkit.validation import Validator
import os
import re
import shutil
import logging
import pandas as pd
import sqlite3

class HCPDDataDoer:
    class DatabaseManager:
        def __init__(self, db_name="file_status.db"):
            self.db_name = db_name
            self.timeout = 10
            self.setup_database()

        def setup_database(self):
            conn = sqlite3.connect(self.db_name, timeout = self.timeout)

            # Check if the table exists
            table_exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files'").fetchone()

            if table_exists:
                # If the table exists, check its columns
                columns = [column[1] for column in conn.execute("PRAGMA table_info(files)").fetchall()]

                # Ensure each column is present, if not, add it
                expected_columns = ['id', 'filepath', 'status', 'pid', 'session', 'task', 'data_type', 'file_type']
                for col in expected_columns:
                    if col not in columns:
                        conn.execute(f"ALTER TABLE files ADD COLUMN {col}")
                        conn.commit()
            else:
                # If the table doesn't exist, create it with all columns
                conn.execute('''
                CREATE TABLE files (
                    id INTEGER PRIMARY KEY,
                    filepath TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    pid TEXT,
                    session TEXT,
                    task TEXT,
                    data_type TEXT,
                    file_type TEXT
                )
                ''')
                conn.commit()

            conn.close()

        def update_file(self, filepath):
            status = 'built' if os.path.exists(filepath) else 'missing'

            parsed_info = self.parse_filename(filepath)
            conn = sqlite3.connect(self.db_name, timeout = self.timeout)

            # Check if filepath already exists
            query = f"SELECT * FROM files WHERE filepath='{filepath}'"
            df = pd.read_sql(query, conn)

            data_to_insert = {
                "filepath": filepath,
                "status": status,
                "pid": parsed_info["pid"],
                "session": parsed_info["session"],
                "task": parsed_info["task"],
                "data_type": parsed_info["data_type"],
                "file_type": parsed_info["file_type"]
            }

            if df.empty:
                # Insert new record
                new_data = pd.DataFrame([data_to_insert])
                new_data.to_sql('files', conn, if_exists='append', index=False)
            else:
                # Update existing record
                for column, value in data_to_insert.items():
                    update_query = f"UPDATE files SET {column}='{value}' WHERE filepath='{filepath}'"
                    conn.execute(update_query)
                conn.commit()

            conn.close()
            return status

        def get_file_status(self, filepath):
            file_info = self.get_file_info(filepath)
            if file_info:
                return file_info['status']
            return None

        def get_file_info(self, filepath):
            conn = sqlite3.connect(self.db_name, timeout = self.timeout)
            conn.execute("PRAGMA journal_mode=WAL")

            query = f"SELECT * FROM files WHERE filepath='{filepath}'"
            df = pd.read_sql(query, conn)

            conn.close()

            if not df.empty:
                return df.iloc[0].to_dict()
            return None

        def parse_filename(self, filename):
            match = re.search(r"/(?P<pid>HCD\d+)_(?P<session>V\d+)_MR/MNINonLinear/Results/tfMRI_\w+/tfMRI_(?P<task>\w+)_(?:AP|PA).*/(?P<data_type>Parcellated|Grayordinates)Stats/.*\.(?P<file_type>\w{1,5}$)", filename)

            if not match:
                return {"pid": None, "session": None, "task": None, "data_type": None, "file_type": None}

            pid = match.group("pid")
            session = match.group("session")
            task = match.group("task")
            data_type = match.group("data_type")
            file_type = match.group("file_type")

            return {"pid": pid, "session": session, "task": task, "data_type": data_type, "file_type": file_type}

    def __init__(self, c):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        self.logger = self.setup_logging(log_file = c.log_file, log_level = c.log_level)
        self.database = self.DatabaseManager(c.database_file)
        pass
    
    def setup_logging(self, log_file: str = None, log_level: str ='INFO'):
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

    def remove_task_files(self, targetdir: str, task: str, hcdsession: str = None):
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
            self.logger.info(f"Cleaning {task} for {HCDID}")
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
                self.logger.exception("An exception occurred: %s", e)

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
                            self.logger.debug(f"Removed directory: {adir}")
                        except Exception as e:
                            self.logger.exception("An exception occurred: %s", e)

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
                            self.logger.debug(f"Removed file: {afsf}")
                        except Exception as e:
                            self.logger.exception("An exception occurred: %s", e)

    def queued_update_file(self, queue):
        while True:
            item = queue.get()
            if item is None:
                break  # Sentinel value to exit loop
            text_file = item
            status = self.database.update_file(text_file)
            logger.debug(f"Status: {status} for {text_file}")
        
    def extract_parcellated_chunk(self, c, task, chunk, queue):
        for _, row in chunk.iterrows():
            pid = row['pid']
            hcp_tasks = row['hcp_task'].split("@")

            self.logger.info(f"Extracting data for {pid}: {hcp_tasks}")

            for hcp_task in hcp_tasks:
                d = re.match(".*_(AP|PA)$", hcp_task)[1]
                l1path = c.l1dir.format(studyfolder = c.studyfolder,
                                        pid = pid,
                                        task = f"tfMRI_{short_task}_{d}")
                parcellated_stats_dir = os.path.join(
                    l1path,
                    f"tfMRI_{task.replace('-', '_')}_{d}_hp200_s4_level1_hp0_clean_ColeAnticevic.feat",
                    "ParcellatedStats"
                )

                self.logger.debug(f"parcellated_stats_dir is {parcellated_stats_dir}")
                cope_files = [
                    f for f 
                    in os.listdir(parcellated_stats_dir) 
                    if re.match("cope\d{1,2}.ptseries.nii", f)
                ]
                for cope in cope_files:
                    text_file = os.path.join(parcellated_stats_dir, re.sub(r"\.nii$", ".txt", cope))
                    status = self.database.update_file(text_file)
                    if status == 'missing':
                        cope_file = os.path.join(parcellated_stats_dir, cope)
                        cmd = f"wb_command -cifti-convert -to-text {cope_file} {text_file}"
                        self.logger.debug(f"command is {cmd}")
                        wb_command = c.run(cmd)
                        self.logger.debug(f"wb_command: {wb_command}")
                        queue.put(text_file)
                    else:
                        self.logger.debug(f"file exists: {text_file}")
    
    def extract_parcellated_parallel(self, c, task, id_list_files):
        short_task = re.match(r"^(CARIT|GUESSING).*", task)[1]
        self.logger.debug(f"SLURM_CPUS_PER_TASK is {os.getenv('SLURM_CPUS_PER_TASK')}")
        NCPU = int(os.getenv('SLURM_CPUS_PER_TASK')) if os.getenv('SLURM_CPUS_PER_TASK') else cpu_count()
        
        logger.debug(f"Number of CPUs: {NCPU}")
        if NCPU is None:
            raise ValueError("Cannot determine the number of CPUs")
        
        df_list = []
        for id_list_file in id_list_files:
            df_list.append(pd.read_table(id_list_file, sep=" ", header=None, names=["pid", "hcp_task", "fsf"]))
        id_list = pd.concat(df_list, axis=0)
        
        # Set up a queue for database writes and a process for the database writer
        db_queue = Queue()
        db_process = Process(target=self.queued_update_file, args=(db_queue))
        db_process.start()

        # Split id_list into chunks for each process
        chunks = np.array_split(id_list, NCPU)
        
        processes = []
        for chunk in chunks:
            p = Process(target=self.extract_parcellated_chunk, args=(c, task, chunk, db_queue))
            processes.append(p)
            p.start()

        # Wait for all processes to finish
        for p in processes:
            p.join()

        # Signal the database writer to finish and wait for it
        db_queue.put(None)
        db_process.join()

        self.logger.info("All data extracted!")
        
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
    datadoer = HCPDDataDoer(c)

    hcdsession_text = hcdsession
    if not hcdsession_text:
        hcdsession_text = "All"
    datadoer.logger.info(f"Running clean for task: {task}, targetdir: {targetdir}, hcdsession: {hcdsession_text}")
    datadoer.remove_task_files(targetdir=targetdir, task=task, hcdsession=hcdsession)

@task(help={'task': 'The task name: [CARIT-PREPOT | CARIT-PREVCOND | GUESSING]', 
            'test': 'Run on a small subset of data', 
            'max_jobs': 'Number of concurrent SLURM jobs to run'})
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
    datadoer = HCPDDataDoer(c)

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

        array_job_range = f"0-{num_lines-1}" if num_lines - 1 > 0 else "0"
        # Add the job submission command with appropriate arguments to the list
        run_me += f" --array={array_job_range}%{max_jobs} sbatch_TaskfMRIAnalysis.bash {id_list_file} {short_task} {parcellated_flag}"
        
        try:
            datadoer.logger.info(f"Running {run_me}")
            result = c.run(run_me)
            datadoer.logger.info(result)
        except Exception as e:
            datadoer.logger.exception(f"Failed to run sbatch job: {e}")
      
@task(help={'task': 'The task name: [CARIT-PREPOT | CARIT-PREVCOND | GUESSING]', 
            'test': 'Run on a small subset of data', 
            'max_jobs': 'Number of concurrent SLURM jobs to run', 
            'parallel': 'Is this a parallel job? Intended for interal use.', 
            'id_list_file': 'First-level PID list file. Intended for interal use.'}) 
def extract_parcellated(c, task: str, test=False, max_jobs: int = 200, parallel=False, id_list_file=['id_list_files']):
    datadoer = HCPDDataDoer(c)
    VALID_TASKS = ["CARIT-PREPOT", "CARIT-PREVCOND", "GUESSING"]
    if task not in VALID_TASKS:
        raise ValueError(f"Task is misspecified: Please provide a valid task. Valid tasks are {VALID_TASKS}")

    if parallel:
        try:
            logger.debug(f"id_list_files = {id_list_files}")
            datadoer.extract_parcellated_parallel(c, task=task, id_list_files=id_list_files)
        except Exception as e:
            datadoer.logger.exception(f"Could not run parallel job: {e}")
    else:
        id_list_files = [ 
            f"first_level/{task}-l1-list_{i}run.txt" 
            for i in range(1, 3) 
        ]
        for id_list_file in id_list_files:
            id_list = pd.read_table(id_list_file, sep=" ", header=None, names=["pid", "task", "fsf"])
            nrow = id_list.shape[0]
            if test:
                nrow = 0
            # Batch processing logic using sbatch
            sbatch_template = f"""
{c.sbatch_header}
#SBATCH -c {c.maxcpu}
. PYTHON_MODULES.txt
. workbench-1.3.2.txt
mamba activate hcpl
invoke extract-parcellated --task {task} --parallel --id-list-file "{id_list_file}"
EOF
"""
            cmd = f"sbatch <<EOF {sbatch_template}"
            datadoer.logger.debug(f"Command:\n\n{cmd}")
            sbatch_result = c.run(cmd)
            datadoer.logger.info(f"{sbatch_result.stdout}\n{sbatch_result.stderr}")

ns = Collection(clean, build_first, extract_parcellated)
ns.configure({'log_level': "INFO", 'log_file': "invoke.log"})  
