import os
import re
import shutil
import inspect
import logging
import pandas as pd
import sqlite3
import numpy as np
import pyarrow.feather as feather
from invoke import task, Collection
from multiprocessing import Process, Manager, cpu_count


class HCPDDataDoer:
    class DatabaseManager:
        """
        Manages interactions and configurations with a SQLite3 database.

        Attributes:
            outerself: An external class instance to connect to HCPDDataDoer.
            db_name (str): The name of the SQLite3 database.
            timeout (int): The time, in seconds, to wait before raising a timeout error when connecting to the database.
        """
        
        def __init__(self, outerself, db_name="file_status.db"):
            """
            Initializes the DatabaseManager with provided parameters.

            Args:
                outerself: Reference to an instance from an external class, HCPDDataDoer.
                db_name (str, optional): Name of the SQLite3 database file. Defaults to 'file_status.db'.
            """
            self.outerself = outerself
            self.db_name = db_name
            self.timeout = 20
            self.setup_database()
            

        def setup_database(self):
            """
            Sets up the SQLite3 database, creating necessary tables and columns if they do not exist.
            """
            self.outerself.logger.info(f"Connecting to {self.db_name}") 
            with sqlite3.connect(self.db_name, timeout=self.timeout) as conn:
            
                self.outerself.logger.debug(f"Checking if table exists...") 
                table_exists = conn.execute("PRAGMA table_info(files)").fetchall()

                if table_exists:
                    self.outerself.logger.debug(f"Table exists, checking columns...") 
                    columns = {column[1] for column in table_exists}

                    # Compare current columns with expected columns and add any that are missing
                    expected_columns = {'id', 'filepath', 'status', 'pid', 'session', 'task', 'data_type', 'file_type'}
                    missing_columns = expected_columns - columns
                    for col in missing_columns:
                        conn.execute(f"ALTER TABLE files ADD COLUMN {col}")
                        conn.commit()
                else:
                    # Create the 'files' table if it doesn't already exist
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

        def update_files(self, filepaths):
            #remove sentinel value
            filepaths = [f for f in filepaths if f is not None]
            if filepaths:
                parsed_infos = [self.parse_filename(filepath) for filepath in filepaths]
                statuses = ['built' if os.path.exists(filepath) else 'missing' for filepath in filepaths]

                with sqlite3.connect(self.db_name, timeout=self.timeout) as conn:
                    for filepath, status, parsed_info in zip(filepaths, statuses, parsed_infos):
                        data_to_insert = {
                            "filepath": filepath,
                            "status": status,
                            "pid": parsed_info["pid"],
                            "session": parsed_info["session"],
                            "task": parsed_info["task"],
                            "data_type": parsed_info["data_type"],
                            "file_type": parsed_info["file_type"]
                        }

                        # Check if filepath already exists
                        query = "SELECT * FROM files WHERE filepath=?"
                        df = pd.read_sql(query, conn, params=(filepath,))

                        if df.empty:
                            # Insert new record
                            columns = ', '.join(data_to_insert.keys())
                            placeholders = ', '.join('?' for _ in data_to_insert)
                            insert_query = f"INSERT INTO files ({columns}) VALUES ({placeholders})"
                            conn.execute(insert_query, tuple(data_to_insert.values()))
                        else:
                            # Update existing record
                            update_cols = ', '.join(f"{key}=?" for key in data_to_insert.keys())
                            update_query = f"UPDATE files SET {update_cols} WHERE filepath=?"
                            conn.execute(update_query, tuple(data_to_insert.values()) + (filepath,))

                    conn.commit()
        
        def get_database(self, task, file_type, data_type):
            conn = sqlite3.connect(self.db_name)

            df = pd.read_sql_query(f"""
SELECT * FROM files WHERE 
file_type IS '{file_type}' AND
task IS '{task}' AND
data_type IS '{data_type}'
""", conn)

            # Close the connection
            conn.close()
            return(df)
        
        def get_file_status(self, filepath):
            file_info = self.get_file_info(filepath)
            if file_info:
                return file_info['status']
            return None

        def get_file_info(self, filepath):
            conn = sqlite3.connect(self.db_name, timeout = self.timeout)

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

    manager = None
    db_queue = None
    db_condition = None
    db_processed_events = None
    logging_queue = None
    pdata_queue = None

    @classmethod
    def _initialize_class_attributes(cls):
        if cls.manager is None:
            cls.manager = Manager()
        if cls.db_queue is None:
            cls.db_queue = cls.manager.Queue()
        if cls.db_condition is None:
            cls.db_condition = cls.manager.Condition()
        if cls.db_processed_events is None:
            cls.db_processed_events = cls.manager.dict()
        if cls.logging_queue is None:
            cls.logging_queue = cls.manager.Queue()
        if cls.pdata_queue is None:
            cls.pdata_queue = cls.manager.Queue()
    
    def __init__(self, c, no_db = False):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        self._initialize_class_attributes()
        self.logger = self.setup_logging(log_file = c.log_file, log_level = c.log_level)
        self.logger.debug(f"Logger started, setting up database")
        self.database = None
        if not no_db:
            self.database = self.DatabaseManager(outerself=self, db_name=c.database_file)
        self.logger.debug(f"Data Doer Initialized!")

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

    def log_msg(self, msg, level):
        HCPDDataDoer.logging_queue.put((f"{inspect.stack()[1].function} - {msg}", level))
    
    def logging_process(self):
        while True:
            record = HCPDDataDoer.logging_queue.get()
            if record == 'terminate':
                break
            message, level_name = record
            log_method = getattr(self.logger, level_name)
            log_method(message)
            
    def queued_update_files(self, batch_size=50, log_interval=50):
        processed = 0
        self.log_msg(f"Queue processor started", 'info')
        while True:
            batch = []

            with HCPDDataDoer.db_condition:
                while len(batch) < batch_size:
                    if not HCPDDataDoer.db_queue.empty():
                        item = HCPDDataDoer.db_queue.get()
                        if item is None:  # Sentinel value to exit loop
                            batch.append(None)
                            break
                        batch.append(item)
                    else:
                        HCPDDataDoer.db_condition.wait(1)  # wait 1 second for more items to arrive

            if not batch:
                continue

            self.log_msg(f"Processing batch with length {len(batch)}", 'debug')
            try:
                self.database.update_files(batch)
                processed += len(batch)
            except Exception as e:
                self.log_msg(f"Failed to process batch of length {len(batch)}: {e}", 'error')
                raise Exception

            if processed % log_interval == 0:
                self.log_msg(f"Processed {processed} items. Items left in queue: {HCPDDataDoer.db_queue.qsize()}", 'info')

            if None in batch:  # check for the sentinel value
                break
      
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
        
    def extract_parcellated_chunk(self, c, task, chunk, chunk_i):
        short_task = re.match(r"^(CARIT|GUESSING).*", task)[1]
        self.log_msg(f"Chunk {chunk_i} shape: {chunk.shape}", 'info')
        for index, row in chunk.iterrows():
            pid = row['pid']
            hcp_tasks = row['hcp_task'].split("@")

            self.log_msg(f"Extracting data for {pid}, index {index} in chunk {chunk_i}: {hcp_tasks}", 'debug')
            try:
                for hcp_task in hcp_tasks:
                    d = re.match(".*_(AP|PA)$", hcp_task)[1]
                    try:
                        l1path = c.l1dir.format(studyfolder = c.studyfolder,
                                                pid = pid,
                                                task = f"tfMRI_{short_task}_{d}")
                    except Exception as e:
                        self.log_msg(f"Could not make path: {e}", 'exception')

                    parcellated_stats_dir = os.path.join(
                        l1path,
                        f"tfMRI_{task.replace('-', '_')}_{d}_hp200_s4_level1_hp0_clean_ColeAnticevic.feat",
                        "ParcellatedStats"
                    )
                    try:
                        cope_files = [
                            f for f 
                            in os.listdir(parcellated_stats_dir) 
                            if re.match("(var)*cope\d{1,2}.ptseries.nii", f)
                        ]
                        for cope in cope_files:
                            text_file = os.path.join(parcellated_stats_dir, re.sub(r"\.nii$", ".txt", cope))
                            status = self.database.get_file_status(text_file)
                            self.log_msg(f"Status is <<<{status}>>> for: {text_file}", 'debug')
                            try:
                                if not status or status == 'missing':
                                    cope_file = os.path.join(parcellated_stats_dir, cope)
                                    cmd = f"wb_command -cifti-convert -to-text {cope_file} {text_file}"
                                    self.log_msg(f"command is {cmd}", 'debug')
                                    wb_command = c.run(cmd)
                                    self.log_msg(f"wb_command output: {wb_command}", 'debug')
                                    with HCPDDataDoer.db_condition:
                                        event_for_text_file = HCPDDataDoer.manager.Event()
                                        HCPDDataDoer.db_queue.put(text_file) 
                                        HCPDDataDoer.db_condition.notify_all()
                                        self.log_msg(f"Updating database with results.", 'debug')
                                else:
                                    self.log_msg(f"file exists: {text_file}", 'debug')
                            except Exception as e:
                                self.log_msg(f"Something went wrong with {cope} with status '{status}': {e}", 'error')
                    except Exception as e:
                        self.log_msg(f"Could not extract data for {pid} {hcp_task}: {e}", 'exception')
                self.log_msg(f"Done with {pid}", 'debug')
            except Exception as e:
                self.log_msg(f"Failed to extract data for {pid}: {e}", 'exception')
        self.log_msg(f"Done with chunk {chunk_i}", 'info')
    
    def extract_parcellated_parallel(self, c, task, id_list_file, test):
        self.logger.debug(f"SLURM_CPUS_PER_TASK is {os.getenv('SLURM_CPUS_PER_TASK')}")
        NCPU = int(os.getenv('SLURM_CPUS_PER_TASK')) if os.getenv('SLURM_CPUS_PER_TASK') else cpu_count()
        
        self.logger.debug(f"Number of CPUs: {NCPU}")
        if NCPU is None:
            raise ValueError("Cannot determine the number of CPUs")
        
        df_list = []
        for id_file in id_list_file:
            df_list.append(pd.read_table(id_file, sep=" ", header=None, names=["pid", "hcp_task", "fsf"]))
        id_list = pd.concat(df_list, axis=0)
        id_list_rows = id_list.shape[0]
        self.logger.info(f"ID List df has shape: {id_list.shape}")
        
        logging_p = Process(target=self.logging_process)
        logging_p.start()
        
        db_process = Process(target=self.queued_update_files)
        db_process.start()

        # Split id_list into chunks for each process
        if test is True:
            id_list = id_list.iloc[range(0, NCPU-3),:]
        chunks = np.array_split(id_list, NCPU - 3)
        
        self.log_msg(f"Running {len(chunks)} processes.", 'info')
        
        processes = []
        for i, chunk in enumerate(chunks):
            p = Process(target=self.extract_parcellated_chunk, args=(c, task, chunk, i))
            processes.append(p)
            p.start()

        self.log_msg(f"Waiting for workers to finish", 'info')
        # Wait for all processes to finish
        for p in processes:
            p.join()
        self.log_msg(f"Workers finished, shutting down queue worker", 'info')
        
        with HCPDDataDoer.db_condition:
            HCPDDataDoer.db_queue.put(None)
            HCPDDataDoer.db_condition.notify_all()
        db_process.join()
        
        self.log_msg(f"Workers finished, shutting down log worker", 'info')
        HCPDDataDoer.logging_queue.put('terminate')
        logging_p.join()
        
        self.logger.info("All data extracted!")
        
        db_df = self.database.get_database(task, 'txt', 'Parcellated')
        db_pid_sess_rows = db_df.loc[:, ['pid', 'session']].drop_duplicates().shape[0]
        if db_pid_sess_rows != id_list_rows:
            self.logger.warning(f"Database sessions not equal to ID list sessions: {db_pid_sess_rows} v {id_list_rows}")
        
    def parse_parcellated_text_file_name(self, filename):
        filename_re = r'.*(?P<id>HCD.*)_(?P<session>V[123])_\w+.*tfMRI_.*tfMRI_(?P<scan>.*)_(?P<direction>AP|PA).*?(?P<file>(var)*cope.*)\..*series\.txt'
        match = re.match(filename_re, filename)
        
        if not match:
            return {"id": None, "session": None, "scan": None, "direction": None, "file": None}
    
        return match.groupdict()
    
    def combine_parcellated_data_chunk(self, chunk):
        dataframes_list = []
            
        self.log_msg(f"Combining data for {chunk.shape[0]} files", 'info')
        for _, row in chunk.iterrows():
            self.log_msg(f"reading file: {row['filepath']}", 'debug')
            try:
                df_temp = pd.read_table(row['filepath'], sep = " ", header=None, names=["value"])
            except Exception as e:
                self.log_msg(f"Could not read file: {e}", 'debug')

            self.log_msg(f"Parsing file name to add extra information to data table...", 'debug')
            
            try:
                filename_match_data = self.parse_parcellated_text_file_name(row['filepath'])
            except Exception as e:
                self.log_msg(f"Could not parse text file name: {e}", 'exception')
            
            for col, value in filename_match_data.items():
                df_temp[col] = value
            
            dataframes_list.append(df_temp)

        if len(dataframes_list) == 1:
            parcellated_data = dataframes_list[0]
        else:
            parcellated_data = pd.concat(dataframes_list, ignore_index=True)
            
        try:
            HCPDDataDoer.pdata_queue.put(parcellated_data, block = False)
        except Exception as e:
            self.log_msg(f"Queue full: {e}", 'exception')
        finally:
            self.log_msg(f"Resulting data chunk has {parcellated_data.shape[0]} rows", 'info')
    
    def combine_parcellated_data(self, c, task, test):
        save_file = f"parcellated-data_{task}.feather"
        self.logger.debug(f"SLURM_CPUS_PER_TASK is {os.getenv('SLURM_CPUS_PER_TASK')}")
        NCPU = int(os.getenv('SLURM_CPUS_PER_TASK')) if os.getenv('SLURM_CPUS_PER_TASK') else cpu_count()
        
        self.logger.debug(f"Number of CPUs: {NCPU}")
        if NCPU is None:
            raise ValueError("Cannot determine the number of CPUs")
        
        conn = sqlite3.connect(self.database.db_name)

        # Query the database to load the entire 'files' table into a DataFrame
        text_file_df = pd.read_sql_query(f"""
SELECT * FROM files WHERE 
file_type IS 'txt' AND
task IS '{task}' AND
data_type IS 'Parcellated'
""", conn)

        # Close the connection
        conn.close()

        if (text_file_df.shape[0] == 0  
            or not all(text_file_df.status == 'built')
            or any(text_file_df.status == 'missing')):
            #ensure everything expected to be built has been built.
            self.logger.exception(f"No data: {text_file_df.shape[0] == 0}")
            self.logger.exception(f"Not all built: {not all(text_file_df.status == 'built')}")
            self.logger.exception(f"Any missing: {any(text_file_df.status == 'missing')}")
            raise ValueError(f"Data is not all extracted. Please rerun `invoke extract-parcellated-parallel --task {task}` and check output")
        
        if test:
            text_file_df = text_file_df.iloc[range(0, NCPU - 2)]
        
        # Split id_list into chunks for each process
        chunks = np.array_split(text_file_df, NCPU - 2) #one main and one logging process
                
        logging_p = Process(target=self.logging_process)
        logging_p.start()
        
        self.log_msg(f"Running all processes...", 'debug')
        processes = []
        for chunk in chunks:
            self.log_msg(f"Sending chunk with shape {chunk.shape} to worker", 'debug')
            try:
                p = Process(target=self.combine_parcellated_data_chunk, args=(chunk,))
                processes.append(p)
                p.start()
            except Exception as e:
                self.log_msg(f"Failed to start process: {e}", 'exception')
        self.log_msg(f"Processes running...", 'debug')

        for p in processes:
            p.join()
            if p.exitcode != 0:
                self.log_msg(f"Process {p.name} terminated with exit code {p.exitcode}", 'exception')
        
        HCPDDataDoer.logging_queue.put('terminate')
        logging_p.join()
        
        self.logger.debug(f"Processes finished.")
        
        results = []
        self.logger.debug(f"Collecing results...")
        while not HCPDDataDoer.pdata_queue.empty():
            results.append(HCPDDataDoer.pdata_queue.get())
        
        self.logger.debug(f"Concatenating resulting list of length: {len(results)}")
        parcellated_data = pd.concat(results, ignore_index=True)
        self.logger.info(f"parcellated_data shape: {parcellated_data.shape}")
        try:
            feather.write_feather(parcellated_data, save_file)
            self.logger.info(f"All data combined and saved to {save_file}!")
        except Exception as e:
            self.logger.exception(f"Failed to save feather")
            
    def shutdown(self):
        pass    
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
    datadoer.shutdown()

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
    datadoer.shutdown()
      
@task(iterable=['id_list_files'],
      help={'task': 'The task name: [CARIT-PREPOT | CARIT-PREVCOND | GUESSING]', 
            'parallel': 'Is this a parallel job? Intended for interal use.', 
            'id_list_file': 'First-level PID list file. Intended for interal use.'}) 
def extract_parcellated(c, task: str, parallel=False, id_list_file=[], test=False):
    datadoer = HCPDDataDoer(c)
    VALID_TASKS = ["CARIT-PREPOT", "CARIT-PREVCOND", "GUESSING"]
    if task not in VALID_TASKS:
        raise ValueError(f"Task is misspecified: Please provide a valid task. Valid tasks are {VALID_TASKS}")

    if parallel:
        try:
            datadoer.logger.debug(f"id_list_file = {id_list_file}")
            datadoer.extract_parcellated_parallel(c, task=task, id_list_file=id_list_file, test=test)
        except Exception as e:
            datadoer.logger.exception(f"Could not run parallel job: {e}")
    else:
        id_list_file_args = ' '.join([ 
            f"--id-list-file \"first_level/{task}-l1-list_{i}run.txt\"" 
            for i in range(1, 3) 
        ])
        
        test_flag = ""
        if test is True:
            test_flag = " --test"
        invoke_parallel_cmd = f"invoke extract-parcellated --task {task} --parallel {id_list_file_args}{test_flag}"
        
        sbatch_template = f"""
{c.sbatch_header}
#SBATCH -c {c.maxcpu}
. PYTHON_MODULES.txt
. workbench-1.3.2.txt
mamba activate hcpl
{invoke_parallel_cmd}
EOF
"""
        cmd = f"sbatch <<EOF {sbatch_template}"
        datadoer.logger.debug(f"Sbatch Command:\n\n{cmd}")
        datadoer.logger.info(f"Invoke Command:\n\n{invoke_parallel_cmd}")
        sbatch_result = c.run(cmd)
        datadoer.logger.info(f"{sbatch_result.stdout}\n{sbatch_result.stderr}")
    datadoer.shutdown()

@task
def combine_parcellated_data(c, task: str, parallel=False, test=False):
    datadoer = HCPDDataDoer(c)
    VALID_TASKS = ["CARIT-PREPOT", "CARIT-PREVCOND", "GUESSING"]
    if task not in VALID_TASKS:
        raise ValueError(f"Task is misspecified: Please provide a valid task. Valid tasks are {VALID_TASKS}")
    if parallel:
        try:
            datadoer.combine_parcellated_data(c, task=task, test=test)
        except Exception as e:
            datadoer.logger.exception(f"Could not run parallel job: {e}")
    else:
        test_flag = ""
        if test is True:
            test_flag = " --test"
        invoke_parallel_cmd = f"invoke combine-parcellated-data --task {task} --parallel{test_flag}"
        sbatch_template = f"""
{c.sbatch_header}
#SBATCH -c {c.maxcpu}
. PYTHON_MODULES.txt
. workbench-1.3.2.txt
mamba activate hcpl
{invoke_parallel_cmd}
EOF
"""
        cmd = f"sbatch <<EOF {sbatch_template}"
        datadoer.logger.debug(f"Command:\n\n{cmd}")
        sbatch_result = c.run(cmd)
        datadoer.logger.info(f"{sbatch_result.stdout}\n{sbatch_result.stderr}")
    datadoer.shutdown()

@task
def cifti_thresh(c, cifti, surfaces_dir="group_level_vwise/surface", z=5, mm2=9, mm3=21):
    """
    Apply a threshold to a CIFTI file and produce associated outputs.

    Parameters:
    - c: A context or command interface object.
    - cifti: Path to the CIFTI file to threshold.
    - surfaces_dir (optional): Path to the surfaces directory. Default is "group_level_vwise/surface".
    - z (optional): z score cutoff. Default is 5.
    - mm2 (optional): Minimum surface area size. Default is 9.
    - mm3 (optional): Minimum volume size. Default is 21.

    Returns:
    None. The function will write outputs to disk and log relevant information.
    """
    
    # Convert input values to float
    z = float(z)
    mm2 = float(mm2)
    mm3 = float(mm3)
    
    # Initialize a configuration dictionary for cluster commands to write out what we did.
    clust_config_dict = {'Z': z,
                         'mm2': mm2,
                         'mm3': mm3,
                         'pos_clust_cmd': None,
                         'neg_clust_cmd': None,
                         'join_clust_cmd': None}
    
    datadoer = HCPDDataDoer(c, no_db = True)
    datadoer.logger.info(f"Creating thresholded map for {cifti}")

    try:
        if not os.path.exists(cifti):
            raise ValueError("No file found")
        cifti_base = re.match('(.*)\.dtseries\.nii', cifti)
        if cifti_base:
            cifti_base = cifti_base[1]
        else:
            raise ValueError("Cannot parse filename")
        cifti_pos_out = f"{cifti_base}_posclust.dtseries.nii"
        cifti_neg_out = f"{cifti_base}_negclust.dtseries.nii"
        cifti_out = f"{cifti_base}_clust.dtseries.nii"
        datadoer.logger.info(f"Out file is {cifti_out}")

        #Ensure we load the modules and run one big command joined by && 
        with c.prefix("module load ncf/1.0.0-fasrc01 connectome_workbench/1.5.0-centos6_x64-ncf"):
            pos_clust_cmd = f"wb_command -cifti-find-clusters {cifti} {z} {mm2} {z} {mm3} COLUMN {cifti_pos_out} -left-surface {os.path.join(surfaces_dir, 'S1200.L.inflated_MSMAll.32k_fs_LR.surf.gii')} -right-surface {os.path.join(surfaces_dir, 'S1200.R.inflated_MSMAll.32k_fs_LR.surf.gii')}"
            neg_clust_cmd = f"wb_command -cifti-find-clusters {cifti} {-z} {mm2} {-z} {mm3} COLUMN {cifti_neg_out} -less-than -left-surface {os.path.join(surfaces_dir, 'S1200.L.inflated_MSMAll.32k_fs_LR.surf.gii')} -right-surface {os.path.join(surfaces_dir, 'S1200.R.inflated_MSMAll.32k_fs_LR.surf.gii')}"
            join_clust_cmd = f"wb_command -cifti-math '100 * (x + y)' {cifti_out} -var x {cifti_pos_out} -var y {cifti_neg_out}"
            cmd_out = c.run(' && '.join([pos_clust_cmd, neg_clust_cmd, join_clust_cmd]))
        
        # Check if the clustered CIFTI file was generated and log/store the results
        if os.path.exists(cifti_out):
            datadoer.logger.info(f"Sucessfully created {cifti_out}")
            clust_config_dict['pos_clust_cmd'] = pos_clust_cmd
            clust_config_dict['neg_clust_cmd'] = neg_clust_cmd
            clust_config_dict['join_clust_cmd'] = join_clust_cmd
            pd.DataFrame([clust_config_dict]).to_csv(f"{cifti_base}_clust.csv")
        else:
            raise FileNotFoundError(f"Failed to create {cifti_out}")
    except Exception as e:
        datadoer.logger.error(f"Could not make cluster threshold map: {e}.")
    
ns = Collection(clean, build_first, extract_parcellated, combine_parcellated_data, cifti_thresh)
ns.configure({'log_level': "INFO", 'log_file': "invoke.log"})
