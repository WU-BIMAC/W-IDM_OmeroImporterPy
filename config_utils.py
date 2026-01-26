from html import parser
import os
import pathlib
import sys
from time import time
import constants
from file_utils import printToConsole, writeConfigFile, writeToLog
from cryptography.fernet import Fernet
from data_classes import GlobalConfig, EmailConfig
from email_utils import sendErrorEmail

def readConfigFile(path):
    """
    Read and parse configuration files from directory.
    
    Reads both the key file and config file, returning the parameters
    and encryption key.
    
    Args:
        path (str): Directory path containing config files
        
    Returns:
        tuple[dict, str]: 
            - Dictionary of configuration parameters
            - Encryption key as string
            
    Raises:
        SystemExit: If configuration files are not found
    """
    config_file = os.path.join(path, constants.configFileName)
    key_file = os.path.join(path, constants.keyFileName)
    
    if not os.path.exists(config_file) or not os.path.exists(key_file):
        printToConsole("Error: Configuration files not found")
        writeToLog("ERROR: Configuration files not found")
        sys.exit(1)
        
    with open(key_file, "r") as f:
        key = f.readline().strip()
    
    params = {}
    with open(config_file, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and not line.startswith("//"):
                tokens = line.split(" = ")
                if len(tokens) == 2:
                    params[tokens[0]] = tokens[1]
    
    return params, key

def decrypt_config_values(params, key):
    """
    Decrypt sensitive configuration values using Fernet encryption.
    
    Decrypts OMERO credentials, email credentials, and Backblaze keys
    that were previously encrypted.
    
    Args:
        params (dict): Dictionary containing encrypted values
        key (str): Encryption key for decryption
        
    Returns:
        dict: Dictionary with decrypted sensitive values
        
    Raises:
        cryptography.fernet.InvalidToken: If key is invalid
        KeyError: If required keys are missing
    """
    f = Fernet(key.encode())
    decrypted_params = {}
    OMERO_CREDENTIAL_KEYS = {
    constants.p_omeroUsername,
    constants.p_omeroPSW,
    constants.p_userEmail,
    constants.p_b2_endpoint,
    constants.p_b2_bucketName,
    constants.p_b2_appKeyId,
    constants.p_b2_appKey,
    constants.p_emailFrom,
    constants.p_emailFromPSW
    }
    for key_param, value in params.items():
        if key_param in OMERO_CREDENTIAL_KEYS:
            # Decrypt the value
            decrypted_params[key_param] = f.decrypt(value.encode()).decode()
        else:
            decrypted_params[key_param] = value

    return decrypted_params

def setGlobalDataClass(config_data):
    """
    Create GlobalConfig dataclass from configuration dictionary.
    
    Maps configuration parameters to GlobalConfig attributes,
    handling both global and B2-specific configurations.
    
    Args:
        config_data (dict): Dictionary of configuration parameters
        
    Returns:
        GlobalConfig: Populated GlobalConfig instance
        
    Raises:
        KeyError: If required configuration keys are missing
    """
    thostName = config_data.get(constants.p_omeroHostname)
    tport = int(config_data.get(constants.p_omeroPort, 4064))
    tserName = config_data.get(constants.p_omeroUsername)
    tuserPSW = config_data.get(constants.p_omeroPSW)
    ttarget = config_data.get(constants.p_target)
    tdestination = config_data.get(constants.p_dest, None)
    thasDelete = config_data.get(constants.p_delete, False) == "True"
    thasMMA = config_data.get(constants.p_mma, False) == "True"
    thasB2 = config_data.get(constants.p_b2, False) == "True"
    # ---- startTime / endTime ----
    startTime = config_data.get(constants.p_startTime)
    endTime = config_data.get(constants.p_endTime)
    tstartTimeHr = tstartTimeMin = None
    tendTimeHr = tendTimeMin = None

    if startTime:
        try:
            time.strptime(startTime, "%H:%M")
        except ValueError:
            parser.error(f"Invalid time format for {constants.p_startTime}, expected hh:mm. 24-hour format")
            sys.exit(1)
        teDataSplit = startTime.split(":")
        tstartTimeHr = int(teDataSplit[0])
        tstartTimeMin = int(teDataSplit[1])

    if endTime:
        try:
            time.strptime(endTime, "%H:%M")
        except ValueError:
            parser.error(f"Invalid time format for {constants.p_endTime}, expected hh:mm. 24-hour format")
            sys.exit(1)
        teDataSplit = endTime.split(":")
        tendTimeHr = int(teDataSplit[0])
        tendTimeMin = int(teDataSplit[1])
        
    tb2Endpoint = None
    tb2BucketName = None
    tb2AppKeyId = None
    tb2AppKey = None
    if thasB2:
        tb2Endpoint = config_data.get(constants.p_b2_endpoint, None)
        tb2BucketName = config_data.get(constants.p_b2_bucketName, None)
        tb2AppKeyId = config_data.get(constants.p_b2_appKeyId, None)
        tb2AppKey = config_data.get(constants.p_b2_appKey, None)

    return GlobalConfig(
        hostName=thostName,
        port=tport,
        userName=tserName,
        userPSW=tuserPSW,
        target=ttarget,
        destination=tdestination,
        hasDelete=thasDelete,
        hasMMA=thasMMA,
        hasB2=thasB2,
        b2Endpoint=tb2Endpoint,
        b2BucketName=tb2BucketName,
        b2AppKeyId=tb2AppKeyId,
        b2AppKey=tb2AppKey,
        startTimeHr=tstartTimeHr,
        startTimeMin=tstartTimeMin,
        endTimeHr=tendTimeHr,
        endTimeMin=tendTimeMin
    )

def createGlobalConfig(args, base_path):
    """
    Create global configuration files from command line arguments.
    
    Generates encrypted configuration files for global application settings
    including OMERO connection, email settings, and optional Backblaze config.
    
    Args:
        args (argparse.Namespace): Parsed command line arguments
        base_path (str): Directory where config files will be saved
        
    Raises:
        IOError: If file creation fails
        OSError: If file creation fails
    """
    key = Fernet.generate_key()
    f = Fernet(key)
    cfg = {
        constants.p_key: key.decode(),
        constants.p_omeroHostname: args.hostname,
        constants.p_omeroPort: str(args.port),
        constants.p_target: args.target,
        constants.p_omeroUsername: f.encrypt(args.user.encode()).decode(),
        constants.p_omeroPSW: f.encrypt(args.password.encode()).decode(),
        constants.p_emailFrom: f.encrypt(args.sml.encode()).decode(),
        constants.p_emailFromPSW: f.encrypt(args.smlp.encode()).decode(),
        constants.p_adminsEmail: args.aml,
        constants.p_delete: args.delete,
        constants.p_mma: args.mma,
        constants.p_b2: False
    }

    if args.destination:
        cfg[constants.p_dest] = args.destination
    if args.b2:
        endpoint, bucket, appKeyId, appKey = args.b2.split("#")
        cfg[constants.p_b2] = True
        cfg[constants.p_b2_endpoint] = f.encrypt(endpoint.encode()).decode()
        cfg[constants.p_b2_bucketName] = f.encrypt(bucket.encode()).decode()
        cfg[constants.p_b2_appKeyId] = f.encrypt(appKeyId.encode()).decode()
        cfg[constants.p_b2_appKey] = f.encrypt(appKey.encode()).decode()

    if args.ts:
        cfg[constants.p_startTime] = args.ts
    if args.te:
        cfg[constants.p_endTime] = args.te

    writeConfigFile(base_path, cfg)
    msg = "Global configuration file generated"
    writeToLog(msg)
    printToConsole(msg)

def createUserConfig(args):
    """
    Create user-specific configuration files.
    
    Generates encrypted configuration files for individual users in their
    designated directories, overriding global settings when specified.
    
    Args:
        args (argparse.Namespace): Parsed command line arguments
            Must include userDirectory attribute
            
    Raises:
        IOError: If file creation fails
        OSError: If file creation fails
    """
    key = Fernet.generate_key()
    f = Fernet(key)

    cfg = {
        constants.p_key: key.decode(),
        constants.p_omeroUsername: f.encrypt(args.user.encode()).decode(),
        constants.p_omeroPSW: f.encrypt(args.password.encode()).decode(),
        constants.p_userEmail: f.encrypt(args.ml.encode()).decode(),
        constants.p_delete: args.delete,
        constants.p_mma: args.mma,
        constants.p_b2: False
    }

    if args.destination:
        cfg[constants.p_dest] = args.destination
    if args.b2:
        endpoint, bucket, appKeyId, appKey = args.b2.split("#")
        cfg[constants.p_b2] = True
        cfg[constants.p_b2_endpoint] = f.encrypt(endpoint.encode()).decode()
        cfg[constants.p_b2_bucketName] = f.encrypt(bucket.encode()).decode()
        cfg[constants.p_b2_appKeyId] = f.encrypt(appKeyId.encode()).decode()
        cfg[constants.p_b2_appKey] = f.encrypt(appKey.encode()).decode()

    user_dir = pathlib.Path(args.userDirectory)
    writeConfigFile(user_dir, cfg)

    msg = f"User configuration file generated in {user_dir}"
    writeToLog(msg)
    printToConsole(msg)

def getUserConfig(eConfig: EmailConfig, gConfig: GlobalConfig, user_full_path, user_conn):
    """
    Load and apply user-specific configuration.
    
    Reads user config file, decrypts values, and updates the global
    configuration and email configuration with user-specific overrides (Add the information).
    
    Args:
        eConfig (EmailConfig): Current email configuration
        gConfig (GlobalConfig): Current global configuration
        user_full_path (str): Full path to user directory
        user_conn: OMERO connection object for the user
        
    Returns:
        tuple[EmailConfig, GlobalConfig]: Updated configurations
        
    Raises:
        SystemExit: If user config files are invalid or missing
    """
    # Read user config
    uParameters_undecripted, encryption_key = readConfigFile(user_full_path)
    printToConsole("USER PARAMETERS CONFIG INIT")
    printToConsole(str(uParameters_undecripted))
    # Decrypt sensitive values ​​(username and password)
    printToConsole("DECRYPTING USER PARAMETERS CONFIG VALUES...")
    uParameters = decrypt_config_values(uParameters_undecripted, encryption_key)
    gConfig.udestination = uParameters.get(constants.p_dest, None)
    gConfig.uhasDelete = uParameters.get(constants.p_delete, False) == "True"
    gConfig.uhasMMA = uParameters.get(constants.p_mma, False) == "True"
    gConfig.uhasB2 = uParameters.get(constants.p_b2, False) == "True"
    if gConfig.uhasB2:
        gConfig.ub2Endpoint = uParameters.get(constants.p_b2_endpoint, None)
        gConfig.ub2BucketName = uParameters.get(constants.p_b2_bucketName, None)
        gConfig.ub2AppKeyId = uParameters.get(constants.p_b2_appKeyId, None)
        gConfig.ub2AppKey = uParameters.get(constants.p_b2_appKey, None)
    eConfig.emailTo = None
    if uParameters.get(constants.p_userEmail) is None:
        user = user_conn.getUser()
        eConfig.emailTo = user.getEmail()
    else:
        eConfig.emailTo = uParameters.get(constants.p_userEmail)
    printToConsole("USER PARAMETERS CONFIG DECRYPTED")
    return eConfig, gConfig

def evalDestB2(destination, hasB2, b2Endpoint, b2BucketName, b2AppKeyId, b2AppKey, eConfig: EmailConfig, userName=None):
    """
    Validate destination and Backblaze configuration.
    
    Checks that either a local destination directory exists or
    all required Backblaze B2 credentials are provided.
    
    It is not simplified to gConfig in order to evaluate global 
    and specific user configurations in the same way.
    
    Args:
        destination (str): Local destination directory path
        hasB2 (bool): Whether Backblaze B2 is enabled
        b2Endpoint (str): Backblaze B2 endpoint URL
        b2BucketName (str): Backblaze B2 bucket name
        b2AppKeyId (str): Backblaze B2 application key ID
        b2AppKey (str): Backblaze B2 application key
        eConfig (EmailConfig): Email configuration for error reporting
        userName (str, optional): User name for error messages
        
    Returns:
        bool: True if configuration is valid, False otherwise
        
    Note:
        Sends error email if validation fails
    """
    if destination is not None:
        flag = True
        error = ""
        try:
            destPath = pathlib.Path(destination).resolve()
            if not destPath.exists():
                error = ("Destination directory doesn't exists ")
                flag = False
            if not destPath.is_dir():
                error = ("Destination directory is not a directory ")
                flag = False
        except IOError as e:
            error = ("Exception trying to determine if destination directory exists and is a directory ")
            flag = False
        if not flag:
            if userName is not None:
                error = (f"User {userName}: " + error)
            writeToLog("ERROR: " + error)
            printToConsole("ERROR: " + error)
            sendErrorEmail(eConfig, error)
            return False
    if hasB2:
        print("It has B2: " + str(hasB2))
        required_fields = {
            "Backblaze endpoint": b2Endpoint,
            "Backblaze bucket name": b2BucketName,
            "Backblaze application key ID": b2AppKeyId,
            "Backblaze application key": b2AppKey,
        }
        for name, value in required_fields.items():
            if value is None:
                error = (f"{name} not defined")
                if userName is not None:
                    error = error + (f" for user {userName}")
                writeToLog("ERROR: " + error)
                printToConsole("ERROR: " + error)
                sendErrorEmail(eConfig, error)
                return False
    return True

