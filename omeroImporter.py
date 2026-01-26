# =========================
# STANDARD LIBRARY
# =========================
import os
import sys
import pathlib

# =========================
# OMEROIMPORTER LIBRARYS
# =========================
import constants
from file_utils import initFiles, printToConsole, readPreviousImportedFile
from config_utils import (createGlobalConfig, createUserConfig,
                          evalDestB2, readConfigFile, decrypt_config_values,
                          setGlobalDataClass)
from data_classes import GlobalConfig, EmailConfig, ImportContext
from cli_parser import parseArguments
from omero_connector import establishRootConnection
from import_workflow import UserFolderIteration

def main(argv):
    """
    Main entry point for OMERO Importer application.
    
    Orchestrates the complete import workflow:
    1. Initialize logging and configuration
    2. Parse command line arguments
    3. Handle configuration modes (-cfg, -ucfg)
    4. Load and decrypt global configuration
    5. Validate destination/B2 configuration
    6. Establish OMERO root connection
    7. Process each user's data
    8. Clean up and exit
    
    Args:
        argv (list): Command line arguments
        
    Returns:
        None
        
    Raises:
        SystemExit: For configuration modes or fatal errors
        Exception: For unexpected runtime errors
        
    Note:
        Supports three modes: normal execution, global config creation,
        and user config creation
    """
    # 1. Set localPath and initialize log files
    try:
        localPath = pathlib.Path(__file__).parent.resolve(strict=True)
    except FileNotFoundError:
        localPath = pathlib.Path().resolve()

    initFiles(localPath)
    printToConsole("LOG FILE INIT")

    # 2. Argument parsing
    args = parseArguments(argv[1:])

    # 3. -cfg mode - create global config and exit
    if args.mode == "cfg":
        createGlobalConfig(args, localPath)
        return

    # 4. -ucfg mode → create user config and exit (not nedded)
    if args.mode == "ucfg":
        createUserConfig(args)
        return
    
    # 5. Normal flow (without -cfg or -ucfg).
    # It is assumed that a global configuration already exists and those values ​​will be used.
    config_data_undecripted, encryption_key = readConfigFile(localPath)
    printToConsole("GLOBAL PARAMETERS CONFIG INIT")
    printToConsole(str(config_data_undecripted))

    # Decrypt sensitive values ​​(username and password)
    printToConsole("DECRYPTING GLOBAL PARAMETERS CONFIG VALUES...")
    config_data = decrypt_config_values(config_data_undecripted, encryption_key)
    printToConsole("DECRYPTED GLOBAL PARAMETERS CONFIG INIT")
    printToConsole(str(config_data))
    gConfig = setGlobalDataClass(config_data)
    eConfig = EmailConfig()
    eConfig.adminsEmailTo = config_data.get(constants.p_adminsEmail)
    eConfig.emailFrom = config_data.get(constants.p_emailFrom)
    eConfig.emailFromPSW = config_data.get(constants.p_emailFromPSW)
    printToConsole(str(gConfig))
    
    
    if not evalDestB2(gConfig.destination, gConfig.hasB2, gConfig.b2Endpoint, gConfig.b2BucketName, gConfig.b2AppKeyId, gConfig.b2AppKey, eConfig):
        sys.exit(1)
    
    # 6. Load previous imports
    iContext = ImportContext()
    iContext.fullImportedData = readPreviousImportedFile(localPath)
    iContext.currentImportedData = {}

    # 7. Generate global connection as root
    root_conn = establishRootConnection(config_data)
    print("Root connection successfully established")
    root_conn.c.enableKeepAlive(60)
    
    if not os.path.exists(gConfig.target) or not os.path.isdir(gConfig.target):
        print(f"Error: The target directory does not exist: {gConfig.target}")
        sys.exit(1)

    # 8. Process each user folder  
    UserFolderIteration(root_conn, iContext, eConfig, gConfig)

    # 4. Close root connection at the end
    printToConsole("\nClosing root connection...")
    root_conn.close()
    printToConsole("Completed successfully.")



if __name__ == "__main__":
    main(sys.argv)
