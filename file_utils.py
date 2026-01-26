from datetime import datetime
import json
import os
import pathlib
import constants

outputLogFilePath = None
outputImportedFilePath = None

def initFiles(path):
    """
    Initialize log and imported files in the specified directory.
    
    Creates two files with timestamped names:
    1. Log file for recording execution events
    2. Imported data file for tracking import results
    
    Args:
        path (str): Directory path where the files will be created
        
    Raises:
        IOError: If file creation fails
        OSError: If file creation fails
    """
    now = datetime.now()
    nowFormat = now.strftime(constants.dateFormatter)

    global outputLogFilePath
    outputLogFilePath = os.path.join(path, nowFormat + "_" + constants.outputLogFileName)

    try:
        open(outputLogFilePath, "x")
    except (IOError, OSError) as e:
        printToConsole("Creating log file failed for " + outputLogFilePath)
        printToConsole(repr(e))

    global outputImportedFilePath
    outputImportedFilePath = os.path.join(
        path, nowFormat + "_" + constants.outputImportedFileName
    )

    try:
        open(outputImportedFilePath, "x")
    except (IOError, OSError) as e:
        printToConsole("Creating log file failed for " + outputImportedFilePath)
        printToConsole(repr(e))

def printToConsole(s):
    """
    Print a timestamped message to the console.
    
    Formats the message with current timestamp and prints it to stdout.
    
    Args:
        s (str): Message to print
    """
    now = datetime.now()
    nowFormat = now.strftime(constants.dateFormatter)
    print(nowFormat + " - " + s + "\n")

def writeToLog(s):
    """
    Write a timestamped message to the log file.
    
    Appends the message to the global log file with timestamp.
    If the log file cannot be opened or written, falls back to console.
    
    Args:
        s (str): Message to write to log
        
    Raises:
        IOError: If file cannot be opened
        OSError: If file cannot be opened
        FileNotFoundError: If log file path doesn't exist
        PermissionError: If insufficient permissions to write
    """
    now = datetime.now()
    nowFormat = now.strftime(constants.dateFormatter)
    try:
        with open(outputLogFilePath, "a") as f:
            try:
                f.write(nowFormat + " : " + s)
                f.write("\n")
                f.close()
            except (FileNotFoundError, PermissionError, OSError) as e:
                printToConsole("Writing to log file failed for " + outputLogFilePath)
                printToConsole(repr(e))
    except (IOError, OSError) as e:
        printToConsole("Opening log file failed for " + outputLogFilePath)
        printToConsole(repr(e))

def writeConfigFile(path, dict):
    """
    Write configuration dictionary to files.
    
    Creates two files:
    1. Key file with encryption key
    2. Config file with all other parameters
    
    Args:
        path (str): Directory path where files will be created
        dict (dict): Configuration dictionary containing all parameters
        
    Raises:
        IOError: If file creation fails
        OSError: If file creation fails
        FileNotFoundError: If path doesn't exist
        PermissionError: If insufficient permissions
    """
    configFilePath = os.path.join(path, constants.configFileName)
    keyFilePath = os.path.join(path, constants.keyFileName)
    try:
        with open(keyFilePath, "w") as f:
            try:
                f.write(str(dict[constants.p_key]))
                f.close()
            except (FileNotFoundError, PermissionError, OSError) as e:
                printToConsole("Writing key file failed for " + keyFilePath)
                printToConsole(repr(e))
    except (IOError, OSError) as e:
        printToConsole("Writing key file failed for " + keyFilePath)
        printToConsole(repr(e))
    try:
        with open(configFilePath, "w") as f:
            try:
                for key in dict:
                    if key == constants.p_key:
                        continue
                    value = dict[key]
                    f.write(str(key) + " = " + str(value))
                    f.write("\n")
                f.close()
            except (FileNotFoundError, PermissionError, OSError) as e:
                printToConsole("Writing config file failed for " + configFilePath)
                printToConsole(repr(e))
    except (IOError, OSError) as e:
        printToConsole("Writing config file failed for " + configFilePath)
        printToConsole(repr(e))

def readPreviousImportedFile(path):
    """
    Read previously imported data from JSON file.
    
    Loads the import history from a previous execution to track
    what has already been imported.
    
    Args:
        path (str): Directory path containing the imported data file
        
    Returns:
        dict | None: Dictionary of previously imported data if file exists,
                     None if file doesn't exist or cannot be read
        
    Raises:
        JSONDecodeError: If file contains invalid JSON
        IOError: If file cannot be opened
        OSError: If file cannot be opened
    """
    importedFilePath = os.path.join(path, constants.outputPreviousImportedFileName)
    if not pathlib.Path(importedFilePath).resolve().exists():
        return None
    try:
        with open(importedFilePath, "r") as f:
            try:
                data = json.load(f)
                return data
            except (FileNotFoundError, PermissionError, OSError) as e:
                message = "Reading previous imported failed for " + importedFilePath
                writeToLog("ERROR: " + message)
                writeToLog(repr(e))
                printToConsole(message)
                printToConsole(repr(e))
    except (IOError, OSError) as e:
        message = "Opening previous imported failed for " + importedFilePath
        writeToLog("ERROR: " + message)
        writeToLog(repr(e))
        printToConsole(message)
        printToConsole(repr(e))

def writeCurrentImported(dict):
    """
    Write current import data to the JSON output file.
    
    Serializes the current import state dictionary to JSON and writes it
    to the global imported file path. This file tracks what has been
    imported in the current execution.
    
    Args:
        data_dict (dict): Dictionary containing current import data
            with structure: {user: {project: {dataset: {image: {...}}}}}
            
    Raises:
        IOError: If file cannot be opened for writing
        OSError: If file cannot be opened for writing
        JSONEncodeError: If dictionary contains non-serializable data
        
    Note:
        Uses the global variable outputImportedFilePath which should be
        initialized by initFiles() before calling this function.
        Logs errors to both console and log file if writing fails.
    """
    try:
        with open(outputImportedFilePath, "w") as f:
            try:
                json.dump(dict, f)
            except (FileNotFoundError, PermissionError, OSError) as e:
                message = (
                    "Writing current imported file failed for " + outputImportedFilePath
                )
                writeToLog("ERROR: " + message)
                writeToLog(repr(e))
                printToConsole(message)
                printToConsole(repr(e))
    except (IOError, OSError) as e:
        message = "Writing current imported file failed for " + outputImportedFilePath
        writeToLog("ERROR: " + message)
        writeToLog(repr(e))
        printToConsole(message)
        printToConsole(repr(e))


def writePreviousImported(path, dict):
    """
    Write import data to the previous imports JSON file.
    
    Saves the import state dictionary to a persistent JSON file that
    tracks imports across multiple executions. This file is used to
    determine what has already been imported in previous runs.
    
    Args:
        path (str): Directory path where the file should be saved
        data_dict (dict): Dictionary containing complete import data
            from the current execution
            
    Raises:
        IOError: If file cannot be opened for writing
        OSError: If file cannot be opened for writing
        JSONEncodeError: If dictionary contains non-serializable data
        
    Note:
        The filename is determined by constants.outputPreviousImportedFileName.
        Overwrites any existing file with the same name.
        Logs errors to both console and log file if writing fails.
    """
    importedFilePath = os.path.join(path, constants.outputPreviousImportedFileName)
    try:
        with open(importedFilePath, "w") as f:
            try:
                json.dump(dict, f)
            except (FileNotFoundError, PermissionError, OSError) as e:
                message = (
                    "Writing previous imported file failed for " + importedFilePath
                )
                writeToLog("ERROR: " + message)
                writeToLog(repr(e))
                printToConsole(message)
                printToConsole(repr(e))
    except (IOError, OSError) as e:
        message = "Writing previous imported file failed for " + importedFilePath
        writeToLog("ERROR: " + message)
        writeToLog(repr(e))
        printToConsole(message)
        printToConsole(repr(e))

