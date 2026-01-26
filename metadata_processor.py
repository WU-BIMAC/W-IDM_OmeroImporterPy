import pandas as pd
import pathlib
import constants
from email_utils import sendErrorEmail
from file_utils import printToConsole, writeToLog
import xlrd
from data_classes import EmailConfig, WrappedException

def collect_metadata(projectPath, eConfig: EmailConfig):
    """
    Collect metadata from a project directory.

    This is the main entry point for metadata collection, which calls
    the Excel parsing function and handles any errors.

    Args:
        projectPath (str): Path to the project directory.
        eConfig (EmailConfig): Email configuration for error reporting.

    Returns:
        dict | None: A dictionary of metadata if successful, None otherwise.

    Raises:
        WrappedException: If metadata collection fails with a known error.
    """
    printToConsole("Collecting metadata from project path: " + projectPath)
    data = None
    try:
        data = collectMetadataFromExcel(projectPath)
    except WrappedException as e:
        error = e.message
        printToConsole("ERROR collecting metadata: " + error)
        writeToLog(error)
        writeToLog(repr(e.exception))
        sendErrorEmail(eConfig, error + "\n" + repr(e.exception))
    except Exception as e:
        printToConsole("ERROR collecting metadata: " + repr(e))
        writeToLog(repr(e))
        sendErrorEmail(eConfig, repr(e))
    return data

def collectMetadataFromExcel(path):
    """
    Collect metadata from Excel files in a directory.

    Parses Excel files with specific sheet names (project, dataset, imageList)
    and structures the data into a nested dictionary.

    Args:
        path (str): Directory path containing Excel files.

    Returns:
        dict: Structured metadata with project, dataset, and image information.

    Note:
        The Excel files are expected to have specific sheet names and header rows.
    """
    data = {}
    targetPath = pathlib.Path(path).resolve()
    for path in targetPath.iterdir():
        if path.is_dir():
            continue
        name = path.name
        if ".xlsx" in name or ".xlsm" in name:
            ssFileProjectData = pd.read_excel(
                path, sheet_name=constants.excel_project, header=8
            ).fillna(constants.excel_replaceNaN)
            ssProjectData = parseSpreadsheetData(ssFileProjectData, constants.excel_projectName)
            projectName = list(ssProjectData.keys())[0]
            if projectName not in data:
                # TODO IF PROJECT NAME CHANGED IN SAME PROJECT DIRECTORY IF OVERRIDE OVERRIDE IF NOT SEND ERROR EMAIL
                data.update(ssProjectData)
            if constants.metadata_datasets not in data[projectName]:
                data[projectName][constants.metadata_datasets] = {}
            ssFileDatasetData = pd.read_excel(
                path, sheet_name=constants.excel_dataset, header=8
            ).fillna(constants.excel_replaceNaN)
            ssDatasetData = parseSpreadsheetData(ssFileDatasetData, constants.excel_datasetName)
            datasetName = list(ssDatasetData.keys())[0]
            if datasetName not in data[projectName][constants.metadata_datasets]:
                data[projectName][constants.metadata_datasets].update(ssDatasetData)
            if constants.metadata_images not in data[projectName][constants.metadata_datasets][datasetName]:
                data[projectName][constants.metadata_datasets][datasetName][constants.metadata_images] = {}
            ssFileImageListData = pd.read_excel(
                path, sheet_name=constants.excel_imageList, header=12
            ).fillna(constants.excel_replaceNaN)
            ssImageListData = parseImageListSpreadsheetData(ssFileImageListData)
            data[projectName][constants.metadata_datasets][datasetName][
                constants.metadata_images
            ] = ssImageListData
    printToConsole("Data:")
    printToConsole(str(data))
    return data

def parseSpreadsheetData(ssData, objectNameKey):
    """
    Parse spreadsheet data for projects or datasets.

    Args:
        ssData (pd.DataFrame): DataFrame containing the spreadsheet data.
        objectNameKey (str): The column name that contains the object name (e.g., 'Project Name').

    Returns:
        dict: A dictionary with the object name as key and module data as nested dictionaries.

    Note:
        The spreadsheet data is expected to have 'Module', 'Key', and 'Value' columns.
    """
    data = {}
    objectData = {}
    module = None
    objectName = None
    modules = ssData[constants.excel_module].values
    keys = ssData[constants.excel_key].values
    values = ssData[constants.excel_value].values
    for i in range(0, len(keys)):
        if modules[i] != constants.excel_replaceNaN:
            module = modules[i]
            if not module in objectData:
                objectData[module] = {}
        key = keys[i]
        if key == constants.excel_replaceNaN:
            continue
        value = values[i]
        if value == constants.excel_replaceNaN:
            continue
            # value = "NA"
        if isinstance(value, str):
            value = value.strip()
        if key == objectNameKey:
            objectName = value
        objectData[module][key] = value
    cleanObjectData = {k: v for k, v in objectData.items() if v != None and v != {}}
    data[objectName] = cleanObjectData
    return data

def parseImageListSpreadsheetData(ssData):
    """
    Parse image list spreadsheet data.

    Args:
        ssData (pd.DataFrame): DataFrame containing the image list data.

    Returns:
        list: A list of dictionaries, each representing an image's metadata.

    Note:
        The image list sheet is expected to have a specific format with tags as comma-separated values.
    """
    data = []
    keys = ssData.columns
    size = len(ssData[keys[0]])
    for i in range(0, size):
        objectData = {}
        for key in keys:
            if key == constants.excel_replaceNaN:
                continue
            value = ssData[key][i]
            if key == constants.metadata_image_tags1 or key == constants.metadata_image_tags2:
                if value == constants.excel_replaceNaN:
                    value = []
                else:
                    values = value.split(",")
                    value = values
            if value == constants.excel_replaceNaN:
                continue
            if isinstance(value, str):
                value = value.strip()
            objectData[key] = value
        if objectData != None and objectData != {}:
            data.append(objectData)
            # data[i] = objectData
    return data

