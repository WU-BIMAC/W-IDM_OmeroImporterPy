from datetime import datetime
import pathlib
import os
import sys
import time
import json
import shutil
import re
import pandas as pd
import xlrd

# BACKBLAZE
import boto3  # REQUIRED! - Details here: https://pypi.org/project/boto3/
from botocore.exceptions import ClientError
from botocore.config import Config

# from dotenv import load_dotenv  # Project Must install Python Package:  python-dotenv

# EMAIL
import email
import smtplib
import ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# CRYPTO
from cryptography.fernet import Fernet

# OMERO
import ezomero as ezome
import omero.clients
from omero.gateway import (
    ProjectWrapper,
    DatasetWrapper,
    ImageWrapper,
    MapAnnotationWrapper,
    TagAnnotationWrapper,
)
from omero.model import (
    ProjectI,
    DatasetI,
    ImageI,
    ProjectDatasetLinkI,
)
from omero.gateway import BlitzGateway

dateFormatter = "%d-%m-%Y_%H-%M-%S"

outputPreviousImportedFileName = "OmeroImporter_previousImported.txt"
outputLogFileName = "OmeroImporter_log.txt"
# outputMetadataLogFileName = "OmeroImporter_metadata_log.txt"
outputImportedFileName = "OmeroImporter_imported.txt"
# outputMetadataFileName = "OmeroImporter_metadata.txt"
configFileFolder = "OmeroImporter"
configFileName = "OmeroImporter.cfg"
keyFileName = "OmeroImporter.key"

outputLogFilePath = None
# outputMetadataLogFilePath = None
outputImportedFilePath = None
# outputMetadataFilePath = None

parameters = None
p_omeroHostname = "hostName"
p_omeroPort = "port"
p_omeroUsername = "userName"
p_omeroPSW = "userPassword"
p_target = "target"
p_dest = "destination"
# p_headless = "headless"
p_delete = "hasDelete"
p_mma = "hasMMA"
p_b2 = "hasB2"
p_b2_endpoint = "b2Endpoint"
p_b2_bucketName = "b2BucketName"
p_b2_appKeyId = "b2AppKeyId"
p_b2_appKey = "b2AppKey"
p_userEmail = "userEmail"
p_adminsEmail = "adminsEmail"
p_emailFrom = "senderEmail"
p_emailFromPSW = "sendEmailPSW"
p_endTimeHr = "endTimeHr"
p_endTimeMin = "endTimeMin"
p_key = "key"

import_status = "import"
import_status_imported = "imported"
import_status_pimported = "previously imported"
import_status_found = "found"
import_status_id = "id"
import_path = "path"
import_annotate = "annotated"

metadata_datasets = "datasets"
metadata_images = "images"
metadata_image_name = "Image_Name"
metadata_image_new_name = "New_Image_Name"
metadata_image_path = "Image_Path"
metadata_image_tags = "Tags"

excel_module_ome = "OMERO specific"
excel_project = "Project"
excel_projectName = "Project_Name"
excel_dataset = "Dataset"
excel_datasetName = "Dataset_Name"
excel_imageList = "Image-List"
excel_module = "Module"
excel_key = "Key"
excel_value = "Value"
excel_replaceNaN = "EMPTY-PD-VALUE"


class WrappedException(Exception):
    def __init__(self, info, e):
        self.exception = e
        super().__init__(info)


# Return a boto3 client object for B2 service
def get_b2_client(endpoint, keyID, applicationKey):
    b2_client = boto3.client(
        service_name="s3",
        endpoint_url=endpoint,
        aws_access_key_id=keyID,
        aws_secret_access_key=applicationKey,
    )
    return b2_client


# Return a boto3 resource object for B2 service
def get_b2_resource(endpoint, keyID, applicationKey):
    b2 = boto3.resource(
        service_name="s3",
        endpoint_url=endpoint,
        aws_access_key_id=keyID,
        aws_secret_access_key=applicationKey,
        config=Config(
            signature_version="s3v4",
        ),
    )
    return b2


def upload_file(bucketName, filePath, fileName, b2, b2path=None):
    # filePath = directory + '/' + file
    remotePath = b2path
    if remotePath is None:
        remotePath = fileName
    else:
        remotePath = re.sub(r"\\", "/", remotePath)
    printToConsole("remotePath " + remotePath)
    try:
        response = b2.Bucket(bucketName).upload_file(filePath, remotePath)
    except ClientError as ce:
        raise
    return response


def mergeDictionaries(dict1, dict2):
    dict = deepCopyDictionary(dict1)
    mergedDict = deepMergeDictionaries(dict, dict2)
    return mergedDict


def deepCopyDictionary(dict1):
    newDict = {}
    for key in dict1:
        if isinstance(dict1[key], dict):
            newDict[key] = deepCopyDictionary(dict1[key])
        else:
            newDict[key] = dict1[key]
    return newDict


def deepMergeDictionaries(dict1, dict2):
    newDict = dict1
    for key in dict2:
        if isinstance(dict2[key], dict):
            if key in dict1:
                newDict[key] = deepMergeDictionaries(dict1[key], dict2[key])
            else:
                newDict[key] = deepCopyDictionary(dict2[key])
        else:
            newDict[key] = dict2[key]
    return newDict


def writeConfigFile(path, dict):
    configFilePath = os.path.join(path, configFileName)
    keyFilePath = os.path.join(path, keyFileName)
    try:
        with open(keyFilePath, "w") as f:
            try:
                f.write(str(dict[p_key]))
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
                    if key == p_key:
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


def readConfigFile(path):
    configFile = os.path.join(path, configFileName)
    configFilePath = pathlib.Path(configFile).resolve()
    keyFile = os.path.join(path, keyFileName)
    keyFilePath = pathlib.Path(keyFile).resolve()
    key = None
    params = {}
    if not configFilePath.exists() or not keyFilePath.exists():
        return params
    try:
        with open(keyFilePath, "r") as f:
            try:
                key = f.readline().strip()
                f.close()
                params[p_key] = key
            except (FileNotFoundError, PermissionError, OSError) as e:
                message = "Opening key file failed for " + keyFilePath
                writeToLog("ERROR: " + message)
                writeToLog(repr(e))
                printToConsole(message)
                printToConsole(repr(e))
    except (IOError, OSError) as e:
        message = "Reading key file failed for " + keyFilePath
        writeToLog("ERROR: " + message)
        writeToLog(repr(e))
        printToConsole(message)
        printToConsole(repr(e))
    try:
        with open(configFilePath, "r") as f:
            try:
                while True:
                    line = f.readline()
                    if not line:
                        break
                    data = line.strip()
                    if data.startswith("//") or data.startswith("#"):
                        continue
                    else:
                        tokens = data.split(" = ")
                        val = tokens[1]
                        params[tokens[0]] = val
                f.close()
                return params
            except (FileNotFoundError, PermissionError, OSError) as e:
                message = "Opening config file failed for " + configFilePath
                writeToLog("ERROR: " + message)
                writeToLog(repr(e))
                printToConsole(message)
                printToConsole(repr(e))
    except (IOError, OSError) as e:
        message = "Reading config file failed for " + configFilePath
        writeToLog("ERROR: " + message)
        writeToLog(repr(e))
        printToConsole(message)
        printToConsole(repr(e))


def writeCurrentImported(dict):
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
    importedFilePath = os.path.join(path, outputPreviousImportedFileName)
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


def readPreviousImportedFile(path):
    importedFilePath = os.path.join(path, outputPreviousImportedFileName)
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


def initFiles(path):
    now = datetime.now()
    nowFormat = now.strftime(dateFormatter)

    global outputLogFilePath
    outputLogFilePath = os.path.join(path, nowFormat + "_" + outputLogFileName)

    try:
        open(outputLogFilePath, "x")
    except (IOError, OSError) as e:
        printToConsole("Creating log file failed for " + outputLogFilePath)
        printToConsole(repr(e))

    # global outputMetadataLogFilePath
    # outputMetadataLogFilePath = os.path.join(
    #     path, nowFormat + "_" + outputMetadataLogFileName
    # )
    # try:
    #     open(outputMetadataLogFilePath, "x")
    # except (IOError, OSError) as e:
    #     writeToLog("Creating log file failed for " + outputMetadataLogFilePath + "\n")
    #     writeToLog(repr(e) + "\n")

    global outputImportedFilePath
    outputImportedFilePath = os.path.join(
        path, nowFormat + "_" + outputImportedFileName
    )

    try:
        open(outputImportedFilePath, "x")
    except (IOError, OSError) as e:
        printToConsole("Creating log file failed for " + outputImportedFilePath)
        printToConsole(repr(e))

    # global outputMetadataFilePath
    # outputMetadataFilePath = os.path.join(
    #     path, nowFormat + "_" + outputMetadataFileName
    # )
    # try:
    #     open(outputMetadataFilePath, "x")
    # except (IOError, OSError) as e:
    #     writeToLog("Creating log file failed for " + outputMetadataFilePath + "\n")
    #     writeToLog(repr(e) + "\n")


def printToConsole(s):
    now = datetime.now()
    nowFormat = now.strftime(dateFormatter)
    print(nowFormat + " - " + s + "\n")


def writeToLog(s):
    now = datetime.now()
    nowFormat = now.strftime(dateFormatter)
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


# def writeToMetadataLogFile(s):
#     now = datetime.now()
#     nowFormat = now.strftime(dateFormatter)
#     try:
#         with open(outputMetadataLogFilePath, "a") as f:
#             try:
#                 f.write(nowFormat + " : " + s)
#                 f.write("\n")
#                 f.close()
#             except (FileNotFoundError, PermissionError, OSError) as e:
#                 printToConsole(
#                     "Writing to log file failed for " + outputMetadataLogFilePath
#                 )
#                 printToConsole(repr(e))
#     except (IOError, OSError) as e:
#         printToConsole("Opening log file failed for " + outputMetadataLogFilePath)
#         printToConsole(repr(e))


# def writeToPreviousMetadataFile(imported):
#     now = datetime.now()
#     nowFormat = now.strftime(dateFormatter)
#     try:
#         with open(outputMetadataFilePath, "a") as f:
#             try:
#                 f.write("Files Metadata was written for:\n")
#                 for s in imported:
#                     f.write(s)
#                     f.write("\n")
#                 f.close()
#             except (FileNotFoundError, PermissionError, OSError) as e:
#                 printToConsole(
#                     "Writing to log file failed for " + outputMetadataFilePath
#                 )
#                 printToConsole(repr(e))
#     except (IOError, OSError) as e:
#         printToConsole("Opening log file failed for " + outputMetadataFilePath)
#         printToConsole(repr(e))


def sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW):
    subject = "Omero Import error report"
    text = "Omero Importer job has been terminated due to the following error:\n"
    text += error + "\n\n"
    if emailTo != None and emailFrom != None and emailFromPSW != None:
        sendEmail(emailTo, subject, text, emailFrom, emailFromPSW)
    if adminsEmailTo != None and emailFrom != None and emailFromPSW != None:
        # printToConsole("emailFrom " + str(emailFrom))
        # printToConsole("emailFromPSW " + str(emailFromPSW))
        # printToConsole("emailTo " + str(emailTo))
        sendAdminEmail(adminsEmailTo, subject, text, emailFrom, emailFromPSW)


def sendCompleteEmail(
    emailTo, adminsEmailTo, hasNewImport, results, emailFrom, emailFromPSW
):
    subject = "Omero Importer job completion report"
    text = "Omero Importer job successfully complete.\n"
    if hasNewImport:
        text += "The following structure has been created:\n"
        for projectKey in results:
            projectData = results[projectKey]
            text += "Project: " + projectKey + " " + projectData[import_status]
            if import_annotate in projectData:
                if projectData[import_status] == import_status_pimported:
                    text += " , metadata updated\n"
                else:
                    text += " , metadata written\n"
            else:
                text += "\n"
            for datasetKey in projectData:
                datasetData = projectData[datasetKey]
                if not isinstance(datasetData, dict):
                    continue
                text += "Dataset: " + datasetKey + " " + datasetData[import_status]
                if import_annotate in datasetData:
                    if datasetData[import_status] == import_status_pimported:
                        text += " , metadata updated\n"
                    else:
                        text += " , metadata written\n"
                else:
                    text += "\n"
                for imageKey in datasetData:
                    imageData = datasetData[imageKey]
                    if not isinstance(imageData, dict):
                        continue
                    text += "Image: " + imageKey + " " + imageData[import_status]
                    if import_annotate in imageData:
                        if imageData[import_status] == import_status_pimported:
                            text += " , metadata updated\n"
                        else:
                            text += " , metadata written\n"
                    else:
                        text += "\n"
    else:
        text += "No new structure was created.\n"
    text += "\n"
    if hasNewImport:
        sendEmail(emailTo, subject, text, emailFrom, emailFromPSW)
        sendAdminEmail(adminsEmailTo, subject, text, emailFrom, emailFromPSW)


def sendEmail(emailTo, subject, text, emailFrom, emailFromPSW):

    body = text
    body += "\nThis is an automatic message from an unsupervised email address, please do not reply to this mail.\n"
    body += "If you need assistance contact caterina.strambio@umassmed.edu"

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = emailFrom
    if isinstance(emailTo, str):
        message["To"] = emailTo
    else:
        message["To"] = ", ".join(emailTo)

    message.attach(MIMEText(body, "plain"))

    email = message.as_string()

    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(emailFrom, emailFromPSW)
        server.sendmail(emailFrom, emailTo, email)


def sendAdminEmail(emailTo, subject, text, emailFrom, emailFromPSW):

    body = text
    body += "\nThis is an automatic message from an unsupervised email address, please do not reply to this mail.\n"
    body += "If you need assistance contact caterina.strambio@umassmed.edu"

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = emailFrom
    if isinstance(emailTo, str):
        message["To"] = emailTo
    else:
        message["To"] = ", ".join(emailTo)

    message.attach(MIMEText(body, "plain"))

    f1 = outputLogFilePath
    f1Path = pathlib.Path(f1)
    # Open file in binary mode
    with open(f1Path, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    # Encode file in ASCII characters to send by email
    encoders.encode_base64(part)
    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {f1Path.name}",
    )
    # Add attachment to message and convert message to string
    message.attach(part)

    # f2 = outputMetadataLogFilePath
    # # Open file in binary mode
    # with open(f2, "rb") as attachment:
    #     # Add file as application/octet-stream
    #     # Email client can usually download this automatically as attachment
    #     part = MIMEBase("application", "octet-stream")
    #     part.set_payload(attachment.read())
    # # Encode file in ASCII characters to send by email
    # encoders.encode_base64(part)
    # # Add header as key/value pair to attachment part
    # part.add_header(
    #     "Content-Disposition",
    #     f"attachment; filename= {f2}",
    # )
    # # Add attachment to message and convert message to string
    # message.attach(part)

    f3 = outputImportedFilePath
    f3Path = pathlib.Path(f3)
    # Open file in binary mode
    with open(f3Path, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    # Encode file in ASCII characters to send by email
    encoders.encode_base64(part)
    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {f3Path.name}",
    )
    # Add attachment to message and convert message to string
    message.attach(part)

    # f4 = outputMetadataFilePath
    # # Open file in binary mode
    # with open(f4, "rb") as attachment:
    #     # Add file as application/octet-stream
    #     # Email client can usually download this automatically as attachment
    #     part = MIMEBase("application", "octet-stream")
    #     part.set_payload(attachment.read())
    # # Encode file in ASCII characters to send by email
    # encoders.encode_base64(part)
    # # Add header as key/value pair to attachment part
    # part.add_header(
    #     "Content-Disposition",
    #     f"attachment; filename= {f4}",
    # )
    # # Add attachment to message and convert message to string
    # message.attach(part)

    email = message.as_string()

    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(emailFrom, emailFromPSW)
        server.sendmail(emailFrom, emailTo, email)


def readCSVFile(path):
    try:
        with open(path) as f:
            try:
                dataArray = []
                dataDict = {}
                isImageListFile = False
                keys = None
                imageIndex = 0
                while True:
                    line = f.readline()
                    if not line:
                        break
                    data = line.strip()
                    if "- PROJECT" in data or "- DATASET" in data:
                        continue
                    if "- IMAGE" in data:
                        isImageListFile = True
                        continue
                    if "Key,Value" in data:
                        continue
                    if "Image_Name" in data:
                        keys = data.split(",")
                        continue

                    if not isImageListFile:
                        dataSplit = data.split(",")
                        key = dataSplit[0]
                        value = dataSplit[1]
                        dataDict[key] = value
                    else:
                        dataSplit = data.split(",")
                        i = 0
                        dataArray.append({})
                        for key in keys:
                            value = dataSplit[i]
                            i = i + 1
                            if key == metadata_image_tags:
                                if value == "":
                                    tags = []
                                else:
                                    tags = value.split("#")
                                dataArray[imageIndex][key] = tags
                            else:
                                dataArray[imageIndex][key] = value
                        imageIndex = imageIndex + 1
                f.close()
                if isImageListFile:
                    return dataArray
                return dataDict
            except Exception as e:
                raise WrappedException("Read file failed for " + repr(path), e)
    except Exception as e:
        raise WrappedException("Open file failed for " + repr(path), e)


def collectMetadataFromCSV(path):
    data = {}
    projects = {}
    datasets = {}
    images = {}
    targetPath = pathlib.Path(path).resolve()
    for path in targetPath.iterdir():
        if path.is_dir():
            continue
        name = path.name
        if ".csv" not in name:
            continue
        nameParts = name.split("#")
        parts = len(nameParts)
        if parts == 1:
            # project
            projectName = name.replace(".csv", "")
            try:
                projectData = readCSVFile(path)
            except Exception as e:
                raise
            projects[projectName] = projectData
        elif parts == 2:
            # dataset
            projectName = nameParts[0]
            datasetName = nameParts[1].replace(".csv", "")
            try:
                datasetData = readCSVFile(path)
            except Exception as e:
                raise
            if projectName not in datasets:
                datasets[projectName] = {}
            datasets[projectName][datasetName] = datasetData
        elif parts == 3:
            # image list
            projectName = nameParts[0]
            datasetName = nameParts[1]
            try:
                imageData = readCSVFile(path)
            except Exception as e:
                raise
            if projectName not in images:
                images[projectName] = {}
            # images[projectName][datasetName] = []
            images[projectName][datasetName] = imageData

    for projectKey in projects:
        data[projectKey] = projects[projectKey]
    for projectKey in datasets:
        if metadata_datasets not in data[projectKey]:
            data[projectKey][metadata_datasets] = {}
        for datasetKey in datasets[projectKey]:
            data[projectKey][metadata_datasets][datasetKey] = datasets[projectKey][
                datasetKey
            ]
    for projectKey in images:
        for datasetKey in images[projectKey]:
            if metadata_images not in data[projectKey][metadata_datasets][datasetKey]:
                data[projectKey][metadata_datasets][datasetKey][metadata_images] = {}
            data[projectKey][metadata_datasets][datasetKey][metadata_images] = images[
                projectKey
            ][datasetKey]
    printToConsole("Data:")
    printToConsole(str(data))
    return data


def parseImageListSpreadsheetData(ssData):
    data = []
    keys = ssData.columns
    size = len(ssData[keys[0]])
    for i in range(0, size):
        objectData = {}
        for key in keys:
            if key == excel_replaceNaN:
                continue
            value = ssData[key][i]
            if key == metadata_image_tags:
                if value == excel_replaceNaN:
                    value = []
                else:
                    values = value.split(",")
                    value = values
            if value == excel_replaceNaN:
                continue
            if isinstance(value, str):
                value = value.strip()
            objectData[key] = value
        if objectData != None and objectData != {}:
            data.append(objectData)
            # data[i] = objectData
    return data


def parseSpreadsheetData(ssData, objectNameKey):
    data = {}
    objectData = {}
    module = None
    objectName = None
    modules = ssData[excel_module].values
    keys = ssData[excel_key].values
    values = ssData[excel_value].values
    for i in range(0, len(keys)):
        if modules[i] != excel_replaceNaN:
            module = modules[i]
            if not module in objectData:
                objectData[module] = {}
        key = keys[i]
        if key == excel_replaceNaN:
            continue
        value = values[i]
        if value == excel_replaceNaN:
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


def collectMetadataFromExcel(path):
    data = {}
    targetPath = pathlib.Path(path).resolve()
    for path in targetPath.iterdir():
        if path.is_dir():
            continue
        name = path.name
        if ".xlsx" in name or ".xlsm" in name:
            ssFileProjectData = pd.read_excel(
                path, sheet_name=excel_project, header=9
            ).fillna(excel_replaceNaN)
            ssProjectData = parseSpreadsheetData(ssFileProjectData, excel_projectName)
            projectName = list(ssProjectData.keys())[0]
            if projectName not in data:
                # TODO IF PROJECT NAME CHANGED IN SAME PROJECT DIRECTORY IF OVERRIDE OVERRIDE IF NOT SEND ERROR EMAIL
                data.update(ssProjectData)
            if metadata_datasets not in data[projectName]:
                data[projectName][metadata_datasets] = {}
            ssFileDatasetData = pd.read_excel(
                path, sheet_name=excel_dataset, header=9
            ).fillna(excel_replaceNaN)
            ssDatasetData = parseSpreadsheetData(ssFileDatasetData, excel_datasetName)
            datasetName = list(ssDatasetData.keys())[0]
            if datasetName not in data[projectName][metadata_datasets]:
                data[projectName][metadata_datasets].update(ssDatasetData)
            if metadata_images not in data[projectName][metadata_datasets][datasetName]:
                data[projectName][metadata_datasets][datasetName][metadata_images] = {}
            ssFileImageListData = pd.read_excel(
                path, sheet_name=excel_imageList, header=12
            ).fillna(excel_replaceNaN)
            ssImageListData = parseImageListSpreadsheetData(ssFileImageListData)
            data[projectName][metadata_datasets][datasetName][
                metadata_images
            ] = ssImageListData
    printToConsole("Data:")
    printToConsole(str(data))
    return data


def main(argv, argc):
    if len(argv) > 1 and argv[1] == "-h":
        print("Help for Omero Importer CL")
        print("-cfg <options>, to create a global config file")
        print("options (* required):")
        print("*-H <hostname>")
        print("-p <port>, default is 4064")
        print("*-u <admin userName>")
        print("*-psw <admin password>")
        print("*-t <target>, target directory to launch the importer")
        print(
            "-d <destination>, destination directory where to move files after import (in this case if not specified copy does not happen)"
        )
        print("-del, to delete files after import and copy, default is false")
        print("-mma, to add microscope and acquisition settings file, default is false")
        print(
            "-b2 <endpoint#bucketName#appKeyId#appKey>, to use backblaze as destination for copy (conflict with -d)"
        )
        print(
            "-te <hh:mm>, to specify the time limit after which the application should auto terminate, default is non-stop"
        )
        print("*-sml <email address> to set up automatic email sender")
        print("*-smlp <password> to set up automatic email sender password")
        print(
            "*-aml <email address1#email address2:...> to set up automatic email to admin upon error or completion"
        )
        print("#####")
        print("*-u <user userName>")
        print("*-psw <user password>")
        print(
            "-ucfg <userDirectory> <options> to create a user config file in a specific directory, conflicting user options override global options"
        )
        print("options (* required):")
        print(
            "-d <destination>, destination directory where to move files after import (in this case if not specified copy does not happen)"
        )
        print("-del, to delete files after import and copy")
        print(
            "-b2 <endpoint#bucketName#appKeyId#appKey>, to use backblaze as destination for copy (conflict with -d)"
        )
        print("-mma, to add microscope and acquisition settings file")
        print(
            "*-ml <email address1:email address2:...> to set up automatic email upon error or completion"
        )
        quit()

    localPath = None
    try:
        localPath = pathlib.Path(__file__).parent.resolve(strict=True)
    except FileNotFoundError:
        localPath = pathlib.Path().resolve()
    initFiles(localPath)
    printToConsole("LOG FILE INIT")

    isCfg = False
    isUCfg = False

    # Both param
    destination_g = None
    hasDelete_g = False
    hasMMA_g = False
    hasB2_g = False
    b2Endpoint_g = None
    b2BucketName_g = None
    b2AppKeyId_g = None
    b2AppKey_g = None

    # Global param
    hostName = None
    port = 4064
    target = None
    endTimeHr = None
    endTimeMin = None
    adminsEmailTo = None
    emailFrom = None
    emailFromPSW = None

    # User param
    userDirectoryPath = None
    userName_g = None
    userPSW_g = None
    emailTo = None
    isAdmin = False

    for i in range(1, argc):
        arg = argv[i]
        if arg == "-cfg":
            isCfg = True
        elif arg == "-ucfg":
            isUCfg = True
            userDirectory = argv[i + 1]
            if userDirectory == None:
                error = "user directory cannot be undefined with the -ucfg option, application terminated."
                writeToLog("ERROR: " + error)
                printToConsole("ERROR: " + error)
                quit()
            try:
                userDirectoryPath = pathlib.Path(userDirectory).resolve()
                if not userDirectoryPath.exists():
                    # if not os.path.exists(tmpTarget):
                    error = (
                        "User directory "
                        + userDirectory
                        + " doesn't exists, application terminated."
                    )
                    writeToLog("ERROR: " + error)
                    printToConsole("ERROR: " + error)
                    quit()
                if not userDirectoryPath.is_dir():
                    # if not os.path.isdir(tmpTarget):
                    error = (
                        "User directory "
                        + userDirectory
                        + " is not a directory, application terminated."
                    )
                    writeToLog("ERROR: " + error)
                    printToConsole("ERROR: " + error)
                    quit()
                # target = tmpTarget
            except IOError as e:
                error = (
                    "Something went wrong trying to determine if user directory "
                    + userDirectory
                    + " exists and is a directory, application terminated."
                )
                writeToLog("ERROR: " + error)
                printToConsole("ERROR: " + error)
                quit()
        elif arg == "-H":
            hostName = argv[i + 1]
        elif arg == "-p":
            port = argv[i + 1]
        elif arg == "-u":
            userName_g = argv[i + 1]
        elif arg == "-psw":
            userPSW_g = argv[i + 1]
        elif arg == "-t":
            target = argv[i + 1]
        elif arg == "-d":
            destination_g = argv[i + 1]
        elif arg == "-del":
            hasDelete_g = True
        elif arg == "-mma":
            hasMMA_g = True
        elif arg == "-b2":
            hasB2_g = True
            b2Data = argv[i + 1]
            b2DataSplit = b2Data.split("#")
            if (len(b2DataSplit) < 4) or (len(b2DataSplit) > 4):
                error = (
                    "wrong number of arguments in -b2 option, application terminated."
                )
                writeToLog("ERROR: " + error)
                printToConsole("ERROR: " + error)
                quit()
            b2Endpoint_g = b2DataSplit[0]
            b2BucketName_g = b2DataSplit[1]
            b2AppKeyId_g = b2DataSplit[2]
            b2AppKey_g = b2DataSplit[3]
        elif arg == "-ml":
            mlData = argv[i + 1]
            mlDataSplit = mlData.split(":")
            if len(mlDataSplit) > 2:
                emailTo = mlDataSplit
            emailTo = mlData
        elif arg == "-aml":
            amlData = argv[i + 1]
            amlDataSplit = amlData.split(":")
            if len(amlDataSplit) > 2:
                adminsEmailTo = amlDataSplit
            adminsEmailTo = amlData
        elif arg == "-sml":
            emailFrom = argv[i + 1]
        elif arg == "-smlp":
            emailFromPSW = argv[i + 1]
        elif arg == "-te":
            teData = argv[i + 1]
            teDataSplit = teData.split(":")
            if (len(teDataSplit) < 2) or (len(teDataSplit) > 2):
                error = (
                    "wrong number of arguments in -te option, application terminated."
                )
                writeToLog("ERROR: " + error)
                printToConsole("ERROR: " + error)
                quit()
            endTimeHr = teDataSplit[0]
            endTimeMin = teDataSplit[1]
        else:
            if not arg.startswith("-"):
                continue
            printToConsole(
                "Option "
                + arg
                + " not recognized, please use -h to review available options, application terminated."
            )
            quit()

    if isCfg:
        dict = {}
        key = Fernet.generate_key()
        f = Fernet(key)
        dict[p_key] = key.decode()
        dict[p_omeroHostname] = hostName
        dict[p_omeroPort] = port
        dict[p_target] = target
        dict[p_omeroUsername] = f.encrypt(bytes(userName_g, "utf-8")).decode()
        dict[p_omeroPSW] = f.encrypt(bytes(userPSW_g, "utf-8")).decode()
        if destination_g != None:
            dict[p_dest] = destination_g
        if hasDelete_g:
            dict[p_delete] = hasDelete_g
        if hasMMA_g:
            dict[p_mma] = hasMMA_g
        if hasB2_g:
            dict[p_b2] = hasB2_g
            dict[p_b2_endpoint] = f.encrypt(bytes(b2Endpoint_g, "utf8")).decode()
            dict[p_b2_bucketName] = f.encrypt(bytes(b2BucketName_g, "utf8")).decode()
            dict[p_b2_appKeyId] = f.encrypt(bytes(b2AppKeyId_g, "utf8")).decode()
            dict[p_b2_appKey] = f.encrypt(bytes(b2AppKey_g, "utf8")).decode()
        # if startTime != None:
        #     dict[p_startTime] = startTime
        if endTimeHr != None and endTimeMin != None:
            dict[endTimeHr] = endTimeHr
            dict[endTimeMin] = endTimeMin
        dict[p_adminsEmail] = adminsEmailTo
        dict[p_emailFrom] = f.encrypt(bytes(emailFrom, "utf8")).decode()
        dict[p_emailFromPSW] = f.encrypt(bytes(emailFromPSW, "utf8")).decode()
        writeConfigFile(localPath, dict)
        message = "Global configuration file generated"
        writeToLog(message)
        printToConsole(message)
        quit()
    elif isUCfg:
        dict = {}
        key = Fernet.generate_key()
        f = Fernet(key)
        dict[p_key] = key.decode()
        dict[p_omeroUsername] = f.encrypt(bytes(userName_g, "utf-8")).decode()
        dict[p_omeroPSW] = f.encrypt(bytes(userPSW_g, "utf-8")).decode()
        if destination_g != None:
            dict[p_dest] = destination_g
        if hasDelete_g:
            dict[p_delete] = hasDelete_g
        if hasMMA_g:
            dict[p_mma] = hasMMA_g
        if hasB2_g:
            dict[p_b2] = hasB2_g
            dict[p_b2_endpoint] = f.encrypt(bytes(b2Endpoint_g, "utf8")).decode()
            dict[p_b2_bucketName] = f.encrypt(bytes(b2BucketName_g, "utf8")).decode()
            dict[p_b2_appKeyId] = f.encrypt(bytes(b2AppKeyId_g, "utf8")).decode()
            dict[p_b2_appKey] = f.encrypt(bytes(b2AppKey_g, "utf8")).decode()
        dict[p_userEmail] = f.encrypt(bytes(emailTo, "utf8")).decode()
        writeConfigFile(userDirectoryPath, dict)
        message = "User configuration file generated in " + userDirectory
        writeToLog(message)
        printToConsole(message)
        quit()

    # Read global parameters
    parameters = readConfigFile(localPath)
    if parameters == None:
        error = (
            "Reading global parameters from config file failed, application terminated."
        )
        writeToLog("ERROR: " + error)
        printToConsole("ERROR: " + error)
        quit()
    eKey = parameters[p_key]
    if eKey == None:
        error = "Reading encryption key for global parameters failed, application terminated."
        writeToLog("ERROR: " + error)
        printToConsole("ERROR: " + error)
        quit()
    f = Fernet(eKey)
    for key in parameters:
        if key.startswith("#"):
            continue
        value = parameters[key]
        if key == p_omeroUsername:
            userName_g = str(f.decrypt(value).decode())
            isAdmin = True
        if key == p_omeroPSW:
            userPSW_g = str(f.decrypt(value).decode())
        if key == p_omeroHostname:
            hostName = value
        if key == p_omeroPort:
            port = value
        if key == p_target:
            target = value
        if key == p_dest:
            destination_g = value
        if key == p_delete:
            hasDelete_g = value
        if key == p_mma:
            hasMMA_g = value
        if key == p_b2:
            hasB2_g = value
        if key == p_b2_endpoint:
            b2Endpoint_g = str(f.decrypt(value).decode())
        if key == p_b2_bucketName:
            b2BucketName_g = str(f.decrypt(value).decode())
        if key == p_b2_appKeyId:
            b2AppKeyId_g = str(f.decrypt(value).decode())
        if key == p_b2_appKey:
            b2AppKey_g = str(f.decrypt(value).decode())
        if key == p_adminsEmail:
            adminsEmailTo = value
        if key == p_emailFrom:
            emailFrom = str(f.decrypt(value).decode())
        if key == p_emailFromPSW:
            emailFromPSW = str(f.decrypt(value).decode())
        if key == p_endTimeHr:
            endTimeHr = value
        if key == p_endTimeMin:
            endTimeMin = value
    printToConsole("GLOBAL CONFIG READ")

    if emailFrom == None:
        error = "Automatic email sender must be set, application terminated."
        writeToLog("ERROR: " + error)
        printToConsole("ERROR: " + error)
        quit()
    if emailFromPSW == None:
        error = "Automatic email sender password must be set, application terminated."
        writeToLog("ERROR: " + error)
        printToConsole("ERROR: " + error)
        quit()

    if hostName == None:
        error = "Hostname must be set, application terminated."
        writeToLog("ERROR: " + error)
        printToConsole("ERROR: " + error)
        sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
        quit()

    portI = None
    try:
        port = int(port)
        if int(port) == port:
            portI = int(port)
    except TypeError as e:
        error = "Port is not a valid number, application terminated."
        writeToLog("ERROR: " + error)
        writeToLog(repr(e))
        printToConsole("ERROR: " + error)
        printToConsole(repr(e))
        sendErrorEmail(emailTo, adminsEmailTo, error + repr(e), emailFrom, emailFromPSW)
        quit()

    if hasB2_g and (
        (b2AppKeyId_g == None) or (b2AppKey_g == None) or (b2BucketName_g == None)
    ):
        error = "Some bucket information for backblaze backup not been specified, application terminated."
        writeToLog("ERROR: " + error)
        printToConsole("ERROR: " + error)
        sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
        quit()

    # if (endTimeHr == None) or (endTimeMin == None):
    #     error = "Hour or minute have not been specified."
    #     writeToLog("ERROR: " + error)
    #     printToConsole("ERROR: " + error)
    #     sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
    #     quit()

    endTimeHrI = None
    endTimeMinI = None
    if endTimeHr != None:
        try:
            if int(endTimeHr) == endTimeHr:
                endTimeHrI = int(endTimeHr)
        except TypeError as e:
            error = (
                "Hour value for end time is not a valid number, application terminated."
            )
            writeToLog("ERROR: " + error)
            writeToLog(repr(e))
            printToConsole("ERROR: " + error)
            printToConsole(repr(e))
            sendErrorEmail(
                emailTo, adminsEmailTo, error + repr(e), emailFrom, emailFromPSW
            )
            quit()

    if endTimeMin != None:
        try:
            if int(endTimeMin) == endTimeMin:
                endTimeMinI = int(endTimeMin)
        except TypeError as e:
            error = "Minute value for end time is not a valid number, application terminated."
            writeToLog("ERROR: " + error)
            writeToLog(repr(e))
            printToConsole("ERROR: " + error)
            printToConsole(repr(e))
            sendErrorEmail(
                emailTo, adminsEmailTo, error + repr(e), emailFrom, emailFromPSW
            )
            quit()

    if target == None:
        error = "Target directory has not been specified, application terminated."
        writeToLog("ERROR: " + error)
        printToConsole("ERROR: " + error)
        sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
        quit()
    try:
        targetPath = pathlib.Path(target).resolve()
        if not targetPath.exists():
            # if not os.path.exists(tmpTarget):
            error = "Target directory doesn't exists, application terminated."
            writeToLog("ERROR: " + error)
            printToConsole("ERROR: " + error)
            sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
            quit()
        if not targetPath.is_dir():
            # if not os.path.isdir(tmpTarget):
            error = "Target directory is not a directory, application terminated."
            writeToLog("ERROR: " + error)
            printToConsole("ERROR: " + error)
            sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
            quit()
        # target = tmpTarget
    except IOError as e:
        error = "Exception trying to determine if target directory exists and is a directory, application terminated."
        writeToLog("ERROR: " + error)
        writeToLog(repr(e))
        printToConsole("ERROR: " + error)
        printToConsole(repr(e))
        sendErrorEmail(emailTo, adminsEmailTo, error + repr(e), emailFrom, emailFromPSW)
        quit()

    if destination_g != None:
        try:
            destPath = pathlib.Path(destination_g).resolve()
            if not destPath.exists():
                error = "Destination directory doesn't exists, application terminated."
                writeToLog("ERROR: " + error)
                printToConsole("ERROR: " + error)
                sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
                quit()
            if not destPath.is_dir():
                error = (
                    "Destination directory is not a directory, application terminated."
                )
                writeToLog("ERROR: " + error)
                printToConsole("ERROR: " + error)
                sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
                quit()
            # destination = tmpDest
        except IOError as e:
            error = "Exception trying to determine if destination directory exists and is a directory, application terminated."
            writeToLog("ERROR: " + error)
            writeToLog(repr(e))
            printToConsole("ERROR: " + error)
            printToConsole(repr(e))
            sendErrorEmail(
                emailTo, adminsEmailTo, error + repr(e), emailFrom, emailFromPSW
            )
            quit()

    printToConsole("GLOBAL PARAMETERS CONFIG INIT")
    printToConsole(str(parameters))
    endTimePassed = False

    fullImportedData = readPreviousImportedFile(localPath)
    currentImportedData = {}
    targetPath = pathlib.Path(target).resolve()
    for userPath in targetPath.iterdir():
        if userPath.is_file():
            continue
        userFolder = userPath.name
        currentImportedData[userFolder] = {}
        userCurrentImportedData = currentImportedData[userFolder]
        userFullImportedData = None
        if fullImportedData != None and userFolder in fullImportedData:
            userFullImportedData = fullImportedData[userFolder]
        userName = None
        userPSW = None
        emailTo = None
        destination = None
        hasDelete = None
        hasMMA = None
        hasB2 = None
        b2Endpoint = None
        b2BucketName = None
        b2AppKeyId = None
        b2AppKey = None
        uParameters = readConfigFile(userPath)
        # Read user parameters
        # if uParameters == None:
        #     error = (
        #         "Reading user parameters from config file failed, user folder "
        #         + userPath
        #         + " skipped."
        #     )
        #     writeToLog("ERROR: " + error)
        #     printToConsole("ERROR: " + error)
        #     sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
        #     continue
        if uParameters != None and uParameters != {}:
            eKey = uParameters[p_key]
            if eKey == None:
                error = (
                    "Reading encryption key for user parameters failed, user folder "
                    + userPath
                    + " skipped."
                )
                writeToLog("ERROR: " + error)
                printToConsole("ERROR: " + error)
                sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
                continue

            f = Fernet(eKey)
            for key in uParameters:
                if key.startswith("#"):
                    continue
                value = uParameters[key]
                if key == p_omeroUsername:
                    userName = str(f.decrypt(value).decode())
                if key == p_omeroPSW:
                    userPSW = str(f.decrypt(value).decode())
                if key == p_userEmail:
                    emailTo = str(f.decrypt(value).decode())
                if key == p_dest:
                    destination = value
                if key == p_delete:
                    hasDelete = value
                if key == p_mma:
                    hasMMA = value
                if key == p_b2:
                    hasB2 = value
                if key == p_b2_endpoint:
                    b2Endpoint = str(f.decrypt(value).decode())
                if key == p_b2_bucketName:
                    b2BucketName = str(f.decrypt(value).decode())
                if key == p_b2_appKeyId:
                    b2AppKeyId = str(f.decrypt(value).decode())
                if key == p_b2_appKey:
                    b2AppKey = str(f.decrypt(value).decode())

        if userName == None:
            userName = userName_g
        if userName == None:
            error = (
                "No Omero admin or user Username has not been specified, user folder "
                + str(userPath)
                + " skipped."
            )
            writeToLog("ERROR: " + error)
            printToConsole("ERROR: " + error)
            sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
            continue

        if userPSW == None:
            userPSW = userPSW_g
        if userPSW == None:
            error = (
                "No Omero admin or user Password has not been specified, user folder "
                + str(userPath)
                + " skipped."
            )
            writeToLog("ERROR: " + error)
            printToConsole("ERROR: " + error)
            sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
            continue

        # if emailTo == None:
        #     error = (
        #         "User email has not been specified, user folder "
        #         + str(userPath)
        #         + " skipped."
        #     )
        #     writeToLog("ERROR: " + error)
        #     printToConsole("ERROR: " + error)
        #     sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
        #     continue

        if destination != None:
            try:
                destPath = pathlib.Path(destination).resolve()
                if not destPath.exists():
                    error = (
                        "User destination directory doesn't exists, user folder "
                        + str(userPath)
                        + " skipped."
                    )
                    writeToLog("ERROR: " + error)
                    printToConsole("ERROR: " + error)
                    sendErrorEmail(
                        emailTo, adminsEmailTo, error, emailFrom, emailFromPSW
                    )
                    continue
                if not destPath.is_dir():
                    error = (
                        "User destination directory is not a directory, user folder "
                        + str(userPath)
                        + " skipped."
                    )
                    writeToLog("ERROR: " + error)
                    printToConsole("ERROR: " + error)
                    sendErrorEmail(
                        emailTo, adminsEmailTo, error, emailFrom, emailFromPSW
                    )
                    continue
                # destination = tmpDest
            except IOError as e:
                error = (
                    "Exception trying to determine if user destination directory exists and is a directory, user folder "
                    + str(userPath)
                    + " skipped."
                )
                writeToLog("ERROR: " + error)
                writeToLog(repr(e))
                printToConsole("ERROR: " + error)
                printToConsole(repr(e))
                sendErrorEmail(
                    emailTo, adminsEmailTo, error + repr(e), emailFrom, emailFromPSW
                )
                continue
        else:
            destination = destination_g

        if hasDelete == None:
            hasDelete = hasDelete_g

        if hasMMA == None:
            hasMMA = hasMMA_g

        if hasB2 != None:
            if hasB2 and (
                (b2AppKeyId == None) or (b2AppKey == None) or (b2BucketName == None)
            ):
                error = (
                    "Some user bucket information for backblaze backup have not been specified, user folder "
                    + str(userPath)
                    + " skipped."
                )
                writeToLog("ERROR: " + error)
                printToConsole("ERROR: " + error)
                sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
                quit()
        else:
            hasB2 = hasB2_g
            b2Endpoint = b2Endpoint_g
            b2BucketName = b2BucketName_g
            b2AppKeyId = b2AppKeyId_g
            b2AppKey = b2AppKey_g

        printToConsole("USER PARAMETERS CONFIG INIT")
        printToConsole(str(uParameters))

        # Call function to return reference to B2 service
        b2 = None
        if hasB2:
            b2 = get_b2_resource(b2Endpoint, b2AppKeyId, b2AppKey)
        # Call function to return reference to B2 service
        # b2_client = get_b2_client(b2Endpoint, b2AppKeyId, b2AppKey)

        # conn = BlitzGateway(
        #     userName, userPSW, host=hostName, port=portI, secure=True
        # )
        # conn.connect()
        conn = ezome.connect(
            host=hostName,
            port=portI,
            user=userName,
            password=userPSW,
            group="",
            secure=True,
        )
        omeConnUser = conn.getUser()
        omeConnUserName = omeUser.getName()
        if conn == None:
            error = "Connection error"
            writeToLog("ERROR: " + error)
            printToConsole("ERROR: " + error)
            sendErrorEmail(emailTo, adminsEmailTo, error, emailFrom, emailFromPSW)
            quit()
        conn.c.enableKeepAlive(60)

        omeUserName = None
        userConn = None
        # userConn = None
        if omeConnUser.lower() != userFolder.lower() and isAdmin and conn.isFullAdmin():
            if conn.isFullAdmin():
                omeUser = conn.getObject("Experimenter", attributes={"omeName": userFolder})
                omeUserName = omeUser.getName()
                # userConn = conn.suConn(omeUser.getName())
                if emailTo == None:
                    emailTo = omeUser.getEmail()
                # message = "Admin switched to user " + omeUser.getName()
                # writeToLog(message)
                # printToConsole(message)
            else:
                error = (
                    "Cannot find user "
                    + userFolder
                    + ", current connection doesn't have proper admin rights."
                )
                printToConsole(error)
                writeToLog(error)
                continue

        # find group_id using group name ?
        # userConn.SERVICE_OPTS.setOmeroGroup(group_id)
        # session = userConn.getSession()
        # Explore User Projects
        hasNewImport = False
        for projectPath in userPath.iterdir():
            if projectPath.is_file():
                continue
            # projectCFolder = os.path.join(userFolder, projectPath.name)
            if omeUserName != None:
                userConn = conn.suConn(omeUserName)
            else:
                userConn = conn
            data = None
            namespace = omero.constants.metadata.NSCLIENTMAPANNOTATION
            try:
                data = collectMetadataFromExcel(projectPath)
            except WrappedException as e:
                error = e.message
                writeToLog(error)
                writeToLog(repr(e.exception))
                sendErrorEmail(
                    emailTo,
                    adminsEmailTo,
                    error + "\n" + repr(e.exception),
                    emailFrom,
                    emailFromPSW,
                )
            except Exception as e:
                writeToLog(error)
                writeToLog(repr(e))
                sendErrorEmail(
                    emailTo,
                    adminsEmailTo,
                    error + "\n" + repr(e),
                    emailFrom,
                    emailFromPSW,
                )
            if data == None or data == {}:
                continue
            for projectKey in data:
                project = data[projectKey]
                projectFullImportedData = None
                projectCurrentImportedData = None
                if userFullImportedData != None and projectKey in userFullImportedData:
                    projectFullImportedData = userFullImportedData[projectKey]
                if projectKey not in userCurrentImportedData:
                    currentImportedData[userFolder][projectKey] = {}
                projectCurrentImportedData = currentImportedData[userFolder][projectKey]
                projectID = None
                omeProject = None
                projectQName = os.path.join(userFolder, projectKey)
                projectCurrentImportedData[import_path] = projectQName
                if projectFullImportedData == None:
                    omeProject = userConn.getObject(
                        "Project", attributes={"name": projectKey}
                    )
                    if omeProject == None:
                        newProject = ProjectWrapper(userConn, ProjectI())
                        newProject.setName(projectKey)
                        newProject.save()
                        omeProject = newProject
                        projectCurrentImportedData[import_status] = (
                            import_status_imported
                        )
                        projectID = newProject._obj.id.val
                        writeToLog(
                            "Project created for "
                            + projectQName
                            + " ("
                            + str(projectID)
                            + ")"
                        )
                        hasNewImport = True
                    else:
                        projectID = omeProject._obj.id.val
                        projectCurrentImportedData[import_status] = import_status_found
                        writeToLog(
                            "Project found for "
                            + projectQName
                            + " ("
                            + str(projectID)
                            + ")"
                        )
                else:
                    projectID = projectFullImportedData[import_status_id]
                    omeProject = userConn.getObject("Project", projectID)
                    projectCurrentImportedData[import_status] = import_status_pimported
                    writeToLog(
                        "Project previously imported for "
                        + projectQName
                        + " ("
                        + str(projectID)
                        + ")"
                    )

                projectCurrentImportedData[import_status_id] = projectID

                # TODO should we always update the annotation? or only if not previously imported?
                # ATM only if not previously imported

                projectKeyValueData = []
                # projectKeyValueData = {}
                # for projAnnKey in project:
                #     if projAnnKey == metadata_datasets:
                #         continue
                #     projectKeyValueData.append([projAnnKey, project[projAnnKey]])
                for moduleKey in project:
                    if moduleKey == metadata_datasets or moduleKey == excel_module_ome:
                        continue
                    projectKeyValueData.append([moduleKey, ""])
                    for projAnnKey in project[moduleKey]:
                        value = project[moduleKey][projAnnKey]
                        dataSplit = None
                        if "description" not in projAnnKey.lower() and isinstance(
                            value, str
                        ):
                            dataSplit = value.split(",")
                        if dataSplit != None and len(dataSplit) > 1:
                            for i in range(0, len(dataSplit)):
                                projectKeyValueData.append(
                                    [projAnnKey + "_" + str(i), str(dataSplit[i])]
                                )
                        else:
                            projectKeyValueData.append([projAnnKey, str(value)])
                if (
                    projectFullImportedData == None
                    or import_annotate not in projectFullImportedData
                    # or projectFullImportedData[import_annotate] == False
                ):
                    if len(projectKeyValueData) > 0:
                        newProjMapAnn = MapAnnotationWrapper(userConn)
                        newProjMapAnn.setNs(namespace)
                        newProjMapAnn.setValue(projectKeyValueData)
                        # newProjMapAnn.setNs(moduleKey)
                        # newProjMapAnn.setValue(projectKeyValueData[moduleKey])
                        newProjMapAnn.save()
                        omeProject.linkAnnotation(newProjMapAnn)
                        projectCurrentImportedData[import_annotate] = (
                            newProjMapAnn._obj.id.val
                        )
                        # if not import_annotate in projectCurrentImportedData:
                        #     projectCurrentImportedData[import_annotate] = {}
                        # projectCurrentImportedData[import_annotate][
                        #     moduleKey
                        # ] = newProjMapAnn._obj.id.val
                        writeToLog(
                            "Annotation created for "
                            + projectQName
                            + " ("
                            + str(projectID)
                            + ")"
                        )
                        # writeToLog(
                        #     "Annotation created for "
                        #     + projectQName
                        #     + " - "
                        #     + moduleKey
                        #     + " ("
                        #     + str(projectID)
                        #     + ")"
                        # )
                        hasNewImport = True
                else:
                    if len(projectKeyValueData) > 0:
                        projMapAnnID = projectFullImportedData[import_annotate]
                        newProjMapAnn = userConn.getObject(
                            "MapAnnotation", projMapAnnID
                        )
                        newProjMapAnn.setValue(projectKeyValueData)
                        newProjMapAnn.save()
                    writeToLog(
                        "Annotation updated for "
                        + projectQName
                        + " ("
                        + str(projectID)
                        + ")"
                    )
                    hasNewImport = True

                if omeUserName != None:
                    userConn.close()

                for datasetKey in project[metadata_datasets]:
                    dataset = project[metadata_datasets][datasetKey]

                    if omeUserName != None:
                        userConn = conn.suConn(omeUserName)
                        # userConn.c.enableKeepAlive(60)
                    else:
                        userConn = conn

                    datasetFullImportedData = None
                    datasetCurrentImportedData = None
                    if (
                        projectFullImportedData != None
                        and datasetKey in projectFullImportedData
                    ):
                        datasetFullImportedData = projectFullImportedData[datasetKey]
                    if datasetKey not in projectCurrentImportedData:
                        projectCurrentImportedData[datasetKey] = {}
                    datasetCurrentImportedData = projectCurrentImportedData[datasetKey]
                    datasetID = None
                    omeDataset = None
                    datasetQName = os.path.join(projectQName, datasetKey)
                    datasetCurrentImportedData[import_path] = datasetQName
                    if datasetFullImportedData == None:
                        dsIDs = ezome.get_dataset_ids(userConn, project=projectID)
                        omeDataset = None
                        for dsID in dsIDs:
                            omeDS = userConn.getObject("Dataset", dsID)
                            if omeDS.getName() == datasetKey:
                                omeDataset = omeDS
                        # omeDataset = userConn.getObject(
                        #     "Dataset", attributes={"name": datasetKey}
                        # )
                        if omeDataset == None:
                            newDataset = DatasetWrapper(userConn, DatasetI())
                            newDataset.setName(datasetKey)
                            newDataset.save()
                            omeDataset = newDataset
                            datasetID = newDataset._obj.id.val
                            link = ProjectDatasetLinkI()
                            link.setChild(DatasetI(datasetID, False))
                            link.setParent(ProjectI(projectID, False))
                            userConn.getUpdateService().saveObject(link)
                            datasetCurrentImportedData[import_status] = (
                                import_status_imported
                            )
                            writeToLog(
                                "Dataset created for "
                                + datasetQName
                                + " ("
                                + str(datasetID)
                                + ")"
                            )
                            hasNewImport = True
                        else:
                            datasetID = omeDataset._obj.id.val
                            datasetCurrentImportedData[import_status] = (
                                import_status_found
                            )
                            writeToLog(
                                "Dataset found for "
                                + datasetQName
                                + " ("
                                + str(datasetID)
                                + ")"
                            )
                    else:
                        datasetID = datasetFullImportedData[import_status_id]
                        omeDataset = userConn.getObject("Dataset", datasetID)
                        datasetCurrentImportedData[import_status] = (
                            import_status_pimported
                        )
                        writeToLog(
                            "Dataset previously imported for "
                            + datasetQName
                            + " ("
                            + str(datasetID)
                            + ")"
                        )

                    datasetCurrentImportedData[import_status_id] = datasetID

                    datasetKeyValueData = []
                    # for dsAnnKey in dataset:
                    #     if dsAnnKey == metadata_images:
                    #         continue
                    #     datasetKeyValueData.append([dsAnnKey, dataset[dsAnnKey]])
                    for moduleKey in dataset:
                        if (
                            moduleKey == metadata_images
                            or moduleKey == excel_module_ome
                        ):
                            continue
                        datasetKeyValueData.append([moduleKey, ""])
                        for dsAnnKey in dataset[moduleKey]:
                            value = dataset[moduleKey][dsAnnKey]
                            dataSplit = None
                            if "description" not in dsAnnKey.lower() and isinstance(
                                value, str
                            ):
                                dataSplit = value.split(",")
                            if dataSplit != None and len(dataSplit) > 1:
                                for i in range(0, len(dataSplit)):
                                    datasetKeyValueData.append(
                                        [dsAnnKey + "_" + str(i), str(dataSplit[i])]
                                    )
                            else:
                                datasetKeyValueData.append([dsAnnKey, str(value)])
                    if (
                        datasetFullImportedData == None
                        or import_annotate not in datasetFullImportedData
                        # or datasetFullImportedData[import_annotate] == False
                    ):
                        if len(datasetKeyValueData) > 0:
                            newDsMapAnn = MapAnnotationWrapper(userConn)
                            newDsMapAnn.setNs(namespace)
                            newDsMapAnn.setValue(datasetKeyValueData)
                            newDsMapAnn.save()
                            omeDataset.linkAnnotation(newDsMapAnn)
                            datasetCurrentImportedData[import_annotate] = (
                                newDsMapAnn._obj.id.val
                            )
                            writeToLog(
                                "Annotation created for "
                                + datasetQName
                                + " ("
                                + str(datasetID)
                                + ")"
                            )
                            hasNewImport = True
                        else:
                            if len(datasetKeyValueData) > 0:
                                dsMapAnnID = datasetFullImportedData[import_annotate]
                                newDsMapAnn = userConn.getObject(
                                    "MapAnnotation", dsMapAnnID
                                )
                                newDsMapAnn.setValue(datasetKeyValueData)
                            writeToLog(
                                "Annotation updated for "
                                + datasetQName
                                + " ("
                                + str(datasetID)
                                + ")"
                            )
                            hasNewImport = True

                    if omeUserName != None:
                        userConn.close()

                    for image in dataset[metadata_images]:
                        # TODO: add time check somewhere below here

                        if omeUserName != None:
                            userConn = conn.suConn(omeUserName)
                            # userConn.c.enableKeepAlive(60)
                        else:
                            userConn = conn

                        printToConsole("image metadata")
                        printToConsole(str(image))

                        imageName = image[metadata_image_name]
                        imageNewName = image[metadata_image_new_name]
                        imagePath = image[metadata_image_path]
                        imageTags = image[metadata_image_tags]

                        # imageFolderPath = image["Image_Path"]
                        # imagePath = os.path.join(imageFolderPath, imageName)

                        imageFullImportedData = None
                        imageCurrentImportedData = None
                        if (
                            datasetFullImportedData != None
                            and imageName in datasetFullImportedData
                        ):
                            imageFullImportedData = datasetFullImportedData[imageName]
                        if imageName not in datasetCurrentImportedData:
                            datasetCurrentImportedData[imageName] = {}
                        imageCurrentImportedData = datasetCurrentImportedData[imageName]
                        imageID = None
                        omeImage = None
                        imageQName = imagePath.replace(target, "")[1:]
                        # imageQName = os.path.join(datasetQName, relImagePath)
                        imageCurrentImportedData[import_path] = imageQName
                        if imageFullImportedData == None:
                            omeImage = None
                            imgIDs = ezome.get_image_ids(userConn, dataset=datasetID)
                            for imgID in imgIDs:
                                omeImg = userConn.getObject("Image", imgID)
                                if omeImg.getName() == imageNewName:
                                    omeImage = omeImg
                            # omeImage = userConn.getObject(
                            #     "Image", attributes={"name": imageNewName}
                            # )
                            if omeImage == None:
                                imageID = ezome.ezimport(
                                    userConn,
                                    imagePath,
                                    projectID,
                                    datasetID,
                                )[0]
                                newImage = userConn.getObject("Image", imageID)
                                newImage.setName(imageNewName)
                                newImage.save()
                                omeImage = newImage
                                imageCurrentImportedData[import_status] = (
                                    import_status_imported
                                )
                                imageID = newImage._obj.id.val
                                writeToLog(
                                    "Image imported for "
                                    + imageQName
                                    + " ("
                                    + str(imageID)
                                    + ")"
                                )
                                hasNewImport = True
                                if destination != None:
                                    imageCopyPath = imagePath.replace(
                                        target, destination
                                    )
                                    imageCopyFolderPath = imageCopyPath.replace(
                                        imageName, ""
                                    )
                                    os.makedirs(imageCopyFolderPath, exist_ok=True)
                                    # TODO copy mma file here?
                                    # TODO regex imageName*.json?
                                    shutil.copy2(imagePath, imageCopyFolderPath)
                                    writeToLog("Image copied for " + imageQName)
                                if hasB2:
                                    try:
                                        response = upload_file(
                                            b2BucketName,
                                            imagePath,
                                            imageName,
                                            b2,
                                            imageQName,
                                        )
                                        printToConsole("RESPONSE:  " + str(response))
                                        # generate_friendly_url(NEW_BUCKET_NAME, endpoint, b2)
                                    except ClientError as e:
                                        error = (
                                            "Client error during backblaze upload for "
                                            + imageQName
                                        )
                                        writeToLog("ERROR: " + error)
                                        writeToLog(repr(e))
                                        printToConsole("ERROR: " + error)
                                        printToConsole(repr(e))
                                        sendErrorEmail(
                                            emailTo,
                                            adminsEmailTo,
                                            error + repr(e),
                                            emailFrom,
                                            emailFromPSW,
                                        )

                                if hasDelete:
                                    # TODO directories not removed because of CSV and MMA files?
                                    os.remove(imagePath)

                            else:
                                imageID = omeImage._obj.id.val
                                imageCurrentImportedData[import_status] = (
                                    import_status_found
                                )
                                writeToLog(
                                    "Image found for "
                                    + imageQName
                                    + " ("
                                    + str(imageID)
                                    + ")"
                                )
                        else:
                            imageID = imageFullImportedData[import_status_id]
                            omeImage = userConn.getObject("Image", imageID)
                            imageID = omeImage._obj.id.val
                            imageCurrentImportedData[import_status] = (
                                import_status_pimported
                            )
                            writeToLog(
                                "Image previously imported for "
                                + imageQName
                                + " ("
                                + str(imageID)
                                + ")"
                            )

                        imageCurrentImportedData[import_status_id] = imageID

                        imageKeyValueData = []
                        for imgAnnKey in image:
                            if (
                                imgAnnKey == metadata_image_new_name
                                or imgAnnKey == metadata_image_path
                                or imgAnnKey == metadata_image_tags
                            ):
                                continue
                            imageKeyValueData.append([imgAnnKey, str(image[imgAnnKey])])
                        if (
                            imageFullImportedData == None
                            or import_annotate not in imageFullImportedData
                            # or imageFullImportedData[import_annotate] == False
                        ):
                            if len(imageKeyValueData) > 0:
                                newImgMapAnn = MapAnnotationWrapper(userConn)
                                newImgMapAnn.setNs(namespace)
                                newImgMapAnn.setValue(imageKeyValueData)
                                newImgMapAnn.save()
                                omeImage.linkAnnotation(newImgMapAnn)
                                writeToLog(
                                    "Annotation created for "
                                    + imageQName
                                    + " ("
                                    + str(imageID)
                                    + ")"
                                )
                            if len(imageTags) > 0:
                                for imgTag in imageTags:
                                    omeTags = userConn.getObjects(
                                        "TagAnnotation",
                                        attributes={"textValue": imgTag},
                                    )
                                    omeTagAnn = None
                                    for omeTagTmp in omeTags:
                                        omeTagAnn = omeTagTmp
                                        break
                                    if omeTagAnn == None:
                                        newImgTagAnn = TagAnnotationWrapper(userConn)
                                        newImgTagAnn.setValue(imgTag)
                                        newImgTagAnn.save()
                                        omeTagAnn = newImgTagAnn
                                    omeImage.linkAnnotation(omeTagAnn)
                                writeToLog(
                                    "Tags created for "
                                    + imageQName
                                    + " ("
                                    + str(imageID)
                                    + ")"
                                )
                            imageCurrentImportedData[import_annotate] = (
                                newImgMapAnn._obj.id.val
                            )
                            hasNewImport = True
                        else:
                            if len(imageKeyValueData) > 0:
                                imgMapAnnID = imageFullImportedData[import_annotate]
                                newImgMapAnn = userConn.getObject(
                                    "MapAnnotation", imgMapAnnID
                                )
                                newImgMapAnn.setValue(imageKeyValueData)
                            writeToLog(
                                "Annotation updated for "
                                + imageQName
                                + " ("
                                + str(imageID)
                                + ")"
                            )
                            hasNewImport = True

                        if omeUserName != None:
                            userConn.close()

                        if endTimePassed:
                            break
                    if endTimePassed:
                        break
                if endTimePassed:
                    break
            if endTimePassed:
                break

        sendCompleteEmail(
            emailTo,
            adminsEmailTo,
            hasNewImport,
            currentImportedData[userFolder],
            emailFrom,
            emailFromPSW,
        )

        conn.close()
        if endTimePassed:
            break

    writeCurrentImported(currentImportedData)
    if fullImportedData != None:
        mergedImportedData = mergeDictionaries(currentImportedData, fullImportedData)
    else:
        mergedImportedData = deepCopyDictionary(currentImportedData)
    printToConsole("mergedImportedData " + str(mergedImportedData))
    writePreviousImported(localPath, mergedImportedData)


if __name__ == "__main__":
    main(sys.argv, len(sys.argv))
