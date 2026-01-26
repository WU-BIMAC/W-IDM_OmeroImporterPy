import datetime
import os
from file_utils import printToConsole
import constants
from data_classes import EmailConfig, GlobalConfig, ImportContext
from config_utils import evalDestB2, getUserConfig
from omero_connector import switch_to_user
import omero.clients
from metadata_processor import collect_metadata
from omero_annotations import ensure_dataset, annotate_dataset, ensure_image, annotate_image, ensure_project, annotate_project
from email_utils import sendCompleteEmail

def UserFolderIteration(root_conn, iContext: ImportContext, eConfig: EmailConfig, gConfig: GlobalConfig):
    """
    Iterate over user folders and process each user's data.

    This is the main loop that switches to each user and processes their
    projects, datasets, and images.

    Args:
        root_conn: The root OMERO connection.
        iContext (ImportContext): The import context.
        eConfig (EmailConfig): The email configuration.
        gConfig (GlobalConfig): The global configuration.

    Returns:
        None

    Note:
        Each user folder corresponds to an OMERO user.
    """
    for user_folder in os.listdir(gConfig.target):
        user_full_path = os.path.join(gConfig.target, user_folder)
        if os.path.isdir(user_full_path):
            printToConsole(f"\nProcessing user: {user_folder}")
        
            # Switch to specific user
            user_conn = switch_to_user(root_conn, user_folder)
            if user_conn:
                # Add user config
                eConfig, gConfig = getUserConfig(eConfig, gConfig, user_full_path, user_conn)
                if not evalDestB2(gConfig.udestination, gConfig.uhasB2, gConfig.ub2Endpoint, gConfig.ub2BucketName, gConfig.ub2AppKeyId, gConfig.ub2AppKey, eConfig, user_folder):
                    printToConsole(f"Skipping user {user_folder}")
                    continue
                try:
                    # Process user
                    process_user(user_conn, user_folder, iContext, eConfig, gConfig)
                        
                    # Send completion email
                    sendCompleteEmail(eConfig, iContext.hasNewImport, iContext.currentImportedData[user_folder])
                    
                    # Close user connection (return to root implicitly)
                    user_conn.close()
                    printToConsole(f"User {user_folder} connection closed, returning to root")
                        
                except Exception as e:
                    printToConsole(f"Error processing user {user_folder}: {str(e)}")
                    if user_conn:
                        user_conn.close()
            else:
                printToConsole(f"Could not switch to user: {user_folder}")

def process_user(user_conn, user_folder, iContext: ImportContext, eConfig: EmailConfig, gConfig: GlobalConfig):
    """
    Process a single user's data.

    Args:
        user_conn: The OMERO connection for the user.
        user_folder (str): The user's folder name.
        iContext (ImportContext): The import context.
        eConfig (EmailConfig): The email configuration.
        gConfig (GlobalConfig): The global configuration.

    Returns:
        None

    Note:
        Collects metadata from the user's project directories and processes each project.
    """
    user_path = os.path.join(gConfig.target, user_folder)
    if not os.path.isdir(user_path):
        return
    
    printToConsole(f"Processing images for user: {user_folder}")
    iContext.currentImportedData[user_folder] = {}
    iContext.userCurrentImportedData = iContext.currentImportedData[user_folder]
    if iContext.fullImportedData != None and user_folder in iContext.fullImportedData:
        iContext.userFullImportedData = iContext.fullImportedData[user_folder]

    # Search for user projects (subdirectories)
    for project_name in os.listdir(user_path):
        project_path = os.path.join(user_path, project_name)
        if not os.path.isdir(project_path):
            continue
            
        printToConsole(f"Processing project: {project_name}")
        data = collect_metadata(project_path, eConfig)
        if data == None or data == {}:
            printToConsole(" No metadata found for project: " + project_name)
            return
        printToConsole(f"Metadata collected for project: {project_name}")
        namespace = omero.constants.metadata.NSCLIENTMAPANNOTATION
        iContext.hasNewImport = False
        iContext.currentImportedData[user_folder] = {}
        for projectKey, projectData in data.items():
            if gConfig.endTimePassed:
                printToConsole("Current time is outside the configured time window. Skipping further processing.")
                return
            process_project(
                projectKey,
                projectData,
                iContext,
                user_folder,
                user_conn,
                eConfig,
                gConfig,
                namespace
            )

def process_project(projectKey, projectData, iContext: ImportContext, userFolder, user_conn, eConfig: EmailConfig, gConfig: GlobalConfig, namespace):
    """
    Process a single project.

    Args:
        projectKey (str): The project name.
        projectData (dict): The project metadata.
        iContext (ImportContext): The import context.
        userFolder (str): The user folder name.
        user_conn: The OMERO user connection.
        eConfig (EmailConfig): The email configuration.
        gConfig (GlobalConfig): The global configuration.
        namespace (str): The namespace for map annotations.

    Returns:
        None
    """
    # Historical and current data
    iContext.projectFullImportedData = None
    if iContext.userFullImportedData is not None and projectKey in iContext.userFullImportedData:
        iContext.projectFullImportedData = iContext.userFullImportedData[projectKey]

    if projectKey not in iContext.currentImportedData[userFolder]:
        iContext.currentImportedData[userFolder][projectKey] = {}
    iContext.projectCurrentImportedData = iContext.currentImportedData[userFolder][projectKey]

    # Create or recover project
    omeProject, projectID, projectStatus = ensure_project(
        projectKey,
        userFolder,
        user_conn,
        iContext
    )

    iContext.projectCurrentImportedData[constants.import_status] = projectStatus
    iContext.projectCurrentImportedData[constants.import_status_id] = projectID

    # Annotate Project
    annotate_project(
        projectData,
        omeProject,
        projectID,
        user_conn,
        namespace,
        iContext
    )

    # Process datasets
    datasets = projectData.get(constants.metadata_datasets, {})
    for datasetKey, datasetData in datasets.items():
        if gConfig.endTimePassed:
            # printToConsole("Current time is outside the configured time window. Skipping further processing.")
            return
        process_dataset(
            datasetKey,
            datasetData,
            projectID,
            user_conn,
            eConfig,
            gConfig,
            namespace,
            iContext
        )

def process_dataset(datasetKey, datasetData, projectID, user_conn, eConfig: EmailConfig, gConfig: GlobalConfig, namespace, iContext: ImportContext):
    """
    Process a single dataset within a project.

    Args:
        datasetKey (str): The dataset name.
        datasetData (dict): The dataset metadata.
        projectID (int): The parent project ID.
        user_conn: The OMERO user connection.
        eConfig (EmailConfig): The email configuration.
        gConfig (GlobalConfig): The global configuration.
        namespace (str): The namespace for map annotations.
        iContext (ImportContext): The import context.

    Returns:
        None
    """
    iContext.datasetFullImportedData = None
    if iContext.projectFullImportedData is not None and datasetKey in iContext.projectFullImportedData:
        iContext.datasetFullImportedData = iContext.projectFullImportedData[datasetKey]

    if datasetKey not in iContext.projectCurrentImportedData:
        iContext.projectCurrentImportedData[datasetKey] = {}
    iContext.datasetCurrentImportedData = iContext.projectCurrentImportedData[datasetKey]

    projectQName = iContext.projectCurrentImportedData[constants.import_path]

    omeDataset, datasetID, datasetStatus = ensure_dataset(
        datasetKey,
        projectID,
        projectQName,
        user_conn,
        iContext
    )

    iContext.datasetCurrentImportedData[constants.import_status] = datasetStatus
    iContext.datasetCurrentImportedData[constants.import_status_id] = datasetID

    annotate_dataset(
        datasetData,
        omeDataset,
        datasetID,
        user_conn,
        namespace,
        iContext
    )

    # Image processing
    images = datasetData.get(constants.metadata_images, [])
    for image in images:
        if gConfig.endTimePassed:
            # printToConsole("Current time is outside the configured time window. Skipping further processing.")
            return
        process_image(
            image,
            datasetID,
            projectID,
            user_conn,
            eConfig,
            gConfig,
            namespace,
            iContext
        )

def process_image(image, datasetID, projectID, user_conn, eConfig, gConfig: GlobalConfig, namespace, iContext: ImportContext):
    """
    Process a single image within a dataset.

    Args:
        image (dict): The image metadata.
        datasetID (int): The parent dataset ID.
        projectID (int): The parent project ID.
        user_conn: The OMERO user connection.
        eConfig (EmailConfig): The email configuration.
        gConfig (GlobalConfig): The global configuration.
        namespace (str): The namespace for map annotations.
        iContext (ImportContext): The import context.

    Returns:
        None

    Note:
        This function handles image import, annotation, and post-import operations.
    """
    imageName = image[constants.metadata_image_name]
    imagePath = image[constants.metadata_image_path]

    imageQName = imagePath.replace(gConfig.target, "")[1:]

    iContext.imageFullImportedData = None
    if (
        iContext.datasetFullImportedData is not None
        and imageName in iContext.datasetFullImportedData
    ):
        iContext.imageFullImportedData = iContext.datasetFullImportedData[imageName]

    if imageName not in iContext.datasetCurrentImportedData:
        iContext.datasetCurrentImportedData[imageName] = {}

    iContext.imageCurrentImportedData = iContext.datasetCurrentImportedData[imageName]

    iContext.imageCurrentImportedData[constants.import_path] = imageQName

    omeImage, imageID, status = ensure_image(
        image,
        datasetID,
        projectID,
        user_conn,
        gConfig,
        eConfig,
        iContext
    )

    iContext.imageCurrentImportedData[constants.import_status] = status
    iContext.imageCurrentImportedData[constants.import_status_id] = imageID
    iContext.imageCurrentImportedData[constants.import_path] = imageQName
    
    # -----------------------------
    # Metadatos + tags
    # -----------------------------
    annotate_image(
        image,
        omeImage,
        imageID,
        user_conn,
        namespace,
        iContext
    )
    if check_time_window(gConfig):
        # printToConsole("Current time is outside the configured time window. Skipping further processing.")
        return

from datetime import datetime

def check_time_window(gConfig: GlobalConfig):
    """
    Allow execution only if current time is inside the configured time window.
    If no window is configured, execution is allowed by default.

    Returns:
        bool: True  -> execution allowed
              False -> execution NOT allowed
    """

    # Default: execution allowed
    gConfig.endTimePassed = False

    # Window not fully configured → allow execution
    if (
        gConfig.startTimeHr is None
        or gConfig.startTimeMin is None
        or gConfig.endTimeHr is None
        or gConfig.endTimeMin is None
    ):
        gConfig.endTimePassed = False
        return False

    now = datetime.now()

    # Before start → outside window
    if (
        now.hour < gConfig.startTimeHr
        or (now.hour == gConfig.startTimeHr and now.minute < gConfig.startTimeMin)
    ):
        gConfig.endTimePassed = True
        return True

    # After end → outside window
    if (
        now.hour > gConfig.endTimeHr
        or (now.hour == gConfig.endTimeHr and now.minute > gConfig.endTimeMin)
    ):
        gConfig.endTimePassed = True
        return True

    # Inside window
    return False

