from omero.gateway import (
    ProjectWrapper,
    DatasetWrapper,
    MapAnnotationWrapper,
    TagAnnotationWrapper,
)
from omero.model import (
    ProjectI,
    DatasetI,
    ProjectDatasetLinkI,
)
from file_utils import writeToLog, printToConsole
import os
import constants
import ezomero as ezome
from data_classes import GlobalConfig, EmailConfig, ImportContext
from botocore.exceptions import ClientError
from backblaze_b2_utils import get_b2_resource, upload_file
import shutil
from email_utils import sendErrorEmail

def ensure_project(projectKey, user_folder, user_conn, iContext: ImportContext):
    """
    Ensure a project exists in OMERO, creating it if necessary.

    Args:
        projectKey (str): The project name.
        user_folder (str): The user folder name (for logging).
        user_conn: The OMERO user connection.
        iContext (ImportContext): The import context for tracking.

    Returns:
        tuple[ProjectWrapper, int, str]: 
            - The project wrapper object.
            - The project ID.
            - The import status (imported, found, previously_imported).
    """
    projectQName = os.path.join(user_folder, projectKey)
    iContext.projectCurrentImportedData[constants.import_path] = projectQName

    if iContext.projectFullImportedData is None:
        omeProject = user_conn.getObject("Project", attributes={"name": projectKey})

        if omeProject is None:
            newProject = ProjectWrapper(user_conn, ProjectI())
            newProject.setName(projectKey)
            newProject.save()

            projectID = newProject._obj.id.val
            status = constants.import_status_imported
            iContext.hasNewImport = True
            writeToLog(f"Project created for {projectQName} ({projectID})")
            return newProject, projectID, status

        projectID = omeProject._obj.id.val
        writeToLog(f"Project found for {projectQName} ({projectID})")
        return omeProject, projectID, constants.import_status_found

    projectID = iContext.projectFullImportedData[constants.import_status_id]
    omeProject = user_conn.getObject("Project", projectID)
    writeToLog(f"Project previously imported for {projectQName} ({projectID})")
    return omeProject, projectID, constants.import_status_previously_imported
def annotate_project(projectData, omeProject, projectID, user_conn, namespace, iContext: ImportContext):
    """
    Annotate a project with metadata.

    Args:
        projectData (dict): The project metadata.
        omeProject (ProjectWrapper): The project wrapper.
        projectID (int): The project ID.
        user_conn: The OMERO user connection.
        namespace (str): The namespace for the map annotation.
        iContext (ImportContext): The import context for tracking.

    Returns:
        None

    Note:
        Creates a new map annotation or updates an existing one.
    """
    kvData = build_kv_metadata(
        projectData,
        skip_keys=[constants.metadata_datasets, constants.excel_module_ome]
    )

    if not kvData:
        return

    if iContext.projectFullImportedData is None or constants.import_annotate not in iContext.projectFullImportedData:
        ann = MapAnnotationWrapper(user_conn)
        ann.setNs(namespace)
        ann.setValue(kvData)
        ann.save()
        omeProject.linkAnnotation(ann)

        iContext.projectCurrentImportedData[constants.import_annotate] = ann._obj.id.val
        writeToLog(f"Annotation created for Project ({projectID})")
    else:
        annID = iContext.projectFullImportedData[constants.import_annotate]
        ann = user_conn.getObject("MapAnnotation", annID)
        ann.setValue(kvData)
        ann.save()
        writeToLog(f"Annotation updated for Project ({projectID})")

    iContext.hasNewImport = True

def ensure_dataset(datasetKey, projectID, projectQName, user_conn, iContext: ImportContext):
    """
    Ensure a dataset exists in a project, creating it if necessary.

    Args:
        datasetKey (str): The dataset name.
        projectID (int): The parent project ID.
        projectQName (str): The qualified project name (for logging).
        user_conn: The OMERO user connection.
        iContext (ImportContext): The import context for tracking.

    Returns:
        tuple[DatasetWrapper, int, str]: 
            - The dataset wrapper object.
            - The dataset ID.
            - The import status (imported, found, previously_imported).
    """
    datasetQName = os.path.join(projectQName, datasetKey)
    iContext.datasetCurrentImportedData[constants.import_path] = datasetQName
    if iContext.datasetFullImportedData is None:
        for dsID in ezome.get_dataset_ids(user_conn, project=projectID):
            omeDS = user_conn.getObject("Dataset", dsID)
            if omeDS.getName() == datasetKey:
                writeToLog(f"Dataset found for {datasetQName} ({dsID})")
                return omeDS, dsID, constants.import_status_found

        newDataset = DatasetWrapper(user_conn, DatasetI())
        newDataset.setName(datasetKey)
        newDataset.save()

        datasetID = newDataset._obj.id.val
        link = ProjectDatasetLinkI()
        link.setChild(DatasetI(datasetID, False))
        link.setParent(ProjectI(projectID, False))
        user_conn.getUpdateService().saveObject(link)

        iContext.hasNewImport = True
        writeToLog(f"Dataset created for {datasetQName} ({datasetID})")
        return newDataset, datasetID, constants.import_status_imported

    datasetID = iContext.datasetFullImportedData[constants.import_status_id]
    omeDataset = user_conn.getObject("Dataset", datasetID)
    writeToLog(f"Dataset previously imported for {datasetQName} ({datasetID})")
    return omeDataset, datasetID, constants.import_status_pimported
def annotate_dataset(datasetData, omeDataset, datasetID, user_conn, namespace, iContext: ImportContext):
    """
    Annotate a dataset with metadata.

    Args:
        datasetData (dict): The dataset metadata.
        omeDataset (DatasetWrapper): The dataset wrapper.
        datasetID (int): The dataset ID.
        user_conn: The OMERO user connection.
        namespace (str): The namespace for the map annotation.
        iContext (ImportContext): The import context for tracking.

    Returns:
        None

    Note:
        Creates a new map annotation or updates an existing one.
    """
    kvData = build_kv_metadata(datasetData, skip_keys=[constants.metadata_images, constants.excel_module_ome])
    if not kvData:
        return

    if iContext.datasetFullImportedData is None or constants.import_annotate not in iContext.datasetFullImportedData:
        ann = MapAnnotationWrapper(user_conn)
        ann.setNs(namespace)
        ann.setValue(kvData)
        ann.save()
        omeDataset.linkAnnotation(ann)
        iContext.datasetCurrentImportedData[constants.import_annotate] = ann._obj.id.val
        writeToLog(f"Annotation created for Dataset ({datasetID})")
    else:
        annID = iContext.datasetFullImportedData[constants.import_annotate]
        ann = user_conn.getObject("MapAnnotation", annID)
        ann.setValue(kvData)
        ann.save()
        writeToLog(f"Annotation updated for Dataset ({datasetID})")

    iContext.hasNewImport = True

def ensure_image(image, datasetID, projectID, user_conn, gConfig: GlobalConfig, eConfig: EmailConfig, iContext: ImportContext):
    """
    Ensure an image exists in a dataset, importing it if necessary.

    Args:
        image (dict): The image metadata.
        datasetID (int): The parent dataset ID.
        projectID (int): The parent project ID.
        user_conn: The OMERO user connection.
        gConfig (GlobalConfig): The global configuration.
        eConfig (EmailConfig): The email configuration for error reporting.
        iContext (ImportContext): The import context for tracking.

    Returns:
        tuple: (omeImage, imageID, status) where:
            - omeImage: The image wrapper object.
            - imageID (int): The image ID.
            - status (str): The import status.

    Note:
        If the image is new, it is imported using ezomero.ezimport.
    """
    imageName = image[constants.metadata_image_name]
    imageNewName = image[constants.metadata_image_new_name]
    imagePath = image[constants.metadata_image_path]
    # Ruta lógica usada para logs y post-procesamiento
    imageQName = imagePath.replace(gConfig.target, "")[1:]

    if iContext.imageFullImportedData is not None:
        imageID = iContext.imageFullImportedData[constants.import_status_id]
        omeImage = user_conn.getObject("Image", imageID)
        writeToLog(f"Image previously imported ({imageID})")
        return omeImage, imageID, constants.import_status_pimported

    for imgID in ezome.get_image_ids(user_conn, dataset=datasetID):
        omeImg = user_conn.getObject("Image", imgID)
        if omeImg.getName() == imageNewName:
            writeToLog(f"Image found ({imgID})")
            return omeImg, imgID, constants.import_status_found

    imageID = ezome.ezimport(user_conn, imagePath, projectID, datasetID)[0]
    omeImage = user_conn.getObject("Image", imageID)
    omeImage.setName(imageNewName)
    omeImage.save()

    handle_image_post_import(imagePath, imageName, imageQName, gConfig, eConfig)

    iContext.hasNewImport = True
    writeToLog(f"Image imported ({imageID})")

    return omeImage, imageID, constants.import_status_imported
def annotate_image(image, omeImage, imageID, user_conn, namespace, iContext: ImportContext):
    """
    Annotate an image with metadata and tags.

    Args:
        image (dict): The image metadata.
        omeImage: The image wrapper object.
        imageID (int): The image ID.
        user_conn: The OMERO user connection.
        namespace (str): The namespace for the map annotation.
        iContext (ImportContext): The import context for tracking.

    Returns:
        None

    Note:
        Creates map annotations for key-value pairs and links tag annotations.
    """
    imageKeyValueData = []

    for key, value in image.items():
        if key in (
            constants.metadata_image_new_name,
            constants.metadata_image_path,
            constants.metadata_image_mma,
            constants.metadata_image_tags1,
            constants.metadata_image_tags2,
        ):
            continue
        imageKeyValueData.append([key, str(value)])

    if (
        iContext.imageFullImportedData is None
        or constants.import_annotate not in iContext.imageFullImportedData
    ):
        if imageKeyValueData:
            newImgMapAnn = MapAnnotationWrapper(user_conn)
            newImgMapAnn.setNs(namespace)
            newImgMapAnn.setValue(imageKeyValueData)
            newImgMapAnn.save()
            omeImage.linkAnnotation(newImgMapAnn)

            iContext.imageCurrentImportedData[constants.import_annotate] = (
                newImgMapAnn._obj.id.val
            )

            writeToLog(
                f"Annotation created for Image ({imageID})"
            )
            iContext.hasNewImport = True
    else:
        if imageKeyValueData:
            imgMapAnnID = iContext.imageFullImportedData[constants.import_annotate]
            imgMapAnn = user_conn.getObject("MapAnnotation", imgMapAnnID)
            imgMapAnn.setValue(imageKeyValueData)
            imgMapAnn.save()

            writeToLog(
                f"Annotation updated for Image ({imageID})"
            )
            iContext.hasNewImport = True

    imageTags = []
    if constants.metadata_image_tags1 in image:
        imageTags = image[constants.metadata_image_tags1]
    elif constants.metadata_image_tags2 in image:
        imageTags = image[constants.metadata_image_tags2]

    if imageTags:
        for tag in imageTags:
            omeTags = user_conn.getObjects(
                "TagAnnotation",
                attributes={"textValue": tag}
            )
            omeTagAnn = next(iter(omeTags), None)

            if omeTagAnn is None:
                omeTagAnn = TagAnnotationWrapper(user_conn)
                omeTagAnn.setValue(tag)
                omeTagAnn.save()

            omeImage.linkAnnotation(omeTagAnn)

        writeToLog(
            f"Tags created for Image ({imageID})"
        )
        iContext.hasNewImport = True

def build_kv_metadata(data, skip_keys):
    """
    Build key-value metadata list for OMERO map annotations.

    Args:
        data (dict): The metadata dictionary for a project, dataset, or image.
        skip_keys (list): Keys to skip (e.g., 'datasets', 'images').

    Returns:
        list: A list of [key, value] pairs for map annotation.

    Note:
        Handles comma-separated values by splitting them into multiple key-value pairs.
    """
    kv = []

    for moduleKey, moduleData in data.items():
        if moduleKey in skip_keys:
            continue

        kv.append([moduleKey, ""])
        for key, value in moduleData.items():
            if isinstance(value, str) and "description" not in key.lower():
                split = value.split(",")
                if len(split) > 1:
                    for i, v in enumerate(split):
                        kv.append([f"{key}_{i}", v])
                    continue
            kv.append([key, str(value)])

    return kv

# Due to circular import, this function is located here. Before import_workflow.py
def handle_image_post_import(imagePath, imageName, imageQName, gConfig: GlobalConfig, eConfig: EmailConfig):
    """
    Handle post-import operations for an image.

    This includes copying to a local destination, uploading to Backblaze B2,
    and optionally deleting the original file.

    Args:
        imagePath (str): The local path of the original image.
        imageName (str): The image file name.
        imageQName (str): The qualified image name (for logging).
        gConfig (GlobalConfig): The global configuration.
        eConfig (EmailConfig): The email configuration for error reporting.

    Returns:
        None

    Note:
        The order of operations: local copy -> B2 upload -> delete (if enabled).
    """
    destination = gConfig.udestination if gConfig.udestination is not None else gConfig.destination

    hasB2 = gConfig.uhasB2 if gConfig.uhasB2 is not False else gConfig.hasB2
    b2BucketName = gConfig.ub2BucketName if gConfig.uhasB2 is not False else gConfig.b2BucketName
    b2Endpoint = gConfig.ub2Endpoint if gConfig.uhasB2 is not False else gConfig.b2Endpoint
    b2AppKeyId = gConfig.ub2AppKeyId if gConfig.uhasB2 is not False else gConfig.b2AppKeyId
    b2AppKey = gConfig.ub2AppKey if gConfig.uhasB2 is not False else gConfig.b2AppKey

    b2 = None
    if hasB2:
        b2 = get_b2_resource(b2Endpoint, b2AppKeyId, b2AppKey)

    hasDelete = gConfig.uhasDelete if gConfig.uhasDelete is not False else gConfig.hasDelete

    if destination is not None:
        imageCopyPath = imagePath.replace(gConfig.target, destination)
        imageCopyFolderPath = os.path.dirname(imageCopyPath)
        os.makedirs(imageCopyFolderPath, exist_ok=True)
        shutil.copy2(imagePath, imageCopyFolderPath)
        writeToLog(f"Image copied for {imageQName}")

    if hasB2:
        try:
            response = upload_file(
                b2BucketName,
                imagePath,
                imageName,
                b2,
                imageQName,
            )
            printToConsole(f"B2 upload OK: {response}")
        except ClientError as e:
            error = f"Client error during backblaze upload for {imageQName}"
            writeToLog("ERROR: " + error)
            writeToLog(repr(e))
            printToConsole("ERROR: " + error)
            printToConsole(repr(e))
            sendErrorEmail(eConfig, error + repr(e))

    if hasDelete:
        os.remove(imagePath)
        writeToLog(f"Image deleted for {imageQName}")

