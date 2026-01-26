# constants.py
from datetime import datetime

# Constantes de formato
dateFormatter = "%d-%m-%Y_%H-%M-%S"

# Nombres de archivos
outputPreviousImportedFileName = "OmeroImporter_previousImported.txt"
outputLogFileName = "OmeroImporter_log.txt"
outputImportedFileName = "OmeroImporter_imported.txt"
configFileFolder = "OmeroImporter"
configFileName = "OmeroImporter.cfg"
keyFileName = "OmeroImporter.key"

# Paths globales (se inicializarán en runtime)
outputLogFilePath = None
outputImportedFilePath = None

# Parámetros de configuración
p_omeroHostname = "hostName"
p_omeroPort = "port"
p_omeroUsername = "userName"
p_omeroPSW = "userPassword"
p_target = "target"
p_dest = "destination"
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
p_startTime = "startTime"
p_endTime = "endTime"
p_key = "key"

# Estados de importación
import_status = "import"
import_status_imported = "imported"
import_status_pimported = "previously imported"
import_status_found = "found"
import_status_id = "id"
import_path = "path"
import_annotate = "annotated"

# Claves de metadatos
metadata_datasets = "datasets"
metadata_images = "images"
metadata_image_name = "Image_Name"
metadata_image_new_name = "New_Image_Name"
metadata_image_path = "Image_Path"
metadata_image_mma = "MMA_File_Path"
metadata_image_tags1 = "Tags"
metadata_image_tags2 = "Τags"

# Constantes de Excel
excel_module_ome = "OMERO specific"
excel_project = 0
excel_projectName = "Project_Name"
excel_dataset = 1
excel_datasetName = "Dataset_Name"
excel_imageList = 2
excel_module = "Module"
excel_key = "Key"
excel_value = "Value"
excel_replaceNaN = "EMPTY-PD-VALUE"

# Namespace de OMERO
OMERO_NAMESPACE = "openmicroscopy.org/omero/client/mapAnnotation"