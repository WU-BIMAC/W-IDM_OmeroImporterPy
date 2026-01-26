import constants
import smtplib
import ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pathlib
#from file_utils import outputLogFilePath, outputImportedFilePath
import file_utils
from data_classes import EmailConfig

def sendErrorEmail(eConfig: EmailConfig, error: str):
    """
    Send an error email to the configured recipients.

    The email is sent to both the user (if configured) and the admins.
    The admin email includes log and imported data files as attachments.

    Args:
        eConfig (EmailConfig): The email configuration.
        error (str): The error message to include in the email body.

    Returns:
        None

    Note:
        Uses SMTP_SSL to connect to Gmail's SMTP server on port 465.
    """
    subject = "Omero Import error report"
    body = f"Omero Importer job has been terminated due to the following error:\n{error}\n"
    if eConfig.emailFrom != None and eConfig.emailFromPSW != None:
        if eConfig.emailTo:
            sendEmail(eConfig, subject, body)
        if eConfig.adminsEmailTo:
            sendAdminEmail(eConfig, subject, body, attachments=[file_utils.outputLogFilePath, file_utils.outputImportedFilePath])

def sendCompleteEmail(eConfig: EmailConfig, hasNewImport: bool, results: dict):
    """
    Send a completion email with import results.

    Summarizes the import results (what was imported/found) and sends
    to the user and admins (with attachments).

    Args:
        eConfig (EmailConfig): The email configuration.
        hasNewImport (bool): Whether new items were imported.
        results (dict): The import results to summarize.

    Returns:
        None
    """
    subject = "Omero Importer job completion report"
    body = "Omero Importer job successfully complete.\n"

    if hasNewImport:
        for projectKey, projectData in results.items():
            body += f"Project: {projectKey} {projectData[constants.import_status]}\n"
            for datasetKey, datasetData in projectData.items():
                if not isinstance(datasetData, dict):
                    continue
                body += f"Dataset: {datasetKey} {datasetData[constants.import_status]}\n"
                for imageKey, imageData in datasetData.items():
                    if not isinstance(imageData, dict):
                        continue
                    body += f"Image: {imageKey} {imageData[constants.import_status]}\n"
    else:
        body += "No new structure was created.\n"

    if eConfig.emailTo:
        sendEmail(eConfig, subject, body)
    if eConfig.adminsEmailTo:
        sendAdminEmail(eConfig, subject, body, attachments=[file_utils.outputLogFilePath, file_utils.outputImportedFilePath])

def sendEmail(eConfig: EmailConfig, subject: str, body: str):
    """
    Send a plain text email without attachments.

    Args:
        eConfig (EmailConfig): The email configuration.
        subject (str): The email subject.
        body (str): The email body.

    Returns:
        None

    Raises:
        smtplib.SMTPException: If the email cannot be sent.
    """
    body += "\n\nThis is an automatic message from an unsupervised email address, please do not reply."
    
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = eConfig.emailFrom
    if isinstance(eConfig.emailTo, str):
        message["To"] = eConfig.emailTo
    else:
        message["To"] = ", ".join(eConfig.emailTo)

    message.attach(MIMEText(body, "plain"))

    email_str = message.as_string()
    recipients = eConfig.emailTo if isinstance(eConfig.emailTo, list) else [eConfig.emailTo]

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(eConfig.emailFrom, eConfig.emailFromPSW.replace(" ", ""))
        server.sendmail(eConfig.emailFrom, recipients, email_str)

def sendAdminEmail(eConfig: EmailConfig, subject: str, body: str, attachments: list):
    """
    Send an email with attachments (intended for administrators).

    Args:
        eConfig (EmailConfig): The email configuration.
        subject (str): The email subject.
        body (str): The email body.
        attachments (list): List of file paths to attach.

    Returns:
        None

    Raises:
        smtplib.SMTPException: If the email cannot be sent.
        FileNotFoundError: If an attachment file does not exist.
    """
    body += "\n\nThis is an automatic message from an unsupervised email address, please do not reply."

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = eConfig.emailFrom
    if isinstance(eConfig.emailTo, str):
        message["To"] = eConfig.emailTo
    else:
        message["To"] = ", ".join(eConfig.emailTo)

    message.attach(MIMEText(body, "plain"))

    for filepath in attachments:
        path = pathlib.Path(filepath)
        with open(path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={path.name}")
        message.attach(part)

    email_str = message.as_string()
    recipients = eConfig.adminsEmailTo if isinstance(eConfig.adminsEmailTo, list) else [eConfig.adminsEmailTo]

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(eConfig.emailFrom, eConfig.emailFromPSW.replace(" ", ""))
        server.sendmail(eConfig.emailFrom, recipients, email_str)

