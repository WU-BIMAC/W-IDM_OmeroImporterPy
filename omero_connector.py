import sys
from file_utils import printToConsole
import constants
import ezomero as ezome

def establishRootConnection(config_data):
    """
    Establish a root connection to OMERO using administrator credentials.

    Args:
        config_data (dict): Configuration data containing host, port, username, and password.

    Returns:
        BlitzGateway: An active OMERO connection with root privileges.

    Raises:
        RuntimeError: If the connection cannot be established.
        Exception: For other connection errors.
    """
    hostname = config_data.get(constants.p_omeroHostname)
    port = int(config_data.get(constants.p_omeroPort, 4064))
    target_path = config_data.get(constants.p_target)
    root_username = config_data.get(constants.p_omeroUsername)  # Decrypted root user
    root_password = config_data.get(constants.p_omeroPSW)  # Decrypted root password
    if not all([hostname, target_path, root_username, root_password]):
        printToConsole("Error: Incomplete configuration")
        sys.exit(1)
    printToConsole("Connecting to OMERO as root...")
    try:
        root_conn = ezome.connect(
            host=hostname,
            port=port,
            user=root_username,
            password=root_password,
            group=0, 
            secure=True
        )
        
        if not root_conn:
            raise RuntimeError("Could not connect to OMERO")

        return root_conn
    except Exception as e:
        printToConsole(f"Error establishing root connection: {str(e)}")
        raise

def switch_to_user(root_conn, target_username):
    """
    Switch from the root connection to a specific user's context.

    Args:
        root_conn (BlitzGateway): The root connection (must have admin privileges).
        target_username (str): The username to switch to.

    Returns:
        BlitzGateway | None: A new connection as the target user, or None if the user is not found or switch fails.

    Note:
        This uses the `suConn` method to impersonate the target user.
    """
    try:
        # Verify that the root connection has administrator privileges
        if not root_conn.isFullAdmin():
            printToConsole(f"Error The root connection does not have administrator privileges")
            return None
        
        # Search for the target user in OMERO
        target_user = root_conn.getObject(
            "Experimenter", 
            attributes={"omeName": target_username}
        )
        
        if target_user is None:
            printToConsole(f"Alert User {target_username} not found in OMERO")
            return None
        
        # Perform user switch
        user_conn = root_conn.suConn(target_username)
        printToConsole(f"Successful switch from root to user: {target_username}")
        return user_conn
        
    except Exception as e:
        printToConsole(f"Error switching to user {target_username}: {str(e)}")
        return None

