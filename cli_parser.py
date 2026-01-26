import argparse
import pathlib
from file_utils import printToConsole, writeToLog
import sys
import time

def parseArguments(argv=None):
    """
    Parse command line arguments for OMERO Importer.
    
    Supports three modes:
    1. No arguments: Normal execution with existing config
    2. -cfg: Create global configuration
    3. -ucfg: Create user configuration
    
    Args:
        argv (list, optional): Command line arguments. Defaults to sys.argv[1:]
        
    Returns:
        argparse.Namespace: Parsed arguments with additional mode attribute
        
    Raises:
        SystemExit: If arguments are invalid or missing required options
    """
    parser = argparse.ArgumentParser(
        description="Help for Omero Importer CL",
        add_help=True
    )

    # -----------------------------------------
    # Exclusionary group: -cfg y -ucfg
    # -----------------------------------------
    mode_group = parser.add_mutually_exclusive_group()

    mode_group.add_argument(
        "-cfg",
        action="store_true",
        help="<options>, to create a global config file. options required -H -p -u -psw -t"
    )

    mode_group.add_argument(
        "-ucfg",
        nargs=1,   # requiere un <userDirectory>
        metavar="userDirectory",
        help="<options> to create a user config file in a specific directory, conflicting user options override global options. options required -u -psw -ml"
    )

    # -----------------------------------------
    # Arguments exclusive to the -cfg mode
    # -----------------------------------------
    parser.add_argument("-H", "--hostname", help="OMERO host")
    parser.add_argument("-p", "--port", type=int, default=4064, help="OMERO port (default 4064)")
    parser.add_argument("-t", "--target", help="Target directory for importer")

    parser.add_argument("-ts", help="Daily start time hh:mm (default non-stop)")
    parser.add_argument("-te", help="Daily end time hh:mm (default non-stop)")

    parser.add_argument("-sml", help="Sender email")
    parser.add_argument("-smlp", help="Sender email password")
    parser.add_argument("-aml", help="Admin email list (address1:address2...)")

    # -----------------------------------------
    # Arguments common to both modes
    # -----------------------------------------
    parser.add_argument("-u", "--user", help="User name (admin for -cfg)")
    parser.add_argument("-psw", "--password", help="Password")
    parser.add_argument("-d", "--destination", help="Destination directory where to move files after import (in this case if not specified copy does not happen)")
    parser.add_argument("-del", "--delete", action="store_true", help="Delete files after import, default is false")
    parser.add_argument("-mma", action="store_true", help="Add microscope & acquisition file, default is false")
    parser.add_argument("-b2", help="Backblaze: <endpoint#bucketName#appKeyId#appKey>")

    # -----------------------------------------
    # Arguments exclusive to the -ucfg mode
    # -----------------------------------------
    parser.add_argument("-ml", help="User email list (address1:address2...)")

    # -----------------------------------------
    # Initial parse
    # -----------------------------------------
    args = parser.parse_args(argv)

    # -----------------------------------------
    # 1. No arguments - allowed
    # -----------------------------------------
    if argv is None:
        argv = sys.argv[1:]

    if len(argv) == 0:
        args.mode = "no-args"
        return args

    # -----------------------------------------
    # 2. If there are arguments, -cfg or -ucfg should be used
    # -----------------------------------------
    if not (args.cfg or args.ucfg):
        parser.error("When providing arguments, you must use either -cfg or -ucfg.")

    # -----------------------------------------
    # 3. Global conflict validation (-d vs -b2)
    # -----------------------------------------
    if args.destination and args.b2:
        parser.error("Options -d and -b2 cannot be used together.")

    # -----------------------------------------
    # 4. Validation of arguments values
    # -----------------------------------------
    validateArgsValues(parser, args)

    # -----------------------------------------
    # 5. Validation by mode
    # -----------------------------------------
    # -----------------------------
    # MODE -cfg
    # -----------------------------
    if args.cfg:
        args.mode = "cfg"

        missing = []

        if not args.hostname:
            missing.append("-H <hostname>")
        if not args.user:
            missing.append("-u <admin userName>")
        if not args.password:
            missing.append("-psw <admin password>")
        if not args.target:
            missing.append("-t <target directory>")
        if not args.sml:
            missing.append("-sml <email>")
        if not args.smlp:
            missing.append("-smlp <password>")
        if not args.aml:
            missing.append("-aml <email list>")

        if missing:
            parser.error("Missing required options for -cfg:\n  " + "\n  ".join(missing))

        return args

    # -----------------------------
    # MODE -ucfg
    # -----------------------------
    if args.ucfg:
        args.mode = "ucfg"
        args.userDirectory = args.ucfg[0]
        args.userDirectory = validateUserDirectoryPath(parser, args)
            
        missing = []

        if not args.user:
            missing.append("-u <user userName>")
        if not args.password:
            missing.append("-psw <user password>")
        if not args.ml:
            missing.append("-ml <email list>")

        if missing:
            parser.error("Missing required options for -ucfg:\n  " + "\n  ".join(missing))

        return args

    return args

def validateUserDirectoryPath(parser, args):
    """
    Validate and resolve user directory path.
    
    Checks that the specified user directory exists and is accessible.
    
    Args:
        parser (argparse.ArgumentParser): Argument parser for error reporting
        args (argparse.Namespace): Parsed arguments containing userDirectory
        
    Returns:
        str: Validated and resolved directory path
        
    Raises:
        SystemExit: If directory is invalid or inaccessible
    """
    try:
        userDirectoryPath = pathlib.Path(args.userDirectory).resolve()

        if not userDirectoryPath.exists():
            error = f"User directory {args.userDirectory} doesn't exist, application terminated."
            parser.error(error)
            writeToLog("ERROR: " + error)
            printToConsole("ERROR: " + error)
            sys.exit(1)

        if not userDirectoryPath.is_dir():
            error = f"User directory {args.userDirectory} is not a directory, application terminated."
            parser.error(error)
            writeToLog("ERROR: " + error)
            printToConsole("ERROR: " + error)
            sys.exit(1)

        return str(userDirectoryPath)

    except Exception:
        error = (f"Something went wrong validating directory {args.userDirectory}, "
                     "application terminated.")
        parser.error(error)
        writeToLog("ERROR: " + error)
        printToConsole("ERROR: " + error)
        sys.exit(1)

def validateArgsValues(parser, args):
    """
    Validate argument values for correctness.
    
    Validates:
    - Backblaze B2 argument format
    - Email list formats
    - Time format strings
    
    Args:
        parser (argparse.ArgumentParser): Argument parser for error reporting
        args (argparse.Namespace): Parsed arguments to validate
        
    Raises:
        SystemExit: If any validation fails
    """
    if args.b2:
        b2Parts = args.b2.split("#")
        if len(b2Parts) != 4:
            parser.error("Wrong number of arguments for -b2, expected <endpoint#bucketName#appKeyId#appKey>")
            sys.exit(1)

    if args.ml is not None and args.ml.strip() != "":
        # emailList is a list of email addresses (split from the -ml argument)
        emailList = args.ml.split(":")
        if any(not email or "@" not in email for email in emailList):
            parser.error("Invalid email address in -ml email list.")
            sys.exit(1)
        elif len(emailList) > 2:
            args.ml = emailList

    if args.aml is not None and args.aml.strip() != "":
        # emailList is a list of email addresses (split from the -aml argument)
        emailList = args.aml.split(":")
        if any(not email or "@" not in email for email in emailList):
            parser.error("Invalid email address in -aml email list.")
            sys.exit(1)
        elif len(emailList) > 2:
            args.aml = emailList

    if args.ts:
        try:
            time.strptime(args.ts, "%H:%M")
        except ValueError:
            parser.error("Invalid time format for -ts, expected hh:mm. 24-hour format")
            sys.exit(1)

    if args.te:
        try:
            time.strptime(args.te, "%H:%M")
        except ValueError:
            parser.error("Invalid time format for -te, expected hh:mm. 24-hour format")
            sys.exit(1)

