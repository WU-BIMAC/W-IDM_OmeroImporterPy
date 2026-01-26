from dataclasses import dataclass
from typing import Dict, List, Optional, Union


class WrappedException(Exception):
    def __init__(self, info, e):
        self.exception = e
        super().__init__(info)


@dataclass
class EmailConfig:
    emailFrom: str = ""
    emailFromPSW: str = ""
    emailTo: Union[str, List[str]] = ""
    adminsEmailTo: Union[str, List[str]] = ""

@dataclass
class GlobalConfig:
    userName: str
    userPSW: str
    target: str

    hasDelete: bool
    hasMMA: bool
    hasB2: bool

    hostName: str
    port: int
    
    destination: Optional[str] = None
    b2Endpoint: Optional[str] = None
    b2BucketName: Optional[str] = None
    b2AppKeyId: Optional[str] = None
    b2AppKey: Optional[str] = None
    
    # Scheduling specific config
    startTimeHr: Optional[int] = None
    startTimeMin: Optional[int] = None
    endTimeHr: Optional[int] = None
    endTimeMin: Optional[int] = None
    endTimePassed: Optional[bool] = False

    # User specific config
    udestination: Optional[str] = None
    uhasDelete: bool = False
    uhasMMA: bool = False
    uhasB2: bool = False
    ub2Endpoint: Optional[str] = None
    ub2BucketName: Optional[str] = None
    ub2AppKeyId: Optional[str] = None
    ub2AppKey: Optional[str] = None

@dataclass
class ImportContext:
    fullImportedData: Dict = None
    currentImportedData: Dict = None
    hasNewImport: bool = False    
    # User specific
    userCurrentImportedData: Dict = None
    userFullImportedData: Optional[Dict] = None
    # Project specific
    projectFullImportedData: Optional[Dict] = None
    projectCurrentImportedData: Dict = None
    # Dataset specific
    datasetFullImportedData: Optional[Dict] = None
    # Image specific
    imageFullImportedData: Optional[Dict] = None
    imageCurrentImportedData: Dict = None
