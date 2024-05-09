# W-IDM_OmeroImporterPy
The OMERO Importer Python tool was developed at **UMass Chan Medical School** and in collaboration with the **[Canada BioImaging](https://www.canadabioimaging.org)** Open Science Project for use with the Canada BioImaging [National OMERO Image Data Resource](https://omero.med.ualberta.ca/index/).

The OMERO Importer Python tool automatically reads bioimage data and metadata from a workstation running image acquisition or an associated NFS drive and imports it to an available OMERO repository. 

Automated metadata annotation is carried out as follows:
1) **Experimental and sample description metadata** is imported from customized metadata-collection Excel spreadsheets and written in OMERO as key/value pairs, tags, and OMERO.tables. Metadata annotation can be associated with a Project, Dataset, or Image as appropriate.
2) [NBO-Q]() compliant **Microscopy metadata** is imported from existing Microscope-Hardware- and Microscope-Settings.JSON files produced using the [Micro-Meta App](https://github.com/WU-BIMAC/MicroMetaApp-Electron) and attached to the relevant Image in OMERO. The content of these JSON files can then be visualized using the [Micro-Meta App OMERO-plugin](https://github.com/WU-BIMAC/MicroMetaApp-Omero).

The OMERO Importer Python tool can be used in conjunction with Metadata annotation Excel spreadsheets and with the [OMERO Importer Excel Helper Python](https://github.com/WU-BIMAC/W-IDM_OmeroImporterExcelHelperPy) tool to facilitate harvesting Image file names and capturing sub-directory structures into tag annotations.

More information about how to use these tools can be found on [ReadTheDocs](https://omeroimporterpy-docs.readthedocs.io/en/latest/#).
