
namespace MDR_Importer;

class DataTransferrer
{
    private readonly ISource _source;
    private readonly ILoggingHelper _loggingHelper;

    private readonly ForeignTableManager _ftm;
    private readonly StudyDataAdder _studyAdder;
    private readonly DataObjectDataAdder _objectAdder;
    private readonly StudyDataEditor _studyEditor;
    private readonly DataObjectDataEditor _objectEditor;

    public DataTransferrer(ISource source, ILoggingHelper loggingHelper)
    {
        _source = source;
        _loggingHelper = loggingHelper;    
        
        var connString = _source.db_conn ?? "";
        _ftm = new ForeignTableManager(connString);
        _studyAdder = new StudyDataAdder(connString, _loggingHelper);
        _objectAdder = new DataObjectDataAdder(connString, _loggingHelper);
        _studyEditor = new StudyDataEditor(connString, _loggingHelper);
        _objectEditor = new DataObjectDataEditor(connString, _loggingHelper);
    }

    public void EstablishForeignMonTables(ICredentials creds)
    {
        if (creds.Username is not null && creds.Password is not null)
        {
            _ftm.EstablishMonForeignTables(creds.Username, creds.Password);
        }
    }

    public void DropForeignMonTables()
    {
        _ftm.DropMonForeignTables();
    }

    public void AddNewStudies(int importId)
    {
        _studyAdder.TransferStudies();
        _studyAdder.TransferStudyIdentifiers();
        _studyAdder.TransferStudyTitles();

        // These are database dependent

        if (_source.has_study_references is true) _studyAdder.TransferStudyReferences();
        if (_source.has_study_contributors is true) _studyAdder.TransferStudyContributors();
        if (_source.has_study_topics is true) _studyAdder.TransferStudyTopics();
        if (_source.has_study_features is true) _studyAdder.TransferStudyFeatures();
        if (_source.has_study_relationships is true) _studyAdder.TransferStudyRelationships();
        if (_source.has_study_links is true) _studyAdder.TransferStudyLinks();
        if (_source.has_study_countries is true) _studyAdder.TransferStudyCountries();
        if (_source.has_study_locations is true) _studyAdder.TransferStudyLocations();
        if (_source.has_study_ipd_available is true) _studyAdder.TransferStudyIpdAvailable();
        _loggingHelper.LogLine("Added new source specific study data");

       // study_adder.UpdateStudiesLastImportedDate(import_id, source.id);

        _studyAdder.TransferStudyHashes();
        _loggingHelper.LogLine("Added new study hashes");
        _loggingHelper.LogLine("");
    }


    public void AddNewDataObjects(int importId)
    {
        _objectAdder.TransferDataObjects();

        _objectAdder.TransferObjectInstances();
        _objectAdder.TransferObjectTitles();

        // These are database dependent.		
        
        if (_source.has_object_datasets is true) _objectAdder.TransferDataSetProperties();
        if (_source.has_object_dates is true) _objectAdder.TransferObjectDates();
        if (_source.has_object_rights is true) _objectAdder.TransferObjectRights();
        if (_source.has_object_relationships is true) _objectAdder.TransferObjectRelationships();
        if (_source.has_object_pubmed_set is true)
        {
            _objectAdder.TransferObjectContributors();
            _objectAdder.TransferObjectTopics();
            _objectAdder.TransferObjectComments();
            _objectAdder.TransferObjectDescriptions();
            _objectAdder.TransferObjectIdentifiers();
            _objectAdder.TransferObjectDBLinks();
            _objectAdder.TransferObjectPublicationTypes();
        }
        _loggingHelper.LogLine("Added new source specific object data");

        _objectAdder.TransferObjectHashes();
        _loggingHelper.LogLine("Added new object hashes");
    }

    public void UpdateDatesOfData()
    {
        if (_source.has_study_tables is true)
        {
            _studyEditor.UpdateDateOfStudyData();
        }
        _objectEditor.UpdateDateOfDataObjectData();
    }


    public void UpdateEditedStudyData(int importId)
    {
        _studyEditor.EditStudies();
        _studyEditor.EditStudyIdentifiers();
        _studyEditor.EditStudyTitles();

        // these are database dependent
        if (_source.has_study_references is true) _studyEditor.EditStudyReferences();
        if (_source.has_study_contributors is true) _studyEditor.EditStudyContributors();
        if (_source.has_study_topics is true) _studyEditor.EditStudyTopics();
        if (_source.has_study_features is true) _studyEditor.EditStudyFeatures();
        if (_source.has_study_relationships is true) _studyEditor.EditStudyRelationships();
        if (_source.has_study_links is true) _studyEditor.EditStudyLinks();
        if (_source.has_study_countries is true) _studyEditor.EditStudyCountries();
        if (_source.has_study_locations is true) _studyEditor.EditStudyLocations();
        if (_source.has_study_ipd_available is true) _studyEditor.EditStudyIpdAvailable();

        //study_editor.UpdateStudiesLastImportedDate(import_id, _source.id);

        _studyEditor.UpdateStudyCompositeHashes();
        _studyEditor.AddNewlyCreatedStudyHashTypes();
        _studyEditor.DropNewlyDeletedStudyHashTypes();

        _loggingHelper.LogLine("Edited study data");
        _loggingHelper.LogLine("");
    }


    public void UpdateEditedDataObjectData(int importId)
    {
        _objectEditor.EditDataObjects();

        _objectEditor.EditObjectInstances();
        _objectEditor.EditObjectTitles();

        // these are database dependent		

        if (_source.has_object_datasets is true) _objectEditor.EditDataSetProperties();      
        if (_source.has_object_dates is true) _objectEditor.EditObjectDates();
        if (_source.has_object_rights is true) _objectEditor.EditObjectRights();
        if (_source.has_object_relationships is true) _objectEditor.EditObjectRelationships();
        if (_source.has_object_pubmed_set is true)
        {
            _objectEditor.EditObjectContributors();
            _objectEditor.EditObjectTopics();
            _objectEditor.EditObjectComments();
            _objectEditor.EditObjectDescriptions();
            _objectEditor.EditObjectIdentifiers();
            _objectEditor.EditObjectDBLinks();
            _objectEditor.EditObjectPublicationTypes();
        }

        _objectEditor.UpdateObjectCompositeHashes();
        _objectEditor.AddNewlyCreatedObjectHashTypes();
        _objectEditor.DropNewlyDeletedObjectHashTypes();

        _loggingHelper.LogLine("Edited data object data");
    }


    public void RemoveDeletedStudyData(int importId)
    {
        int res = _studyEditor.DeleteStudyRecords("studies");
        _studyEditor.DeleteStudyRecords("study_identifiers");
        _studyEditor.DeleteStudyRecords("study_titles");
        _studyEditor.DeleteStudyRecords("study_hashes"); ;

        // these are database dependent
        if (_source.has_study_references is true) _studyEditor.DeleteStudyRecords("study_references");
        if (_source.has_study_contributors is true) _studyEditor.DeleteStudyRecords("study_contributors");
        if (_source.has_study_topics is true) _studyEditor.DeleteStudyRecords("study_topics");
        if (_source.has_study_features is true) _studyEditor.DeleteStudyRecords("study_features"); ;
        if (_source.has_study_relationships is true) _studyEditor.DeleteStudyRecords("study_relationships");
        if (_source.has_study_links is true) _studyEditor.DeleteStudyRecords("study_links");
        if (_source.has_study_countries is true) _studyEditor.DeleteStudyRecords("study_countries");
        if (_source.has_study_locations is true) _studyEditor.DeleteStudyRecords("study_locations");
        if (_source.has_study_ipd_available is true) _studyEditor.DeleteStudyRecords("study_ipd_available");

        _studyEditor.UpdateStudiesDeletedDate(importId, _source.id);

        _loggingHelper.LogLine("Deleted " + res.ToString() + " study records and related data");
    }


    public void RemoveDeletedDataObjectData(int importId)
    {
        int res = _objectEditor.DeleteObjectRecords("data_objects");
        _objectEditor.DeleteObjectRecords("object_instances");
        _objectEditor.DeleteObjectRecords("object_titles");
        _objectEditor.DeleteObjectRecords("object_hashes");

        // these are database dependent		

        if (_source.has_object_datasets is true) _objectEditor.DeleteObjectRecords("object_datasets");
        if (_source.has_object_dates is true) _objectEditor.DeleteObjectRecords("object_dates");
        if (_source.has_object_pubmed_set is true)
        {
            _objectEditor.DeleteObjectRecords("object_contributors"); ;
            _objectEditor.DeleteObjectRecords("object_topics");
            _objectEditor.DeleteObjectRecords("object_comments");
            _objectEditor.DeleteObjectRecords("object_descriptions");
            _objectEditor.DeleteObjectRecords("object_identifiers");
            _objectEditor.DeleteObjectRecords("object_db_links");
            _objectEditor.DeleteObjectRecords("object_publication_types"); ;
        }

        if (!_source.has_study_tables is true)
        {
            _objectEditor.UpdateObjectsDeletedDate(importId, _source.id);
        }

        _loggingHelper.LogLine("Deleted " + res.ToString() + " object records and related data");
    }

/*
    public void UpdateFullRecordHashes()
    {
        if (_source.has_study_tables is true)
        {
            _studyEditor.UpdateFullStudyHash();
        }
        _objectEditor.UpdateFullObjectHash();
        _loggingHelper.LogLine("Full hash values updated");
    }
*/

/*
    public void UpdateStudiesLastImportedDate(int importId)
    {
        _studyEditor.UpdateStudiesLastImportedDate(importId, _source.id);
    }


    public void UpdateObjectsLastImportedDate(int importId)
    {
        _objectEditor.UpdateObjectsLastImportedDate(importId, _source.id);
    }
    */
}