
namespace MDR_Importer;

class DataTransferManager
{
    private readonly Source _source;
    private readonly ILoggingHelper _loggingHelper;

    private readonly ForeignTableManager _ftm;
    private readonly StudyDataAdder _studyAdder;
    private readonly DataObjectDataAdder _objectAdder;
    private readonly StudyDataDeleter _studyDeleter;
    private readonly DataObjectDataEditor _objectDeleter;

    public DataTransferManager(Source source, ILoggingHelper loggingHelper)
    {
        _source = source;
        _loggingHelper = loggingHelper;    
        
        var db_conn = _source.db_conn ?? "";
        _ftm = new ForeignTableManager(db_conn);
        _studyAdder = new StudyDataAdder(db_conn, _loggingHelper);
        _objectAdder = new DataObjectDataAdder(db_conn, _loggingHelper);
        _studyDeleter = new StudyDataDeleter(db_conn, _loggingHelper);
        _objectDeleter = new DataObjectDataEditor(db_conn, _loggingHelper);
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

    public void AddStudies(int AddType)
    {
        _studyAdder.TransferStudies();
        _studyAdder.TransferStudyIdentifiers();
        _studyAdder.TransferStudyTitles();

        // These are database dependent

        if (_source.has_study_references is true) _studyAdder.TransferStudyReferences();
        if (_source.has_study_people is true) _studyAdder.TransferStudyPeople();
        if (_source.has_study_organisations is true) _studyAdder.TransferStudyOrganisations();
        if (_source.has_study_topics is true) _studyAdder.TransferStudyTopics();
        if (_source.has_study_topics is true) _studyAdder.TransferStudyConditions();
        if (_source.has_study_features is true) _studyAdder.TransferStudyFeatures();
        if (_source.has_study_relationships is true) _studyAdder.TransferStudyRelationships();
        if (_source.has_study_links is true) _studyAdder.TransferStudyLinks();
        if (_source.has_study_countries is true) _studyAdder.TransferStudyCountries();
        if (_source.has_study_locations is true) _studyAdder.TransferStudyLocations();
        if (_source.has_study_ipd_available is true) _studyAdder.TransferStudyIpdAvailable();
        if (_source.has_study_ipd_available is true) _studyAdder.TransferStudyIEC();

       // study_adder.UpdateStudiesLastImportedDate(import_id, source.id);

    }

    public void AddDataObjects(int importId)
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
            _objectAdder.TransferObjectPeople();
            _objectAdder.TransferObjectOrganisations();
            _objectAdder.TransferObjectTopics();
            _objectAdder.TransferObjectComments();
            _objectAdder.TransferObjectDescriptions();
            _objectAdder.TransferObjectIdentifiers();
            _objectAdder.TransferObjectDBLinks();
            _objectAdder.TransferObjectPublicationTypes();
        }
    }

    
    public void DeleteMatchedStudyData(int importId)
    {

        int res = _studyDeleter.DeleteStudyRecords("studies");
        _studyDeleter.DeleteStudyRecords("study_identifiers");
        _studyDeleter.DeleteStudyRecords("study_titles");

        // these are database dependent
        if (_source.has_study_references is true) _studyDeleter.DeleteStudyRecords("study_references");
        if (_source.has_study_people is true) _studyDeleter.DeleteStudyRecords("study_people");
        if (_source.has_study_organisations is true) _studyDeleter.DeleteStudyRecords("study_organisations");
        if (_source.has_study_topics is true) _studyDeleter.DeleteStudyRecords("study_topics");
        if (_source.has_study_features is true) _studyDeleter.DeleteStudyRecords("study_features");
        if (_source.has_study_relationships is true) _studyDeleter.DeleteStudyRecords("study_relationships");
        if (_source.has_study_links is true) _studyDeleter.DeleteStudyRecords("study_links");
        if (_source.has_study_countries is true) _studyDeleter.DeleteStudyRecords("study_countries");
        if (_source.has_study_locations is true) _studyDeleter.DeleteStudyRecords("study_locations");
        if (_source.has_study_conditions is true) _studyDeleter.DeleteStudyRecords("study_conditions");
        if (_source.has_study_ipd_available is true) _studyDeleter.DeleteStudyRecords("study_ipd_available");
        if (_source.has_study_iec is true) _studyDeleter.DeleteStudyRecords("study_iec");  // needs expanding
                                                                                           

        _loggingHelper.LogLine("Deleted " + res.ToString() + " study records and related data");

    }


    public void DeleteMatchedDataObjectData(int importId)
    {

        int res = _objectDeleter.DeleteObjectRecords("data_objects");
        _objectDeleter.DeleteObjectRecords("object_instances");
        _objectDeleter.DeleteObjectRecords("object_titles");
        _objectDeleter.DeleteObjectRecords("object_hashes");

        // these are database dependent		

        if (_source.has_object_datasets is true) _objectDeleter.DeleteObjectRecords("object_datasets");
        if (_source.has_object_dates is true) _objectDeleter.DeleteObjectRecords("object_dates");
        if (_source.has_object_pubmed_set is true)
        {
            _objectDeleter.DeleteObjectRecords("object_people");
            _objectDeleter.DeleteObjectRecords("object_organisations");
            _objectDeleter.DeleteObjectRecords("object_topics");
            _objectDeleter.DeleteObjectRecords("object_comments");
            _objectDeleter.DeleteObjectRecords("object_descriptions");
            _objectDeleter.DeleteObjectRecords("object_identifiers");
            _objectDeleter.DeleteObjectRecords("object_db_links");
            _objectDeleter.DeleteObjectRecords("object_publication_types");
        }

        _loggingHelper.LogLine("Deleted " + res.ToString() + " object records and related data");
    }
    
}