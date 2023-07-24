namespace MDR_Importer;

class DataTransferManager
{
    private readonly Source _source;
    private readonly DataAdder _dataAdder;
    private readonly StudyDataDeleter _studyDeleter;
    private readonly ObjectDataDeleter _objectDeleter;

    public DataTransferManager(Source source, bool newTables, ILoggingHelper loggingHelper)
    {
        _source = source;
        var _loggingHelper = loggingHelper;
        string db_conn = _source.db_conn ?? "";

        _dataAdder = new DataAdder(db_conn, _loggingHelper);
        _studyDeleter = new StudyDataDeleter(db_conn, _loggingHelper, newTables);
        _objectDeleter = new ObjectDataDeleter(db_conn, _loggingHelper, newTables);
    }

    public ImportEvent CreateImportEvent(int importId, bool? tables_rebuilt)
    {
        ImportEvent import = new ImportEvent(importId, _source.id, tables_rebuilt);
        return import;
    }
  
    public void ImportStudyData(ImportEvent import)
    {
        // Operations carried out in this 'immediate' delete then add pattern so that if an error
        // occurs in the deletion process (as did happen with some compactions, before debugging) the 
        // addition process is not blocked after deletions were largely completed. Though the bugs
        // should have been completely removed, this is a 'safety-first' pattern, if a rather clumsy one!
        // Deletions are not required if new tables have been constructed, but this is checked within
        // a single test in the Deleter class, rather tha having multiple tests below,

        _studyDeleter.DeleteRecords("studies");
        import.num_sd_studies = _dataAdder.AddStudyData("studies");

        _studyDeleter.DeleteRecords("study_identifiers");
        _dataAdder.AddStudyData("study_identifiers");

        _studyDeleter.DeleteRecords("study_titles");
        _dataAdder.AddStudyData("study_titles");

        // these are database dependent
        if (_source.has_study_references is true)
        {
            _studyDeleter.DeleteRecords("study_references");
            _dataAdder.AddStudyData("study_references");
        }

        if (_source.has_study_people is true)
        {
            _studyDeleter.DeleteRecords("study_people");
            _dataAdder.AddStudyData("study_people");
        }

        if (_source.has_study_organisations is true)
        {
            _studyDeleter.DeleteRecords("study_organisations");
            _dataAdder.AddStudyData("study_organisations");
        }

        if (_source.has_study_topics is true)
        {
            _studyDeleter.DeleteRecords("study_topics");
            _dataAdder.AddStudyData("study_topics");
        }

        if (_source.has_study_features is true)
        {
            _studyDeleter.DeleteRecords("study_features");
            _dataAdder.AddStudyData("study_features");
        }

        if (_source.has_study_relationships is true)
        {
            _studyDeleter.DeleteRecords("study_relationships");
            _dataAdder.AddStudyData("study_relationships");
        }

        if (_source.has_study_links is true)
        {
            _studyDeleter.DeleteRecords("study_links");
            _dataAdder.AddStudyData("study_links");
        }

        if (_source.has_study_countries is true)
        {
            _studyDeleter.DeleteRecords("study_countries");
            _dataAdder.AddStudyData("study_countries");
        }

        if (_source.has_study_locations is true)
        {
            _studyDeleter.DeleteRecords("study_locations");
            _dataAdder.AddStudyData("study_locations");
        }

        if (_source.has_study_conditions is true)
        {
            _studyDeleter.DeleteRecords("study_conditions");
            _dataAdder.AddStudyData("study_conditions");
        }

        if (_source.has_study_ipd_available is true)
        {
            _studyDeleter.DeleteRecords("study_ipd_available");
            _dataAdder.AddStudyData("study_ipd_available");
        }

        if (_source.has_study_iec is true)
        {
            if (_source.study_iec_storage_type == "Single Table")
            {
                _studyDeleter.DeleteRecords("study_iec");
                _dataAdder.AddIECData("study_iec");
            }

            if (_source.study_iec_storage_type == "By Year Groupings")
            {
                _studyDeleter.DeleteRecords("study_iec_upto12");
                _dataAdder.AddIECData("study_iec_upto12");
                _studyDeleter.DeleteRecords("study_iec_13to19");
                _dataAdder.AddIECData("study_iec_13to19");
                _studyDeleter.DeleteRecords("study_iec_20on");
                _dataAdder.AddIECData("study_iec_20on");
            }

            if (_source.study_iec_storage_type == "By Years")
            {
                _studyDeleter.DeleteRecords("study_iec_null");
                _dataAdder.AddIECData("study_iec_null");
                _studyDeleter.DeleteRecords("study_iec_pre06");
                _dataAdder.AddIECData("study_iec_pre06");
                _studyDeleter.DeleteRecords("study_iec_0608");
                _dataAdder.AddIECData("study_iec_0608");
                _studyDeleter.DeleteRecords("study_iec_0910");
                _dataAdder.AddIECData("study_iec_0910");
                _studyDeleter.DeleteRecords("study_iec_1112");
                _dataAdder.AddIECData("study_iec_1112");
                _studyDeleter.DeleteRecords("study_iec_1314");
                _dataAdder.AddIECData("study_iec_1314");

                for (int i = 15; i < 30; i++)
                {
                    _studyDeleter.DeleteRecords($"study_iec_{i}");
                    _dataAdder.AddIECData($"study_iec_{i}");
                }
            }
        }
    }


    public void ImportObjectData(ImportEvent import)
    {
        _objectDeleter.DeleteRecords("data_objects");
        import.num_sd_objects = _dataAdder.AddObjectData("data_objects");
        
        _objectDeleter.DeleteRecords("object_instances");
        _dataAdder.AddObjectData("object_instances");
        _objectDeleter.DeleteRecords("object_titles");
        _dataAdder.AddObjectData("object_titles");

        // these are database dependent		

        if (_source.has_object_datasets is true)
        {
            _objectDeleter.DeleteRecords("object_datasets");
            _dataAdder.AddObjectData("object_datasets");
        }

        if (_source.has_object_dates is true)
        {
            _objectDeleter.DeleteRecords("object_dates");
            _dataAdder.AddObjectData("object_dates");
        }

        if (_source.has_object_rights is true)
        {
            _objectDeleter.DeleteRecords("object_rights");
            _dataAdder.AddObjectData("object_rights");
        }

        if (_source.has_object_relationships is true)
        {
            _objectDeleter.DeleteRecords("object_relationships");
            _dataAdder.AddObjectData("object_relationships");
        }

        if (_source.has_object_pubmed_set is true)
        {
            _objectDeleter.DeleteRecords("object_people");
            _dataAdder.AddObjectData("object_people");
            
            _objectDeleter.DeleteRecords("object_organisations");
            _dataAdder.AddObjectData("object_organisations");
            
            _objectDeleter.DeleteRecords("object_topics");
            _dataAdder.AddObjectData("object_topics");
            
            _objectDeleter.DeleteRecords("object_comments");
            _dataAdder.AddObjectData("object_comments");
            
            _objectDeleter.DeleteRecords("object_descriptions");
            _dataAdder.AddObjectData("object_descriptions");
            
            _objectDeleter.DeleteRecords("object_identifiers");
            _dataAdder.AddObjectData("object_identifiers");
            
            _objectDeleter.DeleteRecords("object_db_links");
            _dataAdder.AddObjectData("object_db_links");
            
            _objectDeleter.DeleteRecords("object_publication_types");
            _dataAdder.AddObjectData("object_publication_types");
            
            _objectDeleter.DeleteRecords("journal_details");
            _dataAdder.AddObjectData("journal_details");
        }
    }
}