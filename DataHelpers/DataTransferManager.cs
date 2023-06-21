namespace MDR_Importer;

class DataTransferManager
{
    private readonly Source _source;
    private readonly StudyDataAdder _studyAdder;
    private readonly ObjectDataAdder _objectAdder;
    private readonly StudyDataDeleter _studyDeleter;
    private readonly ObjectDataDeleter _objectDeleter;

    public DataTransferManager(Source source, bool newTables, ILoggingHelper loggingHelper)
    {
        _source = source;
        var _loggingHelper = loggingHelper;
        string db_conn = _source.db_conn ?? "";

        _studyAdder = new StudyDataAdder(db_conn, _loggingHelper);
        _objectAdder = new ObjectDataAdder(db_conn, _loggingHelper);
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
        import.num_sd_studies = _studyAdder.AddData("studies");

        _studyDeleter.DeleteRecords("study_identifiers");
        _studyAdder.AddData("study_identifiers");

        _studyDeleter.DeleteRecords("study_titles");
        _studyAdder.AddData("study_titles");

        //int res = _deleter.DeleteRecords("st", "studies");

        // these are database dependent
        if (_source.has_study_references is true)
        {
            _studyDeleter.DeleteRecords("study_references");
            _studyAdder.AddData("study_references");
        }

        if (_source.has_study_people is true)
        {
            _studyDeleter.DeleteRecords("study_people");
            _studyAdder.AddData("study_people");
        }

        if (_source.has_study_organisations is true)
        {
            _studyDeleter.DeleteRecords("study_organisations");
            _studyAdder.AddData("study_organisations");
        }

        if (_source.has_study_topics is true)
        {
            _studyDeleter.DeleteRecords("study_topics");
            _studyAdder.AddData("study_topics");
        }

        if (_source.has_study_features is true)
        {
            _studyDeleter.DeleteRecords("study_features");
            _studyAdder.AddData("study_features");
        }

        if (_source.has_study_relationships is true)
        {
            _studyDeleter.DeleteRecords("study_relationships");
            _studyAdder.AddData("study_relationships");
        }

        if (_source.has_study_links is true)
        {
            _studyDeleter.DeleteRecords("study_links");
            _studyAdder.AddData("study_links");
        }

        if (_source.has_study_countries is true)
        {
            _studyDeleter.DeleteRecords("study_countries");
            _studyAdder.AddData("study_countries");
        }

        if (_source.has_study_locations is true)
        {
            _studyDeleter.DeleteRecords("study_locations");
            _studyAdder.AddData("study_locations");
        }

        if (_source.has_study_conditions is true)
        {
            _studyDeleter.DeleteRecords("study_conditions");
            _studyAdder.AddData("study_conditions");
        }

        if (_source.has_study_ipd_available is true)
        {
            _studyDeleter.DeleteRecords("study_ipd_available");
            _studyAdder.AddData("study_ipd_available");
        }

        if (_source.has_study_iec is true)
        {
            if (_source.study_iec_storage_type == "Single Table")
            {
                _studyDeleter.DeleteRecords("study_iec");
                _studyAdder.AddIECData("study_iec");
            }

            if (_source.study_iec_storage_type == "By Year Groupings")
            {
                _studyDeleter.DeleteRecords("study_iec_upto12");
                _studyAdder.AddIECData("study_iec_upto12");
                _studyDeleter.DeleteRecords("study_iec_13to19");
                _studyAdder.AddIECData("study_iec_13to19");
                _studyDeleter.DeleteRecords("study_iec_20on");
                _studyAdder.AddIECData("study_iec_20on");
            }

            if (_source.study_iec_storage_type == "By Years")
            {
                _studyDeleter.DeleteRecords("study_iec_null");
                _studyAdder.AddIECData("study_iec_null");
                _studyDeleter.DeleteRecords("study_iec_pre06");
                _studyAdder.AddIECData("study_iec_pre06");
                _studyDeleter.DeleteRecords("study_iec_0608");
                _studyAdder.AddIECData("study_iec_0608");
                _studyDeleter.DeleteRecords("study_iec_0910");
                _studyAdder.AddIECData("study_iec_0910");
                _studyDeleter.DeleteRecords("study_iec_1112");
                _studyAdder.AddIECData("study_iec_1112");
                _studyDeleter.DeleteRecords("study_iec_1314");
                _studyAdder.AddIECData("study_iec_1314");

                for (int i = 15; i < 30; i++)
                {
                    _studyDeleter.DeleteRecords($"study_iec_{i}");
                    _studyAdder.AddIECData($"study_iec_{i}");
                }
            }
        }
    }


    public void ImportObjectData(ImportEvent import)
    {
        _objectDeleter.DeleteRecords("data_objects");
        import.num_sd_objects = _objectAdder.AddDataObjects();
        
        _objectDeleter.DeleteRecords("object_instances");
        _objectAdder.AddData("object_instances");
        _objectDeleter.DeleteRecords("object_titles");
        _objectAdder.AddData("object_titles");

        // these are database dependent		

        if (_source.has_object_datasets is true)
        {
            _objectDeleter.DeleteRecords("object_datasets");
            _objectAdder.AddData("object_datasets");
        }

        if (_source.has_object_dates is true)
        {
            _objectDeleter.DeleteRecords("object_dates");
            _objectAdder.AddData("object_dates");
        }

        if (_source.has_object_rights is true)
        {
            _objectDeleter.DeleteRecords("object_rights");
            _objectAdder.AddData("object_rights");
        }

        if (_source.has_object_relationships is true)
        {
            _objectDeleter.DeleteRecords("object_relationships");
            _objectAdder.AddData("object_relationships");
        }

        if (_source.has_object_pubmed_set is true)
        {
            _objectDeleter.DeleteRecords("object_people");
            _objectAdder.AddData("object_people");
            
            _objectDeleter.DeleteRecords("object_organisations");
            _objectAdder.AddData("object_organisations");
            
            _objectDeleter.DeleteRecords("object_topics");
            _objectAdder.AddData("object_topics");
            
            _objectDeleter.DeleteRecords("object_comments");
            _objectAdder.AddData("object_comments");
            
            _objectDeleter.DeleteRecords("object_descriptions");
            _objectAdder.AddData("object_descriptions");
            
            _objectDeleter.DeleteRecords("object_identifiers");
            _objectAdder.AddData("object_identifiers");
            
            _objectDeleter.DeleteRecords("object_db_links");
            _objectAdder.AddData("object_db_links");
            
            _objectDeleter.DeleteRecords("object_publication_types");
            _objectAdder.AddData("object_publication_types");
            
            _objectDeleter.DeleteRecords("journal_details");
            _objectAdder.AddData("journal_details");
        }
    }
}