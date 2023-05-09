
using Dapper;
using Npgsql;

namespace MDR_Importer;

class DataTransferManager
{
    private readonly Source _source;
    private readonly string _db_conn;
    private readonly StudyDataAdder _studyAdder;
    private readonly ObjectDataAdder _objectAdder;
    private readonly DataDeleter _deleter;

    public DataTransferManager(Source source, ILoggingHelper loggingHelper)
    {
        _source = source;
        var _loggingHelper = loggingHelper;    
        _db_conn = _source.db_conn ?? "";
        
        _studyAdder = new StudyDataAdder(_db_conn, _loggingHelper);
        _objectAdder = new ObjectDataAdder(_db_conn, _loggingHelper);
        _deleter = new DataDeleter(_db_conn, _loggingHelper);
    }

    public ImportEvent CreateImportEvent(int importId, bool? tables_rebuilt)
    {
        ImportEvent import = new ImportEvent(importId, _source.id, tables_rebuilt)
        {
            num_sd_studies = _source.has_study_tables is true ? GetTableCount("studies") : 0,
            num_sd_objects = GetTableCount("data_objects")
        };
        return import;
    }
    
    
    private int GetTableCount(string table_name)
    {
        string sql_string = @"SELECT count(*) FROM sd." + table_name;
        using NpgsqlConnection Conn = new(_db_conn);
        return Conn.ExecuteScalar<int>(sql_string);
    }
    
    
    public void AddStudyData()
    {
        _studyAdder.AddData("studies");
        _studyAdder.AddData("study_identifiers");
        _studyAdder.AddData("study_titles");

        // These are database dependent

        if (_source.has_study_references is true) _studyAdder.AddData("study_references");
        if (_source.has_study_people is true) _studyAdder.AddData("study_people");
        if (_source.has_study_organisations is true) _studyAdder.AddData("study_organisations");
        if (_source.has_study_topics is true) _studyAdder.AddData("study_topics");
        if (_source.has_study_relationships is true) _studyAdder.AddData("study_relationships");
        if (_source.has_study_features is true) _studyAdder.AddData("study_features");
        if (_source.has_study_links is true) _studyAdder.AddData("study_links");
        if (_source.has_study_countries is true) _studyAdder.AddData("study_countries");
        if (_source.has_study_locations is true) _studyAdder.AddData("study_locations");
        if (_source.has_study_ipd_available is true) _studyAdder.AddData("study_ipd_available");
        if (_source.has_study_conditions is true) _studyAdder.AddData("study_conditions");
        if (_source.has_study_iec is true)
        {
            if (_source.study_iec_storage_type == "Single Table")
            {
                _studyAdder.AddIECData("study_iec", "study_iec");
            }
            if (_source.study_iec_storage_type == "By Year Groupings")
            {
                _studyAdder.AddIECData("study_iec", "study_iec_upto12");
                _studyAdder.AddIECData("study_iec", "study_iec_13to19");
                _studyAdder.AddIECData("study_iec", "study_iec_20on");
            }
            if (_source.study_iec_storage_type == "By Years")
            {
                _studyAdder.AddIECData("study_iec", "study_iec_null");
                _studyAdder.AddIECData("study_iec", "study_iec_pre06");
                _studyAdder.AddIECData("study_iec", "study_iec_0608");
                _studyAdder.AddIECData("study_iec", "study_iec_0910");
                _studyAdder.AddIECData("study_iec", "study_iec_1112");
                _studyAdder.AddIECData("study_iec", "study_iec_1314");
                for (int i = 15; i < 30; i++)
                {
                    _studyAdder.AddIECData("study_iec", $"study_iec_{i}");
                }
            }
        }
    }

    public void AddObjectData()
    {
        _objectAdder.AddDataObjects();
        _objectAdder.AddData("object_instances");
        _objectAdder.AddData("object_titles");

        // These are database dependent.		
        
        if (_source.has_object_datasets is true) _objectAdder.AddData("object_datasets");
        if (_source.has_object_dates is true) _objectAdder.AddData("object_dates");
        if (_source.has_object_rights is true) _objectAdder.AddData("object_rights");
        if (_source.has_object_relationships is true) _objectAdder.AddData("object_relationships");
        if (_source.has_object_pubmed_set is true)
        {
            _objectAdder.AddData("object_people");
            _objectAdder.AddData("object_organisations");
            _objectAdder.AddData("object_topics");
            _objectAdder.AddData("object_comments");
            _objectAdder.AddData("object_descriptions");
            _objectAdder.AddData("object_identifiers");
            _objectAdder.AddData("object_db_links");
            _objectAdder.AddData("object_publication_types");
            _objectAdder.AddData("journal_details");
        }
    }

    
    public int DeleteMatchedStudyData()
    {
        int res = _deleter.DeleteRecords("st", "studies");
        _deleter.DeleteRecords("st","study_identifiers");
        _deleter.DeleteRecords("st","study_titles");

        // these are database dependent
        if (_source.has_study_references is true) _deleter.DeleteRecords("st","study_references");
        if (_source.has_study_people is true) _deleter.DeleteRecords("st","study_people");
        if (_source.has_study_organisations is true) _deleter.DeleteRecords("st","study_organisations");
        if (_source.has_study_topics is true) _deleter.DeleteRecords("st","study_topics");
        if (_source.has_study_features is true) _deleter.DeleteRecords("st","study_features");
        if (_source.has_study_relationships is true) _deleter.DeleteRecords("st","study_relationships");
        if (_source.has_study_links is true) _deleter.DeleteRecords("st","study_links");
        if (_source.has_study_countries is true) _deleter.DeleteRecords("st","study_countries");
        if (_source.has_study_locations is true) _deleter.DeleteRecords("st","study_locations");
        if (_source.has_study_conditions is true) _deleter.DeleteRecords("st","study_conditions");
        if (_source.has_study_ipd_available is true) _deleter.DeleteRecords("st","study_ipd_available");
        if (_source.has_study_iec is true)
        {
            if (_source.study_iec_storage_type == "Single Table")
            {
                _deleter.DeleteRecords("st","study_iec");
            }
            if (_source.study_iec_storage_type == "By Year Groupings")
            {
                _deleter.DeleteRecords("st","study_iec_upto12");
                _deleter.DeleteRecords("st","study_iec_13to19");
                _deleter.DeleteRecords("st","study_iec_20on");
            }
            if (_source.study_iec_storage_type == "By Years")
            {
                _deleter.DeleteRecords("st","study_iec_null");
                _deleter.DeleteRecords("st","study_iec_pre06");
                _deleter.DeleteRecords("st","study_iec_0608");
                _deleter.DeleteRecords("st","study_iec_0910");
                _deleter.DeleteRecords("st","study_iec_1112");
                _deleter.DeleteRecords("st","study_iec_1314");
                for (int i = 15; i < 30; i++)
                {
                    _deleter.DeleteRecords("st", $"study_iec_{i}");
                }
            }
        }
        return res;
    }

    public int DeleteMatchedObjectData()
    {

        int res = _deleter.DeleteRecords("ob", "data_objects");
        _deleter.DeleteRecords("ob","object_instances");
        _deleter.DeleteRecords("ob","object_titles");

        // these are database dependent		

        if (_source.has_object_datasets is true) _deleter.DeleteRecords("ob","object_datasets");
        if (_source.has_object_dates is true) _deleter.DeleteRecords("ob","object_dates");
        if (_source.has_object_pubmed_set is true)
        {
            _deleter.DeleteRecords("ob","object_people");
            _deleter.DeleteRecords("ob","object_organisations");
            _deleter.DeleteRecords("ob","object_topics");
            _deleter.DeleteRecords("ob","object_comments");
            _deleter.DeleteRecords("ob","object_descriptions");
            _deleter.DeleteRecords("ob","object_identifiers");
            _deleter.DeleteRecords("ob","object_db_links");
            _deleter.DeleteRecords("ob","object_publication_types");
            _deleter.DeleteRecords("ob","journal_details");
        }
        return res;
    }

}