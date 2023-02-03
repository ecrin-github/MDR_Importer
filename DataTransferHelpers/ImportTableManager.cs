
using Dapper;
using Npgsql;
namespace MDR_Importer;

class ImportTableManager
{
    private readonly Source _source;
    private readonly ILoggingHelper _loggingHelper;
    private readonly string _connString;

    public ImportTableManager(Source source, ILoggingHelper loggingHelper)
    {
        _source = source;
        _connString = source.db_conn ?? "";
        _loggingHelper = loggingHelper;       
    }
    
    public void ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(_connString);
        conn.Execute(sql_string);

    }
    
    public void CreateImportTables()
    {
        string sql_string;
        if (_source.has_study_tables is true)
        {
            sql_string = @"DROP TABLE IF EXISTS sd.study_recs;
                   CREATE TABLE sd.study_recs(
                   sd_sid                 VARCHAR         NOT NULL PRIMARY KEY
                  , status                 INT             NOT NULL
                  );";
            ExecuteSQL(sql_string);
            _loggingHelper.LogLine("Created study recs table");
        }
        
        sql_string = @"DROP TABLE IF EXISTS sd.object_recs;
        CREATE TABLE sd.object_recs(
            sd_oid                  VARCHAR         NOT NULL PRIMARY KEY
          , status                  INT             NOT NULL
        );";
        ExecuteSQL(sql_string);
        _loggingHelper.LogLine("Created object recs table");
    }
    
  
    public void FillImportTables()
    {
        string sql_string;
        if (_source.has_study_tables is true)
        {
            // IdentifyNewStudies(); 
            
            sql_string = @"INSERT INTO sd.study_recs (sd_sid, status)
                SELECT s.sd_sid, 1 from sd.studies s
                LEFT JOIN ad.studies a
                on s.sd_sid = a.sd_sid 
                WHERE a.sd_sid is null;";
            ExecuteSQL(sql_string);
            
            // IdentifyMatchedStudies();
            sql_string = @"INSERT INTO sd.study_recs (sd_sid, status)
            SELECT s.sd_sid, 2 from sd.studies s
            INNER JOIN ad.studies a
            on s.sd_sid = a.sd_sid;";
            ExecuteSQL(sql_string);
        }

        _loggingHelper.LogLine("Filled study recs table");

        // IdentifyNewDataObjects();
        sql_string = @"INSERT INTO sd.object_recs(sd_oid, status)
            SELECT d.sd_oid, 1 from sd.data_objects d
            LEFT JOIN ad.data_objects a
            on d.sd_oid = a.sd_oid
            WHERE a.sd_oid is null;";
        ExecuteSQL(sql_string);
        
        // IdentifyMatchedDataObjects();
        sql_string = @"INSERT INTO sd.object_recs(sd_oid, status)
            SELECT d.sd_oid, 2 from sd.data_objects d
            INNER JOIN ad.data_objects a
            on d.sd_oid = a.sd_oid;";
        ExecuteSQL(sql_string);
        _loggingHelper.LogLine("Filled object recs table");
       
    }

    
    public ImportEvent CreateImportEvent(int importId)
    {
        ImportEvent import = new ImportEvent(importId, _source.id);
        if (_source.has_study_tables is true)
        {
            import.num_new_studies = GetScalarDBValue("to_ad_study_recs", 1);
            import.num_matched_studies = GetScalarDBValue("to_ad_study_recs", 2);
        }
        else
        {
            import.num_new_studies = 0;
            import.num_matched_studies = 0;
        }
        import.num_new_objects = GetScalarDBValue("to_ad_object_recs", 1);
        import.num_matched_objects = GetScalarDBValue("to_ad_object_recs", 2); 

        return import;
    }
    
    private int GetScalarDBValue(string table_name, int status)
    {
        string sql_string = @"SELECT count(*) FROM sd." + table_name + 
                            " WHERE status = " + status.ToString();
        using var Conn = new NpgsqlConnection(_connString);
        return Conn.ExecuteScalar<int>(sql_string);
    }
}