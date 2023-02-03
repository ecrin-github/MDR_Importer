using Dapper;
using Npgsql;
namespace MDR_Importer;

class ImportTableCreator
{
    string connstring;

    public ImportTableCreator(string _connstring)
    {
        connstring = _connstring;
    }

    public void ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(connstring);
        conn.Execute(sql_string);
    }
}


class ImportTableManager
{
    private readonly string connstring;

    public ImportTableManager(string _connstring)
    {
        connstring = _connstring;
    }

    public void ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(connstring);
        conn.Execute(sql_string);
    }

    public void IdentifyNewStudies()
    {
        string sql_string = @"INSERT INTO sd.to_ad_study_recs (sd_sid, status)
                SELECT s.sd_sid, 1 from sd.studies s
                LEFT JOIN ad.studies a
                on s.sd_sid = a.sd_sid 
                WHERE a.sd_sid is null;";
        ExecuteSQL(sql_string);
    }


    public void IdentifyMatchedStudies()
    {
        string sql_string = @"INSERT INTO sd.to_ad_study_recs (sd_sid, status)
            SELECT s.sd_sid, 2 from sd.studies s
            INNER JOIN ad.studies a
            on s.sd_sid = a.sd_sid;";
        ExecuteSQL(sql_string);
    }


    public void IdentifyNewDataObjects()
    {
        string sql_string = @"INSERT INTO sd.to_ad_object_recs(sd_oid, status)
            SELECT d.sd_oid, 1 from sd.data_objects d
            LEFT JOIN ad.data_objects a
            on d.sd_oid = a.sd_oid
            WHERE a.sd_oid is null;";
            ExecuteSQL(sql_string);
    }


    public void IdentifyMatchedDataObjects()
    {
        string sql_string = @"INSERT INTO sd.to_ad_object_recs(sd_oid, status)
            SELECT d.sd_oid, 2 from sd.data_objects d
            INNER JOIN ad.data_objects a
            on d.sd_oid = a.sd_oid;";
        ExecuteSQL(sql_string);
    }

    public ImportEvent CreateImportEvent(int import_id)
    {
        ImportEvent import = new ImportEvent(import_id, _source.id);
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
        using (var Conn = new NpgsqlConnection(connstring))
        {
            return Conn.ExecuteScalar<int>(sql_string);
        }
    }

}
