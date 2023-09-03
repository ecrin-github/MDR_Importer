using Dapper;
using Npgsql;
namespace MDR_Importer;

public class TestHelper
{
    private readonly string _db_conn;
    private readonly ILoggingHelper _logging_helper;
    private readonly DBUtilities _dbu;
    private readonly Source _source;
    private readonly FieldLists fl;
    
    public TestHelper( Source source, ILoggingHelper logging_helper)
    {
        _source = source;
        _logging_helper = logging_helper;
        _db_conn = source.db_conn!;
        _dbu = new DBUtilities(_db_conn, logging_helper);
        fl = new FieldLists();
    }

    public int EstablishTempStudyTestList()
    {
        // List has to be derived from the mn.source dta 'for testing' studies
        // but they also have to be present in the sd data
        
        using var conn = new NpgsqlConnection(_db_conn);
        string sql_string = @"DROP TABLE IF EXISTS mn.test_study_list;
                        CREATE TABLE mn.test_study_list as 
                        SELECT sd.sd_sid from mn.source_data sd
                        inner join sd.studies s
                        on sd.sd_sid = s.sd_sid
                        WHERE sd.for_testing = true";
        conn.Execute(sql_string);

        sql_string = @"select count(*) from mn.test_study_list";
        int res = conn.ExecuteScalar<int>(sql_string);
        if (res > 0)
        {
            sql_string = @"DROP TABLE IF EXISTS mn.test_object_list;
                                CREATE TABLE mn.test_object_list as 
                                SELECT sd_oid from sd.data_objects sdo
                                inner join mn.test_study_list tsl
                                on sdo.sd_sid = tsl.sd_sid";
            conn.Execute(sql_string);
        }
        return res;
    }
   
    public int EstablishTempObjectTestList()
    {
        // List has to be derived from the mn.source data 'for testing' objects
        // but they also have to be present in the sd data
        
        using var conn = new NpgsqlConnection(_db_conn);
        string sql_string = @"DROP TABLE IF EXISTS mn.test_object_list;
                        CREATE TABLE mn.test_object_list as 
                        SELECT sd.sd_oid from mn.source_data sd
                        inner join sd.data_objects s
                        on sd.sd_oid = s.sd_oid
                        WHERE sd.for_testing = true";
        conn.Execute(sql_string);
        
        sql_string = @"select count(*) from mn.test_object_list";
        return conn.ExecuteScalar<int>(sql_string);
    }
    
    public void TeardownTempTestDataTables()
    {
        string sql_string = @"DROP TABLE IF EXISTS mn.test_study_list;
                            DROP TABLE IF EXISTS mn.test_object_list;";
        using var conn = new NpgsqlConnection(_db_conn);
        conn.Execute(sql_string);
    }
    
    private void delete_study_test_recs(string table_name)
    {
        string sql_string = $@"Delete from ad.{table_name} s
                               using mn.test_study_list t
                               where s.sd_sid = t.sd_sid ";
        using var conn = new NpgsqlConnection(_db_conn);
        _dbu.ExecuteDeleteSQL(sql_string, table_name, 200000);
    }
    
    private void delete_object_test_recs(string table_name)
    {
        string sql_string = $@"Delete from ad.{table_name} s
                               using mn.test_object_list t
                               where s.sd_oid = t.sd_oid ";
        using var conn = new NpgsqlConnection(_db_conn);
        _dbu.ExecuteDeleteSQL(sql_string, table_name, 200000);
    }

    public void DeleteCurrentTestStudyData()
    {
        // these common to all databases

        delete_study_test_recs("studies");
        delete_study_test_recs("study_titles");
        delete_study_test_recs("study_identifiers");

        // these are database dependent
        if (_source.has_study_topics is true) delete_study_test_recs("study_topics");
        if (_source.has_study_conditions is true) delete_study_test_recs("study_conditions");
        if (_source.has_study_features is true) delete_study_test_recs("study_features");
        if (_source.has_study_people is true) delete_study_test_recs("study_people");
        if (_source.has_study_organisations is true) delete_study_test_recs("study_organisations");
        if (_source.has_study_references is true) delete_study_test_recs("study_references");
        if (_source.has_study_relationships is true) delete_study_test_recs("study_relationships");
        if (_source.has_study_links is true) delete_study_test_recs("study_links");
        if (_source.has_study_countries is true) delete_study_test_recs("study_countries");
        if (_source.has_study_locations is true) delete_study_test_recs("study_locations");
        if (_source.has_study_ipd_available is true) delete_study_test_recs("study_ipd_available");
        if (_source.has_study_iec is true)
        {
            if (_source.study_iec_storage_type == "Single Table")
            {
                delete_study_test_recs("study_iec");
            }

            if (_source.study_iec_storage_type == "By Year Groupings")
            {
                delete_study_test_recs("study_iec_upto12");
                delete_study_test_recs("study_iec_13to19");
                delete_study_test_recs("study_iec_20on");
            }

            if (_source.study_iec_storage_type == "By Years")
            {
                delete_study_test_recs("study_iec_null");
                delete_study_test_recs("study_iec_pre06");
                delete_study_test_recs("study_iec_0608");
                delete_study_test_recs("study_iec_0910");
                delete_study_test_recs("study_iec_1112");
                delete_study_test_recs("study_iec_1314");
                for (int i = 15; i <= 30; i++)
                {
                    delete_study_test_recs($"study_iec_{i}");
                }
            }
        }
        _logging_helper.LogLine("Study test data deleted");
    }

    public void DeleteCurrentTestObjectData()
    {
        // object tables - these common to all databases

        delete_object_test_recs("data_objects");
        delete_object_test_recs("object_titles");
        
        // these are database dependent		

        if (_source.has_object_datasets is true) delete_object_test_recs("object_datasets");
        if (_source.has_object_dates is true) delete_object_test_recs("object_dates");
        if (_source.has_object_instances is true) delete_object_test_recs("object_instances");
        if (_source.has_object_people is true) delete_object_test_recs("object_people");
        if (_source.has_object_organisations is true) delete_object_test_recs("object_organisations");
        if (_source.has_object_topics is true) delete_object_test_recs("object_topics");
        if (_source.has_object_comments is true) delete_object_test_recs("object_comments");
        if (_source.has_object_descriptions is true) delete_object_test_recs("object_descriptions");
        if (_source.has_object_identifiers is true) delete_object_test_recs("object_identifiers");
        if (_source.has_object_db_links is true) delete_object_test_recs("object_db_links");
        if (_source.has_object_publication_types is true) delete_object_test_recs("object_publication_types");
        if (_source.has_journal_details is true) delete_object_test_recs("journal_details");
        if (_source.has_object_relationships is true) delete_object_test_recs("object_relationships");
        if (_source.has_object_rights is true) delete_object_test_recs("object_rights");
        
        _logging_helper.LogLine("Object test data deleted");
    }
    
    private void import_study_test_recs(string table_name, string template_table = "")
    {
        if (template_table == "")
        {
            template_table = table_name;
        }
        string fields = fl.addStudyFields[template_table];
        string insert_fields = fl.addStudyFields_insert[template_table];
        
        string sql_string = $@"INSERT INTO ad.{table_name} ({fields})
        SELECT {insert_fields}
        FROM sd.{table_name} s inner join mn.test_study_list tsl
        on s.sd_sid = tsl.sd_sid;";
        using var conn = new NpgsqlConnection(_db_conn);
        conn.Execute(sql_string);
        _logging_helper.LogLine($"ad.{table_name} test data imported");
    }
    
    private void import_object_test_recs(string table_name)
    {
        string fields = fl.addObjectFields[table_name];
        string insert_fields = fl.addObjectFields_insert[table_name];
        
        string sql_string = $@"INSERT INTO ad.{table_name} ({fields})
        SELECT {insert_fields}
        FROM sd.{table_name} s inner join mn.test_object_list tsl
        on s.sd_oid = tsl.sd_oid;";
        using var conn = new NpgsqlConnection(_db_conn);
        conn.Execute(sql_string);
        _logging_helper.LogLine($"ad.{table_name} test data imported");
    }
    
    public void ImportTestStudyData()
    {
        // these common to all databases

        import_study_test_recs("studies");
        import_study_test_recs("study_titles");
        import_study_test_recs("study_identifiers");

        // these are database dependent
        if (_source.has_study_topics is true) import_study_test_recs("study_topics");
        if (_source.has_study_conditions is true) import_study_test_recs("study_conditions");
        if (_source.has_study_features is true) import_study_test_recs("study_features");
        if (_source.has_study_people is true) import_study_test_recs("study_people");
        if (_source.has_study_organisations is true) import_study_test_recs("study_organisations");
        if (_source.has_study_references is true) import_study_test_recs("study_references");
        if (_source.has_study_relationships is true) import_study_test_recs("study_relationships");
        if (_source.has_study_links is true) import_study_test_recs("study_links");
        if (_source.has_study_countries is true) import_study_test_recs("study_countries");
        if (_source.has_study_locations is true) import_study_test_recs("study_locations");
        if (_source.has_study_ipd_available is true) import_study_test_recs("study_ipd_available");
        if (_source.has_study_iec is true)
        {
            if (_source.study_iec_storage_type == "Single Table")
            {
                import_study_test_recs("study_iec");
            }

            if (_source.study_iec_storage_type == "By Year Groupings")
            {
                import_study_test_recs("study_iec_upto12", "study_iec");
                import_study_test_recs("study_iec_13to19", "study_iec");
                import_study_test_recs("study_iec_20on", "study_iec");
            }

            if (_source.study_iec_storage_type == "By Years")
            {
                import_study_test_recs("study_iec_null", "study_iec");
                import_study_test_recs("study_iec_pre06", "study_iec");
                import_study_test_recs("study_iec_0608", "study_iec");
                import_study_test_recs("study_iec_0910", "study_iec");
                import_study_test_recs("study_iec_1112", "study_iec");
                import_study_test_recs("study_iec_1314", "study_iec");
                for (int i = 15; i <= 30; i++)
                {
                    import_study_test_recs($"study_iec_{i}", "study_iec");
                }
            }
        }
        _logging_helper.LogLine("Study test data added");
    }


    public void ImportTestObjectData()
    {
        // object tables - these common to all databases

        import_object_test_recs("data_objects");
        import_object_test_recs("object_titles");

        // these are database dependent		

        if (_source.has_object_datasets is true) import_object_test_recs("object_datasets");
        if (_source.has_object_dates is true) import_object_test_recs("object_dates");
        if (_source.has_object_instances is true) import_object_test_recs("object_instances");       
        if (_source.has_object_people is true) import_object_test_recs("object_people");
        if (_source.has_object_organisations is true) import_object_test_recs("object_organisations");
        if (_source.has_object_topics is true) import_object_test_recs("object_topics");
        if (_source.has_object_comments is true) import_object_test_recs("object_comments");
        if (_source.has_object_descriptions is true) import_object_test_recs("object_descriptions");
        if (_source.has_object_identifiers is true) import_object_test_recs("object_identifiers");
        if (_source.has_object_db_links is true) import_object_test_recs("object_db_links");
        if (_source.has_object_publication_types is true) import_object_test_recs("object_publication_types");
        if (_source.has_journal_details is true) import_object_test_recs("journal_details");
        if (_source.has_object_relationships is true) import_object_test_recs("object_relationships");
        if (_source.has_object_rights is true) import_object_test_recs("object_rights");
        
        _logging_helper.LogLine("Object test data added");
    }

}



