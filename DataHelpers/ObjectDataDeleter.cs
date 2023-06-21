using Dapper;
using Npgsql;

namespace MDR_Importer;

class ObjectDataDeleter
{
    private readonly string _db_conn;
    private readonly ILoggingHelper _logging_helper;
    private readonly  DBUtilities _dbu;
    private readonly  bool _newTables;
    
    public ObjectDataDeleter(string db_conn, ILoggingHelper logging_helper, bool newTables)
    {
        _db_conn = db_conn;
        _logging_helper = logging_helper;
        _newTables = newTables;
        _dbu = new DBUtilities(db_conn, logging_helper);
    }
    
    private readonly Dictionary<string, string> objectFields = new() 
    {
        { "data_objects", @"sd_oid, sd_sid, 
        title, version, display_title, doi, doi_status_id, publication_year,
        object_class_id, object_type_id, 
        managing_org_id, managing_org, managing_org_ror_id, lang_code, access_type_id,
        access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
        add_study_topics, datetime_of_data_fetch, added_on, coded_on" },
        { "object_datasets", @"sd_oid, record_keys_type_id, record_keys_details, 
        deident_type_id, deident_direct, deident_hipaa,
        deident_dates, deident_nonarr, deident_kanon, deident_details,
        consent_type_id, consent_noncommercial, consent_geog_restrict,
        consent_research_type, consent_genetic_only, consent_no_methods, consent_details, added_on" },
        { "object_instances", @"sd_oid, system_id, system,
        url, url_accessible, url_last_checked, resource_type_id,
        resource_size, resource_size_units, resource_comments, added_on, coded_on " },
        { "object_titles", @"sd_oid, title_type_id, title_text, lang_code,
        lang_usage_id, is_default, comments, added_on" },
        { "object_dates", @"sd_oid, date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details, added_on" },
        { "object_people", @"sd_oid, contrib_type_id, person_given_name, 
        person_family_name, person_full_name, orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id, added_on, coded_on" },
        { "object_organisations", @"sd_oid, contrib_type_id, organisation_id, 
        organisation_name, organisation_ror_id, added_on, coded_on" },
        { "object_topics", @"sd_oid, topic_type_id, original_value, original_ct_type_id,
         original_ct_code, mesh_code, mesh_value, added_on, coded_on" },
        { "object_comments", @"sd_oid, ref_type, ref_source, pmid, pmid_version, notes, added_on" },
        { "object_descriptions", @"sd_oid, description_type_id, label, description_text, lang_code, added_on" },
        { "object_identifiers", @"sd_oid, identifier_value, identifier_type_id, 
        source_id, source, source_ror_id, identifier_date, added_on, coded_on" },
        { "object_db_links", @"sd_oid, db_sequence, db_name, id_in_db, added_on" },
        { "object_publication_types", @"sd_oid, type_name, added_on" },
        { "object_rights", @"sd_oid, rights_name, rights_uri, comments, added_on" },
        { "object_relationships", @"sd_oid, relationship_type_id, target_sd_oid, added_on" },
        { "journal_details", @"sd_oid, pissn, eissn, journal_title, publisher_id, 
        publisher, added_on, coded_on" }
    };
    
    public int GetADRecordCount(string table_name)
    {
        string sql_string = @"select count(*) from ad." + table_name;
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }
    
    public int DeleteRecords(string table_name)
    {
        if (_newTables)
        {
            return 0;   // easier to put a single final check here than have multiple tests in calling code
        }
        _logging_helper.LogLine("");
        
        string sql_string = $@"delete from ad.{table_name} s 
                               using sd.data_objects t
                               where s.sd_oid = t.sd_oid ";
        
        int res = _dbu.ExecuteDeleteSQL(sql_string, table_name, 200000);
        
        // if ad.table is empty reset the sequence number to 1. Otherwise, if ad.table size < 90%
        // of current max id, compact the sequence in the ad table before adding new records from sd

        int ad_size = GetADRecordCount(table_name);
        using var conn = new NpgsqlConnection(_db_conn);
        if (ad_size == 0)
        {
            sql_string = $"SELECT setval('ad.{table_name}_id_seq', 1, false); "; 
            conn.Execute(sql_string);
            _logging_helper.LogLine($"Id sequence for ad.{table_name} reset to 1");
        }
        else
        {
            int max_id = _dbu.GetADRecordMax(table_name);
            if (ad_size < 0.9 * max_id)
            {
                CompactSequence(ad_size, max_id, table_name, objectFields[table_name]);
            }
        }
        return res;
    }
           
    
    private void CompactSequence(int ad_size, int max_id, string table_name, string data_columns)
    {
        string tbl_new = table_name + "_new";
        string sql_string = $@"CREATE TABLE IF NOT EXISTS ad.{tbl_new} (LIKE ad.{table_name} INCLUDING ALL);";
        using var conn = new NpgsqlConnection(_db_conn);
        conn.Execute(sql_string);

        _logging_helper.LogLine($"Compaction required for {table_name}, (max id: {ad_size}, actual size: {max_id})");
        int rec_batch = 200000;
        string fbc = $"records to ad.{tbl_new} from ad.{table_name},";
        if (ad_size > rec_batch)
        {
            for (int i = 0; i <= max_id; i += rec_batch)
            {
                // Sends across all columns in default order

                sql_string = $@"INSERT INTO ad.{tbl_new} ({data_columns})  
                SELECT {data_columns} 
                FROM ad.{table_name} t where t.id > {i} and t.id <= {i + rec_batch}
                order by t.id;";
                int e = i + rec_batch < max_id ? i + rec_batch - 1 : max_id;
                int res = conn.Execute(sql_string);
                _logging_helper.LogLine($"Transferring {res} {fbc} ids {i} to {e}");
            }
        }
        else
        {
            sql_string = $@"INSERT INTO ad.{tbl_new} ({data_columns})  
                SELECT {data_columns} 
                FROM ad.{table_name} t
                order by t.id;";
            int res1 = conn.Execute(sql_string);
            _logging_helper.LogLine($"Transferring {res1} {fbc} as a single batch");
        }

        sql_string = $@"DROP TABLE ad.{table_name};
        ALTER TABLE ad.{tbl_new} RENAME TO {table_name};";
        conn.Execute(sql_string);

        sql_string = $@"ALTER SEQUENCE ad.{tbl_new}_id_seq RENAME TO {table_name}_id_seq; 
                     ALTER INDEX ad.{tbl_new}_sd_oid_idx RENAME TO {table_name}_oid;" ;
        conn.Execute(sql_string);
        
        // Some special cases that have two indexes, so an additional one to rename

        if (table_name == "data_objects")
        {
            sql_string = $@"ALTER INDEX ad.{tbl_new}_sd_sid_idx RENAME TO {table_name}_sid;" ;
            conn.Execute(sql_string);
        }
        if (table_name == "object_relationships")
        {
            sql_string = $@"ALTER INDEX ad.{tbl_new}_target_sd_oid_idx RENAME TO {table_name}_target_oid;" ;
            conn.Execute(sql_string);
        }
        
        _logging_helper.LogLine($"Compacted sequence for ad.{table_name}, to {ad_size} rows");
    }
}

