using Dapper;
using Npgsql;

namespace MDR_Importer;

class StudyDataDeleter
{
    private readonly string _db_conn;
    private readonly ILoggingHelper _logging_helper;
    private readonly  DBUtilities _dbu;
    private readonly  bool _newTables;
    
    public StudyDataDeleter(string db_conn, ILoggingHelper logging_helper, bool newTables)
    {
        _db_conn = db_conn;
        _logging_helper = logging_helper;
        _newTables = newTables;
        _dbu = new DBUtilities(db_conn, logging_helper);
    }
    
    private readonly Dictionary<string, string> studyFields = new() 
    {
        { "studies", @"sd_sid, display_title, 
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id, 
        study_status_id, study_enrolment, study_gender_elig_id, min_age, 
        min_age_units_id, max_age, max_age_units_id, iec_level, datetime_of_data_fetch, added_on" },
        { "study_identifiers", @"sd_sid, identifier_value, identifier_type_id, 
        source_id, source, source_ror_id, identifier_date, identifier_link, added_on, coded_on" },
        { "study_titles", @"sd_sid, title_type_id, title_text, lang_code, lang_usage_id,
        is_default, comments, added_on" },
        { "study_references", @"sd_sid, pmid, citation, doi, type_id, comments, added_on" },
        { "study_people", @"sd_sid, contrib_type_id, person_given_name, 
        person_family_name, person_full_name, orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id, added_on, coded_on" },
        { "study_organisations", @"sd_sid, contrib_type_id, organisation_id, 
        organisation_name, organisation_ror_id, added_on, coded_on" },
        { "study_topics", @"sd_sid, topic_type_id, original_value, original_ct_type_id, 
        original_ct_code, mesh_code, mesh_value, added_on, coded_on" },
        { "study_relationships", @"sd_sid, relationship_type_id, target_sd_sid, added_on" },
        { "study_features", @"sd_sid, feature_type_id, feature_value_id, added_on" },
        { "study_links", @"sd_sid, link_label, link_url, added_on" },
        { "study_countries", @"sd_sid, country_id, country_name, status_id, added_on, coded_on" },
        { "study_locations", @"sd_sid, facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id, added_on, coded_on" },
        { "study_ipd_available", @"sd_sid, ipd_id, ipd_type, ipd_url, ipd_comment, added_on" },
        { "study_conditions", @"sd_sid, original_value, original_ct_type_id, original_ct_code, 
        icd_code, icd_name, added_on, coded_on" },
        { "study_iec", @"sd_sid, seq_num, iec_type_id, split_type, leader, indent_level, 
        sequence_string, iec_text, iec_class_id, iec_class, iec_parsed_text, added_on, coded_on" }
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
                               using sd.studies t
                               where s.sd_sid = t.sd_sid ";
        
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
                string data_columns = table_name.StartsWith("study_iec")
                    ? studyFields["study_iec"]
                    : studyFields[table_name];
                CompactSequence(ad_size, max_id, table_name, data_columns);
                
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
                     ALTER INDEX ad.{tbl_new}_sd_sid_idx RENAME TO {table_name}_sid;" ;
        conn.Execute(sql_string);
        
        // Some special cases that have two indexes, so an additional one to rename
        
        if (table_name == "study_relationships")
        {
            sql_string = $@"ALTER INDEX ad.{tbl_new}_target_sd_sid_idx RENAME TO {table_name}_target_sid;" ;
            conn.Execute(sql_string);
        }
        
        _logging_helper.LogLine($"Compacted sequence for ad.{table_name}, to {ad_size} rows");
    }
}

