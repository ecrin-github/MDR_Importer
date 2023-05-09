using Dapper;
using Npgsql;

namespace MDR_Importer;

class DataDeleter
{
    private readonly string _db_conn;
    private readonly ILoggingHelper _logging_helper;
    private readonly  DBUtilities _dbu;
    
    public DataDeleter(string db_conn, ILoggingHelper logging_helper)
    {
        _db_conn = db_conn;
        _logging_helper = logging_helper;
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
    
    public int GetNextSequenceValue(string table_name)
    {
        string sql_string = $@"SELECT nextval('ad.{table_name}_id_seq')";
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }
    
    public int DeleteRecords(string table_type, string table_name)
    {
        string sql_string = $@"delete from ad.{table_name} s ";
        if (table_type == "st")
        {
            sql_string +=  @"using sd.studies t
             where s.sd_sid = t.sd_sid ";
        }
        else
        {
            sql_string += @" using sd.data_objects t
             where s.sd_oid = t.sd_oid ";
        }
        int res = _dbu.ExecuteDeleteSQL(sql_string, table_name, 200000);
        _logging_helper.LogLine($"Deleted {res} records from ad.{table_name}");
        
        // if ad.table is empty reset the sequence number to 1. Otherwise, if ad.table size < 90%
        // of current max id, compact the sequence in the ad table before adding new records from sd

        int ad_size = GetADRecordCount(table_name);
        using var conn = new NpgsqlConnection(_db_conn);
        if (ad_size == 0)
        {
            sql_string = $"SELECT setval('ad.{table_name}_id_seq', 1) FROM ad.{table_name}"; 
            conn.Execute(sql_string);
        }
        else
        {
            int next_id = GetNextSequenceValue(table_name);
            if (ad_size < 0.9 * next_id)
            {
                if (table_type == "st")
                {
                    string data_columns = table_name.StartsWith("study_iec")
                        ? studyFields["study_iec"]
                        : studyFields[table_name];
                    CompactSequence(ad_size, next_id, table_name, data_columns, "sid");
                }
                else
                {
                    CompactSequence(ad_size, next_id, table_name, objectFields[table_name], "oid");
                }
            }
        }
        return res;
    }
           
    
    private void CompactSequence(int ad_size, int next_id, string table_name, string data_columns, string id_suffix)
    {
        string tbl_new = table_name + "_new";
        string sql_string = $@"CREATE TABLE IF NOT EXISTS ad.{tbl_new} (LIKE ad.{table_name} INCLUDING ALL);";
        using var conn = new NpgsqlConnection(_db_conn);
        conn.Execute(sql_string);

        int rec_batch = 200000;
        if (ad_size > rec_batch)
        {
            for (int i = 0; i <= next_id; i += rec_batch)
            {
                // Sends across all columns in default order

                sql_string = $@"INSERT INTO ad.{tbl_new} ({data_columns})  
                SELECT {data_columns} 
                FROM ad.{table_name} where id > {i} and id <= {i + rec_batch}
                order by id;";
                int e = i + rec_batch < next_id ? i + rec_batch - 1 : next_id;
                int res = conn.Execute(sql_string);
                _logging_helper.LogLine($"Transferring {res} records to ad.{tbl_new} from ad.{table_name}, ids {i} to {e}");
            }
        }
        else
        {
            sql_string = $@"INSERT INTO ad.{tbl_new} ({data_columns})  
                SELECT {data_columns} 
                FROM ad.{table_name} 
                order by id;";
            int res1 = conn.Execute(sql_string);
            _logging_helper.LogLine($"Transferring {res1} records to ad.{tbl_new} from ad.{table_name}, as a single batch");
        }

        sql_string = $@"DROP TABLE ad.{table_name};
        ALTER TABLE ad.{tbl_new} RENAME TO {table_name};";
        conn.Execute(sql_string);

        sql_string = $@"ALTER SEQUENCE ad.{tbl_new}_id_seq RENAME TO {table_name}_id_seq; 
                     ALTER INDEX ad.{tbl_new}_sd_{id_suffix}_idx RENAME TO {table_name}_{id_suffix};" ;
        conn.Execute(sql_string);
        
        // Some special cases that have two indexes, so an additional one to rename

        if (table_name == "data_objects")
        {
            sql_string = $@"ALTER INDEX ad.{tbl_new}_sd_sid_idx RENAME TO {table_name}_sid;" ;
            conn.Execute(sql_string);
        }
        if (table_name == "study_relationships")
        {
            sql_string = $@"ALTER INDEX ad.{tbl_new}_target_sd_sid_idx RENAME TO {table_name}_target_sid;" ;
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

