﻿using Dapper;
using Npgsql;

namespace MDR_Importer;

class DataDeleter
{
    private readonly string _db_conn;
    private readonly ILoggingHelper _logging_helper;
    
    public DataDeleter(string db_conn, ILoggingHelper logging_helper)
    {
        _db_conn = db_conn;
        _logging_helper = logging_helper;
    }
    
    private readonly Dictionary<string, string> studyFields = new() 
    {
        { "studies", @"sd_sid, display_title, 
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id, 
        study_status_id, study_enrolment, study_gender_elig_id, min_age, 
        min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch, added_on" },
        { "study_identifiers", @"sd_sid, identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, identifier_link, added_on, coded_on" },
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
        { "study_iec", @"sd_sid, seq_num, leader, indent_level, level_seq_num,
        iec_type_id, iec_text, iec_class_id, iec_class, iec_parsed_text, added_on, coded_on" }
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
        { "object_instances", @"sd_oid, instance_type_id, repository_org_id, repository_org,
        repository_org_ror_id, url, url_accessible, url_last_checked, resource_type_id,
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
        identifier_org_id, identifier_org, identifier_org_ror_id, identifier_date, added_on, coded_on" },
        { "object_db_links", @"sd_oid, db_sequence, db_name, id_in_db, added_on" },
        { "object_publication_types", @"sd_oid, type_name, added_on" },
        { "object_rights", @"sd_oid, rights_name, rights_uri, comments, added_on" },
        { "object_relationships", @"sd_oid, relationship_type_id, target_sd_oid, added_on" }
    };
    
    public void DeleteStudyRecords(string table_name)
    {
        string sql_string = $@"delete from ad.{table_name} a
          using sd.studies t
          where a.sd_sid = t.sd_sid;";

        using var conn = new NpgsqlConnection(_db_conn);
        int res = conn.Execute(sql_string);
        _logging_helper.LogLine($"Deleted {res} from ad.{table_name}");
        CompactSequence(table_name, studyFields[table_name]);
    }
    
    public void DeleteObjectRecords(string table_name)
    {
        string sql_string = $@"delete from ad.{table_name} a
          using sd.data_objects t
          where a.sd_oid = t.sd_oid;";

        using var conn = new NpgsqlConnection(_db_conn);
        int res = conn.Execute(sql_string);
        _logging_helper.LogLine($"Deleted {res} from ad.{table_name}");
        CompactSequence(table_name, objectFields[table_name]);

    }

    private void CompactSequence(string table_name, string data_columns)
    {
        // from https://dba.stackexchange.com/questions/111823/compacting-a-sequence-in-postgresql.
        
        string tbl_new = table_name + "_new";
        string sql_string = $@"BEGIN;
        LOCK {table_name};
        CREATE TABLE {tbl_new} (LIKE {table_name} INCLUDING ALL);

        INSERT INTO {tbl_new} -- no target list in this case
        SELECT row_number() OVER (ORDER BY id), {data_columns}  -- all columns in default order
        FROM  {table_name};
        ALTER SEQUENCE {table_name}_id_seq OWNED BY {tbl_new}.id;  -- make new table own sequence

        DROP TABLE {tbl_new};
        ALTER TABLE {tbl_new} RENAME TO {table_name};
        SELECT setval('{table_name}_id_seq', max(id)) FROM {table_name};  -- reset sequence
        COMMIT;";

        using var conn = new NpgsqlConnection(_db_conn);
        conn.Execute(sql_string);
    }
}