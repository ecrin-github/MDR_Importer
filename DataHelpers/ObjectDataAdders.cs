

namespace MDR_Importer;

class ObjectDataAdder
{
    private readonly DBUtilities _dbu;

    public ObjectDataAdder(string db_conn, ILoggingHelper logging_helper)
    {
        _dbu = new DBUtilities(db_conn, logging_helper);
    }

    private readonly Dictionary<string, string> addFields = new() 
    {
        { "object_datasets", @"sd_oid, record_keys_type_id, record_keys_details, 
        deident_type_id, deident_direct, deident_hipaa,
        deident_dates, deident_nonarr, deident_kanon, deident_details,
        consent_type_id, consent_noncommercial, consent_geog_restrict,
        consent_research_type, consent_genetic_only, consent_no_methods, consent_details" },
        { "object_instances", @"sd_oid, instance_type_id, repository_org_id, repository_org,
        url, url_accessible, url_last_checked, resource_type_id,
        resource_size, resource_size_units, resource_comments" },
        { "object_titles", @"sd_oid, title_type_id, title_text, lang_code,
        lang_usage_id, is_default, comments" },
        { "object_dates", @"sd_oid, date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details" },
        { "object_people", @"sd_oid, contrib_type_id, person_id, person_given_name, 
        person_family_name, person_full_name, orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id" },
        { "object_organisations", @"sd_oid, contrib_type_id, organisation_id, 
        organisation_name, organisation_ror_id" },
        { "object_topics", @"sd_oid, topic_type_id, original_value, original_ct_type_id,
          original_ct_code, mesh_code, mesh_value" },
        { "object_comments", @"sd_oid, ref_type, ref_source, pmid, pmid_version, notes" },
        { "object_descriptions", @"sd_oid, description_type_id, label, description_text, lang_code" },
        { "object_identifiers", @"sd_oid, identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id, identifier_date" },
        { "object_db_links", @"sd_oid, db_sequence, db_name, id_in_db" },
        { "object_publication_types", @"sd_oid, type_name" },
        { "object_rights", @"sd_oid, rights_name, rights_uri, comments" },
        { "object_relationships", @"sd_oid, relationship_type_id, target_sd_oid" }
    };
    
    public void AddDataObjects()
    {
        string sql_string = @"INSERT INTO ad.data_objects(sd_oid, sd_sid, 
        title, version, display_title, doi, doi_status_id, publication_year,
        object_class_id, object_type_id, 
        managing_org_id, managing_org, managing_org_ror_id, lang_code, access_type_id,
        access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
        add_study_topics, datetime_of_data_fetch)
        SELECT sd_oid, sd_sid, 
        trim(title), version, trim(display_title), doi, doi_status_id, publication_year,
        object_class_id, object_type_id, managing_org_id, 
        managing_org, managing_org_ror_id, lang_code, access_type_id,
        access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
        add_study_topics, datetime_of_data_fetch
        FROM sd.data_objects s ";

        _dbu.ExecuteTransferSQL(sql_string, "data_objects", "Adding");
    }

    public void AddData(string table_name)
    {
        string fields = addFields[table_name];
        
        string sql_string = $@"INSERT INTO ad.{table_name} ({fields})
        SELECT {fields}
        FROM sd.{table_name} s ";

        _dbu.ExecuteTransferSQL(sql_string, table_name, "Adding");
    }
  
}