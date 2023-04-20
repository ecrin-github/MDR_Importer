using Dapper;
using Npgsql;
namespace MDR_Importer;

class ObjectTablesTransferrer
{
    private readonly string _source_id;
    private readonly string _db_conn;

    public ObjectTablesTransferrer(int? source_id, string db_conn)
    {
        _source_id = source_id?.ToString() ?? "";
        _db_conn = db_conn;
    }


    private void Execute_SQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(_db_conn);
        conn.Execute(sql_string);
    }


    public void TransferDataObjects()
    {
        string sql_string = @"INSERT INTO adcomp.data_objects(source_id, sd_oid, sd_sid, 
        display_title, version, doi, doi_status_id, publication_year,
        object_class_id, object_type_id,  
        managing_org_id, managing_org, managing_org_ror_id, lang_code, access_type_id,
        access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
        add_study_topics, datetime_of_data_fetch, object_full_hash)
        SELECT " + _source_id + @", sd_oid, sd_sid, 
        display_title, version, doi, doi_status_id, publication_year,
        object_class_id, object_type_id, 
        managing_org_id, managing_org, managing_org_ror_id, lang_code, access_type_id,
        access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
        add_study_topics, datetime_of_data_fetch, object_full_hash
        FROM ad.data_objects;";

        Execute_SQL(sql_string);
    }


    public void TransferObjectDatasets()
    {
        string sql_string = @"INSERT INTO adcomp.object_datasets(source_id, sd_oid,
        record_keys_type_id, record_keys_details, 
        deident_type_id, deident_direct, deident_hipaa,
        deident_dates, deident_nonarr, deident_kanon, deident_details,
        consent_type_id, consent_noncommercial, consent_geog_restrict,
        consent_research_type, consent_genetic_only, consent_no_methods, consent_details,
        record_hash)
        SELECT " + _source_id + @", sd_oid,
        record_keys_type_id, record_keys_details, 
        deident_type_id, deident_direct, deident_hipaa,
        deident_dates, deident_nonarr, deident_kanon, deident_details,
        consent_type_id, consent_noncommercial, consent_geog_restrict,
        consent_research_type, consent_genetic_only, consent_no_methods, consent_details,
        record_hash
        FROM ad.object_datasets";

        Execute_SQL(sql_string);
    }


    public void TransferObjectInstances()
    {
        string sql_string = @"INSERT INTO adcomp.object_instances(source_id, sd_oid,
        instance_type_id, system_id, system,
        url, url_accessible, url_last_checked, resource_type_id, 
        resource_size, resource_size_units, resource_comments)
        SELECT " + _source_id + @", sd_oid,
        instance_type_id, system_id, system,
        url, url_accessible, url_last_checked, resource_type_id, 
        resource_size, resource_size_units, resource_comments
        FROM ad.object_instances";

        Execute_SQL(sql_string);
    }


    public void TransferObjectTitles()
    {
        string sql_string = @"INSERT INTO adcomp.object_titles(source_id, sd_oid,
        title_type_id, title_text, lang_code,
        lang_usage_id, is_default, comments)
        SELECT " + _source_id + @", sd_oid,
        title_type_id, title_text, lang_code,
        lang_usage_id, is_default, comments
        FROM ad.object_titles";

        Execute_SQL(sql_string);
    }


    public void TransferObjectDates()
    {
        string sql_string = @"INSERT INTO adcomp.object_dates(source_id, sd_oid, 
        date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details)
        SELECT " + _source_id + @", sd_oid,
        date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details
        FROM ad.object_dates";

        Execute_SQL(sql_string);
    }


    public void TransferObjectContributors()
    {
        string sql_string = @"INSERT INTO adcomp.object_contributors(source_id, sd_oid,
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id)
        SELECT " + _source_id + @", sd_oid,
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id
        FROM ad.object_contributors";

        Execute_SQL(sql_string);
    }


    public void TransferObjectTopics()
    {
        string sql_string = @"INSERT INTO adcomp.object_topics(source_id, sd_oid, 
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value)
        SELECT " + _source_id + @", sd_oid,
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value
        FROM ad.object_topics";

        Execute_SQL(sql_string);
    }


    public void TransferObjectComments()
    {
        string sql_string = @"INSERT INTO adcomp.object_comments(source_id, sd_oid, 
        ref_type, ref_source, pmid, pmid_version, notes)
        SELECT " + _source_id + @", sd_oid,
        ref_type, ref_source, pmid, pmid_version, notes
        FROM ad.object_comments";

        Execute_SQL(sql_string);
    }


    public void TransferObjectDescriptions()
    {
        string sql_string = @"INSERT INTO adcomp.object_descriptions(source_id, sd_oid,
        description_type_id, label, description_text,
        lang_code)
        SELECT " + _source_id + @", sd_oid,
        description_type_id, label, description_text, 
        lang_code
        FROM ad.object_descriptions";

        Execute_SQL(sql_string);
    }


    public void TransferObjectidentifiers()
    {
        string sql_string = @"INSERT INTO adcomp.object_identifiers(source_id, sd_oid, 
        identifier_value, identifier_type_id, 
        source_id, source, source_ror_id,
        identifier_date)
        SELECT " + _source_id + @", sd_oid, 
        identifier_value, identifier_type_id, 
        source_id, source, source_ror_id,
        identifier_date
        FROM ad.object_identifiers";

        Execute_SQL(sql_string);
    }


    public void TransferObjectDBLinks()
    {
        string sql_string = @"INSERT INTO adcomp.object_db_links(source_id, sd_oid,
        db_sequence, db_name, id_in_db)
        SELECT " + _source_id + @", sd_oid,
        db_sequence, db_name, id_in_db
        FROM ad.object_db_links";

        Execute_SQL(sql_string);
    }

    public void TransferObjectPublicationTypes()
    {
        string sql_string = @"INSERT INTO adcomp.object_publication_types(source_id, sd_oid, 
        type_name)
        SELECT " + _source_id + @", sd_oid,
        type_name
        FROM ad.object_publication_types";

        Execute_SQL(sql_string);
    }


    public void TransferObjectRights()
    {
        string sql_string = @"INSERT INTO adcomp.object_rights(source_id, sd_oid,
        rights_name, rights_uri, comments)
        SELECT " + _source_id + @", sd_oid,
        rights_name, rights_uri, comments
        FROM ad.object_rights";

        Execute_SQL(sql_string);
    }


    public void TransferObjectRelationships()
    {
        string sql_string = @"INSERT INTO adcomp.object_relationships(source_id, sd_oid, 
        relationship_type_id, target_sd_oid)
        SELECT " + _source_id + @", sd_oid, 
        relationship_type_id, target_sd_oid
        FROM ad.object_relationships";

        Execute_SQL(sql_string);
    }


    public void TransferObjectHashes()
    {
        string sql_string = @"INSERT INTO adcomp.object_hashes(source_id, sd_oid,
        hash_type_id, composite_hash)
        SELECT " + _source_id + @", sd_oid,
        hash_type_id, composite_hash
        FROM ad.object_hashes";

        Execute_SQL(sql_string);
    }

}