

namespace MDR_Importer;

class DataObjectDataAdder
{
    ILoggingHelper _logging_helper;
    DBUtilities dbu;

    public DataObjectDataAdder(string connstring, ILoggingHelper logging_helper)
    {
        _logging_helper = logging_helper;
        dbu = new DBUtilities(connstring, _logging_helper);
    }


    #region Table data transfer

    public void TransferDataObjects()
    {
        string sql_string = @"INSERT INTO ad.data_objects(sd_oid, sd_sid, 
        title, version, display_title, doi, doi_status_id, publication_year,
        object_class_id, object_type_id, 
        managing_org_id, managing_org, managing_org_ror_id, lang_code, access_type_id,
        access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
        add_study_topics, datetime_of_data_fetch, record_hash, object_full_hash)
        SELECT s.sd_oid, s.sd_sid, 
        trim(title), version, trim(display_title), doi, doi_status_id, publication_year,
        object_class_id, object_type_id, managing_org_id, 
        managing_org, managing_org_ror_id, lang_code, access_type_id,
        access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
        add_study_topics, datetime_of_data_fetch, record_hash, object_full_hash
        FROM sd.data_objects s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "data_objects", "Adding");
    }


    public void TransferDataSetProperties()
    {
        string sql_string = @"INSERT INTO ad.object_datasets(sd_oid, 
        record_keys_type_id, record_keys_details, 
        deident_type_id, deident_direct, deident_hipaa,
        deident_dates, deident_nonarr, deident_kanon, deident_details,
        consent_type_id, consent_noncommercial, consent_geog_restrict,
        consent_research_type, consent_genetic_only, consent_no_methods, consent_details,
        record_hash)
        SELECT s.sd_oid, 
        record_keys_type_id, record_keys_details, 
        deident_type_id, deident_direct, deident_hipaa,
        deident_dates, deident_nonarr, deident_kanon, deident_details,
        consent_type_id, consent_noncommercial, consent_geog_restrict,
        consent_research_type, consent_genetic_only, consent_no_methods, consent_details,
        record_hash
        FROM sd.object_datasets s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_datasets", "Adding");
    }


    public void TransferObjectInstances()
    {
        string sql_string = @"INSERT INTO ad.object_instances(sd_oid, 
        instance_type_id, repository_org_id, repository_org,
        url, url_accessible, url_last_checked, resource_type_id,
        resource_size, resource_size_units, resource_comments, record_hash)
        SELECT s.sd_oid, 
        instance_type_id, repository_org_id, repository_org,
        url, url_accessible, url_last_checked, resource_type_id,
        resource_size, resource_size_units, resource_comments, record_hash
        FROM sd.object_instances s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_instances", "Adding");
    }

    public void TransferObjectTitles()
    {
        string sql_string = @"INSERT INTO ad.object_titles(sd_oid, 
        title_type_id, title_text, lang_code,
        lang_usage_id, is_default, comments, record_hash)
        SELECT s.sd_oid, 
        title_type_id, title_text, lang_code,
        lang_usage_id, is_default, comments, record_hash
        FROM sd.object_titles s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_titles", "Adding");
    }
    

    public void TransferObjectDates()
    {
        string sql_string = @"INSERT INTO ad.object_dates(sd_oid, 
        date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details, record_hash)
        SELECT s.sd_oid, 
        date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details, record_hash
        FROM sd.object_dates s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_dates", "Adding");
    }

    public void TransferObjectContributors()
    {
        string sql_string = @"INSERT INTO ad.object_contributors(sd_oid, 
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id, record_hash)
        SELECT s.sd_oid, 
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id, record_hash
        FROM sd.object_contributors s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_contributors", "Adding");
    }

    public void TransferObjectTopics()
    {
        string sql_string = @"INSERT INTO ad.object_topics(sd_oid, 
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value, record_hash)
        SELECT s.sd_oid,  
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value, record_hash
        FROM sd.object_topics s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_topics", "Adding");
    }


    public void TransferObjectComments()
    {
        string sql_string = @"INSERT INTO ad.object_comments(sd_oid, 
        ref_type, ref_source, pmid, pmid_version, notes, record_hash)
        SELECT s.sd_oid,  
        ref_type, ref_source, pmid, pmid_version, notes, record_hash
        FROM sd.object_comments s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_comments", "Adding");
    }


    public void TransferObjectDescriptions()
    {
        string sql_string = @"INSERT INTO ad.object_descriptions(sd_oid, 
        description_type_id, label, description_text, lang_code, 
        record_hash)
        SELECT s.sd_oid, 
        description_type_id, label, description_text, lang_code, 
        record_hash
        FROM sd.object_descriptions s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_descriptions", "Adding");
    }

    public void TransferObjectIdentifiers()
    {
        string sql_string = @"INSERT INTO ad.object_identifiers(sd_oid, 
        identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, record_hash)
        SELECT s.sd_oid, 
        identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, record_hash
        FROM sd.object_identifiers s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_identifiers", "Adding");
    }

    public void TransferObjectDBLinks()
    {
        string sql_string = @"INSERT INTO ad.object_db_links(sd_oid, 
        db_sequence, db_name, id_in_db, record_hash)
        SELECT s.sd_oid, 
        db_sequence, db_name, id_in_db, record_hash
        FROM sd.object_db_links s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_db_links", "Adding");
    }

    public void TransferObjectPublicationTypes()
    {
        string sql_string = @"INSERT INTO ad.object_publication_types(sd_oid, 
        type_name, record_hash)
        SELECT s.sd_oid, 
        type_name, record_hash
        FROM sd.object_publication_types s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_publication_types", "Adding");
    }


    public void TransferObjectRights()
    {
        string sql_string = @"INSERT INTO ad.object_rights(sd_oid, 
        rights_name, rights_uri, comments, record_hash)
        SELECT s.sd_oid, 
        rights_name, rights_uri, comments, record_hash
        FROM sd.object_rights s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_rights", "Adding");
    }


    public void TransferObjectRelationships()
    {
        string sql_string = @"INSERT INTO ad.object_relationships(sd_oid, 
        relationship_type_id, target_sd_oid, record_hash)
        SELECT s.sd_oid, 
        relationship_type_id, target_sd_oid, record_hash
        FROM sd.object_relationships s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "object_relationships", "Adding");
    }

    #endregion

    public void TransferObjectHashes()
    {
        for (int n = 50; n < 64; n++)
        {
            string sql_string = @"INSERT INTO ad.object_hashes(sd_oid, 
             hash_type_id, composite_hash)
             SELECT d.sd_oid,  
             hash_type_id, composite_hash
             FROM sd.object_hashes d
             INNER JOIN sd.to_ad_object_recs nd
             ON d.sd_oid = nd.sd_oid
             WHERE nd.status = 1
             and d.hash_type_id = " + n.ToString();

            int res = dbu.ExecuteSQL(sql_string);
            if (res > 0)
            {
                string hashType = GetHashType(n);
                _logging_helper.LogLine("Inserting " + res.ToString() + " new object hashes - type " + n.ToString() + ": " + hashType);
            }
        }
    }


    private string GetHashType(int n)
    {
        string hashType = "??????";
        switch (n)
        {
            case 50: { hashType = "datasets"; break; }
            case 51: { hashType = "instances"; break; }
            case 52: { hashType = "titles"; break; }
            case 53: { hashType = "dates"; break; }
            case 54: { hashType = "topics"; break; }
            case 55: { hashType = "contributors"; break; }
            case 56: { hashType = "relationships"; break; }
            case 57: { hashType = "descriptions"; break; }
            case 58: { hashType = "languages"; break; }
            case 59: { hashType = "rights"; break; }
            case 60: { hashType = "db links"; break; }
            case 61: { hashType = "comments"; break; }
            case 62: { hashType = "publication types"; break; }
            case 63: { hashType = "identifiers"; break; }
        }
        return hashType;
    }
}