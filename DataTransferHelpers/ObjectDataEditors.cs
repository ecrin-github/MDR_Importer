
namespace MDR_Importer;

class DataObjectDataEditor
{
    ILoggingHelper _logging_helper;
    DBUtilities dbu;

    public DataObjectDataEditor(string connstring, ILoggingHelper logging_helper)
    {
        _logging_helper = logging_helper;
        dbu = new DBUtilities(connstring, _logging_helper);
    }


    public void EditDataObjects()
    {
        // if the record hash for the data object has changed, then 
        // the data in the data objects records should be changed

        string sql_string = @"UPDATE ad.data_objects a
         set 
         title = trim(t.title),
         version = t.version,
         display_title = trim(t.display_title), 
         doi = t.doi,  
         doi_status_id = t.doi_status_id,  
         publication_year = t.publication_year, 
         object_class_id = t.object_class_id,  
         object_type_id = t.object_type_id,  
         managing_org_id = t.managing_org_id,  
         managing_org = t.managing_org,  
         managing_org_ror_id = t.managing_org_ror_id,
         lang_code = t.lang_code, 
         access_type_id = t.access_type_id, 
         access_details = t.access_details,  
         access_details_url = t.access_details_url,  
         url_last_checked = t.url_last_checked, 
         eosc_category = t.eosc_category,
         add_study_contribs = t.add_study_contribs, 
         add_study_topics = t.add_study_topics,  
         datetime_of_data_fetch = t.datetime_of_data_fetch,  
         record_hash = t.record_hash, 
         last_edited_on = current_timestamp
         from (select so.* from sd.data_objects so
           INNER JOIN sd.to_ad_object_recs ts
           ON so.sd_oid = ts.sd_oid ";

        string base_string = @" where ts.object_rec_status = 2) t
                      where a.sd_oid = t.sd_oid";

        dbu.EditEntityRecords(sql_string, base_string, "data_objects");
    }


    public void EditDataSetProperties()
    {
        // if the record hash for the dataset properties has changed, then 
        // the data should be changed

        string sql_string = @"UPDATE ad.object_datasets a
         set 
         record_keys_type_id = t.record_keys_type_id, 
         record_keys_details = t.record_keys_details, 
         deident_type_id = t.deident_type_id, 
         deident_direct = t.deident_direct,
         deident_hipaa = t.deident_hipaa,
         deident_dates = t.deident_dates, 
         deident_nonarr = t.deident_nonarr, 
         deident_kanon = t.deident_kanon, 
         deident_details = t.deident_details,
         consent_type_id = t.consent_type_id, 
         consent_noncommercial = t.consent_noncommercial, 
         consent_geog_restrict = t.consent_geog_restrict,
         consent_research_type = t.consent_research_type, 
         consent_genetic_only = t.consent_genetic_only, 
         consent_no_methods = t.consent_no_methods, 
         consent_details = t.consent_details,  
         record_hash = t.record_hash, 
         last_edited_on = current_timestamp
           from (select so.* from sd.object_datasets so
           INNER JOIN sd.to_ad_object_recs ts
           ON so.sd_oid = ts.sd_oid ";

        string base_string = @" where ts.object_dataset_status = 4) t
                      where a.sd_oid = t.sd_oid";

        dbu.EditEntityRecords(sql_string, base_string, "object_datasets");
    }


    #region Table data edits

    public void EditObjectInstances()
    {
        string sql_string = dbu.GetObjectTString(51);
        string sql_stringD = sql_string + dbu.GetObjectDeleteString("object_instances"); 

        string sql_stringI = sql_string + @"INSERT INTO ad.object_instances(sd_oid, 
        instance_type_id, repository_org_id, repository_org,
        url, url_accessible, url_last_checked, resource_type_id,
        resource_size, resource_size_units, resource_comments, record_hash)
        SELECT s.sd_oid, 
        instance_type_id, repository_org_id, repository_org,
        url, url_accessible, url_last_checked, resource_type_id,
        resource_size, resource_size_units, resource_comments, record_hash
        FROM sd.object_instances s
        INNER JOIN t
        on s.sd_oid = t.sd_oid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "object_instances");
    }


    public void EditObjectTitles()
    {
        string sql_string = dbu.GetObjectTString(52);
        string sql_stringD = sql_string + dbu.GetObjectDeleteString("object_titles"); 

        string sql_stringI = sql_string + @"INSERT INTO ad.object_titles(sd_oid, 
        title_type_id, title_text, lang_code,
        lang_usage_id, is_default, comments, record_hash)
        SELECT s.sd_oid, 
        title_type_id, title_text, lang_code,
        lang_usage_id, is_default, comments, record_hash
        FROM sd.object_titles s
        INNER JOIN t
        on s.sd_oid = t.sd_oid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "object_titles");
    }
    

    public void EditObjectDates()
    {
        string sql_string = dbu.GetObjectTString(53);
        string sql_stringD = sql_string + dbu.GetObjectDeleteString("object_dates");

        string sql_stringI = sql_string + @"INSERT INTO ad.object_dates(sd_oid, 
        date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details, record_hash)
        SELECT s.sd_oid, 
        date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details, record_hash
        FROM sd.object_dates s
        INNER JOIN t
        on s.sd_oid = t.sd_oid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "object_dates");
    }

    public void EditObjectContributors()
    {
        string sql_string = dbu.GetObjectTString(55);
        string sql_stringD = sql_string + dbu.GetObjectDeleteString("object_contributors");

        string sql_stringI = sql_string + @"INSERT INTO ad.object_contributors(sd_oid, 
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
        INNER JOIN t
        on s.sd_oid = t.sd_oid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "object_contributors");
    }

    public void EditObjectTopics()
    {
        string sql_string = dbu.GetObjectTString(54);
        string sql_stringD = sql_string + dbu.GetObjectDeleteString("object_topics");

        string sql_stringI = sql_string + @"INSERT INTO ad.object_topics(sd_oid, 
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value, record_hash)
        SELECT s.sd_oid,  
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value, record_hash
        FROM sd.object_topics s
        INNER JOIN t
        on s.sd_oid = t.sd_oid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "object_topics");
    }


    public void EditObjectComments()
    {
        string sql_string = dbu.GetObjectTString(61);
        string sql_stringD = sql_string + dbu.GetObjectDeleteString("object_comments");

        string sql_stringI = sql_string + @"INSERT INTO ad.object_comments(sd_oid, 
        ref_type, ref_source, pmid, pmid_version, notes, record_hash)
        SELECT s.sd_oid,  
        ref_type, ref_source, pmid, pmid_version, notes, record_hash
        FROM sd.object_comments s
        INNER JOIN t
        on s.sd_oid = t.sd_oid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "object_comments");
    }


    public void EditObjectDescriptions()
    {
        string sql_string = dbu.GetObjectTString(57);
        string sql_stringD = sql_string + dbu.GetObjectDeleteString("object_descriptions");

        string sql_stringI = sql_string + @"INSERT INTO ad.object_descriptions(sd_oid, 
        description_type_id, label, description_text, lang_code, 
        record_hash)
        SELECT s.sd_oid, 
        description_type_id, label, description_text, lang_code, 
        record_hash
        FROM sd.object_descriptions s
        INNER JOIN t
        on s.sd_oid = t.sd_oid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "object_descriptions");
    }


    public void EditObjectIdentifiers()
    {
        string sql_string = dbu.GetObjectTString(63);
        string sql_stringD = sql_string + dbu.GetObjectDeleteString("object_identifiers");

        string sql_stringI = sql_string + @"INSERT INTO ad.object_identifiers(sd_oid, 
        identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, record_hash)
        SELECT s.sd_oid, 
        identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, record_hash
        FROM sd.object_identifiers s
        INNER JOIN t
        on s.sd_oid = t.sd_oid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "object_identifiers");
    }


    public void EditObjectDBLinks()
    {
        string sql_string = dbu.GetObjectTString(60);
        string sql_stringD = sql_string + dbu.GetObjectDeleteString("object_db_links");

        string sql_stringI = sql_string + @"INSERT INTO ad.object_db_links(sd_oid, 
        db_sequence, db_name, id_in_db, record_hash)
        SELECT s.sd_oid, 
        db_sequence, db_name, id_in_db, record_hash
        FROM sd.object_db_links s
        INNER JOIN t
        on s.sd_oid = t.sd_oid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "object_db_links");
    }


    public void EditObjectPublicationTypes()
    {
        string sql_string = dbu.GetObjectTString(62);
        string sql_stringD = sql_string + dbu.GetObjectDeleteString("object_publication_types");

        string sql_stringI = sql_string + @"INSERT INTO ad.object_publication_types(sd_oid, 
        type_name, record_hash)
        SELECT s.sd_oid, 
        type_name, record_hash
        FROM sd.object_publication_types s
        INNER JOIN t
        on s.sd_oid = t.sd_oid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "object_publication_types");
    }


    public void EditObjectRights()
    {
        string sql_string = dbu.GetObjectTString(59);
        string sql_stringD = sql_string + dbu.GetObjectDeleteString("object_rights");

        string sql_stringI = sql_string + @"INSERT INTO ad.object_rights(sd_oid, 
        rights_name, rights_uri, comments, record_hash)
        SELECT s.sd_oid, 
        rights_name, rights_uri, comments, record_hash
        FROM sd.object_rights s
        INNER JOIN t
        on s.sd_oid = t.sd_oid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "object_rights");
    }


    public void EditObjectRelationships()
    {
        string sql_string = dbu.GetObjectTString(56);
        string sql_stringD = sql_string + dbu.GetObjectDeleteString("object_relationships");

        string sql_stringI = sql_string + @"INSERT INTO ad.object_relationships(sd_oid, 
        relationship_type_id, target_sd_oid, record_hash)
        SELECT s.sd_oid, 
        relationship_type_id, target_sd_oid, record_hash
        FROM sd.object_relationships s
        INNER JOIN t
        on s.sd_oid = t.sd_oid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "object_relationships");
    }

    #endregion

/*
    public void UpdateObjectsLastImportedDate(int import_id, int? source_id)
    {
        string top_string = @"UPDATE mon_sf.source_data_objects src
                      set last_import_id = " + import_id.ToString() + @", 
                      last_imported = current_timestamp
                      from 
                         (select so.id, so.sd_oid 
                          FROM sd.data_objects so
                          INNER JOIN sd.to_ad_object_recs ts
                          ON so.sd_oid = ts.sd_oid
                          where ts.status in (1, 2, 3) 
                         ";
        string base_string = @" ) s
                          where s.sd_oid = src.sd_id and
                          src.source_id = " + source_id.ToString();

        dbu.UpdateLastImportedDate("data_objects", top_string, base_string);
    }
*/

    public void UpdateDateOfDataObjectData()
    {
       string top_sql = @"with t as
        (
            select so.sd_oid, so.datetime_of_data_fetch
            from sd.data_objects so
            inner join sd.to_ad_object_recs td
            on so.sd_oid = td.sd_oid
            where td.status in (2, 3)";

        string base_sql = @")
        update ad.data_objects s
        set datetime_of_data_fetch = t.datetime_of_data_fetch
        from t
        where s.sd_oid = t.sd_oid";

        dbu.UpdateDateOfData("data_objects", top_sql, base_sql);
    }


    public void UpdateObjectCompositeHashes()
    {
        // Need to ensure that the hashes themselves are all up to date (for the next comparison)
        // Change the ones that have been changed in sd
        // if a very large studies (and therefore hash) table may need to chunk using a link to the 
        // sd.data_objects table....

        string sql_string = @"UPDATE ad.object_hashes ah
                set composite_hash = so.composite_hash
                FROM 
                    (SELECT st.id, ia.sd_oid, ia.hash_type_id, ia.composite_hash
                     FROM sd.to_ad_object_atts ia
                     INNER JOIN sd.data_objects st
                     on ia.sd_oid = st.sd_oid
                     where ia.status = 2) so
                WHERE ah.sd_oid = so.sd_oid
                and ah.hash_type_id = so.hash_type_id ";

        dbu.EditStudyHashes("data_objects", sql_string);
    }

    public void AddNewlyCreatedObjectHashTypes()
    {
        // for new sd_sid / hash type combinations

        string sql_string = @"INSERT INTO ad.object_hashes(sd_oid, 
             hash_type_id, composite_hash)
             SELECT ia.sd_oid, ia.hash_type_id, ia.composite_hash
             FROM sd.to_ad_object_atts ia
             WHERE ia.status = 1";

        int n = dbu.ExecuteSQL(sql_string);
        _logging_helper.LogLine("Inserting " + n.ToString() + " new composite hashes to object hash records");
    }


    public void DropNewlyDeletedObjectHashTypes()
    {
        string sql_string = @"DELETE FROM ad.object_hashes sh
             USING sd.to_ad_object_atts ia
             WHERE sh.sd_oid = ia.sd_oid   
             and sh.hash_type_id = ia.hash_type_id 
             and ia.status = 4";

        int n = dbu.ExecuteSQL(sql_string);
        _logging_helper.LogLine("Dropping " + n.ToString() + " composite hashes from object hash records");
    }


    public int DeleteObjectRecords(string table_name)
    {
        string sql_string = @"with t as (
              select sd_oid from sd.to_ad_object_recs
              where status = 4)
          delete from ad." + table_name + @" a
          using t
          where a.sd_oid = t.sd_oid;";

        return dbu.ExecuteSQL(sql_string);
    }


    public void UpdateObjectsDeletedDate(int import_id, int? source_id)
    {
        string sql_string = @"Update mon_sf.source_data_objects s
        set last_import_id = " + (-1 * import_id).ToString() + @", 
        last_imported = current_timestamp
        from sd.to_ad_object_recs ts
        where s.sd_id = ts.sd_oid and
        s.source_id = " + source_id.ToString() + @"
        and ts.status = 4;";

        dbu.ExecuteSQL(sql_string);
    }


    public void UpdateFullObjectHash()
    {
        // Ensure object_full_hash is updated to reflect new value
        // The object record itself may not have changed, so the object
        // record update above cannot be used to make the edit.
        
        string sql_string = @"UPDATE ad.data_objects a
                set object_full_hash = so.object_full_hash
                FROM sd.data_objects so
                WHERE so.sd_oid = a.sd_oid ";

        // Chunked by the dbu routine to 100,000 records at a time

        dbu.UpdateFullHashes("data_objects", sql_string);
    }

}