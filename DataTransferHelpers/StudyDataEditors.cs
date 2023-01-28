namespace MDR_Importer;

class StudyDataEditor
{
    ILoggingHelper _logging_helper;
    DBUtilities dbu;

    public StudyDataEditor(string connstring, ILoggingHelper logging_helper)
    {
        _logging_helper = logging_helper;
        dbu = new DBUtilities(connstring, _logging_helper);
    }
    
    public void EditStudies()
    {
        // if the record hash for the study has changed, then the data in the studies records should be changed

        string sql_string = @"update ad.studies a
          set
              display_title = t.display_title,
              title_lang_code = t.title_lang_code, 
              brief_description = t.brief_description, 
              data_sharing_statement = t.data_sharing_statement,
              study_start_year = t.study_start_year,
              study_start_month = t.study_start_month,
              study_type_id = t.study_type_id, 
              study_status_id = t.study_status_id,
              study_enrolment = t.study_enrolment, 
              study_gender_elig_id = t.study_gender_elig_id, 
              min_age = t.min_age, 
              min_age_units_id = t.min_age_units_id, 
              max_age = t.max_age,
              max_age_units_id = t.max_age_units_id, 
              datetime_of_data_fetch = t.datetime_of_data_fetch,
              record_hash = t.record_hash,
              last_edited_on = current_timestamp
          from (select so.* from sd.studies so
                INNER JOIN sd.to_ad_study_recs ts
                ON so.sd_sid = ts.sd_sid ";

        string base_string = @" where ts.study_rec_status = 2) t
                      where a.sd_sid = t.sd_sid";

        dbu.EditEntityRecords(sql_string, base_string, "studies");
    }


    #region Table data edits

    public void EditStudyIdentifiers()
    {
        // Where the composite hash value indicates that a change has taken place in one or more 
        // of the records replace the whole set for the relevant studies
        // Composite hash id for study identifiers = 11

        string sql_string = dbu.GetStudyTString(11);
        string sql_stringD = sql_string + dbu.GetStudyDeleteString("study_identifiers");

        string sql_stringI = sql_string + @"INSERT INTO ad.study_identifiers(sd_sid,
        identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, identifier_link, record_hash)
        SELECT s.sd_sid, 
        identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, identifier_link, record_hash
        FROM sd.study_identifiers s
        INNER JOIN t
        on s.sd_sid = t.sd_sid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "study_identifiers");
    }


    public void EditStudyTitles()
    {

        // Where the composite hash value indicates that a change has taken place in one or more 
        // of the records replace the whole set for the relevant studies
        // Composite hash id for study titles = 12

        string sql_string = dbu.GetStudyTString(12);
        string sql_stringD = sql_string + dbu.GetStudyDeleteString("study_titles");

        string sql_stringI = sql_string + @"INSERT INTO ad.study_titles(sd_sid,
        title_text, title_type_id, lang_code, lang_usage_id,
        is_default, comments, record_hash)
        SELECT s.sd_sid, 
        title_text, title_type_id, lang_code, lang_usage_id,
        is_default, comments, record_hash
        FROM sd.study_titles s
        INNER JOIN t
        on s.sd_sid = t.sd_sid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "study_titles");
    }


    public void EditStudyReferences()
    {
        // Where the composite hash value indicates that a change has taken place in one or more 
        // of the records replace the whole set for the relevant studies
        // Composite hash id for study references = 17

        string sql_string = dbu.GetStudyTString(17);
        string sql_stringD = sql_string + dbu.GetStudyDeleteString("study_references");

        string sql_stringI = sql_string + @"INSERT INTO ad.study_references(sd_sid,
        pmid, citation, doi, comments, record_hash)
        SELECT s.sd_sid, 
        pmid, citation, doi, comments, record_hash
        FROM sd.study_references s
        INNER JOIN t
        on s.sd_sid = t.sd_sid;";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "study_references");
    }

    public void EditStudyContributors()
    {
        // Where the composite hash value indicates that a change has taken place in one or more 
        // of the records replace the whole set for the relevant studies
        // Composite hash id for study contributors = 15

        string sql_string = dbu.GetStudyTString(15);
        string sql_stringD = sql_string + dbu.GetStudyDeleteString("study_contributors");

        string sql_stringI = sql_string + @"INSERT INTO ad.study_contributors(sd_sid,
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id, record_hash)
        SELECT s.sd_sid, 
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id, record_hash
        FROM sd.study_contributors s
        INNER JOIN t
        on s.sd_sid = t.sd_sid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "study_contributors");
    }


    public void EditStudyTopics()
    {
        // Where the composite hash value indicates that a change has taken place in one or more 
        // of the records replace the whole set for the relevant studies
        // Composite hash id for study topics = 14

        string sql_string = dbu.GetStudyTString(14);
        string sql_stringD = sql_string + dbu.GetStudyDeleteString("study_topics");

        string sql_stringI = sql_string + @"INSERT INTO ad.study_topics(sd_sid,
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value, record_hash)
        SELECT s.sd_sid, 
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value, record_hash
        FROM sd.study_topics s
        INNER JOIN t
        on s.sd_sid = t.sd_sid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "study_topics");
    }


    public void EditStudyRelationships()
    {
        // Where the composite hash value indicates that a change has taken place in one or more 
        // of the records replace the whole set for the relevant studies
        // Composite hash id for study relationships = 16

        string sql_string = dbu.GetStudyTString(16);
        string sql_stringD = sql_string + dbu.GetStudyDeleteString("study_relationships");

        string sql_stringI = sql_string + @"INSERT INTO ad.study_relationships(sd_sid,
        relationship_type_id, target_sd_sid, record_hash)
        SELECT s.sd_sid, 
        relationship_type_id, target_sd_sid, record_hash
        FROM sd.study_relationships s
        INNER JOIN t
        on s.sd_sid = t.sd_sid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "study_relationships");
    }


    public void EditStudyFeatures()
    {
        // Where the composite hash value indicates that a change has taken place in one or more 
        // of the records replace the whole set for the relevant studies
        // Composite hash id for study features = 13

        string sql_string = dbu.GetStudyTString(13);
        string sql_stringD = sql_string + dbu.GetStudyDeleteString("study_features");

        string sql_stringI = sql_string + @"INSERT INTO ad.study_features(sd_sid,
        feature_type_id, feature_value_id, record_hash)
        SELECT s.sd_sid, 
        feature_type_id, feature_value_id, record_hash
        FROM sd.study_features s
        INNER JOIN t
        on s.sd_sid = t.sd_sid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "study_features");
    }


    public void EditStudyLinks()
    {
        // Where the composite hash value indicates that a change has taken place in one or more 
        // of the records replace the whole set for the relevant studies
        // Composite hash id for study links = 18
        
        string sql_string = dbu.GetStudyTString(18);
        string sql_stringD = sql_string + dbu.GetStudyDeleteString("study_links");

        string sql_stringI = sql_string + @"INSERT INTO ad.study_links(sd_sid,
        link_label, link_url, record_hash)
        SELECT s.sd_sid, 
        link_label, link_url, record_hash
        FROM sd.study_links s
        INNER JOIN t
        on s.sd_sid = t.sd_sid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "study_links");
    }


    public void EditStudyCountries()
    {
        // Where the composite hash value indicates that a change has taken place in one or more 
        // of the records replace the whole set for the relevant studies
        // Composite hash id for study links = 18

        string sql_string = dbu.GetStudyTString(21);
        string sql_stringD = sql_string + dbu.GetStudyDeleteString("study_countries");

        string sql_stringI = sql_string + @"INSERT INTO ad.study_countries(sd_sid,
        country_id, country_name, status_id, record_hash)
        SELECT s.sd_sid, 
        country_id, country_name, status_id, record_hash
        FROM sd.study_countries s
        INNER JOIN t
        on s.sd_sid = t.sd_sid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "study_countries");
    }

    public void EditStudyLocations()
    {
        // Where the composite hash value indicates that a change has taken place in one or more 
        // of the records replace the whole set for the relevant studies
        // Composite hash id for study links = 18

        string sql_string = dbu.GetStudyTString(20);
        string sql_stringD = sql_string + dbu.GetStudyDeleteString("study_locations");

        string sql_stringI = sql_string + @"INSERT INTO ad.study_locations(sd_sid,
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id, record_hash)
        SELECT s.sd_sid, 
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id, record_hash
        FROM sd.study_locations s
        INNER JOIN t
        on s.sd_sid = t.sd_sid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "study_locations");
    }

    public void EditStudyIpdAvailable()
    {
        // Where the composite hash value indicates that a change has taken place in one or more 
        // of the records replace the whole set for the relevant studies
        // Composite hash id for study ipd available = 19

        string sql_string = dbu.GetStudyTString(19);
        string sql_stringD = sql_string + dbu.GetStudyDeleteString("study_ipd_available");

        string sql_stringI = sql_string + @"INSERT INTO ad.study_ipd_available(sd_sid,
        ipd_id, ipd_type, ipd_url, ipd_comment, record_hash)
        SELECT s.sd_sid, 
        ipd_id, ipd_type, ipd_url, ipd_comment, record_hash
        FROM sd.study_ipd_available s
        INNER JOIN t
        on s.sd_sid = t.sd_sid";

        dbu.ExecuteDandI(sql_stringD, sql_stringI, "study_ipd_available");
    }

    #endregion

/*
    public void UpdateStudiesLastImportedDate(int import_id, int? source_id)
    {
        string top_string = @"Update mon_sf.source_data_studies src
                      set last_import_id = " + import_id.ToString() + @", 
                      last_imported = current_timestamp
                      from 
                         (select so.id, so.sd_sid 
                         FROM sd.studies so
                         INNER JOIN sd.to_ad_study_recs ts
                         ON so.sd_sid = ts.sd_sid
                         where ts.status in (1, 2, 3) 
                         ";
        string base_string = @" ) s
                          where s.sd_sid = src.sd_id and
                          src.source_id = " + source_id.ToString();

        dbu.UpdateLastImportedDate("studies", top_string, base_string);
    }
*/

    public void UpdateDateOfStudyData()
    {
        string top_sql = @"with t as 
        (   
            select so.sd_sid, so.datetime_of_data_fetch 
            from sd.studies so
            inner join sd.to_ad_study_recs ts
            on so.sd_sid  = ts.sd_sid
            where ts.status in (2,3)";

        string base_sql = @")
        update ad.studies s
        set datetime_of_data_fetch = t.datetime_of_data_fetch
        from t
        where s.sd_sid = t.sd_sid";

        dbu.UpdateDateOfData("studies", top_sql, base_sql);
    }


    public void UpdateStudyCompositeHashes()
    {
        // Need to ensure that the hashes themselves are all up to date (for the next comparison)
        // Change the ones that have been changed in sd
        // if a very large main entity table (and therefore hash) table may need to chunk using a 
        // link to the sd.studies table....

        // for existing, matching sd_sid / hash type combinations

        string sql_string = @"UPDATE ad.study_hashes ah
                set composite_hash = so.composite_hash
                FROM 
                    (SELECT st.id, ia.sd_sid, ia.hash_type_id, ia.composite_hash
                     FROM sd.to_ad_study_atts ia
                     INNER JOIN sd.studies st
                     on ia.sd_sid = st.sd_sid
                     where ia.status = 2) so
                WHERE ah.sd_sid = so.sd_sid
                and ah.hash_type_id = so.hash_type_id ";

        dbu.EditStudyHashes("studies", sql_string);
    }


    public void AddNewlyCreatedStudyHashTypes()
    {
        // for new sd_sid / hash type combinations

        string sql_string = @"INSERT INTO ad.study_hashes(sd_sid, 
             hash_type_id, composite_hash)
             SELECT ia.sd_sid, ia.hash_type_id, ia.composite_hash
             FROM sd.to_ad_study_atts ia
             WHERE ia.status = 1";

        int n = dbu.ExecuteSQL(sql_string);
        _logging_helper.LogLine("Inserting " + n.ToString() + " new composite hashes to study hash records");
    }


    public void DropNewlyDeletedStudyHashTypes()
    {
        string sql_string = @"DELETE FROM ad.study_hashes sh
             USING sd.to_ad_study_atts ia
             WHERE sh.sd_sid = ia.sd_sid   
             and sh.hash_type_id = ia.hash_type_id 
             and ia.status = 4";

        int n = dbu.ExecuteSQL(sql_string);
        _logging_helper.LogLine("Dropping " + n.ToString() + " composite hashes from study hash records");
    }


    public int DeleteStudyRecords(string table_name)
    {
        string sql_string = @"with t as (
              select sd_sid from sd.to_ad_study_recs
              where status = 4)
          delete from ad." + table_name + @" a
          using t
          where a.sd_sid = t.sd_sid;";

        return dbu.ExecuteSQL(sql_string);

    }


    public void UpdateStudiesDeletedDate(int import_id, int? source_id)
    {
        string sql_string = @"Update mon_sf.source_data_studies s
        set last_import_id = " + (-1 * import_id).ToString() + @", 
        last_imported = current_timestamp
        from sd.to_ad_study_recs ts
        where s.sd_id = ts.sd_sid and
        s.source_id = " + source_id.ToString() + @"
        and ts.status = 4";

        dbu.ExecuteSQL(sql_string);
    }


    public void UpdateFullStudyHash()
    {
        // Ensure study_full_hash is updated to reflect new value
        // The study record itself may not have changed, so the study
        // record update above cannot be used to make the edit. 

        string sql_string = @"UPDATE ad.studies a
                set study_full_hash = so.study_full_hash
                FROM sd.studies so
                WHERE so.sd_sid = a.sd_sid ";

        // Chunked by the dbu routine to 100,000 records at a time

        dbu.UpdateFullHashes("studies", sql_string);
    }


    
}