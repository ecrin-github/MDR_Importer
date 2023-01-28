
namespace MDR_Importer;

class StudyDataAdder
{
    ILoggingHelper _logging_helper;
    DBUtilities dbu;

    public StudyDataAdder(string connstring, ILoggingHelper logging_helper)
    {
        _logging_helper = logging_helper;
        dbu = new DBUtilities(connstring, _logging_helper);
    }


    #region Table data transfer

    public void TransferStudies()
    {
        string sql_string = @"INSERT INTO ad.studies (sd_sid, display_title,
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id, 
        study_status_id, study_enrolment, study_gender_elig_id, min_age, 
        min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
        record_hash, study_full_hash)
        SELECT s.sd_sid, display_title,
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id, 
        study_status_id, study_enrolment, study_gender_elig_id, min_age, 
        min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
        record_hash, study_full_hash 
        FROM sd.studies s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "studies", "Adding");
    }

    public void TransferStudyIdentifiers()
    {
        string sql_string = @"INSERT INTO ad.study_identifiers(sd_sid,
        identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, identifier_link, record_hash)
        SELECT s.sd_sid, 
        identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, identifier_link, record_hash
        FROM sd.study_identifiers s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_identifiers", "Adding");
    }

    public void TransferStudyTitles()
    {
        string sql_string = @"INSERT INTO ad.study_titles(sd_sid,
        title_type_id, title_text, lang_code, lang_usage_id,
        is_default, comments, record_hash)
        SELECT s.sd_sid, 
        title_type_id, title_text, lang_code, lang_usage_id,
        is_default, comments, record_hash
        FROM sd.study_titles s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_titles", "Adding");
    }

    public void TransferStudyReferences()
    {
        string sql_string = @"INSERT INTO ad.study_references(sd_sid,
        pmid, citation, doi, comments, record_hash)
        SELECT s.sd_sid, 
        pmid, citation, doi, comments, record_hash
        FROM sd.study_references s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_references", "Adding");
    }

    public void TransferStudyContributors()
    {
        string sql_string = @"INSERT INTO ad.study_contributors(sd_sid,
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
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_contributors", "Adding");
    }

    public void TransferStudyTopics()
    {
        string sql_string = @"INSERT INTO ad.study_topics(sd_sid,
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value, record_hash)
        SELECT s.sd_sid, 
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value, record_hash
        FROM sd.study_topics s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_topics", "Adding");
    }


    public void TransferStudyRelationships()
    {
        string sql_string = @"INSERT INTO ad.study_relationships(sd_sid,
        relationship_type_id, target_sd_sid, record_hash)
        SELECT s.sd_sid, 
        relationship_type_id, target_sd_sid, record_hash
        FROM sd.study_relationships s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_relationships", "Adding");
    }


    public void TransferStudyFeatures()
    {
        string sql_string = @"INSERT INTO ad.study_features(sd_sid,
        feature_type_id, feature_value_id, record_hash)
        SELECT s.sd_sid, 
        feature_type_id, feature_value_id, record_hash
        FROM sd.study_features s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_features", "Adding");
    }


    public void TransferStudyLinks()
    {
        string sql_string = @"INSERT INTO ad.study_links(sd_sid,
        link_label, link_url, record_hash)
        SELECT s.sd_sid, 
        link_label, link_url, record_hash
        FROM sd.study_links s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_links", "Adding");
    }


    public void TransferStudyCountries()
    {
        string sql_string = @"INSERT INTO ad.study_countries(sd_sid,
        country_id, country_name, status_id, record_hash)
        SELECT s.sd_sid, 
        country_id, country_name, status_id, record_hash
        FROM sd.study_countries s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_countries", "Adding");
    }


    public void TransferStudyLocations()
    {
        string sql_string = @"INSERT INTO ad.study_locations(sd_sid,
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id, record_hash)
        SELECT s.sd_sid, 
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id, record_hash
        FROM sd.study_locations s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_locations", "Adding");
    }


    public void TransferStudyIpdAvailable()
    {
        string sql_string = @"INSERT INTO ad.study_ipd_available(sd_sid,
        ipd_id, ipd_type, ipd_url, ipd_comment, record_hash)
        SELECT s.sd_sid, 
        ipd_id, ipd_type, ipd_url, ipd_comment, record_hash
        FROM sd.study_ipd_available s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_ipd_available", "Adding");
    }

    #endregion

    public void TransferStudyHashes()
    {
        for (int n = 11; n < 22; n++)
        {
            string sql_string = @"INSERT INTO ad.study_hashes(sd_sid,
              hash_type_id, composite_hash)
              SELECT s.sd_sid, 
              hash_type_id, composite_hash
              FROM sd.study_hashes s
              INNER JOIN sd.to_ad_study_recs ts
              ON s.sd_sid = ts.sd_sid
              where ts.status = 1
              and s.hash_type_id = " + n.ToString();

            int res = dbu.ExecuteSQL(sql_string);
            if (res > 0)
            {
                string hashType = GetHashType(n);
                _logging_helper.LogLine("Inserting " + res.ToString() + " new study hashes - type " + n.ToString() + ": " + hashType);

            }
        }
    }

    private string GetHashType(int n)
    {
        string hashType = "??????";
        switch (n)
        {
            case 11: { hashType = "identifiers"; break; }
            case 12: { hashType = "titles"; break; }
            case 13: { hashType = "features"; break; }
            case 14: { hashType = "topics"; break; }
            case 15: { hashType = "contributors"; break; }
            case 16: { hashType = "relationships"; break; }
            case 17: { hashType = "references"; break; }
            case 18: { hashType = "study links"; break; }
            case 19: { hashType = "ipd available"; break; }
            case 20: { hashType = "locations"; break; }
            case 21: { hashType = "countries"; break; }
        }
        return hashType;
    }
}
