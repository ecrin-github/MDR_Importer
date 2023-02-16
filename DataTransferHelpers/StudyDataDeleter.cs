namespace MDR_Importer;

class StudyDataDeleter
{
    private readonly DBUtilities _dbu;
    
    public StudyDataDeleter(string db_conn, ILoggingHelper logging_helper)
    {
        _dbu = new DBUtilities(db_conn, logging_helper);
    }
    

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

    public int DeleteStudyRecords(string table_name)
    {
        string sql_string = @"with t as (
              select sd_sid from sd.study_recs)
          delete from ad." + table_name + @" a
          using t
          where a.sd_sid = t.sd_sid;";

        return _dbu.ExecuteSQL(sql_string);

    }

}