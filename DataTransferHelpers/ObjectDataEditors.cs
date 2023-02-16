
namespace MDR_Importer;

class DataObjectDataEditor
{
    private readonly DBUtilities _dbu;

    public DataObjectDataEditor(string db_conn, ILoggingHelper logging_helper)
    {
        _dbu = new DBUtilities(db_conn, logging_helper);
    }


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

 public int DeleteObjectRecords(string table_name)
    {
        string sql_string = @"with t as (
              select sd_oid from sd.to_ad_object_recs
              where status = 4)
          delete from ad." + table_name + @" a
          using t
          where a.sd_oid = t.sd_oid;";

        return _dbu.ExecuteSQL(sql_string);
    }
}