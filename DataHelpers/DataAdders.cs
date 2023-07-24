
namespace MDR_Importer;

class DataAdder
{
    private readonly  DBUtilities _dbu;
    private readonly FieldLists fl;

    public DataAdder(string db_conn, ILoggingHelper logging_helper)
    {
        _dbu = new DBUtilities(db_conn, logging_helper);
        fl = new FieldLists();
    }
 
    public int AddStudyData(string table_name)
    {
        string fields = fl.addStudyFields[table_name];
        string insert_fields = fl.addStudyFields_insert[table_name];
        
        string sql_string = $@"INSERT INTO ad.{table_name} ({fields})
        SELECT {insert_fields}
        FROM sd.{table_name} s ";

        int rec_batch = 250000;
        if (table_name == "studies")
        {
            rec_batch = 100000;  // a smaller batch size than the default
        }
        return _dbu.ExecuteTransferSQL(sql_string, table_name, rec_batch);
    }

    public void AddIECData(string table_name)
    {
        string fields = fl.addStudyFields["study_iec"];
        string insert_fields = fl.addStudyFields_insert[table_name];
        
        string sql_string = $@"INSERT INTO ad.{table_name} ({fields})
        SELECT {insert_fields}
        FROM sd.{table_name} s ";

        _dbu.ExecuteTransferSQL(sql_string, table_name);
    }
    
    
    public int AddObjectData(string table_name)
    {
        string fields = fl.addObjectFields[table_name];
        string insert_fields = fl.addObjectFields_insert[table_name];
        
        string sql_string = $@"INSERT INTO ad.{table_name} ({fields})
        SELECT {insert_fields}
        FROM sd.{table_name} s ";

        return _dbu.ExecuteTransferSQL(sql_string, table_name);
    }
    
}
