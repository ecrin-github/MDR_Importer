using Dapper;
using Npgsql;
namespace MDR_Importer;

class DBUtilities
{
    private readonly string _db_conn;
    private readonly ILoggingHelper _logging_helper;

    public DBUtilities(string db_conn, ILoggingHelper logging_helper)
    {
        _db_conn = db_conn;
        _logging_helper = logging_helper;
    }
    
    public int GetSDRecordMax(string table_name)
    {
        string sql_string = @"select max(id) from sd." + table_name;
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }
    
    public int GetADRecordMax(string table_name)
    {
        string sql_string = @"select max(id) from ad." + table_name;
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }
   
    public int ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.Execute(sql_string);
    }


    public int ExecuteTransferSQL(string sql_string, string table_name, int rec_batch = 200000)
    {
        try
        {
            int id_max = GetSDRecordMax(table_name);
            // int rec_batch = 10000;  // for testing 
            int added = 0;
            if (id_max > rec_batch)
            {
                for (int r = 1; r <= id_max; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " where s.id >= " + r + " and s.id < " + (r + rec_batch);
                    int res  = ExecuteSQL(batch_sql_string);
                    added += res;
                    int e  = r + rec_batch < id_max ? r + rec_batch - 1 : id_max;
                    string feedback = $"Adding {res} records to ad.{table_name}, sd ids {r} to {e}";
                    _logging_helper.LogLine(feedback);
                }
                _logging_helper.LogLine($"Added {added} records in total");
            }
            else
            {
                added = ExecuteSQL(sql_string);
                _logging_helper.LogLine($"Adding {added} records to ad.{table_name}, as a single batch");
            }

            return id_max;
        }
        catch (Exception e)
        {
            string res = e.Message;
            _logging_helper.LogError($"In data transfer (adding phase) ({table_name}) to ad table: " + res);
            return 0;
        }
    }
    
    
    public int ExecuteDeleteSQL(string sql_string, string table_name, int rec_batch)
    {
        try
        {
            int id_max = GetADRecordMax(table_name);
            int deleted = 0;
            if (id_max > rec_batch)
            {
                for (int r = 1; r <= id_max; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " and s.id >= " + r + " and s.id < " + (r + rec_batch);
                    int res = ExecuteSQL(batch_sql_string);
                    deleted += res;
                    int e  = r + rec_batch < id_max ? r + rec_batch - 1 : id_max;
                    string feedback = $"Deleting {res} records of ad.{table_name} data, in ids {r} to {e}";
                    _logging_helper.LogLine(feedback);
                }
                _logging_helper.LogLine($"Deleted {deleted} records in total");
            }
            else
            {
                deleted = ExecuteSQL(sql_string);
                _logging_helper.LogLine($"Deleting {deleted} records of ad.{table_name} data, as a single batch");
            }
            return deleted;
        }
        catch (Exception e)
        {
            string res = e.Message;
            _logging_helper.LogError($"In data transfer (deletion phase) ({table_name}) to ad table: " + res);
            return 0;
        }
    }
}
