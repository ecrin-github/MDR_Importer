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

    public int GetRecordCount(string table_name)
    {
        string sql_string = @"select count(*) from sd." + table_name;
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }

    public int ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.Execute(sql_string);
    }


    public void ExecuteTransferSQL(string sql_string, string table_name, int rec_batch = 250000)
    {
        try
        {
            int rec_count = GetRecordCount(table_name);
            // int rec_batch = 10000;  // for testing 
            if (rec_count > rec_batch)
            {
                for (int r = 1; r <= rec_count; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " where s.id >= " + r + " and s.id < " + (r + rec_batch);
                    int n  = ExecuteSQL(batch_sql_string);
                    int e  = r + rec_batch < rec_count ? r + rec_batch - 1 : rec_count;
                    string feedback = $"Adding {n} records of {table_name} data, ids {r} to {e}";
                    _logging_helper.LogLine(feedback);
                }
            }
            else
            {
                int n = ExecuteSQL(sql_string);
                _logging_helper.LogLine($"Adding {n} records to " + table_name + ", as a single batch");
            }
        }
        catch (Exception e)
        {
            string res = e.Message;
            _logging_helper.LogError($"In data transfer (adding phase) ({table_name}) to ad table: " + res);
        }
    }
    
    
    public int ExecuteDeleteSQL(string sql_string, string table_name, int rec_batch)
    {
        try
        {
            int rec_count = GetRecordCount(table_name);
            int n = 0;
            if (rec_count > rec_batch)
            {
                for (int r = 1; r <= rec_count; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " and s.id >= " + r + " and s.id < " + (r + rec_batch);
                    n += ExecuteSQL(batch_sql_string);
                    int e  = r + rec_batch < rec_count ? r + rec_batch - 1 : rec_count;
                    string feedback = $"Deleting {n} records of {table_name} data, ids {r} to {e}";
                    _logging_helper.LogLine(feedback);
                }
            }
            else
            {
                n = ExecuteSQL(sql_string);
                _logging_helper.LogLine($"Deleting {n} records to " + table_name + ", as a single batch");
            }
            return n;
        }
        catch (Exception e)
        {
            string res = e.Message;
            _logging_helper.LogError($"In data transfer (deletion phase) ({table_name}) to ad table: " + res);
            return 0;
        }
    }
}
