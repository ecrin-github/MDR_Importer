using Dapper;
using Npgsql;

namespace MDR_Importer;

public class ForeignTableManager
{
    string _connString;

    public ForeignTableManager(string connString)
    {
        _connString = connString;
    }

    public void EstablishMonForeignTables(string user_name, string password)
    {
        using (var conn = new NpgsqlConnection(_connString))
        {
            string sql_string = @"CREATE EXTENSION IF NOT EXISTS postgres_fdw
                                 schema sd;";
            conn.Execute(sql_string);

            sql_string = @"CREATE SERVER IF NOT EXISTS mon "
                      + @" FOREIGN DATA WRAPPER postgres_fdw
                        OPTIONS (host 'localhost', dbname 'mon', port '5432');";
            conn.Execute(sql_string);

            sql_string = @"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                 SERVER mon 
                 OPTIONS (user '" + user_name + "', password '" + password + "');";
            conn.Execute(sql_string);

            sql_string = @"DROP SCHEMA IF EXISTS mon_sf cascade;
                 CREATE SCHEMA mon_sf; 
                 IMPORT FOREIGN SCHEMA sf
                 FROM SERVER mon 
                 INTO mon_sf;";
            conn.Execute(sql_string);
        }
    }


    public void DropMonForeignTables()
    {
        using (var conn = new NpgsqlConnection(_connString))
        {
            string sql_string = @"DROP USER MAPPING IF EXISTS FOR CURRENT_USER
                 SERVER mon;";
            conn.Execute(sql_string);

            sql_string = @"DROP SERVER IF EXISTS mon CASCADE;";
            conn.Execute(sql_string);

            sql_string = @"DROP SCHEMA IF EXISTS mon_sf;";
            conn.Execute(sql_string);
        }
    }
}

