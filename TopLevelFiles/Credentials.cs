using Microsoft.Extensions.Configuration;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDR_Importer;

public class Credentials : ICredentials
{
    public string? Host { get; set; }
    public string? Username { get; set; }
    public string? Password { get; set; }

    public Credentials(IConfiguration settings)
    {
        Host = settings["host"];
        Username = settings["user"];
        Password = settings["password"];
    }

    public string GetConnectionString(string database_name, bool using_test_data)
    {
        NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
        builder.Host = Host;
        builder.Username = Username;
        builder.Password = Password;
        builder.Database = (using_test_data) ? "test" : database_name;
        return builder.ConnectionString;
    }
}

