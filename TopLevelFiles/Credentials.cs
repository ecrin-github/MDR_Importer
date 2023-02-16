using Microsoft.Extensions.Configuration;
using Npgsql;
namespace MDR_Importer;

public class Credentials : ICredentials
{
    private readonly string? _host;
    public string? Username { get; set; }
    public string? Password { get; set; }

    public Credentials(IConfiguration settings)
    {
        _host = settings["host"];
        Username = settings["user"];
        Password = settings["password"];
    }

    public string GetConnectionString(string database_name, bool using_test_data)
    {
        NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder
        {
            Host = _host,
            Username = Username,
            Password = Password,
            Database = (using_test_data) ? "test" : database_name
        };
        return builder.ConnectionString;
    }
}

