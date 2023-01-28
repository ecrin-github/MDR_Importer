
namespace MDR_Importer;

public interface ICredentials
{
    string? Host { get; set; }
    string? Password { get; set; }
    string? Username { get; set; }

    string GetConnectionString(string databaseName, bool usingTestData);
}

