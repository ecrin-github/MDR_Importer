
namespace MDR_Importer;

public interface ICredentials
{
    string? Password { get; }
    string? Username { get; }
    string GetConnectionString(string databaseName);
}

