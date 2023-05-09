using Dapper;
using Dapper.Contrib.Extensions;
using Npgsql;
namespace MDR_Importer;

public class MonDataLayer : IMonDataLayer
{
    private readonly ICredentials _credentials;
    private readonly string? monConnString;
    
    public MonDataLayer(ICredentials credentials)
    {
        _credentials = credentials;
        monConnString = credentials.GetConnectionString("mon");
    }

    public Credentials Credentials => (Credentials)_credentials;

    public string GetConnectionString(string database_name) =>
                                     _credentials.GetConnectionString(database_name);
           
    public bool SourceIdPresent(int? sourceId)
    {
        string sqlString = "Select id from sf.source_parameters where id = " + sourceId.ToString();
        using NpgsqlConnection conn = new(monConnString);
        int res = conn.QueryFirstOrDefault<int>(sqlString);
        return (res != 0);
    }
    
    public Source? FetchSourceParameters(int? sourceId)
    {
        using NpgsqlConnection conn = new(monConnString);
        return conn.Get<Source>(sourceId);
    }

    public int GetNextImportEventId()
    {
        using NpgsqlConnection conn = new(monConnString);
        string sqlString = "select max(id) from sf.import_events ";
        int lastId = conn.ExecuteScalar<int>(sqlString);
        return (lastId == 0) ? 10001 : lastId + 1;
    }

    public int StoreImportEvent(ImportEvent import)
    {
        import.time_ended = DateTime.Now;
        using NpgsqlConnection conn = new(monConnString);
        return (int)conn.Insert(import);
    }
}