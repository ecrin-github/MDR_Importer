namespace MDR_Importer;

public interface IMonDataLayer
{
    Credentials Credentials { get; }
    
    bool SourceIdPresent(int? source_id);
    string GetConnectionString(string database_name, bool using_test_data);
    Source? FetchSourceParameters(int? source_id);
    int GetNextImportEventId();
    int StoreImportEvent(ImportEvent import);
}

