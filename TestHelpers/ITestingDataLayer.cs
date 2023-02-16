namespace MDR_Importer;

public interface ITestingDataLayer
{
    Credentials Credentials { get; }

    IEnumerable<int> ObtainTestSourceIDs();

    void SetUpADCompositeTables();
    void RetrieveSDData(Source source);
    void RetrieveADData(Source source);
    void TransferADDataToComp(Source source);
    void ApplyScriptedADChanges();
    void ConstructDiffReport();
}

