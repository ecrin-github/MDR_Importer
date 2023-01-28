
namespace MDR_Importer;

public interface ITestingDataLayer
{
    Credentials Credentials { get; }

    IEnumerable<int> ObtainTestSourceIDs();

    void SetUpADCompositeTables();
    void RetrieveSDData(ISource source);
    void RetrieveADData(ISource source);
    void TransferADDataToComp(ISource source);
    void ApplyScriptedADChanges();
    void ConstructDiffReport();
}

