
namespace MDR_Importer;

class ADCompTableBuilder
{
    private string _db_conn;

    public ADCompTableBuilder(string db_conn)
    {
        _db_conn = db_conn;
    }

    public void BuildStudyTables()
    {
        ADCompStudyTableCreator studytablebuilder = new ADCompStudyTableCreator(_db_conn);

        studytablebuilder.create_table_studies();
        studytablebuilder.create_table_study_identifiers();
        studytablebuilder.create_table_study_titles();
        studytablebuilder.create_table_study_features();
        studytablebuilder.create_table_study_topics();
        studytablebuilder.create_table_study_contributors();
        studytablebuilder.create_table_study_references();
        studytablebuilder.create_table_study_relationships();
        studytablebuilder.create_table_ipd_available();
        studytablebuilder.create_table_study_links();
        studytablebuilder.create_table_study_countries();
        studytablebuilder.create_table_study_locations();
        studytablebuilder.create_table_study_hashes();
    }


    public void BuildObjectTables()
    {
        ADCompObjectTableCreator objecttablebuilder = new ADCompObjectTableCreator(_db_conn);

        objecttablebuilder.create_table_data_objects();
        objecttablebuilder.create_table_object_datasets();
        objecttablebuilder.create_table_object_dates();
        objecttablebuilder.create_table_object_instances();
        objecttablebuilder.create_table_object_contributors();
        objecttablebuilder.create_table_object_titles();
        objecttablebuilder.create_table_object_topics();
        objecttablebuilder.create_table_object_descriptions();
        objecttablebuilder.create_table_object_identifiers();
        objecttablebuilder.create_table_object_db_links();
        objecttablebuilder.create_table_object_publication_types();
        objecttablebuilder.create_table_object_rights();
        objecttablebuilder.create_table_object_comments();
        objecttablebuilder.create_table_object_relationships();
        objecttablebuilder.create_table_object_hashes();
    }

}

