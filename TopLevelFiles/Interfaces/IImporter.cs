using System;
using System.Collections.Generic;
using System.Text;

namespace MDR_Importer;
public interface IImporter
{
    int Run(Options opts);

}

