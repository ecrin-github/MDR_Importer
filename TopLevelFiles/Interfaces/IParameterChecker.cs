using System;
using System.Collections.Generic;
using System.Text;

namespace MDR_Importer;

interface IParameterChecker
{
        Options ObtainParsedArguments(string[] args);

}

