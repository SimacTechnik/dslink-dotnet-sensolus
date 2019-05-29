using System;
using System.Collections.Generic;
using System.Text;

namespace dslink_dotnet_sensolus.DataModels
{
    interface IKeyValue<T>
    {
        T GetKeyValue();
    }
}
