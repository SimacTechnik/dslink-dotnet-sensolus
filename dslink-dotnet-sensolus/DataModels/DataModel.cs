using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace dslink_dotnet_sensolus.DataModels
{
    interface DataModel<T>
    {
        List<T> FromDataReader(IDataReader reader);
        List<T> FromSensolus(JArray jArray);
        string InsertSql(List<T> list);
        string DeleteSql(List<T> list);
    }

    interface IKeyValue<T>
    {
       T GetKeyValue();
    }
}
