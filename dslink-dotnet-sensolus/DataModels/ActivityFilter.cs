using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dslink_dotnet_sensolus.DataModels
{
    public struct ActivityFilter
    {
        public string serial { get; set; }
        public DateTime from { get; set; }
        public DateTime to { get; set; }


        public static JArray Serialize(List<ActivityFilter> list)
        {
            return JArray.FromObject(list.Select(x => new
            {
                serial = x.serial,
                from = x.from.ToString("yyyy-MM-ddTHH:mm:sszz00"),
                to = x.to.ToString("yyyy-MM-ddTHH:mm:sszz00")
            }).ToArray());
        }
    }
}
