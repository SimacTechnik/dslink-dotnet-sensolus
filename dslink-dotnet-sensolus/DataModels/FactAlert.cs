using System;
using System.Collections.Generic;
using System.Data;

namespace dslink_dotnet_sensolus
{
    public class FactAlert
    {
        public DateTime Alerttime { get; set; }
        public string Alerttype { get; set; }
        public long Alertruleid { get; set; }
        public string Trackerserial { get; set; }
        public string Severity { get; set; }
        public string Alerttitle { get; set; }
        public long Alertactivity { get; set; }
        public long Rulerecid { get; set; }
        public DateTime? Alertclear { get; set; }

        public static List<FactAlert> FromDataReader(IDataReader reader)
        {
            List<FactAlert> list = new List<FactAlert>();
            while (reader.Read())
            {
                FactAlert obj = new FactAlert();
                obj.Alerttime = (DateTime)reader["alerttime"];
                obj.Alerttype = (string)reader["alerttype"];
                obj.Alertruleid = (long)reader["alertruleid"];
                obj.Trackerserial = (string)reader["trackerserial"];
                obj.Severity = (string)reader["severity"];
                obj.Alerttitle = (string)reader["alerttitle"];
                obj.Alertactivity = (long)reader["alertactivity"];
                obj.Rulerecid = (long)reader["rulerecid"];
                obj.Alertclear = (DateTime?)reader["alertclear"];
                list.Add(obj);
            }
            return list;
        }
    }
}
