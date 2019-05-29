using dslink_dotnet_sensolus.DataModels;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace dslink_dotnet_sensolus
{
    public class FactAlert : DataModel<FactAlert>, IKeyValue<string>
    {
        public static FactAlert EmptyInstance { get; set; } = new FactAlert();
        public DateTime Alerttime { get; set; }
        public string Alerttype { get; set; }
        public long Alertruleid { get; set; }
        public string Trackerserial { get; set; }
        public string Severity { get; set; }
        public string Alerttitle { get; set; }
        public long Alertactivity { get; set; }
        public long Rulerecid { get; set; }
        public DateTime? Alertclear { get; set; }

        private Dictionary<long, DimRule> rules;

        private Dictionary<string, FactActivity> activities;

        public void SetRules(Dictionary<long, DimRule> rules)
        {
            this.rules = rules;
        }

        public void SetActivities(Dictionary<string, FactActivity> activities)
        {
            this.activities = activities;
        }

        public List<FactAlert> FromDataReader(IDataReader reader)
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
                obj.Alerttitle = (DBNull.Value == reader["alerttitle"]) ? null : (string)reader["alerttitle"];
                obj.Alertactivity = (long)reader["alertactivity"];
                obj.Rulerecid = (long)reader["rulerecid"];
                obj.Alertclear = (DBNull.Value == reader["alertclear"]) ? null : (DateTime?)reader["alertclear"];
                list.Add(obj);
            }
            return list;
        }

        public string DeleteSql(List<FactAlert> list)
        {
            throw new NotImplementedException();
        }

        public List<FactAlert> FromSensolus(JArray jArray)
        {
            return jArray.Select(x => new FactAlert
            {
                Alerttime = DateTime.Parse(x["date"].Value<string>()),
                Alerttype = x["alertType"].Value<string>(),
                Alertruleid = x["alertRule"]["id"].Value<long>(),
                Trackerserial = x["monitoredEntity"]["sigfoxDevice"]["serial"].Value<string>(),
                Severity = x["alertRule"]["severity"].Value<string>(),
                Alerttitle = x["title"]?.Value<string>(),
                Alertactivity = activities[DateTime.Parse(x["date"].Value<string>()).ToString()+ x["monitoredEntity"]["sigfoxDevice"]["serial"].Value<string>()].Id,
                Rulerecid = rules[x["alertRule"]["id"].Value<long>()].Recid,
                Alertclear = x["clear"] == null ? (DateTime?)null : DateTime.Parse(x["clear"].Value<string>())
            }).ToList();
        }

        public string GetKeyValue()
        {
            return $"{Alerttime},{Alerttype},{Alertruleid},{Trackerserial}";
        }

        public string InsertSql(List<FactAlert> list)
        {
            StringBuilder sb = new StringBuilder("INSERT INTO Fact_Alert (alerttime, alerttype, alertruleid, trackerserial, severity, alerttitle, alertactivity, rulerecid, alertclear) VALUES ");
            sb.Append(String.Join(", ", list.Select(x => $"({SqlConvert.Convert(x.Alerttime)}," +
            $"{SqlConvert.Convert(x.Alerttype)}, " +
            $"{SqlConvert.Convert(x.Alertruleid)}, " +
            $"{SqlConvert.Convert(x.Trackerserial)}, " +
            $"{SqlConvert.Convert(x.Severity)}, " +
            $"{SqlConvert.Convert(x.Alerttitle)}, " +
            $"{SqlConvert.Convert(x.Alertactivity)}, " +
            $"{SqlConvert.Convert(x.Rulerecid)}, " +
            $"{SqlConvert.Convert(x.Alertclear)})").ToList()));
            return sb.ToString();
        }
    }
}
