using dslink_dotnet_sensolus.DataModels;
using Newtonsoft.Json.Linq;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace dslink_dotnet_sensolus
{
    public class FactAlert : IKeyValue<string>, IEquatable<FactAlert>
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

        public bool Equals(FactAlert other)
        {
            return Alerttime == other.Alerttime &&
                Alerttype == other.Alerttype &&
                Alertruleid == other.Alertruleid &&
                Trackerserial == other.Trackerserial &&
                Severity == other.Severity &&
                Alerttitle == other.Alerttitle &&
                Alertactivity == other.Alertactivity &&
                Rulerecid == other.Rulerecid &&
                Alertclear == other.Alertclear;
        }

        public string GetKeyValue()
        {
            return $"{Alerttime},{Alerttype},{Alertruleid},{Trackerserial}";
        }
    }
    public static class FactAlertExtensions
    {

        public static void Insert(this DatabaseWrapper conn, List<FactAlert> list)
        {
            if(list.Count == 0)
            {
                return;
            }
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
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        public static void Update(this DatabaseWrapper conn, List<FactAlert> list)
        {
            if(list.Count == 0)
            {
                return;
            }
            StringBuilder sb = new StringBuilder("DELETE FROM Fact_Alert WHERE concat_ws(',', alerttime, alerttype, alertruleid, trackerserial) in (");
            sb.Append(String.Join(", ", list.Select(x => $"'{x.Alerttime.ToString("yyyy-MM-dd HH:mm:sszz").Replace("'", "''")},{x.Alerttype},{x.Alertruleid},{x.Trackerserial}'")) + ')');
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
            conn.Insert(list);
        }

        public static List<FactAlert> GetAlerts(this DatabaseWrapper conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM Fact_Alert;";
            List<FactAlert> data = new List<FactAlert>();
            using (IDataReader reader = cmd.ExecuteReader())
            {
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
                    data.Add(obj);
                }
            }
            return data;
        }


        public static List<FactAlert> GetAlerts(this API api, string[] serials, Dictionary<string, FactActivity> activities, Dictionary<long, DimRule> rules)
        {
            List<FactAlert> output = new List<FactAlert>();
            foreach (var serial in serials)
            {
                JArray jResponse = api.Call($"/api/v1/devices/{serial}/alerts/historical", Method.GET, null);
                output.AddRange(
                    jResponse
                    .Select(x => new FactAlert
                    {
                        Alerttime = DateTime.Parse(x["date"].Value<string>()),
                        Alerttype = x["alertType"].Value<string>(),
                        Alertruleid = x["alertRule"]["id"].Value<long>(),
                        Trackerserial = x["monitoredEntity"]["sigfoxDevice"]["serial"].Value<string>(),
                        Severity = x["alertRule"]["severity"].Value<string>(),
                        Alerttitle = x["title"]?.Value<string>(),
                        Alertactivity = activities.ContainsKey(DateTime.Parse(x["date"].Value<string>()).ToString() + x["monitoredEntity"]["sigfoxDevice"]["serial"].Value<string>()) ? activities[DateTime.Parse(x["date"].Value<string>()).ToString() + x["monitoredEntity"]["sigfoxDevice"]["serial"].Value<string>()].Id : -1,
                        Rulerecid = rules.ContainsKey(x["alertRule"]["id"].Value<long>()) ? rules[x["alertRule"]["id"].Value<long>()].Recid : -1,
                        Alertclear = x["clear"] == null ? (DateTime?)null : DateTime.Parse(x["clear"].Value<string>())
                    })
                    .Where(x => x.Alertactivity != -1 && x.Rulerecid != -1)
                    .ToList());
                jResponse = api.Call($"/api/v1/devices/{serial}/alerts/active", Method.GET, null);
                output.AddRange(
                    jResponse
                    .Select(x => new FactAlert
                    {
                        Alerttime = DateTime.Parse(x["date"].Value<string>()),
                        Alerttype = x["alertType"].Value<string>(),
                        Alertruleid = x["alertRule"]["id"].Value<long>(),
                        Trackerserial = x["monitoredEntity"]["sigfoxDevice"]["serial"].Value<string>(),
                        Severity = x["alertRule"]["severity"].Value<string>(),
                        Alerttitle = x["title"]?.Value<string>(),
                        Alertactivity = activities.ContainsKey(DateTime.Parse(x["date"].Value<string>()).ToString() + x["monitoredEntity"]["sigfoxDevice"]["serial"].Value<string>()) ? activities[DateTime.Parse(x["date"].Value<string>()).ToString() + x["monitoredEntity"]["sigfoxDevice"]["serial"].Value<string>()].Id : -1,
                        Rulerecid = rules.ContainsKey(x["alertRule"]["id"].Value<long>()) ? rules[x["alertRule"]["id"].Value<long>()].Recid : -1,
                        Alertclear = (DateTime?)null
                    })
                    .Where(x => x.Alertactivity != -1 && x.Rulerecid != -1)
                    .ToList());
            }
            return output;
        }

        public static void ClearAlerts(this DatabaseWrapper conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM Fact_Alert;";
            cmd.ExecuteNonQuery();
        }
    }
}
