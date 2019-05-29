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
    public class FactTracker : IKeyValue<string>
    {
        public DateTime Ts { get; set; }
        public string Serial { get; set; }
        public long Trackerrecid { get; set; }
        public int? Battpercent { get; set; }
        public int? Estbattlife { get; set; }

        public string GetKeyValue()
        {
            return Ts.ToString() + Serial;
        }
    }
    public static class FactTrackerExtensions
    {

        public static void Insert(this DatabaseWrapper conn, List<FactTracker> list)
        {
            if(list.Count == 0)
            {
                return;
            }
            StringBuilder sb = new StringBuilder("INSERT INTO Fact_Tracker (ts, serial, trackerrecid, battpercent, estbattlife) VALUES ");
            sb.Append(String.Join(", ", list.Select(x => $"({SqlConvert.Convert(x.Ts)}," +
            $"{SqlConvert.Convert(x.Serial)}, " +
            $"{SqlConvert.Convert(x.Trackerrecid)}, " +
            $"{SqlConvert.Convert(x.Battpercent)}, " +
            $"{SqlConvert.Convert(x.Estbattlife)})")));
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        public static List<FactTracker> GetFactTrackers(this DatabaseWrapper conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM Fact_Tracker;";
            List<FactTracker> data = new List<FactTracker>();
            using (IDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    FactTracker obj = new FactTracker();
                    obj.Ts = (DateTime)reader["ts"];
                    obj.Serial = (string)reader["serial"];
                    obj.Trackerrecid = (long)reader["trackerrecid"];
                    obj.Battpercent = (DBNull.Value == reader["battpercent"]) ? null : (int?)reader["battpercent"];
                    obj.Estbattlife = (DBNull.Value == reader["estbattlife"]) ? null : (int?)reader["estbattlife"];
                    data.Add(obj);
                }
            }
            return data;
        }

        public static List<FactTracker> GetFactTrackers(this API api, Dictionary<string, DimTracker> trackers)
        {
            JArray jResponse = api.Call(@"/api/v2/devices", Method.GET, null);
            return jResponse
                .Where(x => x["batteryInfo"] != null && x["batteryInfo"]["updatedAt"] != null)
                .Select(x => 
                new FactTracker
                    {
                        Ts = DateTime.Parse(x["batteryInfo"]["updatedAt"].Value<string>()),
                        Serial = x["serial"].Value<string>(),
                        Trackerrecid = trackers[x["serial"].Value<string>()].Recid,
                        Battpercent = x["batteryInfo"]["batteryLevelPercentage"]?.Value<int?>(),
                        Estbattlife = x["batteryInfo"]["estimatedRemainingBatteryLife"]?.Value<int?>()
                    }
                )
                .ToList();
        }
    }
}
