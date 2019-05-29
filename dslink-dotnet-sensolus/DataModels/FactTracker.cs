using dslink_dotnet_sensolus.DataModels;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace dslink_dotnet_sensolus
{
    public class FactTracker : DataModel<FactTracker>, IKeyValue<string>
    {
        public static FactTracker EmptyInstance = new FactTracker();
        public DateTime Ts { get; set; }
        public string Serial { get; set; }
        public long Trackerrecid { get; set; }
        public int? Battpercent { get; set; }
        public int? Estbattlife { get; set; }

        private Dictionary<string, DimTracker> trackers;

        public void SetTrackers(Dictionary<string, DimTracker> trackers)
        {
            this.trackers = trackers;
        }

        public List<FactTracker> FromDataReader(IDataReader reader)
        {
            List<FactTracker> list = new List<FactTracker>();
            while(reader.Read())
            {
                FactTracker obj = new FactTracker();
                obj.Ts = (DateTime)reader["ts"];
                obj.Serial = (string)reader["serial"];
                obj.Trackerrecid = (long)reader["trackerrecid"];
                obj.Battpercent = (DBNull.Value == reader["battpercent"]) ? null : (int?)reader["battpercent"];
                obj.Estbattlife = (DBNull.Value == reader["estbattlife"]) ? null : (int?)reader["estbattlife"];
                list.Add(obj);
            }
            return list;
        }

        public string DeleteSql(List<FactTracker> list)
        {
            throw new NotImplementedException();
        }

        public List<FactTracker> FromSensolus(JArray jArray)
        {
            return jArray.Where(x => x["batteryInfo"] != null && x["batteryInfo"]["updatedAt"] != null).Select(x => new FactTracker {
                Ts = DateTime.Parse(x["batteryInfo"]["updatedAt"].Value<string>()),
                Serial = x["serial"].Value<string>(),
                Trackerrecid = trackers[x["serial"].Value<string>()].Recid,
                Battpercent = x["batteryInfo"]["batteryLevelPercentage"]?.Value<int?>(),
                Estbattlife = x["batteryInfo"]["estimatedRemainingBatteryLife"]?.Value<int?>()
            }).ToList();
        }

        public string GetKeyValue()
        {
            return Ts.ToString() + Serial;
        }

        public string InsertSql(List<FactTracker> list)
        {
            StringBuilder sb = new StringBuilder("INSERT INTO Fact_Tracker (ts, serial, trackerrecid, battpercent, estbattlife) VALUES ");
            sb.Append(String.Join(", ", list.Select(x => $"({SqlConvert.Convert(x.Ts)}," +
            $"{SqlConvert.Convert(x.Serial)}, " +
            $"{SqlConvert.Convert(x.Trackerrecid)}, " +
            $"{SqlConvert.Convert(x.Battpercent)}, " +
            $"{SqlConvert.Convert(x.Estbattlife)})")));
            return sb.ToString();
        }
    }
}
