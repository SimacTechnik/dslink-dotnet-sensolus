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
    public class FactActivity : IKeyValue<long>
    {
        public long Id { get; set; }
        public DateTime Evttime { get; set; }
        public DateTime Inserttime { get; set; }
        public string Serial { get; set; }
        public string Evttype { get; set; }
        public double? Lat { get; set; }
        public double? Lon { get; set; }
        public string Src { get; set; }
        public int? Accuracy { get; set; }
        public string Address { get; set; }
        public string Geozones { get; set; }
        public long Trackerrecid { get; set; }

        public long GetKeyValue()
        {
            return this.Id;
        }
    }
    public static class FactActivityExtensions
    {

        public static void Delete(this DatabaseWrapper conn, List<FactActivity> list)
        {
            if (list.Count == 0)
            {
                return;
            }
            StringBuilder sb = new StringBuilder("DELETE FROM Fact_Activity WHERE id IN (");
            sb.Append(String.Join(", ", list.Select(x => SqlConvert.Convert(x.Id))));
            sb.Append(')');
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        public static void Insert(this DatabaseWrapper conn, List<FactActivity> list)
        {
            if(list.Count == 0)
            {
                return;
            }
            StringBuilder sb = new StringBuilder("INSERT INTO Fact_Activity (id, evttime, inserttime, serial, evttype, lat, lon, src, accuracy, address, geozones, trackerrecid) VALUES ");
            sb.Append(String.Join(", ", list.Select(x => $"({SqlConvert.Convert(x.Id)}," +
            $"{SqlConvert.Convert(x.Evttime)}," +
            $"{SqlConvert.Convert(x.Inserttime)}," +
            $"{SqlConvert.Convert(x.Serial)}," +
            $"{SqlConvert.Convert(x.Evttype)}," +
            $"{SqlConvert.Convert(x.Lat)}," +
            $"{SqlConvert.Convert(x.Lon)}," +
            $"{SqlConvert.Convert(x.Src)}," +
            $"{SqlConvert.Convert(x.Accuracy)}," +
            $"{SqlConvert.Convert(x.Address)}," +
            $"{SqlConvert.Convert(x.Geozones)}," +
            $"{SqlConvert.Convert(x.Trackerrecid)})")));
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        public static FactActivity GetLatestActivity(this DatabaseWrapper conn, string serial)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM Fact_Activity WHERE serial = " + SqlConvert.Convert(serial) + " ORDER BY inserttime DESC LIMIT 1";
            using (IDataReader reader = cmd.ExecuteReader())
            {
                if (!reader.Read()) return null;
                FactActivity obj = new FactActivity();
                obj.Id = (long)reader["id"];
                obj.Evttime = (DateTime)reader["evttime"];
                obj.Inserttime = (DateTime)reader["inserttime"];
                obj.Serial = (string)reader["serial"];
                obj.Evttype = (string)reader["evttype"];
                obj.Lat = (DBNull.Value == reader["lat"]) ? null : (double?)reader["lat"];
                obj.Lon = (DBNull.Value == reader["lon"]) ? null : (double?)reader["lon"];
                obj.Src = (string)reader["src"];
                obj.Accuracy = (DBNull.Value == reader["accuracy"]) ? null : (int?)reader["accuracy"];
                obj.Address = (DBNull.Value == reader["address"]) ? null : (string)reader["address"];
                obj.Geozones = (DBNull.Value == reader["geozones"]) ? null : (string)reader["geozones"];
                obj.Trackerrecid = (long)reader["trackerrecid"];
                return obj;
            }
        }

        public static List<FactActivity> GetActivities(this DatabaseWrapper conn, DateTime from, DateTime to)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT * FROM Fact_Activity WHERE evttime BETWEEN '" +
                $"{from.ToString("yyyy-MM-dd HH:mm:ss")}' AND '" +
                $"{to.ToString("yyyy-MM-dd HH:mm:ss")}'";
            List<FactActivity> data = new List<FactActivity>();
            using (IDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    FactActivity obj = new FactActivity();
                    obj.Id = (long)reader["id"];
                    obj.Evttime = (DateTime)reader["evttime"];
                    obj.Inserttime = (DateTime)reader["inserttime"];
                    obj.Serial = (string)reader["serial"];
                    obj.Evttype = (string)reader["evttype"];
                    obj.Lat = (DBNull.Value == reader["lat"]) ? null : (double?)reader["lat"];
                    obj.Lon = (DBNull.Value == reader["lon"]) ? null : (double?)reader["lon"];
                    obj.Src = (string)reader["src"];
                    obj.Accuracy = (DBNull.Value == reader["accuracy"]) ? null : (int?)reader["accuracy"];
                    obj.Address = (DBNull.Value == reader["address"]) ? null : (string)reader["address"];
                    obj.Geozones = (DBNull.Value == reader["geozones"]) ? null : (string)reader["geozones"];
                    obj.Trackerrecid = (long)reader["trackerrecid"];
                    data.Add(obj);
                }
            }
            return data;
        }



        public static List<FactActivity> GetActivities(this API api, DateTime from, DateTime to, Dictionary<string, DimTracker> trackers)
        {
            List<ActivityFilter> filterList = trackers.Keys.Select(x => new ActivityFilter { from = from, to = to, serial = x }).ToList();
            Parameter body = new Parameter("application/json", ActivityFilter.Serialize(filterList).ToString(Newtonsoft.Json.Formatting.None), ParameterType.RequestBody);
            Parameter timeFilter = new Parameter("timeFilter", "byMessageTime", ParameterType.QueryString);
            Parameter[] parameters = new Parameter[] { body, timeFilter };
            List<FactActivity> output = new List<FactActivity>();
            while (filterList.Count != 0)
            {
                body.Value = ActivityFilter.Serialize(filterList).ToString(Newtonsoft.Json.Formatting.None);
                JArray jResponse = api.Call(@"/api/v2/devices/data/aggregated/activity", Method.POST, parameters, true);
                output.AddRange(
                    jResponse
                    .Where(x => x["data"] != null)
                    .Select(x => {
                        return x["data"]
                        .Select(y => new FactActivity
                        {
                            Id = y["id"].Value<long>(),
                            Serial = x["serial"].Value<string>(),
                            Evttime = DateTime.Parse(y["time"].Value<string>()),
                            Inserttime = DateTime.Parse(y["insertTime"].Value<string>()),
                            Evttype = y["state"].Value<string>(),
                            Lat = y["lat"]?.Value<double?>(),
                            Lon = y["lng"]?.Value<double?>(),
                            Src = y["source"].Value<string>(),
                            Accuracy = y["accuracy"]?.Value<int?>(),
                            Address = y["address"]?.Value<string>(),
                            Geozones = y["geozones"]?.ToString(Newtonsoft.Json.Formatting.None),
                            Trackerrecid = trackers[x["serial"].Value<string>()].Recid
                        }).ToList();
                    })
                    .SelectMany(x => x)
                        .ToList()
                    );
                filterList.Clear();
                foreach (var obj in jResponse)
                {
                    if (obj["truncated"].Value<bool>())
                    {
                        filterList.Add(new ActivityFilter
                        {
                            from = obj["data"].Max(x => DateTime.Parse(x["time"].Value<string>())).AddSeconds(1),
                            to = to,
                            serial = obj["serial"].Value<string>()
                        });
                    }
                    else if (obj["skipped"].Value<bool>())
                    {
                        filterList.Add(new ActivityFilter
                        {
                            from = from,
                            to = to,
                            serial = obj["serial"].Value<string>()
                        });
                    }
                }
            }
            return output;
        }

        public static void ClearActivities(this DatabaseWrapper conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM Fact_Activity;";
            cmd.ExecuteNonQuery();
        }
    }
}
