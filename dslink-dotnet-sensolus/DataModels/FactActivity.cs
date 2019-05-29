using dslink_dotnet_sensolus.DataModels;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace dslink_dotnet_sensolus
{
    public class FactActivity : DataModel<FactActivity>, IKeyValue<long>
    {
        public static FactActivity EmptyInstance { get; set; } = new FactActivity();
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

        private Dictionary<string, DimTracker> trackers;

        public void SetTrackers(Dictionary<string, DimTracker> trackers)
        {
            this.trackers = trackers;
        }

        public List<FactActivity> FromDataReader(IDataReader reader)
        {
            List<FactActivity> list = new List<FactActivity>();
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
                list.Add(obj);
            }
            return list;
        }

        public string DeleteSql(List<FactActivity> list)
        {
            StringBuilder sb = new StringBuilder("DELETE FROM Fact_Activity WHERE id IN (");
            sb.Append(String.Join(", ", list.Select(x => SqlConvert.Convert(x.Id))));
            sb.Append(')');
            return sb.ToString();
        }

        public List<FactActivity> FromSensolus(JArray jArray)
        {
            return jArray.Where(x => x["data"] != null).Select(x => {
                return x["data"].Select(y => new FactActivity
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
            }).SelectMany(x => x).ToList();
        }

        public string InsertSql(List<FactActivity> list)
        {
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
            return sb.ToString();
        }

        public long GetKeyValue()
        {
            return this.Id;
        }
    }
}
