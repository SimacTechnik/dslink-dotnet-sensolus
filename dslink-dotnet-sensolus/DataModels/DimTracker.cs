using dslink_dotnet_sensolus.DataModels;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Data;
using System.Linq;
using System.Text;
using RestSharp;

namespace dslink_dotnet_sensolus
{
    public class DimTracker : IEquatable<DimTracker>, IKeyValue<string>
    {
        public long Recid { get; set; }
        public string Serial { get; set; }
        public DateTime Validfrom { get; set; }
        public DateTime? Validto { get; set; }
        public string Name { get; set; }
        public string Productkey { get; set; }
        public string Thirdpartyid { get; set; }
        public string Tags { get; set; }

        public bool Equals(DimTracker other)
        {
            return Serial == other.Serial &&
                Name == other.Name &&
                Productkey == other.Productkey &&
                Thirdpartyid == other.Thirdpartyid &&
                Tags == other.Tags;
        }

        public string GetKeyValue()
        {
            return Serial;
        }
    }
    public static class DimTrackerExtensions {

        public static void Insert(this DatabaseWrapper conn, List<DimTracker> list)
        {
            if(list.Count == 0)
            {
                return;
            }
            StringBuilder sb = new StringBuilder("INSERT INTO Dim_Tracker (serial, name, productkey, thirdpartyid, tags) VALUES ");
            sb.Append(
                String.Join(", ", list.Select(x => $"({SqlConvert.Convert(x.Serial)}, " +
                    $"{SqlConvert.Convert(x.Name)}, " +
                    $"{SqlConvert.Convert(x.Productkey)}, " +
                    $"{SqlConvert.Convert(x.Thirdpartyid)}, " +
                    $"{SqlConvert.Convert(x.Tags)})")
                )
            );
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }
        public static void Delete(this DatabaseWrapper conn, List<DimTracker> list)
        {
            if(list.Count == 0)
            {
                return;
            }
            StringBuilder sb = new StringBuilder("UPDATE Dim_Tracker SET validto = now() WHERE validto IS NULL AND serial IN (");
            sb.Append(String.Join(", ", list.Select(x => $"'{x.Serial.Replace("'", "''")}'")));
            sb.Append(')');
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        public static List<DimTracker> GetDimTrackers(this DatabaseWrapper conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM Dim_Tracker WHERE validto IS NULL;";
            List<DimTracker> data = new List<DimTracker>();
            using (IDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    DimTracker obj = new DimTracker();
                    obj.Recid = (long)reader["recid"];
                    obj.Serial = (string)reader["serial"];
                    obj.Validfrom = (DateTime)reader["validfrom"];
                    obj.Validto = (DBNull.Value == reader["validto"]) ? null : (DateTime?)reader["validto"];
                    obj.Name = (DBNull.Value == reader["name"]) ? null : (string)reader["name"];
                    obj.Productkey = (DBNull.Value == reader["productkey"]) ? null : (string)reader["productkey"];
                    obj.Thirdpartyid = (DBNull.Value == reader["thirdpartyid"]) ? null : (string)reader["thirdpartyid"];
                    obj.Tags = (DBNull.Value == reader["tags"]) ? null : (string)reader["tags"];
                    data.Add(obj);
                }
            }
            return data;
        }

        public static List<DimTracker> GetDimTrackers(this API api)
        {
            JArray jResponse = api.Call(@"/api/v2/devices", Method.GET, null);
            return jResponse.Select(x =>
                new DimTracker
                {
                    Recid = 0,
                    Serial = x["serial"].Value<string>(),
                    Validfrom = DateTime.Now,
                    Validto = null,
                    Name = x["name"]?.Value<string>(),
                    Productkey = x["productKey"]?.Value<string>(),
                    Thirdpartyid = x["thirdPartyId"]?.Value<string>(),
                    Tags = x["deviceTags"]?.ToString(Newtonsoft.Json.Formatting.None)
                })
                .ToList();
        }
    }
}
