using dslink_dotnet_sensolus.DataModels;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Data;
using System.Linq;
using System.Text;

namespace dslink_dotnet_sensolus
{
    public class DimTracker : IEquatable<DimTracker>, DataModel<DimTracker>, IKeyValue<string>
    {
        public static DimTracker EmptyInstance { get; set; } = new DimTracker();
        public long Recid { get; set; }
        public string Serial { get; set; }
        public DateTime Validfrom { get; set; }
        public DateTime? Validto { get; set; }
        public string Name { get; set; }
        public string Productkey { get; set; }
        public string Thirdpartyid { get; set; }
        public string Tags { get; set; }

        public List<DimTracker> FromDataReader(IDataReader reader)
        {
            List<DimTracker> list = new List<DimTracker>();
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
                list.Add(obj);
            }
            return list;
        }

        public List<DimTracker> FromSensolus(JArray jArray)
        {
            return jArray.Select(x =>
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

        public bool Equals(DimTracker other)
        {
            return Serial == other.Serial &&
                Name == other.Name &&
                Productkey == other.Productkey &&
                Thirdpartyid == other.Thirdpartyid &&
                Tags == other.Tags;
        }

        public string InsertSql(List<DimTracker> list)
        {
            StringBuilder sb = new StringBuilder("INSERT INTO Dim_Tracker (serial, name, productkey, thirdpartyid, tags) VALUES ");
            sb.Append(
                String.Join(", ", list.Select(x => $"({SqlConvert.Convert(x.Serial)}, " +
                    $"{SqlConvert.Convert(x.Name)}, " +
                    $"{SqlConvert.Convert(x.Productkey)}, " +
                    $"{SqlConvert.Convert(x.Thirdpartyid)}, " +
                    $"{SqlConvert.Convert(x.Tags)})")
                )
            );
            return sb.ToString();
        }
        public string DeleteSql(List<DimTracker> list)
        {
            StringBuilder sb = new StringBuilder("UPDATE Dim_Tracker SET validto = now() WHERE validto IS NULL AND serial IN (");
            sb.Append(String.Join(", ", list.Select(x => $"'{x.Serial.Replace("'", "''")}'")));
            sb.Append(')');
            return sb.ToString();
        }

        public string GetKeyValue()
        {
            return Serial;
        }
    }
}
