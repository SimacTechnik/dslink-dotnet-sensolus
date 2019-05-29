using dslink_dotnet_sensolus.DataModels;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace dslink_dotnet_sensolus
{
    public class DimZone : IEquatable<DimZone>, DataModel<DimZone>, IKeyValue<long>
    {
        public static DimZone EmptyInstance { get; set; } = new DimZone();
        public long Recid { get; set; }
        public long Id { get; set; }
        public DateTime Validfrom { get; set; }
        public DateTime? Validto { get; set; }
        public string Name { get; set; }
        public string Geometry { get; set; }
        public string Tags { get; set; }

        public List<DimZone> FromDataReader(IDataReader reader)
        {
            List<DimZone> list = new List<DimZone>();
            while(reader.Read())
            {
                DimZone obj = new DimZone();
                obj.Recid = (long)reader["recid"];
                obj.Id = (long)reader["id"];
                obj.Validfrom = (DateTime)reader["validfrom"];
                obj.Validto = (DBNull.Value == reader["validto"]) ? null : (DateTime?)reader["validto"];
                obj.Name = (string)reader["name"];
                obj.Geometry = (string)reader["geometry"];
                obj.Tags = (DBNull.Value == reader["tags"]) ? null : (string)reader["tags"];
                list.Add(obj);
            }
            return list;
        }

        public List<DimZone> FromSensolus(JArray jArray)
        {
            return jArray.Select(x =>
                    new DimZone
                    {
                        Recid = 0,
                        Id = x["id"].Value<long>(),
                        Validfrom = DateTime.Now,
                        Validto = null,
                        Name = x["name"]?.Value<string>(),
                        Geometry = x["coordinates"]?.ToString(Newtonsoft.Json.Formatting.None),
                        Tags = x["zoneTags"]?.Value<string>()
                    }
                )
                .ToList();
        }

        public string InsertSql(List<DimZone> list)
        {
            StringBuilder sb = new StringBuilder("INSERT INTO Dim_Zone (id, name, geometry, tags) VALUES ");
            sb.Append(
                String.Join(", ", list.Select(x => $"({x.Id}, " +
                    $"{SqlConvert.Convert(x.Name)}, " +
                    $"{SqlConvert.Convert(x.Geometry)}, " +
                    $"{SqlConvert.Convert(x.Tags)})")
                )
            );
            return sb.ToString();
        }

        public string DeleteSql(List<DimZone> list)
        {
            StringBuilder sb = new StringBuilder("UPDATE Dim_Zone SET validto = now() WHERE validto IS NULL AND id IN (");
            sb.Append(String.Join(", ", list.Select(x => $"{x.Id}")));
            sb.Append(')');
            return sb.ToString();
        }

        public bool Equals(DimZone other)
        {
            return Id == other.Id &&
                Name == other.Name &&
                Geometry == other.Geometry &&
                Tags == other.Tags;
        }

        public long GetKeyValue()
        {
            return Id;
        }
    }
}
