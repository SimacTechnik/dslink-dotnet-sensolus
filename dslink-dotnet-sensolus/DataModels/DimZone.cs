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
    public class DimZone : IEquatable<DimZone>, IKeyValue<long>
    {
        public long Recid { get; set; }
        public long Id { get; set; }
        public DateTime Validfrom { get; set; }
        public DateTime? Validto { get; set; }
        public string Name { get; set; }
        public string Geometry { get; set; }
        public string Tags { get; set; }

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
    public static class DimZoneExtensions {

        public static void Insert(this DatabaseWrapper conn, List<DimZone> list)
        {
            if(list.Count == 0)
            {
                return;
            }
            StringBuilder sb = new StringBuilder("INSERT INTO Dim_Zone (id, name, geometry, tags) VALUES ");
            sb.Append(
                String.Join(", ", list.Select(x => $"({x.Id}, " +
                    $"{SqlConvert.Convert(x.Name)}, " +
                    $"{SqlConvert.Convert(x.Geometry)}, " +
                    $"{SqlConvert.Convert(x.Tags)})")
                )
            );
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        public static void Delete(this DatabaseWrapper conn, List<DimZone> list)
        {
            if(list.Count == 0)
            {
                return;
            }
            StringBuilder sb = new StringBuilder("UPDATE Dim_Zone SET validto = now() WHERE validto IS NULL AND id IN (");
            sb.Append(String.Join(", ", list.Select(x => $"{x.Id}")));
            sb.Append(')');
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        public static List<DimZone> GetZones(this DatabaseWrapper conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM Dim_Zone WHERE validto IS NULL;";
            List<DimZone> data = new List<DimZone>();
            using (IDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    DimZone obj = new DimZone();
                    obj.Recid = (long)reader["recid"];
                    obj.Id = (long)reader["id"];
                    obj.Validfrom = (DateTime)reader["validfrom"];
                    obj.Validto = (DBNull.Value == reader["validto"]) ? null : (DateTime?)reader["validto"];
                    obj.Name = (string)reader["name"];
                    obj.Geometry = (string)reader["geometry"];
                    obj.Tags = (DBNull.Value == reader["tags"]) ? null : (string)reader["tags"];
                    data.Add(obj);
                }
            }
            return data;
        }



        public static List<DimZone> GetZones(this API api)
        {
            JArray jResponse = api.Call(@"/api/v1/geozones", Method.GET, null);
            return jResponse.Select(x =>
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

        public static void ClearZones(this DatabaseWrapper conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM Dim_Zone;";
            cmd.ExecuteNonQuery();
        }
    }
}
