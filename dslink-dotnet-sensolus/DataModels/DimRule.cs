using dslink_dotnet_sensolus.DataModels;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace dslink_dotnet_sensolus
{
    public class DimRule : IEquatable<DimRule>, DataModel<DimRule>, IKeyValue<long>
    {
        public static DimRule EmptyInstance { get; set; } = new DimRule();
        public long Recid { get; set; }
        public long Id { get; set; }
        public string Title { get; set; }
        public string Type { get; set; }
        public string Deviceserials { get; set; }
        public string Zonelst { get; set; }
        public DateTime Validfrom { get; set; }
        public DateTime? Validto { get; set; }

        public List<DimRule> FromDataReader(IDataReader reader)
        {
            List<DimRule> list = new List<DimRule>();
            while (reader.Read())
            {
                DimRule obj = new DimRule();
                obj.Recid = (long)reader["recid"];
                obj.Id = (long)reader["id"];
                obj.Title = (DBNull.Value == reader["title"]) ? null : (string)reader["title"];
                obj.Type = (string)reader["type"];
                obj.Deviceserials = (DBNull.Value == reader["deviceserials"]) ? null : (string)reader["deviceserials"];
                obj.Zonelst = (DBNull.Value == reader["zonelst"]) ? null : (string)reader["zonelst"];
                obj.Validfrom = (DateTime)reader["validfrom"];
                obj.Validto = (DBNull.Value == reader["validto"]) ? null : (DateTime?)reader["validto"];
                list.Add(obj);
            }
            return list;
        }

        public List<DimRule> FromSensolus(JArray jArray)
        {
            return jArray.Select(x => 
                new DimRule
                {
                    Recid = 0,
                    Id = x["id"].Value<long>(),
                    Title = x["title"]?.Value<string>(),
                    Type = x["alertTypeName"]?.Value<string>(),
                    Deviceserials = x["deviceSerials"]?.ToString(Newtonsoft.Json.Formatting.None),
                    Zonelst = x["definitions"]?["selectedIds"]?.ToString(Newtonsoft.Json.Formatting.None),
                    Validfrom = DateTime.Now,
                    Validto = null
                })
                .ToList();
        }

        public string InsertSql(List<DimRule> list)
        {
            StringBuilder sb = new StringBuilder("INSERT INTO Dim_Rule (id, title, type, deviceserials, zonelst) VALUES ");
            sb.Append(
                String.Join(", ", list.Select(x => $"({x.Id}, '{x.Title?.Replace("'", "''") ?? "NULL"}', '" +
                    $"{x.Type?.Replace("'", "''") ?? "NULL"}', '" +
                    $"{x.Deviceserials?.Replace("'", "''") ?? "NULL"}', '" +
                    $"{x.Zonelst?.Replace("'", "''") ?? "NULL"}')")
                    .ToArray<string>()
                )
            );
            return sb.ToString();
        }

        public string DeleteSql(List<DimRule> list)
        {
            StringBuilder sb = new StringBuilder("UPDATE Dim_Zone SET validto = now() WHERE validto IS NULL AND id IN (");
            sb.Append(String.Join(", ", list.Select(x => $"{x.Id}").ToArray()));
            sb.Append(')');
            return sb.ToString();
        }

        public bool Equals(DimRule other)
        {
            return Id == other.Id &&
                Title == other.Title &&
                Type == other.Type &&
                Deviceserials == other.Deviceserials &&
                Zonelst == other.Zonelst;
        }

        public long GetKeyValue()
        {
            return Id;
        }
    }
}
