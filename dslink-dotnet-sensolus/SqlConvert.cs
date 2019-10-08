using System;
using System.Collections.Generic;
using System.Text;

namespace dslink_dotnet_sensolus
{
    class SqlConvert
    {

        public static string Convert<T>(T val)
        {
            if(val == null)
            {
                return "NULL";
            }
            else
            {
                return $"{val.ToString().Replace("'", "''")}";
            }
        }

        public static string Convert(DateTime val)
        {
            return Convert((DateTime?)val);
        }

        public static string Convert(DateTime? val)
        {
            if (val == null)
            {
                return "NULL";
            }
            else
            {
                return $"'{val.Value.ToString("yyyy-MM-dd HH:mm:ss.ffff").Replace("'", "''")}'";
            }
        }

        public static string Convert(string val)
        {
            if (val == null)
            {
                return "NULL";
            }
            else
            {
                return $"'{val.ToString().Replace("'", "''")}'";
            }
        }


    }
}
