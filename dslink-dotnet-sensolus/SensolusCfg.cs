using System;
using System.Collections.Generic;
using System.Text;

namespace dslink_dotnet_sensolus
{
    struct SensolusCfg
    {
        public int Interval { get; set; }
        public string Host { get; set; }
        public string ApiKey { get; set; }
        public int Port { get; set; }
        public string Database { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public bool Pool { get; set; }
        public bool Clean { get; set; }
        public DateTime LastExecute { get; set; }
    }
}
