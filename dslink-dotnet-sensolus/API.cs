using dslink_dotnet_sensolus.DataModels;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;

namespace dslink_dotnet_sensolus
{
    public class API
    {
        private string apiKey;
        private RestClient client = new RestClient(@"https://stickntrack.sensolus.com:443/rest");
        private RestRequest request = new RestRequest();
        private Dictionary<long, JArray> DataBuffer { get; set; } = new Dictionary<long, JArray>();
        private int reqCount = 0;
        public int Count { get => reqCount; }

        public API(string apiKey)
        {
            this.apiKey = apiKey;
            client.AddDefaultParameter("apiKey", this.apiKey, ParameterType.QueryString);
            client.AddDefaultHeader("Content-Type", "application/json");
        }

        public JArray Call(string resource, Method method, Parameter[] parameters, bool force = false)
        {
            long hash = resource.GetHashCode() + 100 * method.GetHashCode();
            if(!force && DataBuffer.ContainsKey(hash))
            {
                return DataBuffer[hash];
            }
            this.request.Resource = resource;
            this.request.Method = method;
            this.request.Parameters.Clear();
            if (parameters != null)
            {
                this.request.Parameters.AddRange(parameters);
            }
            var tmp = client.Execute(request).Content;
            reqCount++;
            JArray data = JArray.Parse(tmp);
            DataBuffer[hash] = data;
            return data;
        }

        public void ResetCounter()
        {
            reqCount = 0;
        }
    }
}
