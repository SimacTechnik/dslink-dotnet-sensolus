using Newtonsoft.Json.Linq;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;

namespace dslink_dotnet_sensolus
{
    class API
    {
        private string apiKey;
        private RestClient client = new RestClient(@"https://stickntrack.sensolus.com:443/rest");
        private RestRequest request = new RestRequest();

        public API(string apiKey)
        {
            this.apiKey = apiKey;
            client.AddDefaultParameter("apiKey", apiKey);
            client.AddDefaultHeader("Content-Type", "application/json");
        }

        public List<DimTracker> GetTrackers()
        {
            request.Resource = @"/api/v2/devices";
            request.Method = Method.GET;
            var response = client.Execute(request);
            JArray jResponse = JArray.Parse(response.Content);
            return DimTracker.EmptyInstance.FromSensolus(jResponse);
        }

        public List<DimRule> GetRules()
        {
            request.Resource = @"/api/v1/alertrules";
            request.Method = Method.GET;
            var response = client.Execute(request);
            JArray jResponse = JArray.Parse(response.Content);
            return DimRule.EmptyInstance.FromSensolus(jResponse);
        }

        public List<DimZone> GetZones()
        {
            request.Resource = @"/api/v1/geozones";
            request.Method = Method.GET;
            var response = client.Execute(request);
            JArray jResponse = JArray.Parse(response.Content);
            return DimZone.EmptyInstance.FromSensolus(jResponse);
        }
    }
}
