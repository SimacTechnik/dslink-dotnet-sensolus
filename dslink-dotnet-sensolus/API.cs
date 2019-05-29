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
    class API
    {
        private string apiKey;
        private RestClient client = new RestClient(@"https://stickntrack.sensolus.com:443/rest");
        private RestRequest request = new RestRequest();
        private JArray trackerResponse = null;

        public API(string apiKey)
        {
            this.apiKey = apiKey;
            client.AddDefaultParameter("apiKey", apiKey, ParameterType.QueryString);
            client.AddDefaultHeader("Content-Type", "application/json");
        }

        public List<DimTracker> GetDimTrackers()
        {
#if OFFLINE
            var json = "[{\"name\":\"TTTS(beacon-start+stop)\",\"serial\":\"16QHNL\",\"sigfoxContractInfo\":{\"contractType\":\"CONTRACT\",\"activatedAt\":\"2019-01-07T15:05:10+0000\",\"endsAt\":\"2020-01-07T15:05:10+0000\"},\"sigfoxActivationStatus\":\"ACTIVATED\",\"batteryInfo\":{\"batteryLevelPercentage\":92,\"estimatedRemainingBatteryLife\":52,\"updatedAt\":\"2019-05-22T15:06:53+0000\"},\"firstMessageAt\":\"2019-01-07T15:01:29+0000\",\"lastSeenAlive\":\"2019-05-23T06:39:24+0000\",\"productKey\":\"SNT3-ULTRA-ABGSW3(RCX)-v1.4\",\"thirdPartyId\":\"TV001\",\"deviceTags\":[{\"id\":6204,\"name\":\"TomV\"}]},{\"name\":\"TTTS(beacon-never)\",\"serial\":\"F1FWW6\",\"sigfoxContractInfo\":{\"contractType\":\"CONTRACT\",\"activatedAt\":\"2019-03-12T12:41:16+0000\",\"endsAt\":\"2020-03-12T12:41:16+0000\"},\"sigfoxActivationStatus\":\"ACTIVATED\",\"batteryInfo\":{\"batteryLevelPercentage\":98,\"estimatedRemainingBatteryLife\":94,\"updatedAt\":\"2019-05-22T14:02:03+0000\"},\"firstMessageAt\":\"2019-03-12T12:37:36+0000\",\"lastSeenAlive\":\"2019-05-23T06:56:12+0000\",\"productKey\":\"SNT3-ULTRA-ABGSW3(RCX)-v1.4\",\"thirdPartyId\":\"TV002\",\"deviceTags\":[{\"id\":6204,\"name\":\"TomV\"}]},{\"name\":\"SNT3 R6GJWR\",\"serial\":\"R6GJWR\",\"sigfoxContractInfo\":{\"contractType\":\"CONTRACT\",\"activatedAt\":\"2019-01-07T15:05:31+0000\",\"endsAt\":\"2020-01-07T15:05:31+0000\"},\"sigfoxActivationStatus\":\"ACTIVATED\",\"batteryInfo\":{\"batteryLevelPercentage\":96,\"estimatedRemainingBatteryLife\":146,\"updatedAt\":\"2019-05-22T22:53:07+0000\"},\"firstMessageAt\":\"2019-01-07T15:01:51+0000\",\"lastSeenAlive\":\"2019-05-22T22:53:07+0000\",\"productKey\":\"SNT3-ULTRA-ABGSW3(RCX)-v1.4\",\"deviceTags\":[]},{\"name\":\"TTTS(beacon-always)\",\"serial\":\"T7CHVL\",\"sigfoxContractInfo\":{\"contractType\":\"CONTRACT\",\"activatedAt\":\"2019-03-12T12:51:06+0000\",\"endsAt\":\"2020-03-12T12:51:06+0000\"},\"sigfoxActivationStatus\":\"ACTIVATED\",\"batteryInfo\":{\"batteryLevelPercentage\":99,\"estimatedRemainingBatteryLife\":291,\"updatedAt\":\"2019-05-23T04:11:47+0000\"},\"firstMessageAt\":\"2019-03-12T12:45:52+0000\",\"lastSeenAlive\":\"2019-05-23T06:54:48+0000\",\"productKey\":\"SNT3-ULTRA-ABGSW3(RCX)-v1.4\",\"thirdPartyId\":\"TV003\",\"deviceTags\":[{\"id\":6204,\"name\":\"TomV\"}]},{\"name\":\"SNT3 WD7KYG\",\"serial\":\"WD7KYG\",\"sigfoxContractInfo\":{\"contractType\":\"CONTRACT\",\"activatedAt\":\"2019-03-12T12:37:08+0000\",\"endsAt\":\"2020-03-12T12:37:08+0000\"},\"sigfoxActivationStatus\":\"ACTIVATED\",\"batteryInfo\":{\"batteryLevelPercentage\":99,\"estimatedRemainingBatteryLife\":188,\"updatedAt\":\"2019-05-23T09:45:28+0000\"},\"firstMessageAt\":\"2019-03-12T12:33:28+0000\",\"lastSeenAlive\":\"2019-05-23T09:45:28+0000\",\"productKey\":\"SNT3-ULTRA-ABGSW3(RCX)-v1.4\",\"deviceTags\":[]},{\"name\":\"SNT3 XCFJGG\",\"serial\":\"XCFJGG\",\"sigfoxActivationStatus\":\"ACTIVATED\",\"batteryInfo\":{},\"productKey\":\"SNT3-ULTRA-ABGSW3(RCX)-v1.4\",\"deviceTags\":[]}]";
            JArray jResponse = JArray.Parse(json);
#else
            request.Resource = @"/api/v2/devices";
            request.Method = Method.GET;
            var response = client.Execute(request);
            JArray jResponse = JArray.Parse(response.Content);
#endif
            trackerResponse = jResponse;
            return DimTracker.EmptyInstance.FromSensolus(jResponse);
        }

        public List<FactTracker> GetFactTrackers()
        {
            JArray jResponse;
            if(trackerResponse != null)
            {
                jResponse = trackerResponse;
            }
            else
            {
                request.Resource = @"/api/v2/devices";
                request.Method = Method.GET;
                var response = client.Execute(request);
                jResponse = JArray.Parse(response.Content);
            }
            return FactTracker.EmptyInstance.FromSensolus(jResponse);
        }

        public List<DimRule> GetRules()
        {
#if OFFLINE
            var json = "[{\"id\":3323,\"title\":\"Tracker on the move\",\"description\":\"\\u003cbody\\u003e\",\"active\":true,\"deviceSerials\":[\"T7CHVL\",\"F1FWW6\",\"16QHNL\"],\"alertTypeName\":\"GeozoneOutsideAlertType\",\"definitions\":{\"selectedIds\":[4595,4596,4594]},\"alertNotifications\":[{\"emails\":[],\"contacts\":[\"2c9fa4026856261c01685bf747b700a6\"],\"notificationType\":\"EMAIL\"}],\"severity\":\"REMINDER\",\"monitoredItem\":{\"selectedIds\":[6204],\"selectType\":\"TAG\",\"monitoredType\":\"DEVICE\"}},{\"id\":4138,\"title\":\"Tracker in Simac (push)\",\"description\":\"\\u003cbody\\u003e\",\"active\":true,\"deviceSerials\":[\"T7CHVL\",\"F1FWW6\",\"16QHNL\"],\"alertTypeName\":\"GeozoneInsideAlertType\",\"definitions\":{\"selectedIds\":[4596]},\"alertNotifications\":[{\"httpMethod\":\"POST\",\"url\":\"http://anger.sin.cvut.cz:8082/path?serial\\u003d{device_serial}\\u0026time\\u003d{time}\\u0026third_party_id\\u003d{third_party_id}\",\"notificationType\":\"REST_API\"}],\"severity\":\"REMINDER\",\"monitoredItem\":{\"selectedIds\":[6204],\"selectType\":\"TAG\",\"monitoredType\":\"DEVICE\"}},{\"id\":3326,\"title\":\"Tracker in Tom\\u0027s Place\",\"description\":\"\\u003cbody\\u003e\",\"active\":true,\"deviceSerials\":[\"T7CHVL\",\"F1FWW6\",\"16QHNL\"],\"alertTypeName\":\"GeozoneInsideAlertType\",\"definitions\":{\"selectedIds\":[4595]},\"alertNotifications\":[{\"emails\":[],\"contacts\":[\"2c9fa4026856261c01685bf747b700a6\"],\"notificationType\":\"EMAIL\"}],\"severity\":\"REMINDER\",\"monitoredItem\":{\"selectedIds\":[6204],\"selectType\":\"TAG\",\"monitoredType\":\"DEVICE\"}},{\"id\":3324,\"title\":\"Tracker in Simac\",\"description\":\"\\u003cbody\\u003e\",\"active\":true,\"deviceSerials\":[\"T7CHVL\",\"F1FWW6\",\"16QHNL\"],\"alertTypeName\":\"GeozoneInsideAlertType\",\"definitions\":{\"selectedIds\":[4596]},\"alertNotifications\":[{\"emails\":[],\"contacts\":[\"2c9fa4026856261c01685bf747b700a6\"],\"notificationType\":\"EMAIL\"}],\"severity\":\"REMINDER\",\"monitoredItem\":{\"selectedIds\":[6204],\"selectType\":\"TAG\",\"monitoredType\":\"DEVICE\"}},{\"id\":4140,\"title\":\"Tracker in Fosa (push)\",\"description\":\"\\u003cbody\\u003e\",\"active\":true,\"deviceSerials\":[\"T7CHVL\",\"F1FWW6\",\"16QHNL\"],\"alertTypeName\":\"GeozoneInsideAlertType\",\"definitions\":{\"selectedIds\":[4594]},\"alertNotifications\":[{\"httpMethod\":\"POST\",\"url\":\"http://anger.sin.cvut.cz:8082/path?serial\\u003d{device_serial}\\u0026time\\u003d{time}\\u0026third_party_id\\u003d{third_party_id}\",\"notificationType\":\"REST_API\"}],\"severity\":\"REMINDER\",\"monitoredItem\":{\"selectedIds\":[6204],\"selectType\":\"TAG\",\"monitoredType\":\"DEVICE\"}},{\"id\":4141,\"title\":\"Tracker in Tom\\u0027s place (push)\",\"description\":\"\\u003cbody\\u003e\",\"active\":true,\"deviceSerials\":[\"T7CHVL\",\"F1FWW6\",\"16QHNL\"],\"alertTypeName\":\"GeozoneInsideAlertType\",\"definitions\":{\"selectedIds\":[4595]},\"alertNotifications\":[{\"httpMethod\":\"POST\",\"url\":\"http://anger.sin.cvut.cz:8082/path?serial\\u003d{device_serial}\\u0026time\\u003d{time}\\u0026third_party_id\\u003d{third_party_id}\",\"notificationType\":\"REST_API\"}],\"severity\":\"REMINDER\",\"monitoredItem\":{\"selectedIds\":[6204],\"selectType\":\"TAG\",\"monitoredType\":\"DEVICE\"}},{\"id\":3322,\"title\":\"Tracker in Fosa\",\"description\":\"\\u003cbody\\u003e\",\"active\":true,\"deviceSerials\":[\"T7CHVL\",\"F1FWW6\",\"16QHNL\"],\"alertTypeName\":\"GeozoneInsideAlertType\",\"definitions\":{\"selectedIds\":[4594]},\"alertNotifications\":[{\"emails\":[],\"contacts\":[\"2c9fa4026856261c01685bf747b700a6\"],\"notificationType\":\"EMAIL\"}],\"severity\":\"REMINDER\",\"monitoredItem\":{\"selectedIds\":[6204],\"selectType\":\"TAG\",\"monitoredType\":\"DEVICE\"}},{\"id\":4142,\"title\":\"Tracker on the move (push)\",\"description\":\"\\u003cbody\\u003e\",\"active\":true,\"deviceSerials\":[\"T7CHVL\",\"F1FWW6\",\"16QHNL\"],\"alertTypeName\":\"GeozoneOutsideAlertType\",\"definitions\":{\"selectedIds\":[4595,4596,4594]},\"alertNotifications\":[{\"httpMethod\":\"POST\",\"url\":\"http://anger.sin.cvut.cz:8082/path?serial\\u003d{device_serial}\\u0026time\\u003d{time}\\u0026third_party_id\\u003d{third_party_id}\",\"notificationType\":\"REST_API\"}],\"severity\":\"REMINDER\",\"monitoredItem\":{\"selectedIds\":[6204],\"selectType\":\"TAG\",\"monitoredType\":\"DEVICE\"}},{\"id\":4317,\"title\":\"Tracker in zone\",\"description\":\"Tracker entered one of the listed zones\",\"active\":true,\"deviceSerials\":[\"T7CHVL\",\"F1FWW6\",\"16QHNL\"],\"alertTypeName\":\"GeozoneInsideAlertType\",\"definitions\":{\"selectedIds\":[461]},\"alertNotifications\":[{\"emails\":[],\"contacts\":[\"2c9fa4026856261c01685bf747b700a6\"],\"notificationType\":\"EMAIL\"}],\"severity\":\"REMINDER\",\"monitoredItem\":{\"selectedIds\":[6204],\"selectType\":\"TAG\",\"monitoredType\":\"DEVICE\"}}]";
            JArray jResponse = JArray.Parse(json);
#else
            request.Resource = @"/api/v1/alertrules";
            request.Method = Method.GET;
            var response = client.Execute(request);
            JArray jResponse = JArray.Parse(response.Content);
#endif
            return DimRule.EmptyInstance.FromSensolus(jResponse);
        }

        public List<DimZone> GetZones()
        {
#if OFFLINE
            var json = "[{\"coordinates\":[{\"x\":50.03194215019863,\"y\":14.483681917190554},{\"x\":50.031430088201894,\"y\":14.483156204223635},{\"x\":50.03172299107708,\"y\":14.482453465461733},{\"x\":50.031853935314004,\"y\":14.482415914535524},{\"x\":50.0321020392048,\"y\":14.482625126838686},{\"x\":50.03219852370508,\"y\":14.482952356338501},{\"x\":50.03219163196149,\"y\":14.483097195625307}],\"id\":4594,\"name\":\"Fosa\",\"borderColor\":\"#2785db\",\"contentColor\":\"#d2ea16\"},{\"coordinates\":[{\"x\":50.049392411213894,\"y\":14.355842471122742},{\"x\":50.048570513617555,\"y\":14.355778098106386},{\"x\":50.047718288129175,\"y\":14.356099963188173},{\"x\":50.047732067158336,\"y\":14.356808066368105},{\"x\":50.04799042322258,\"y\":14.358808994293213},{\"x\":50.04917539855278,\"y\":14.358508586883547},{\"x\":50.04941101223244,\"y\":14.356577396392824}],\"id\":4596,\"name\":\"Simac\",\"borderColor\":\"#2785db\",\"contentColor\":\"#d2ea16\"},{\"coordinates\":[{\"x\":50.00577037967501,\"y\":14.677482247352602},{\"x\":50.00549110969133,\"y\":14.677358865737917},{\"x\":50.00535802488498,\"y\":14.677600264549257},{\"x\":50.005522139769624,\"y\":14.6780401468277}],\"id\":4595,\"name\":\"Tom\\u0027s\",\"borderColor\":\"#2785db\",\"contentColor\":\"#d2ea16\"}]";
            JArray jResponse = JArray.Parse(json);
#else
            request.Resource = @"/api/v1/geozones";
            request.Method = Method.GET;
            var response = client.Execute(request);
            JArray jResponse = JArray.Parse(response.Content);
#endif
            return DimZone.EmptyInstance.FromSensolus(jResponse);
        }

        public List<FactActivity> GetActivities(DateTime from, DateTime to, string[] serials)
        {
            request.Resource = @"/api/v2/devices/data/aggregated/activity";
            request.Method = Method.POST;
            request.RequestFormat = DataFormat.Json;
            request.AddParameter("timeFilter", "byMessageTime", ParameterType.QueryString);
            List<FactActivity> output = new List<FactActivity>();
            List<ActivityFilter> filterList = serials.Select(x => new ActivityFilter { from = from, to = to, serial = x }).ToList();
            Parameter body = new Parameter("application/json", ActivityFilter.Serialize(filterList).ToString(Newtonsoft.Json.Formatting.None), ParameterType.RequestBody);
            request.AddParameter(body);
            while (filterList.Count != 0){
                body.Value = ActivityFilter.Serialize(filterList).ToString(Newtonsoft.Json.Formatting.None);
                var response = client.Execute(request);
                JArray jResponse = JArray.Parse(response.Content);
                output.AddRange(FactActivity.EmptyInstance.FromSensolus(jResponse));
                filterList.Clear();
                foreach(var obj in jResponse)
                {
                    if(obj["truncated"].Value<bool>())
                    {
                        filterList.Add(new ActivityFilter {
                            from = obj["data"].Max(x => DateTime.Parse(x["time"].Value<string>())).AddSeconds(1),
                            to = to,
                            serial = obj["serial"].Value<string>()
                        });
                    }
                    else if(obj["skipped"].Value<bool>())
                    {
                        filterList.Add(new ActivityFilter {
                            from = from,
                            to = to,
                            serial = obj["serial"].Value<string>()
                        });
                    }
                }
            }
            return output;
        }

        public List<FactAlert> GetAlerts(string[] serials)
        {
            List<FactAlert> output = new List<FactAlert>();
            request.Parameters.Clear();
            foreach (var serial in serials)
            {
                request.Resource = $"/api/v1/devices/{serial}/alerts/historical";
                request.Method = Method.GET;
                var response = client.Execute(request);
                JArray jResponse = JArray.Parse(response.Content);
                output.AddRange(FactAlert.EmptyInstance.FromSensolus(jResponse));
            }
            return output;
        }
    }
}
