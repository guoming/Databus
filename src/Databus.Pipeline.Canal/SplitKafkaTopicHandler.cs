using Confluent.Kafka;
using Hummingbird.Extensions.EventBus.Abstractions;
using Microsoft.AspNetCore.Routing;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Databus.Pipeline.Canal
{
    public class CanalEventEntry
    {
        public dynamic data { get; set; }

        public string database { get; set; }

        public long es { get; set; }

        public long id { get; set; }

        public bool isDdl { get; set; }

        public dynamic mysqlType { get; set; }

        public dynamic old { get; set; }

        public dynamic pkNames { get; set; }


        public string sql { get; set; }

        public dynamic sqlType { get; set; }

        public string table { get; set; }

        public long ts { get; set; }
        public string type { get; set; }

    }

    public class Databus
    {
        public Router[] Routers { get; set; }

        public class Router
        {
            public Input input { get; set; }

            public Output[] output { get; set; }

            public class Input
            {
                public string topic { get; set; }
            }

            public class Output
            {
                public string[] dbInclude { get; set; }

                public string[] tableInclude { get; set; }

                public string topic { get; set; }
            }
        }

    }
    public class SplitKafkaTopicHandler :  IEventBatchHandler<CanalEventEntry>
    {
        private readonly ILogger<SplitKafkaTopicHandler> logger;
        private readonly IEventBus eventBus;
        private readonly Databus databus;
        private static readonly String OP_INSERT = "INSERT";
        private static readonly String OP_UPDATE = "UPDATE";
        private static readonly String OP_DELETE = "DELETE";
        private static readonly String OP_CREATE = "CREATE";

        public SplitKafkaTopicHandler(
            ILogger<SplitKafkaTopicHandler> logger,
            IConfiguration configuration,
            IEventBus eventBus)
        {
            this.logger = logger;
            this.eventBus = eventBus;
            this.databus = configuration.GetSection("Databus").Get<Databus>();

        }
     
        public Task<bool> Handle(CanalEventEntry[] @events, Dictionary<string, object>[] Headers, CancellationToken cancellationToken)
        {

            var eventLogs = new List<Hummingbird.Extensions.EventBus.Models.EventLogEntry>();

            for (int i = 0; i < @events.Length; i++)
            {
                if (@events[i].type == OP_CREATE || @events[i].type == OP_DELETE || @events[i].type == OP_INSERT || @events[i].type == OP_UPDATE)
                {
                    var @event = @events[i];
                    var topic = Headers[i]["x-topic"];
                    var router = databus.Routers.FirstOrDefault(a => topic == a.input.topic);

                    foreach (var rule in router.output)
                    {
                        if ((rule.dbInclude == null || !rule.dbInclude.Any() && rule.dbInclude.Any(a => @event.database.StartsWith(a)))
                            && (rule.tableInclude == null || !rule.tableInclude.Any() && rule.tableInclude.Any(a => @event.table.StartsWith(a))))
                        {
                            var destTopic = rule.topic;
                            var srcTopic = router.input.topic;
                            destTopic = destTopic.Replace("{database}", @event.database);
                            destTopic = destTopic.Replace("{table}", @event.table);

                            logger.LogInformation($"{srcTopic}>{destTopic}");

                            var json = Newtonsoft.Json.JsonConvert.SerializeObject(@event);

                            if (json.Contains("0000-00-00 00:00:00"))
                            {
                                json = json.Replace("\"0000-00-00 00:00:00\"", "null");
                                json = json.Replace("'0000-00-00 00:00:00'", "null");

                                var obj = Newtonsoft.Json.JsonConvert.DeserializeObject<CanalEventEntry>(json);

                                eventLogs.Add(new Hummingbird.Extensions.EventBus.Models.EventLogEntry(destTopic, obj));
                            }
                            else
                            {
                                eventLogs.Add(new Hummingbird.Extensions.EventBus.Models.EventLogEntry(destTopic, @event));
                            }                          

                        }
                    }
                }
            }

            if (eventLogs.Any())
            {
                return eventBus.PublishAsync(eventLogs);
            }
            else
            {
                return Task.FromResult(true);
            }
        }
    }

}
