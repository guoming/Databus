using Com.Ctrip.Framework.Apollo;
using Com.Ctrip.Framework.Apollo.Enums;
using Hummingbird.AspNetCore.HealthChecks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;

namespace CanalTopicExchange
{
    public class Program
    {
        public static void SetApploEnv(string env)
        {
            switch (env)
            {
                case "dev":
                    Environment.SetEnvironmentVariable("APOLLO_ENV", "DEV");
                    break;
                case "test":
                    Environment.SetEnvironmentVariable("APOLLO_ENV", "FAT");
                    break;
                case "sbx":
                    Environment.SetEnvironmentVariable("APOLLO_ENV", "UAT");
                    break;
                case "qas":
                    Environment.SetEnvironmentVariable("APOLLO_ENV", "UAT");
                    break;
                case "live":
                    Environment.SetEnvironmentVariable("APOLLO_ENV", "PRO");
                    break;
                default:
                    break;
            }
        }
        public static void Main(string[] args)
        {
            var env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            Environment.SetEnvironmentVariable("Environment", env);

            if (!new List<string>() { "dev", "test", "sbx", "qas", "live" }.Contains(env))
            {
                throw new ArgumentException("环境变量 ASPNETCORE_ENVIRONMENT 必须是 dev/test/sbx/qas/live");
            }
            SetApploEnv(env);
            var host = BuildWebHost(args);
            host.Run();
        }


        public static IWebHost BuildWebHost(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseStartup<Startup>()
                .UseHealthChecks("/healthcheck")
                .UseMetrics((builderContext, metricsBuilder) => {
                    metricsBuilder.ToPrometheus();
                    metricsBuilder.ToInfluxDb(builderContext.Configuration.GetSection("AppMetrics:Influxdb"));
                })
                .ConfigureAppConfiguration((builderContext, config) =>
                {
                    #region config

                    config.SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("Config/appsettings.json", true, true)
                    .AddEnvironmentVariables()
                    .AddCommandLine(args);

                    #endregion config

                    #region 集成apollo
                    config.AddJsonFileEx("Config/apollo.json", true, true);
                    var apolloConfiguration = config.Build().GetSection("apollo");
                    var Apollo_AppId = apolloConfiguration["AppId"];
                    //成功获取到apollo配置，则集成apollo，apollo 中需将namespace 映射配置文件
                    if (!string.IsNullOrEmpty(Apollo_AppId))
                    {
                        config.AddApollo(apolloConfiguration)
                            .AddDefault(ConfigFileFormat.Xml)
                            .AddDefault(ConfigFileFormat.Json)
                            .AddDefault(ConfigFileFormat.Yml)
                            .AddDefault(ConfigFileFormat.Yaml)
                            .AddDefault()
                            .AddNamespace("appsettings", ConfigFileFormat.Json);
                        config.Build();
                    }
                    #endregion
                })
               .ConfigureLogging((hostingContext, logging) =>
               {
                   logging.AddConsole();
                   logging.AddDebug();
                   logging.ClearProviders();

               })
               .Build();
    }
}