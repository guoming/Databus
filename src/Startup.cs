using Hummingbird.Extensions.EventBus.Abstractions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;

namespace CanalTopicExchange
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;


        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddMvc();

            #region services

            services.AddHttpContextAccessor()
                  .AddTransient<IConfiguration>(a => Configuration)
                    .AddHealthChecks(checks =>
                    {
                        checks.WithDefaultCacheDuration(TimeSpan.FromSeconds(5));
                        checks.WithPartialSuccessStatus(Hummingbird.Extensions.HealthChecks.CheckStatus.Healthy);
                    })
                  .AddHummingbird(hummingbird =>
                  {
                      hummingbird.AddConsulDynamicRoute(Configuration, s =>
                      {

                      });

                      //事件总线
                      hummingbird.AddEventBus(builder =>
                      {
                          builder.AddKafka(option =>
                          {
                              option.WithSenderConfig(new Confluent.Kafka.ProducerConfig()
                              {
                                  EnableDeliveryReports = true,
                                  BootstrapServers = Configuration["Kafka:Sender:bootstrap.servers"],
                                  //  Debug = hostContext.Configuration["Kafka:Sender:Debug"] //  Debug = "broker,topic,msg"
                              });

                              option.WithReceiverConfig(new Confluent.Kafka.ConsumerConfig()
                              {
                                  EnableAutoCommit = false,                                  
                                  AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest,
                                  Debug = Configuration["Kafka:Receiver:Debug"],//"consumer,cgrp,topic,fetch",
                                  GroupId = Configuration["Kafka:Receiver:GroupId"],
                                  //BootstrapServers = "192.168.78.29:9092,192.168.78.30:9092,192.168.78.31:9092",
                                  BootstrapServers = Configuration["Kafka:Receiver:bootstrap.servers"]
                              });
                              option.WithReceiver(
                                  ReceiverAcquireRetryAttempts: 0,
                                  ReceiverHandlerTimeoutMillseconds: 10000);

                              option.WithSender(
                                  AcquireRetryAttempts: 3,
                                  SenderConfirmTimeoutMillseconds: 30000,
                                  SenderConfirmFlushTimeoutMillseconds: 100);
                          });

                      });
                  });


            #endregion services
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            var eventBus = app.ApplicationServices.GetRequiredService<IEventBus>();
            var logger = app.ApplicationServices.GetRequiredService<ILogger<IEventLogger>>();
            var config=app.ApplicationServices.GetRequiredService<IConfiguration>();
            var databus = config.GetSection("Databus").Get<Databus>();

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseHummingbird(humming =>
            {
                humming.UseEventBus(sp =>
                {

                    sp.UseSubscriber(eventbus =>
                    {
                        foreach (var p in databus.Routers)
                        {
                            eventbus.Register<CanalEventEntry, SplitKafkaTopicHandler>("", p.input.topic, 2000);
                        }

                        eventbus.Subscribe((Messages) =>
                        {
                            #region 输出日志

                            foreach (var message in Messages)
                            {
                                logger.LogDebug($"ACK: queue {message.QueueName} routeKey={message.RouteKey} MessageId:{message.MessageId}");
                            }

                            #endregion 输出日志

                        }, async (obj) =>
                        {
                            //是否重新入队
                            return true;
                        });
                    });

                });

            });

          //  app.UseMvc();


        }
    }
}