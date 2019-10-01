namespace Hangfire.SQLite.Sample
{
    using System;
    using System.Threading.Tasks;

    using Hangfire.Sqlite;

    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.AspNetCore.Http;

    class Program
    {
        static async Task Main(string[] args)
        {
            using IWebHost host = CreateWebHost();
            await Console.Out.WriteLineAsync("Kestrel starting...");
            await host.StartAsync();
            await Console.Out.WriteLineAsync("Kestrel started. Press ENTER to stop.");

            BackgroundJob.Enqueue(() => HelloWorld());

            await Console.In.ReadLineAsync();
            await Console.Out.WriteLineAsync("Stopping Kestrel...");
        }

        public static async Task HelloWorld()
        {
            Console.BackgroundColor = ConsoleColor.DarkYellow;
            try
            {
                await Console.Out.WriteLineAsync("Hello SQLite!");
            }
            finally
            {
                Console.ResetColor();
            }
        }

        private static IWebHost CreateWebHost() =>
            new WebHostBuilder().UseKestrel()
                .ConfigureServices(
                    services =>
                    {
                        services.AddHangfire(
                            configuration =>
                            {
                                configuration.UseSimpleAssemblyNameTypeSerializer()
                                    .UseColouredConsoleLogProvider()
                                    .UseRecommendedSerializerSettings()
                                    .UseSqLiteStorage(
                                        "Data Source=sample.sqlite;",
                                        new SqliteStorageOptions { QueuePollInterval = TimeSpan.FromSeconds(1) });
                            });
                        services.AddHangfireServer();
                    })
                .Configure(
                    app =>
                    {
                        app.UseHangfireDashboard();
                        app.Run(async context => await context.Response.WriteAsync("Hello SQLite."));
                    })
                .Build();
    }
}