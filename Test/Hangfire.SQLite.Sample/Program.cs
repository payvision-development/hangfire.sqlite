namespace Hangfire.SQLite.Sample
{
    using System;
    using System.Threading.Tasks;

    using Hangfire.Sqlite;

    class Program
    {
        static async Task Main(string[] args)
        {
            GlobalConfiguration.Configuration.UseSimpleAssemblyNameTypeSerializer()
                .UseColouredConsoleLogProvider()
                .UseRecommendedSerializerSettings()
                .UseSqLiteStorage("Data Source=sample.sqlite;");

            BackgroundJob.Enqueue(() => Console.Out.WriteLineAsync("Hello world!"));

            using (new BackgroundJobServer())
            {
                await Console.In.ReadLineAsync();
            }
        }
    }
}
