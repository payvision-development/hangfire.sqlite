namespace Hangfire.SQLite.Sample
{
    using System;

    using Hangfire.Sqlite;

    class Program
    {
        static void Main(string[] args)
        {
            GlobalConfiguration.Configuration.UseSimpleAssemblyNameTypeSerializer()
                .UseColouredConsoleLogProvider()
                .UseRecommendedSerializerSettings()
                .UseSqLiteStorage("Data Source=sample.sqlite;");

            BackgroundJob.Enqueue(() => Console.WriteLine("Hello world!"));
        }
    }
}
