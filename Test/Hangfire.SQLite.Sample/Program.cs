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
                .UseSqLiteStorage(
                    "Data Source=sample.sqlite;",
                    new SqliteStorageOptions { QueuePollInterval = TimeSpan.FromSeconds(1) });

            BackgroundJob.Enqueue(() => HelloWorld());

            using (new BackgroundJobServer())
            {
                await Console.In.ReadLineAsync();
            }
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
    }
}
