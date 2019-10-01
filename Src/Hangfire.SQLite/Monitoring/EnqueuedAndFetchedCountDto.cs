namespace Hangfire.Sqlite.Monitoring
{
    internal sealed class EnqueuedAndFetchedCountDto
    {
        public int? EnqueuedCount { get; set; }

        public int? FetchedCount { get; set; }
    }
}
