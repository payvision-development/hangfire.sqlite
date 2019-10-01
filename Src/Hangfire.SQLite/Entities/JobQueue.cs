namespace Hangfire.Sqlite.Entities
{
    using System;

    internal sealed class JobQueue
    {
        public int Id { get; set; }

        public long JobId { get; set; }

        public string Queue { get; set; }

        public DateTime FetchedAt { get; set; }
    }
}
