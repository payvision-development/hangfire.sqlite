namespace Hangfire.Sqlite.Entities
{
    using System;

    internal sealed class SqliteState
    {
        public long JobId { get; set; }

        public string Name { get; set; }

        public string Reason { get; set; }

        public DateTime CreatedAt { get; set; }

        public string Data { get; set; }
    }
}