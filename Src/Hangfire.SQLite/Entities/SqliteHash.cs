namespace Hangfire.Sqlite.Entities
{
    using System;

    internal sealed class SqliteHash
    {
        public string Key { get; set; }

        public string Field { get; set; }

        public string Value { get; set; }

        public DateTime? ExpireAt { get; set; }
    }
}
