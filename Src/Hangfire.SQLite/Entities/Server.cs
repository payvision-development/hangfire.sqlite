using System;

namespace Hangfire.Sqlite.Entities
{
    internal sealed class Server
    {
        public string Id { get; set; }

        public string Data { get; set; }

        public DateTime LastHeartbeat { get; set; }
    }
}