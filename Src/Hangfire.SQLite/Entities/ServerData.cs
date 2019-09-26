namespace Hangfire.Sqlite.Entities
{
    using System;

    /// <summary>
    /// Contains the data of a registered server.
    /// </summary>
    internal sealed class ServerData
    {
        /// <summary>
        /// Gets or sets the number of workers running on the server.
        /// </summary>
        public int WorkerCount { get; set; }

        /// <summary>
        /// Gets or sets the queues that the server is handling.
        /// </summary>
        public string[] Queues { get; set; }

        /// <summary>
        /// Gets or sets the last time when the server has been started.
        /// </summary>
        public DateTimeOffset? StartedAt { get; set; }
    }
}
