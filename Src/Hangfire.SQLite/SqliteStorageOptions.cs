namespace Hangfire.Sqlite
{
    using System;

    /// <summary>
    /// Configuration options for a SQLite storage.
    /// </summary>
    public sealed class SqliteStorageOptions
    {
        /// <summary>
        /// Gets or sets a value indicating whether the schema should be initialized along Hangfire process initialization.
        /// <c>true</c> by default.
        /// </summary>
        public bool PrepareSchemaIfNecessary { get; set; } = true;

        /// <summary>
        /// Gets or sets the maximum allowed time to execute a command.
        /// </summary>
        public TimeSpan? CommandTimeout { get; set; }
    }
}
