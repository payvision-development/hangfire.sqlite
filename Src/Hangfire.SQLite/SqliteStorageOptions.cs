namespace Hangfire.Sqlite
{
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
    }
}
