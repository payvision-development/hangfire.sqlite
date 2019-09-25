namespace Hangfire.Sqlite
{
    using System;
    using System.Data.SQLite;

    using Hangfire.Annotations;

    /// <summary>
    /// Extension methods to configure Hangfire in order to use SQLite as storage.
    /// </summary>
    public static class SqliteStorageExtensions
    {
        /// <summary>
        /// Configures Hangfire to use SQLite as storage with default options.
        /// </summary>
        /// <param name="configuration">The Hangfire configuration.</param>
        /// <param name="connectionString">The connection string used to connect SQLite instance.</param>
        /// <returns>The Hangfire configuration.</returns>
        public static IGlobalConfiguration<SqliteStorage> UseSqLiteStorage(
            [NotNull] this IGlobalConfiguration configuration,
            string connectionString) =>
            configuration.UseSqLiteStorage(connectionString, new SqliteStorageOptions());

        /// <summary>
        /// Configures Hangfire to use SQLite as storage.
        /// </summary>
        /// <param name="configuration">The Hangfire configuration.</param>
        /// <param name="connectionString">The connection string used to connect SQLite instance.</param>
        /// <param name="options">The storage options</param>
        /// <returns>The Hangfire configuration.</returns>
        public static IGlobalConfiguration<SqliteStorage> UseSqLiteStorage(
            [NotNull] this IGlobalConfiguration configuration,
            string connectionString,
            SqliteStorageOptions options)
        {
            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            return configuration.UseSqLiteStorage(new SQLiteConnectionStringBuilder(connectionString), options);
        }

        /// <summary>
        /// Configures Hangfire to use SQLite as storage with default options.
        /// </summary>
        /// <param name="configuration">The Hangfire configuration.</param>
        /// <param name="connectionString">The connection string used to connect SQLite instance.</param>
        /// <returns>The Hangfire configuration.</returns>
        public static IGlobalConfiguration<SqliteStorage> UseSqLiteStorage(
            [NotNull] this IGlobalConfiguration configuration,
            SQLiteConnectionStringBuilder connectionString) =>
            configuration.UseSqLiteStorage(connectionString, new SqliteStorageOptions());

        /// <summary>
        /// Configures Hangfire to use SQLite as storage.
        /// </summary>
        /// <param name="configuration">The Hangfire configuration.</param>
        /// <param name="connectionString">The connection string used to connect SQLite instance.</param>
        /// <param name="options">The storage options</param>
        /// <returns>The Hangfire configuration.</returns>
        public static IGlobalConfiguration<SqliteStorage> UseSqLiteStorage(
            [NotNull] this IGlobalConfiguration configuration,
            SQLiteConnectionStringBuilder connectionString,
            SqliteStorageOptions options)
        {
            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }

            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            return configuration.UseStorage(
                new SqliteStorage(connectionString, options));
        }
    }
}
