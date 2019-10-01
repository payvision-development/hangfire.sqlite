namespace Hangfire.Sqlite
{
    using System;

    /// <summary>
    /// Configuration options for a SQLite storage.
    /// </summary>
    public sealed class SqliteStorageOptions
    {
        private TimeSpan queuePollInterval = TimeSpan.FromSeconds(15);

        /// <summary>
        /// Gets or sets a value indicating whether the schema should be initialized along Hangfire process initialization.
        /// <c>true</c> by default.
        /// </summary>
        public bool PrepareSchemaIfNecessary { get; set; } = true;

        /// <summary>
        /// Gets or sets the maximum allowed time to execute a command.
        /// </summary>
        public TimeSpan? CommandTimeout { get; set; }

        /// <summary>
        /// Gets or sets the time interval to execute a polling to check if new jobs are ready.
        /// By default is set to 15 seconds.
        /// </summary>
        public TimeSpan QueuePollInterval
        {
            get => this.queuePollInterval;

            set
            {
                if (value != value.Duration())
                {
                    throw new ArgumentException(
                        $"The {nameof(QueuePollInterval)} property value should be positive. Given: {value}.");
                }

                this.queuePollInterval = value;
            }
        }

        /// <summary>
        /// Gets or sets the limits of items displayed on monitoring dashboard per page.
        /// </summary>
        public int? DashboardJobListLimit { get; set; }
    }
}
