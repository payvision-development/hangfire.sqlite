namespace Hangfire.Sqlite.Installer
{
    using System;
    using System.Data;
    using System.IO;
    using System.Reflection;

    using Dapper;

    using Hangfire.Logging;

    /// <summary>
    /// Initialization of a Hangfire storage for a SQLite database.
    /// </summary>
    internal static class SqliteObjectsInstaller
    {
        /// <summary>
        /// Install the SQLite objects using the specified connection.
        /// </summary>
        /// <param name="connectionFactory">The factory to create a connection where to deploy the database objects.</param>
        public static void Install(Func<IDbConnection> connectionFactory)
        {
            const int RetryAttempts = 3;

            if (connectionFactory == null)
            {
                throw new ArgumentNullException(nameof(connectionFactory));
            }

            ILog log = LogProvider.GetLogger(typeof(SqliteObjectsInstaller));
            log.Info("Start installing Hangfire SQLite objects...");

            string script = typeof(SqliteObjectsInstaller).GetTypeInfo()
                .Assembly.GetStringResource("Hangfire.Sqlite.Installer.Install.sql");

            Exception lastException = null;

            for (int i = 0; i < RetryAttempts; i++)
            {
                try
                {
                    using (IDbConnection connection = connectionFactory())
                    {
                        connection.Execute(script, commandTimeout: 0);
                    }
                }
                catch (Exception exception)
                {
                    lastException = exception;
                    string message = i < RetryAttempts - 1 ? "Retrying..." : string.Empty;
                    log.WarnException(
                        $"An exception occurred while trying to perform the migration.{message}",
                        exception);
                }
            }

            if (lastException != null)
            {
                log.WarnException(
                    "Was unable to perform the Hangfire schema migration due to an exception. Ignore this message unless you've just installed or upgraded Hangfire.",
                    lastException);
            }
            else
            {
                log.Info("Hangfire SQLite objects installed.");
            }
        }

        private static string GetStringResource(this Assembly assembly, string resourceName)
        {
            using (Stream stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null)
                {
                    throw new InvalidOperationException(
                        $"Requested resource `{resourceName}` was not found in the assembly `{assembly}`.");
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}