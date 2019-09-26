namespace Hangfire.Sqlite.Queues
{
    using System;

    using Hangfire.Sqlite.Db;
    using Hangfire.Storage;

    /// <summary>
    /// Represents a fetched job that is connected to repository.
    /// </summary>
    internal sealed class SqliteTransactionJob : IFetchedJob
    {
        private readonly ISession session;

        private readonly object id;

        public SqliteTransactionJob(ISession session, string jobId, long id)
        {
            this.session = session ?? throw new ArgumentNullException(nameof(session));
            this.JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
            this.id = new { id };
        }

        /// <inheritdoc />
        public string JobId { get; }

        /// <inheritdoc />
        public void RemoveFromQueue()
        {
            const string DequeueJobSql = "DELETE FROM [JobQueue] WHERE Id=@id";

            this.session.Execute(DequeueJobSql, this.id);
        }

        /// <inheritdoc />
        public void Requeue()
        {
            const string RequestJobSql = "UPDATE [JobQueue] SET FetchedAt = NULL, LockedBy = NULL WHERE Id=@id";

            this.session.Execute(RequestJobSql, this.id);
        }

        /// <inheritdoc />
        public void Dispose()
        {
        }
    }
}
