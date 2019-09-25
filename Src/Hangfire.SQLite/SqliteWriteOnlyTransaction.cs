namespace Hangfire.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;

    using Hangfire.Common;
    using Hangfire.Sqlite.Db;
    using Hangfire.Sqlite.Queues;
    using Hangfire.States;
    using Hangfire.Storage;

    /// <summary>
    /// <see cref="JobStorageTransaction"/> implementation using a <see cref="IJobStorage"/>.
    /// </summary>
    internal sealed class SqliteWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly Queue<Action<ISession>> commandQueue = new Queue<Action<ISession>>();

        private readonly Queue<Action> afterCommitCommandQueue = new Queue<Action>();

        private readonly IJobStorage storage;

        public SqliteWriteOnlyTransaction(IJobStorage storage) =>
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));

        /// <inheritdoc />
        public override void Commit()
        {
            using (ITransaction transaction = this.storage.BeginTransaction())
            {
                foreach (Action<ISession> command in this.commandQueue)
                {
                    command(transaction);
                }

                transaction.Commit();
            }

            foreach (Action command in this.afterCommitCommandQueue)
            {
                command();
            }
        }

        /// <inheritdoc />
        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void PersistJob(string jobId)
        {
            const string RenovateJobSql = "UPDATE [Job] SET ExpireAt=NULL WHERE Id=@id";
            this.EnqueueCommand(x => x.Execute(RenovateJobSql, new { id = long.Parse(jobId) }));
        }

        /// <inheritdoc />
        public override void SetJobState(string jobId, IState state)
        {
            const string AddAndSetStateSql = "INSERT INTO State (JobId, Name, Reason, CreatedAt, Data) " +
                                             "VALUES (@jobId, @name, @reason, @createdAt, @data);" +
                                             "UPDATE Job SET StateId=last_insert_rowid(), StateName=@name WHERE ID=@jobId;";
            long id = long.Parse(jobId, CultureInfo.InvariantCulture);
            this.EnqueueCommand(
                repository => repository.Execute(
                    AddAndSetStateSql,
                    new
                    {
                        jobId = id,
                        name = state.Name,
                        reason = state.Reason,
                        createdAt = DateTime.UtcNow,
                        data = SerializationHelper.Serialize(state.SerializeData())
                    }));
        }

        /// <inheritdoc />
        public override void AddJobState(string jobId, IState state)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void AddToQueue(string queue, string jobId)
        {
            IPersistentJobQueue jobQueue = this.storage.QueueProviders[queue].JobQueue;
            this.EnqueueCommand(x => jobQueue.Enqueue(x, queue, jobId));
            if (jobQueue is SqliteJobQueue)
            {
                this.afterCommitCommandQueue.Enqueue(() => SqliteJobQueue.NewItemInQueueEvent.Set());
            }
        }

        /// <inheritdoc />
        public override void IncrementCounter(string key)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void DecrementCounter(string key)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void AddToSet(string key, string value)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void AddToSet(string key, string value, double score)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void RemoveFromSet(string key, string value)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void InsertToList(string key, string value)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void RemoveFromList(string key, string value)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void RemoveHash(string key)
        {
            throw new NotImplementedException();
        }

        private void EnqueueCommand(Action<ISession> command) => this.commandQueue.Enqueue(command);
    }
}
