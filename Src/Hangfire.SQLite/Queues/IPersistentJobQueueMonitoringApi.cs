namespace Hangfire.Sqlite.Queues
{
    using System.Collections.Generic;

    using Hangfire.Sqlite.Monitoring;

    internal interface IPersistentJobQueueMonitoringApi
    {
        IEnumerable<string> GetQueues();

        IEnumerable<long> GetEnqueuedJobIds(string queue, int from, int pageSize);

        IEnumerable<long> GetFetchedJobIds(string queue, int from, int pageSize);

        EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue);
    }
}
