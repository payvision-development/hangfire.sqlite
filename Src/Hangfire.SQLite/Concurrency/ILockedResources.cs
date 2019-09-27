namespace Hangfire.Sqlite.Concurrency
{
    using System;

    /// <summary>
    /// Handles the multi-process locking of some resources.
    /// </summary>
    internal interface ILockedResources : IDisposable
    {
        /// <summary>
        /// Locks the specified resource waiting until such resource is released in case of any process else
        /// would have an active lock over the same resource.
        /// </summary>
        /// <param name="resource">The resource to be locked.</param>
        /// <param name="timeout">The maximum waiting time allowed in order to get the lock.</param>
        /// <returns>The lock instance release when called <see cref="IDisposable.Dispose()"/>.</returns>
        /// <exception cref="TimeoutException">If the lock is not available before than the specified timeout.</exception>
        IDisposable Lock(string resource, TimeSpan timeout);
    }
}
