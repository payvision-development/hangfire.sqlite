namespace Hangfire.Sqlite.Concurrency
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// <see cref="ILockedResources"/> extension methods.
    /// </summary>
    internal static class LockedResourcesExtensions
    {
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(15);

        /// <summary>
        /// Locks the specified resources waiting until all resources are released in case of any process else
        /// would have an active lock over some of such resources.
        /// </summary>
        /// <remarks>In this case, 15 seconds timeout will be applied.</remarks>
        /// <param name="locker">The extended <see cref="ILockedResources"/>.</param>
        /// <param name="resources">The resources to be locked.</param>
        /// <returns>The lock instance release when called <see cref="IDisposable.Dispose()"/>.</returns>
        /// <exception cref="TimeoutException">If the lock is not available before than the specified timeout.</exception>
        public static IDisposable Lock(this ILockedResources locker, IEnumerable<string> resources) =>
            locker.Lock(resources, DefaultTimeout);

        /// <summary>
        /// Locks the specified resources waiting until all resources are released in case of any process else
        /// would have an active lock over some of such resources.
        /// </summary>
        /// <param name="locker">The extended <see cref="ILockedResources"/>.</param>
        /// <param name="resources">The resources to be locked.</param>
        /// <param name="timeout">The maximum waiting time allowed in order to get the lock.</param>
        /// <returns>The lock instance release when called <see cref="IDisposable.Dispose()"/>.</returns>
        /// <exception cref="TimeoutException">If the lock is not available before than the specified timeout.</exception>
        public static IDisposable Lock(this ILockedResources locker, IEnumerable<string> resources, TimeSpan timeout) =>
            new LockBundle(resources.Select(x => locker.Lock(x, timeout)));

        private sealed class LockBundle : IDisposable
        {
            private readonly IDisposable[] lockHandlers;

            public LockBundle(IEnumerable<IDisposable> lockHandlers) => this.lockHandlers = lockHandlers.ToArray();

            /// <inheritdoc />
            public void Dispose()
            {
                foreach (IDisposable lockHandler in this.lockHandlers)
                {
                    lockHandler.Dispose();
                }
            }
        }
    }
}
