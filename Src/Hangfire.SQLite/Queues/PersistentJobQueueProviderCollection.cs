// -----------------------------------------------------------------------
// <copyright file="PersistentJobQueueProviderCollection.cs" company="Payvision">
//     Payvision Copyright © 2019
// </copyright>
// -----------------------------------------------------------------------

namespace Hangfire.Sqlite.Queues
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// Contains job queue provider items.
    /// </summary>
    internal sealed class PersistentJobQueueProviderCollection : IEnumerable<IPersistentJobQueueProvider>
    {
        private readonly List<IPersistentJobQueueProvider> providers = new List<IPersistentJobQueueProvider>();

        private readonly Dictionary<string, IPersistentJobQueueProvider> providersByQueue =
            new Dictionary<string, IPersistentJobQueueProvider>(StringComparer.OrdinalIgnoreCase);

        public PersistentJobQueueProviderCollection(IPersistentJobQueueProvider defaultProvider)
        {
            if (defaultProvider == null)
            {
                throw new ArgumentNullException(nameof(defaultProvider));
            }

            this.providers.Add(defaultProvider);
        }

        public IPersistentJobQueueProvider this[string queue]
        {
            get =>
                this.providersByQueue.TryGetValue(queue, out IPersistentJobQueueProvider provider)
                    ? provider
                    : this.providers[0];
            set
            {
                if (queue == null)
                {
                    throw new ArgumentNullException(nameof(queue));
                }

                if (value == null)
                {
                    throw new ArgumentNullException(nameof(value));
                }

                this.providers.Add(this.providersByQueue[queue] = value);
            }
        }

        /// <summary>
        /// Adds the specified job queue provider related to all the given queue names.
        /// </summary>
        /// <param name="provider">The persistent job queue provider.</param>
        /// <param name="queues">The queues to be related.</param>
        public void Add(IPersistentJobQueueProvider provider, IEnumerable<string> queues)
        {
            if (provider == null)
            {
                throw new ArgumentNullException(nameof(provider));
            }

            if (queues == null)
            {
                throw new ArgumentNullException(nameof(queues));
            }

            foreach (string queue in queues)
            {
                this[queue] = provider;
            }
        }

        /// <inheritdoc />
        public IEnumerator<IPersistentJobQueueProvider> GetEnumerator() => this.providers.GetEnumerator();

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
    }
}