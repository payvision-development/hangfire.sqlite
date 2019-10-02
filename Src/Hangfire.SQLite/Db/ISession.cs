namespace Hangfire.Sqlite.Db
{
    using System;
    using System.Collections.Generic;
    using System.Data;

    /// <summary>
    /// Represents a session to a database in order to execute SQL queries.
    /// </summary>
    internal interface ISession
    {
        /// <summary>
        /// Executes the parameterized query.
        /// </summary>
        /// <typeparam name="T">Type of the expected result.</typeparam>
        /// <param name="query">The query to be executed.</param>
        /// <param name="param">The parameter values.</param>
        /// <returns>The query results.</returns>
        IEnumerable<T> Query<T>(string query, object param);

        /// <summary>
        /// Executes the specified query that returns multiple results.
        /// </summary>
        /// <param name="query">The query to be executed.</param>
        /// <param name="param">The query parameter values.</param>
        /// <returns>The multiple result reader.</returns>
        IGridReader QueryMultiple(string query, object param);

        /// <summary>
        /// Executes the parameterized query that returns only one value.
        /// </summary>
        /// <typeparam name="T">Type of the expected result.</typeparam>
        /// <param name="query">The query to be executed.</param>
        /// <param name="param">The parameter values.</param>
        /// <returns>The value result.</returns>
        T ExecuteScalar<T>(string query, object param);

        /// <summary>
        /// Execute parameterized SQL.
        /// </summary>
        /// <param name="query">The parameterized SQL query to be executed.</param>
        /// <param name="param">The parameters.</param>
        /// <returns>The number of affected rows.</returns>
        int Execute(string query, object param);

        /// <summary>
        /// Begins a new transaction on the current storage.
        /// </summary>
        /// <param name="isolationLevel">The isolation level of the transaction.</param>
        /// <returns>The transaction session.</returns>
        ITransaction BeginTransaction(IsolationLevel? isolationLevel);

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
