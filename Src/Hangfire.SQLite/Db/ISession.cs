namespace Hangfire.Sqlite.Db
{
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
    }
}
