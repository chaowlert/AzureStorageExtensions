using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table.Queryable;

// ReSharper disable once CheckNamespace
namespace Microsoft.Azure.Cosmos.Table
{
    public static class TableQueryExtensions
    {
        public static U AggregateSegment<T, U>(this TableQuery<T> query, U seed, Func<IEnumerable<T>, U> selector,
            Func<U, U, U> aggregator)
        {
            TableContinuationToken token = null;
            do
            {
                var segment = query.ExecuteSegmented(token);
                if (segment.Results.Count > 0)
                {
                    var newList = selector(segment.Results);
                    seed = aggregator(seed, newList);
                }

                token = segment.ContinuationToken;
            } while (token != null);

            return seed;
        }

        public static async Task<U> AggregateSegmentAsync<T, U>(this TableQuery<T> query, U seed,
            Func<IEnumerable<T>, U> selector, Func<U, U, U> aggregator)
        {
            TableContinuationToken token = null;
            do
            {
                var segment = await query.ExecuteSegmentedAsync(token);
                if (segment.Results.Count > 0)
                {
                    var newList = selector(segment.Results);
                    seed = aggregator(seed, newList);
                }

                token = segment.ContinuationToken;
            } while (token != null);

            return seed;
        }

        public static void ForEachSegment<T>(this TableQuery<T> query, Action<IList<T>> action)
        {
            TableContinuationToken token = null;
            do
            {
                var segment = query.ExecuteSegmented(token);
                if (segment.Results.Count > 0)
                    action(segment.Results);
                token = segment.ContinuationToken;
            } while (token != null);
        }

        public static async Task ForEachSegmentAsync<T>(this TableQuery<T> query, Action<IList<T>> action)
        {
            TableContinuationToken token = null;
            do
            {
                var segment = await query.ExecuteSegmentedAsync(token);
                if (segment.Results.Count > 0)
                    action(segment.Results);
                token = segment.ContinuationToken;
            } while (token != null);
        }

        public static IEnumerable<U> MapSegment<T, U>(this TableQuery<T> query, Func<IList<T>, U> func)
        {
            TableContinuationToken token = null;
            do
            {
                var segment = query.ExecuteSegmented(token);
                if (segment.Results.Count > 0)
                    yield return func(segment.Results);
                token = segment.ContinuationToken;
            } while (token != null);

        }

        public static async IAsyncEnumerable<U> MapSegmentAsync<T, U>(this TableQuery<T> query, Func<IList<T>, U> func)
        {
            TableContinuationToken token = null;
            do
            {
                var segment = await query.ExecuteSegmentedAsync(token);
                if (segment.Results.Count > 0)
                    yield return func(segment.Results);
                token = segment.ContinuationToken;
            } while (token != null);
        }

        public static async IAsyncEnumerable<T> AsAsyncTableQuery<T>(this IQueryable<T> query)
        {
            var table = query.AsTableQuery();
            TableContinuationToken token = null;
            do
            {
                var segment = await table.ExecuteSegmentedAsync(token);
                foreach (var item in segment.Results)
                {
                    yield return item;
                }
                token = segment.ContinuationToken;
            } while (token != null);
        }
    }
}
