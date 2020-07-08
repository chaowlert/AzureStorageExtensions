using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Cosmos.Table.Queryable;

namespace AzureStorageExtensions
{
    public class CloudTable<T> where T : class, ITableEntity, new()
    {
        public CloudTable(CloudTable cloudTable)
        {
            CloudTableContext = cloudTable;
        }

        public CloudTable CloudTableContext { get; }

        public void Insert(T entity)
        {
            var op = TableOperation.Insert(entity);
            CloudTableContext.Execute(op);
        }

        public Task InsertAsync(T entity)
        {
            var op = TableOperation.Insert(entity);
            return CloudTableContext.ExecuteAsync(op);
        }

        public void InsertOrReplace(T entity)
        {
            var op = TableOperation.InsertOrReplace(entity);
            CloudTableContext.Execute(op);
        }

        public Task InsertOrReplaceAsync(T entity)
        {
            var op = TableOperation.InsertOrReplace(entity);
            return CloudTableContext.ExecuteAsync(op);
        }

        public void ExecuteBatch(TableBatchOperation op)
        {
            if (op.Count == 0)
                return;
            if (op.Count <= 100)
            {
                CloudTableContext.ExecuteBatch(op);
                return;
            }
            foreach (var chunk in op.Chunk(100))
            {
                var chunkOp = new TableBatchOperation();
                foreach (var item in chunk)
                {
                    chunkOp.Add(item);
                }

                CloudTableContext.ExecuteBatch(chunkOp);
            }
        }
        public async Task ExecuteBatchAsync(TableBatchOperation op)
        {
            if (op.Count == 0)
                return;
            if (op.Count <= 100)
            {
                await CloudTableContext.ExecuteBatchAsync(op);
                return;
            }
            foreach (var chunk in op.Chunk(100))
            {
                var chunkOp = new TableBatchOperation();
                foreach (var item in chunk)
                {
                    chunkOp.Add(item);
                }

                await CloudTableContext.ExecuteBatchAsync(chunkOp);
            }
        }

        internal void Bulk(Func<TableBatchOperation, Action<ITableEntity>> func, IEnumerable<ITableEntity> entities, bool checkConcurrency)
        {
            var op = new TableBatchOperation();
            var action = func(op);
            foreach (var item in entities)
            {
                if (!checkConcurrency)
                    item.ETag = "*";
                action(item);
            }
            this.ExecuteBatch(op);
        }

        internal Task BulkAsync(Func<TableBatchOperation, Action<ITableEntity>> func, IEnumerable<ITableEntity> entities, bool checkConcurrency)
        {
            var op = new TableBatchOperation();
            var action = func(op);
            foreach (var item in entities)
            {
                if (!checkConcurrency)
                    item.ETag = "*";
                action(item);
            }
            return this.ExecuteBatchAsync(op);
        }

        public void BulkInsert(IEnumerable<T> entities)
        {
            Bulk(op => op.Insert, entities, true);
        }

        public Task BulkInsertAsync(IEnumerable<T> entities)
        {
            return BulkAsync(op => op.Insert, entities, true);
        }

        public void BulkInsertOrReplace(IEnumerable<T> entities)
        {
            Bulk(op => op.InsertOrReplace, entities, true);
        }

        public Task BulkInsertOrReplaceAsync(IEnumerable<T> entities)
        {
            return BulkAsync(op => op.InsertOrReplace, entities, true);
        }

        public TableQuery<T> Query()
        {
            return CloudTableContext.CreateQuery<T>();
        }

        public TableQuery<T> Query(string partitionKey, IEnumerable<string> rowKeys)
        {
            var p = Expression.Parameter(typeof(T), "p");
            var predicate = rowKeys.Select(
                    item => Expression.Equal(Expression.Property(p, "RowKey"), Expression.Constant(item)))
                .AggregateBalance(Expression.OrElse);

            var linq = (from item in Query()
                where item.PartitionKey == partitionKey
                select item);
            var lambda = Expression.Lambda<Func<T, bool>>(predicate, p);
            return linq.Where(lambda).AsTableQuery();
        }

        public T Retrieve(string partitionKey, string rowKey)
        {
            var op = TableOperation.Retrieve<T>(partitionKey, rowKey);
            var result = CloudTableContext.Execute(op);
            return (T) result.Result;
        }

        public async Task<T> RetrieveAsync(string partitionKey, string rowKey)
        {
            var op = TableOperation.Retrieve<T>(partitionKey, rowKey);
            var result = await CloudTableContext.ExecuteAsync(op);
            return (T) result.Result;
        }

        public async Task<U> RetryAsync<U>(Func<CloudTable<T>, Task<U>> func, IRetryPolicy policy = null)
        {
            policy ??= CloudTableContext.ServiceClient.DefaultRequestOptions.RetryPolicy;
            var retry = 0;
            while (true)
            {
                try
                {
                    return await func(this);
                }
                catch (StorageException e)
                {
                    if (e.RequestInformation.HttpStatusCode != (int) HttpStatusCode.PreconditionFailed &&
                        e.RequestInformation.HttpStatusCode != (int) HttpStatusCode.Conflict)
                        throw;
                    if (!policy.ShouldRetry(retry++, 0, e, out TimeSpan delay, null))
                        throw;
                    await Task.Delay(delay);
                }
            }
        }

        public void Delete(T entity, bool checkConcurrency = false)
        {
            if (!checkConcurrency)
                entity.ETag = "*";
            var op = TableOperation.Delete(entity);
            CloudTableContext.Execute(op);
        }

        public Task DeleteAsync(T entity, bool checkConcurrency = false)
        {
            if (!checkConcurrency)
                entity.ETag = "*";
            var op = TableOperation.Delete(entity);
            return CloudTableContext.ExecuteAsync(op);
        }

        public void Delete(string partitionKey, string rowKey)
        {
            var entity = new T
            {
                PartitionKey = partitionKey,
                RowKey = rowKey,
                ETag = "*",
            };
            var op = TableOperation.Delete(entity);
            CloudTableContext.Execute(op);
        }

        public Task DeleteAsync(string partitionKey, string rowKey)
        {
            var entity = new T
            {
                PartitionKey = partitionKey,
                RowKey = rowKey,
                ETag = "*",
            };
            var op = TableOperation.Delete(entity);
            return CloudTableContext.ExecuteAsync(op);
        }

        public void BulkDelete(IEnumerable<T> entities, bool checkConcurrency = false)
        {
            Bulk(op => op.Delete, entities, checkConcurrency);
        }

        public Task BulkDeleteAsync(IEnumerable<T> entities, bool checkConcurrency = false)
        {
            return BulkAsync(op => op.Delete, entities, checkConcurrency);
        }

        public void DeleteIfExists(string partitionKey, string rowKey)
        {
            try
            {
                this.Delete(partitionKey, rowKey);
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode != 404)
                    throw;
            }
        }

        public async Task DeleteIfExistsAsync(string partitionKey, string rowKey)
        {
            try
            {
                await this.DeleteAsync(partitionKey, rowKey);
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode != 404)
                    throw;
            }
        }

        public void DeleteIfExists(T entity, bool checkConcurrency = false)
        {
            try
            {
                this.Delete(entity, checkConcurrency);
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode != 404)
                    throw;
            }
        }

        public async Task DeleteIfExistsAsync(T entity, bool checkConcurrency = false)
        {
            try
            {
                await this.DeleteAsync(entity, checkConcurrency);
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode != 404)
                    throw;
            }
        }

        public void BulkDeleteIfExists(IEnumerable<T> entities, bool checkConcurrency = false)
        {
            foreach (var chunk in entities.Chunk(100))
            {
                this.bulkDeleteIfExists(chunk, checkConcurrency);
            }
        }

        public async Task BulkDeleteIfExistsAsync(IEnumerable<T> entities, bool checkConcurrency = false)
        {
            foreach (var chunk in entities.Chunk(100))
            {
                await this.bulkDeleteIfExistsAsync(chunk, checkConcurrency);
            }
        }

        void bulkDeleteIfExists(IEnumerable<T> entities, bool checkConcurrency = false)
        {
            try
            {
                this.BulkDelete(entities, checkConcurrency);
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode != 404)
                    throw;
                var list = entities as IList<T> ?? entities.ToList();
                var rowKeys = list.Select(it => it.RowKey);
                var available = this.Query(list[0].PartitionKey, rowKeys).Select(it => it.RowKey).ToHashSet();
                if (available.Count >= list.Count)
                    throw;
                var newList = list.Where(it => available.Contains(it.RowKey));
                bulkDeleteIfExists(newList, checkConcurrency);
            }
        }

        async Task bulkDeleteIfExistsAsync(IEnumerable<T> entities, bool checkConcurrency = false)
        {
            try
            {
                await this.BulkDeleteAsync(entities, checkConcurrency);
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode != 404)
                    throw;
                var list = entities as IList<T> ?? entities.ToList();
                var rowKeys = list.Select(it => it.RowKey);
                var tmp = this.Query(list[0].PartitionKey, rowKeys).Select(it => it.RowKey).AsTableQuery().AsAsyncTableQuery();
                var available = new HashSet<string>();
                await foreach (var item in tmp)
                {
                    available.Add(item);
                }
                if (available.Count >= list.Count)
                    throw;
                var newList = list.Where(it => available.Contains(it.RowKey));
                await bulkDeleteIfExistsAsync(newList, checkConcurrency);
            }
        }

        public void Replace(T entity)
        {
            entity.ETag = "*";
            var op = TableOperation.Replace(entity);
            CloudTableContext.Execute(op);
        }

        public Task ReplaceAsync(T entity)
        {
            entity.ETag = "*";
            var op = TableOperation.Replace(entity);
            return CloudTableContext.ExecuteAsync(op);
        }

        public void BulkReplace(IEnumerable<T> entities)
        {
            Bulk(op => op.Replace, entities, false);
        }

        public Task BulkReplaceAsync(IEnumerable<T> entities)
        {
            return BulkAsync(op => op.Replace, entities, false);
        }

        public void Update(T entity)
        {
            var op = TableOperation.Replace(entity);
            CloudTableContext.Execute(op);
        }

        public Task UpdateAsync(T entity)
        {
            var op = TableOperation.Replace(entity);
            return CloudTableContext.ExecuteAsync(op);
        }

        public void BulkUpdate(IEnumerable<T> entities)
        {
            Bulk(op => op.Replace, entities, true);
        }

        public Task BulkUpdateAsync(IEnumerable<T> entities)
        {
            return BulkAsync(op => op.Replace, entities, true);
        }

        private static ITableEntity Expand(ITableEntity entity)
        {
            if (entity is ExpandableTableEntity)
            {
                var properties = entity.WriteEntity(null);
                ExpandableTableEntity.ExpandDictionary(properties, true);
                return new DynamicTableEntity(entity.PartitionKey, entity.RowKey, entity.ETag, properties);
            }
            else
                return entity;
        }

        public void Merge(ITableEntity entity, bool checkConcurrency = false)
        {
            if (!checkConcurrency)
                entity.ETag = "*";
            var op = TableOperation.Merge(Expand(entity));
            CloudTableContext.Execute(op);
        }

        public Task MergeAsync(ITableEntity entity, bool checkConcurrency = false)
        {
            if (!checkConcurrency)
                entity.ETag = "*";
            var op = TableOperation.Merge(Expand(entity));
            return CloudTableContext.ExecuteAsync(op);
        }

        public void BulkMerge(IEnumerable<ITableEntity> entities, bool checkConcurrency = false)
        {
            Bulk(op => op.Merge, entities.Select(Expand), checkConcurrency);
        }

        public Task BulkMergeAsync(IEnumerable<ITableEntity> entities, bool checkConcurrency = false)
        {
            return BulkAsync(op => op.Merge, entities.Select(Expand), checkConcurrency);
        }
        
        public void InsertOrMerge(ITableEntity entity)
        {
            var op = TableOperation.InsertOrMerge(Expand(entity));
            CloudTableContext.Execute(op);
        }

        public Task InsertOrMergeAsync(ITableEntity entity)
        {
            var op = TableOperation.InsertOrMerge(Expand(entity));
            return CloudTableContext.ExecuteAsync(op);
        }
        
        public void BulkInsertOrMerge(IEnumerable<ITableEntity> entities)
        {
            Bulk(op => op.InsertOrMerge, entities.Select(Expand), true);
        }

        public Task BulkInsertOrMergeAsync(IEnumerable<ITableEntity> entities)
        {
            return BulkAsync(op => op.InsertOrMerge, entities.Select(Expand), true);
        }

    }
}