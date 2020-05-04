using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace AzureStorageExtensions
{
    static class Applier<T>
    {
        public static readonly Func<T, T, bool> Apply = CreateApplyFunc();

        private static Func<T, T, bool> CreateApplyFunc()
        {
            var type = typeof(T);
            var src = Expression.Parameter(type, "src");
            var dest = Expression.Parameter(type, "dest");

            var list = new List<Expression>();
            var isUpdated = Expression.Variable(typeof(bool), "isUpdated");
            list.Add(Expression.Assign(isUpdated, Expression.Constant(false)));

            foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.GetProperty | BindingFlags.SetProperty))
            {
                switch (prop.Name)
                {
                    case "PartitionKey":
                    case "RowKey":
                    case "Timestamp":
                    case "ETag":
                        continue;
                }

                var left = Expression.Property(src, prop);
                var right = Expression.Property(dest, prop);
                var test = Expression.NotEqual(right, left);
                var assign = Expression.Assign(right, left);
                var updated = Expression.Assign(isUpdated, Expression.Constant(true));

                list.Add(Expression.IfThen(test, Expression.Block(assign, updated)));
            }

            list.Add(isUpdated);
            var body = Expression.Block(new[] { isUpdated }, list);
            return Expression.Lambda<Func<T, T, bool>>(body, src, dest).Compile();
        }
    }
}