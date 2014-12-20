using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace AzureStorageExtensions
{
    static class Extensions
    {
        public static T AggregateBalance<T>(this IEnumerable<T> source, Func<T, T, T> func)
        {
            while (true)
            {
                var list = new List<T>();
                var previous = default(T);
                var pair = false;
                foreach (var item in source)
                {
                    if (pair)
                        list.Add(func(previous, item));
                    else
                        previous = item;
                    pair = !pair;
                }
                if (pair)
                    list.Add(previous);

                if (list.Count == 0)
                    throw new InvalidOperationException("No element");
                if (list.Count == 1)
                    return list[0];
                source = list;
            }
        }

        public static U GetValueOrDefault<T, U>(this IDictionary<T, U> dict, T key)
        {
            U value;
            return dict.TryGetValue(key, out value) ? value : default(U);
        }

        public static T[] Concat<T>(params T[][] list)
        {
            var result = new T[list.Sum(a => a.Length)];
            int offset = 0;
            foreach (T[] array in list)
            {
                array.CopyTo(result, offset);
                offset += array.Length;
            }
            return result;
        }

        public static bool ApplyTo<T>(this T src, T dest)
        {
            return Extensions<T>.Apply(src, dest);
        }
    }

    static class Extensions<T>
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
            var body = Expression.Block(new[] {isUpdated}, list);
            return Expression.Lambda<Func<T, T, bool>>(body, src, dest).Compile();
        }
    }
}
