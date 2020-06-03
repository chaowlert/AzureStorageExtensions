using System;
using System.Collections.Generic;
using System.Linq;

namespace AzureStorageExtensions
{
    static class Extensions
    {
        public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> source, int chunksize)
        {
            var list = source as ICollection<T> ?? source.ToList();
            var pos = 0;
            while (list.Count > pos)
            {
                yield return list.Skip(pos).Take(chunksize);
                pos += chunksize;
            }
        }

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
            return dict.TryGetValue(key, out U value) ? value : default(U);
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
    }
}
