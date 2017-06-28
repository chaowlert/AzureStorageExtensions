using AzureStorageExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

static class Extensions
{
    static readonly Task ComplatedTask = Task.Delay(0);
    public static Func<T, CancellationToken, Task> ToTaskFunc<T>(this Action<T> action)
    {
        return (arg, token) =>
        {
            action(arg);
            return ComplatedTask;
        };
    }
    public static Func<T1, T2, CancellationToken, Task> ToTaskFunc<T1, T2>(this Action<T1, T2> action)
    {
        return (arg1, arg2, token) =>
        {
            action(arg1, arg2);
            return ComplatedTask;
        };
    }
    public static Func<T, CancellationToken, Task<TResult>> ToTaskFunc<T, TResult>(this Func<T, TResult> func)
    {
        return (arg, token) =>
        {
            var result = func(arg);
            return Task.FromResult(result);
        };
    }

    public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> source, int chunksize)
    {
        var list = source as IList<T> ?? source.ToList();
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

    public static bool ApplyTo<T>(this T src, T dest)
    {
        return Applier<T>.Apply(src, dest);
    }
}
//}
