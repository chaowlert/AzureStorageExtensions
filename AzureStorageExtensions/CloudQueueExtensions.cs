using AzureStorageExtensions;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

public static class CloudQueueExtensions
{
    private static readonly ConcurrentDictionary<string, bool> knownQueues = new ConcurrentDictionary<string, bool>();
    public static CloudQueue GetQueue(this BaseCloudContext context, string name)
    {
        var queue = context.CloudClient.Queue.GetQueueReference(name);
        if (knownQueues.TryAdd(name, true))
            queue.CreateIfNotExists();
        return queue;
    }

    public static IEnumerable<CloudQueueMessage> FetchAllMessages(this CloudQueue queue)
    {
        var hasNext = true;

        while (hasNext)
        {
            hasNext = false;
            var messages = queue.GetMessages(32, TimeSpan.FromMinutes(5));
            foreach (var message in messages)
            {
                hasNext = true;
                yield return message;
            }
        }
    }

    public static void DeleteMessages(this CloudQueue queue, IEnumerable<CloudQueueMessage> messages)
    {
        foreach (var message in messages)
        {
            queue.DeleteMessage(message);
        }
    }
}
