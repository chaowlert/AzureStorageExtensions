using System;
using System.Collections.Generic;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace Microsoft.Azure.Storage.Queue
{
    public static class CloudQueueExtensions
    {
        public static IEnumerable<CloudQueueMessage> FetchAllMessages(this CloudQueue queue, TimeSpan visibilityTimeout)
        {
            var hasNext = true;
            while (hasNext)
            {
                hasNext = false;
                var messages = queue.GetMessages(32, visibilityTimeout);
                foreach (var message in messages)
                {
                    hasNext = true;
                    yield return message;
                }
            }
        }
        public static async IAsyncEnumerable<CloudQueueMessage> FetchAllMessagesAsync(this CloudQueue queue, TimeSpan visibilityTimeout)
        {
            var hasNext = true;
            while (hasNext)
            {
                hasNext = false;
                var messages = await queue.GetMessagesAsync(32, visibilityTimeout, null, null);
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
        public static async Task DeleteMessagesAsync(this CloudQueue queue, IEnumerable<CloudQueueMessage> messages)
        {
            foreach (var message in messages)
            {
                await queue.DeleteMessageAsync(message);
            }
        }
    }
}