using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace Problem
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var connectionString = File.ReadAllText(@"..\..\..\..\connection.txt");
            var queueName = "concurrent-data-structure-webinar";

            #region Completion with SMBP

            var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            var factory = await MessagingFactory.CreateAsync(namespaceManager.Address, new MessagingFactorySettings
            {
                TransportType = TransportType.NetMessaging,
                TokenProvider = namespaceManager.Settings.TokenProvider
            });
            var sender = await factory.CreateMessageSenderAsync(queueName);

            await sender.SendBatchAsync(Enumerable.Range(0, 2000)
                .Select(i => new BrokeredMessage(Encoding.UTF8.GetBytes(i.ToString())))
                .ToArray());

            var receiver1 = await factory.CreateMessageReceiverAsync(queueName);
            var receiver2 = await factory.CreateMessageReceiverAsync(queueName);

            var message1 = await receiver1.ReceiveAsync();
            try
            {
                Console.WriteLine("Completing on receiver 2");
                await receiver2.CompleteBatchAsync(new []{ message1.LockToken });
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            try
            {
                Console.WriteLine("Completing on receiver 1");
                await receiver1.CompleteBatchAsync(new []{ message1.LockToken });
            }
            catch (Exception)
            {
                // ignore
            }

            #endregion

            Console.ReadLine();

            var semaphore = new SemaphoreSlim(10);

            var locksTokensToComplete = new ConcurrentStack<Guid>();
            var cancellationTokenSource = new CancellationTokenSource();

            _ = Task.Run(async () =>
            {
                while (!cancellationTokenSource.IsCancellationRequested)
                {
                    var receivedMessages = await receiver1.ReceiveBatchAsync(20);
                    foreach (var receivedMessage in receivedMessages)
                    {
                        await semaphore.WaitAsync(cancellationTokenSource.Token);

                        _ = Process(receivedMessage);

                        async Task Process(BrokeredMessage message)
                        {
                            await Task.Yield();
                            locksTokensToComplete.Push(message.LockToken);
                            semaphore.Release();
                        }
                    }
                }
            }, CancellationToken.None);

            _ = Task.Run(async () =>
            {
                while (!cancellationTokenSource.IsCancellationRequested)
                {
                    var receivedMessages = await receiver2.ReceiveBatchAsync(20);
                    foreach (var receivedMessage in receivedMessages)
                    {
                        await semaphore.WaitAsync(cancellationTokenSource.Token);

                        _ = Process(receivedMessage);

                        async Task Process(BrokeredMessage message)
                        {
                            await Task.Yield();
                            locksTokensToComplete.Push(message.LockToken);
                            semaphore.Release();
                        }
                    }
                }
            }, CancellationToken.None);


            var batchedCompletionTasks = new Task[2];
            batchedCompletionTasks[0] = Task.Run(async () =>
            {
                Console.WriteLine("R1: Completion task started");

                var buffer = new Guid[5000];

                while (!cancellationTokenSource.Token.IsCancellationRequested || !locksTokensToComplete.IsEmpty)
                {
                    Console.WriteLine("R1: Attempt to get lock tokens");
                    // running concurrently, two tasks could get to this line, but only one will pop items
                    var count = locksTokensToComplete.TryPopRange(buffer, 0, buffer.Length);

                    if (count == 0)
                    {
                        Console.WriteLine("R1: Waiting a bit");
                        await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationTokenSource.Token)
                            .ConfigureAwait(false);
                        continue;
                    }

                    var toComplete = buffer.Take(count).ToList();
                    Console.WriteLine($"R1: Completing {count} tokens");
                    await receiver1.CompleteBatchAsync(toComplete).ConfigureAwait(false);
                }
            }, CancellationToken.None);

            batchedCompletionTasks[1] = Task.Run(async () =>
            {
                Console.WriteLine("R2: Completion task started");

                var buffer = new Guid[5000];

                while (!cancellationTokenSource.Token.IsCancellationRequested || !locksTokensToComplete.IsEmpty)
                {
                    // running concurrently, two tasks could get to this line, but only one will pop items
                    var count = locksTokensToComplete.TryPopRange(buffer, 0, buffer.Length);

                    if (count == 0)
                    {
                        Console.WriteLine("R2: Waiting a bit");
                        await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationTokenSource.Token)
                            .ConfigureAwait(false);
                        continue;
                    }

                    var toComplete = buffer.Take(count).ToList();
                    Console.WriteLine($"R2: Completing {count} tokens");
                    await receiver2.CompleteBatchAsync(toComplete).ConfigureAwait(false);
                }
            }, CancellationToken.None);
            Console.ReadLine();

            cancellationTokenSource.Cancel();
        }
    }

}
