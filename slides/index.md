- title : Concurrent data structures with examples in Azure Service Bus 
- description : Concurrent data structures with examples in Azure Service Bus 
- author : Daniel Marbach
- theme : night
- transition : default

***

### Introduction

- The examples in this presentation use the [WindowsAzure.ServiceBus](https://www.nuget.org/packages/WindowsAzure.ServiceBus/) approach to illustrate a problem. The package should no longer be used. If you plan to use Azure Service Bus use [Microsoft.Azure.ServiceBus](https://www.nuget.org/packages/Microsoft.Azure.ServiceBus/) or even better [Azure.Messaging.ServiceBus](https://www.nuget.org/packages/Azure.Messaging.ServiceBus) 

---

### Why am I telling you this?

![Copy Paste](images/copy-paste.jpg)

> **Stackoverflow Law**: Good coders borrow, great coders steal. [The Internet](https://stackoverflow.blog/2020/05/20/good-coders-borrow-great-coders-steal/)

***

### Receive/Complete messages

- MessagingFactory = Dedicated TCP connection --> faaast!
- Every `CompleteAsync` is a dedicated call to the cloud --> slooow!


    [lang=cs]
    var factory1 = await MessagingFactory.CreateAsync(address, settings);
    var factory2 = await MessagingFactory.CreateAsync(address, settings;
    var receiver1 = await factory1.CreateMessageReceiverAsync(queueName, ReceiveMode.PeekLock);
    var receiver2 = await factory2.CreateMessageReceiverAsync(queueName, ReceiveMode.PeekLock);

    receiver1.OnMessageAsync(msg => ReceiveMessage(msg, receiver1), ..);
    receiver2.OnMessageAsync(msg => ReceiveMessage(msg, receiver2), ..);

    static async Task ReceiveMessage(BrokeredMessage message, MessageReceiver receiver) {
        // process message
        await receiver.CompleteAsync(message.LockToken)
    }

---

### Can we do better?


    [lang=cs]
    var lockTokensToComplete = new ConcurrentStack<Guid>();
  
    static async Task ReceiveMessage(BrokeredMessage message) {
        // process message
        lockTokensToComplete.Push(message.LockToken);
    }

---

### We need someonen to complete the tokens

    [lang=cs]
    var tokenSource = new CancellationTokenSource();
    var token = tokenSource.Token;
    
    var batchCompletionTask = Task.Run(async () => {
        while(!token.IsCancellationRequested) {
            var lockTokens = new Guid[100];
            int numberOfItems = lockTokensToComplete.TryPopRange(lockTokens)
            if(numberOfItems > 0) {
                await receiveClient.CompleteBatchAsync(lockTokens);
            }
            await Task.Delay(TimeSpan.FromSeconds(5), token);
        }
    });

' Under small load, we complete lock tokens in in batches of one to maximum one hundred tokens. 
' If we receive only a limited number of messages, the loop might complete messages with their tokens one by one (for example when we receive a message every 6 seconds). But what happens when the load increases?
' When we’d received several hundred messages per seconds our randomly chosen “complete every one-hundredth messages” and then “sleep for five seconds” might turn out to be a suboptimal choice
' https://www.planetgeek.ch/2016/12/05/another-attempt-to-batch-complete-with-azure-service-bus/

--- 

### Under concurrency, things might spin

    [lang=cs] 
    static async Task ReceiveMessage(BrokeredMessage message) {
        // process message
        lockTokensToComplete.Push(message.LockToken);
    }

- `NumberOfReceivers` * `ConcurrencyPerReceiver` will push to the concurrent stack

' So for example when we’d use 10 receivers with each a concurrency setting of 32 we’d be ending up pushing lock tokens to the concurrent stack from up to 320 simultaneous operations    

---

> **Are all of the new concurrent collections lock-free?**: ConcurrentQueue<T> and ConcurrentStack<T> are completely lock-free in this way. They will never take a lock, but they may end up spinning and retrying an operation when faced with contention [Old Post](https://blogs.msdn.microsoft.com/pfxteam/2010/01/26/faq-are-all-of-the-new-concurrent-collections-lock-free/)

' Of course the concurrent data structures are getting improved, still spinning and retrying is something that has to taken into account

---

### Under load we might not keep up

- Concurrent receivers can fill the concurrent stack faster with lock tokens than our completion loop manage to complete
- Increased change of lock lost problems under peek lock

---

### Let's fix that

    [lang=cs]
    var completionTasks = new Task[numberOfReceivers];
    
    for(int i = 0; i < numberOfReceivers; i++) { 
        completionTasks[i] = Task.Run(() => BatchCompletionLoop());
    }
    
    static async Task BatchCompletionLoop() {
        while(!token.IsCancellationRequested) {
            var lockTokens = new Guid[100];
            int numberOfItems = lockTokensToComplete.TryPopRange(lockTokens)
            if(numberOfItems > 0) {
                await receiveClient.CompleteBatchAsync(lockTokens);
            }
            await Task.Delay(TimeSpan.FromSeconds(5), token);
        }
    }

- Contention problem is even worse, multiple background completion operations are competing on the concurrent stack
- Same `Task.Delay` without jitter causes a lot to wake up and potentially not succed, wasting a lot of resources

' Even if we had jitter latency might make loops align again over time
' https://www.planetgeek.ch/2016/12/14/batch-completion-with-multiple-receivers-on-azure-service-bus/


---

### There is a dragon hiding here


    [lang=cs]
    await receiveClient.CompleteBatchAsync(lockTokens);

- Complete always on the same receiver
-  Works with SBMP (NetMessaging) but fails with AMQP as a transport type

---


### Surely we can fix that too?

    [lang=cs]
    var lockTokensToComplete = new ConcurrentStack<Guid>[numberOfReceivers];
    // initialize the concurrent stacks
    
    receiveClient1.OnMessageAsync(message => ReceiveMessage(message, lockTokensToComplete[0]);
    ...
    receiveClientN.OnMessageAsync(message => ReceiveMessage(message, lockTokensToComplete[N-1]);
    
    static async Task ReceiveMessage(BrokeredMessage message, ConcurrentStack<Guid> lockTokensToComplete) {
        // process message
        lockTokensToComplete.Push(message.LockToken);
    }

---

### ... and the completion

    [lang=cs]
    for(int i = 0; i < numberOfReceivers; i++) { 
        completionTasks[i] = Task.Run(() => BatchCompletionLoop(receivers[i], lockTokensToComplete[i]));
    }
  
    static async Task BatchCompletionLoop(MessageReceiver receiver, ConcurrentStack<Guid> lockTokensToComplete) {
        while(!token.IsCancellationRequested) {
            var lockTokens = new Guid[100];
            int numberOfItems = lockTokensToComplete.TryPopRange(lockTokens)
            if(numberOfItems > 0) {
                await receiver.CompleteBatchAsync(lockTokens);
            }
            await Task.Delay(TimeSpan.FromSeconds(5), token);
        }
    }

---

### What have we achieved?

- Contention is mostly gone
- Completion is guaranteed to use the same receiver to complete
<br/>
<br/>

### but...

---

- Still wasting a lot of resources due to the wakeup and idle pattern
- Code doesn't really have the necessary elasticity

***

### Multi Producer Concurrent Consumer


- Make sure messages are only completed on the receiver they came from
- Reduce the number of threads used when the number of clients is increased
- Autoscale up under heavy load
- Scale down under light load
- Minimise the contention on the underlying collections used
- Be completely asynchronous
- Implements a push based model from the producer and consumer perspective
- Respect the maximum batch sized defined by the client of the component or a predefined push interval
- Provide FIFO semantics instead of LIFO
- Be as lock-free as possible

---


![Copy Paste](images/mpcc.png)

---

    [lang=cs]
    class MultiProducerConcurrentConsumer<TItem> {
        public MultiProducerConcurrentConsumer(
            int batchSize, TimeSpan pushInterval, 
            int maxConcurrency, int numberOfSlots) { }
    
        public void Start(Func<List<TItem>, int, object, CancellationToken, Task> pump) { }
    
        public void Start(Func<List<TItem>, int, object, CancellationToken, Task> pump, object state) { }
    
        public void Push(TItem item, int slotNumber) { }
    
        public async Task Complete(bool drain = true) { }
    }

' https://www.planetgeek.ch/2017/01/17/introduction-to-the-multiproducerconcurrentconsumer-for-azure-service-bus-message-completion/

---

### Pre-allocate and reuse

- Allocate as much as you need upfront
- Allocate on-demand when you need it


    [lang=cs]
    public MultiProducerConcurrentCompletion(int batchSize, TimeSpan pushInterval, int maxConcurrency, int numberOfSlots) {   
        queues = new ConcurrentQueue<TItem>[numberOfSlots];
        for (var i = 0; i < numberOfSlots; i++)
        {
            queues[i] = new ConcurrentQueue<TItem>();
        }
        
        var maxNumberOfConcurrentOperationsPossible = numberOfSlots * maxConcurrency;
        pushTasks = new List<Task>(maxNumberOfConcurrentOperationsPossible);
    
        itemListBuffer = new ConcurrentQueue<List<TItem>>();
        for (var i = 0; i < maxNumberOfConcurrentOperationsPossible; i++)
        {
            itemListBuffer.Enqueue(new List<TItem>(batchSize));
        }
    }

' The benefit of only allocating when needed is that the memory consumption only grows when it is needed. The downside of this approach is that under highly concurrent scenarios allocating structures in a safe and lock-free way can be tricky.
' https://www.planetgeek.ch/2017/01/19/multiproducerconcurrentconsumer-preallocate-and-reuse/