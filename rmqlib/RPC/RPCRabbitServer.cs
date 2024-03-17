using Google.Protobuf;

namespace rmqlib.RPC;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

public abstract class RpcServer<TRequest,TResponse> :RPCBase<TRequest, TResponse>
    where TRequest: IMessage<TRequest>, new() 
    where TResponse: IMessage<TResponse>, new()
{
    private Func<TRequest, TResponse> fProcess;
    private CancellationTokenSource _cts = new CancellationTokenSource();

    public RpcServer(string? hostname, string exchange, string user, bool autodelete,
        Func<TRequest, TResponse> fProcess) : base(hostname, exchange, user, autodelete)
    {
        this.fProcess = fProcess;
    }
    abstract public TResponse Process(TRequest request);
    
    private static List<byte[]> SplitByteArray(byte[] source)
    {
        const int chunkSize = 1024 * 1024; // 1MB
        var chunks = new List<byte[]>();
        int i = 0;
        for (; i < source.Length; i += chunkSize)
        {
            int actualChunkSize = Math.Min(chunkSize, source.Length - i);
            byte[] chunk = new byte[actualChunkSize];
            Array.Copy(source, i, chunk, 0, actualChunkSize);
            chunks.Add(chunk);
        }
        return chunks;
    }

    public void Stop()
    {
        _cts.Cancel();
        Dispose();
    }
    
    public Task Start()
    {
        Task.Run(() =>
        {
            var consumer = new EventingBasicConsumer(Channel);
            consumer.Received += (model, ea) =>
            {
                List<byte[]> responseBytes = null;

                var props = ea.BasicProperties;
                
                try
                {
                    var body = ea.Body.ToArray();
                    TRequest request = new TRequest();
                    request.MergeFrom(body);
                    TResponse response = fProcess(request);

                    responseBytes = SplitByteArray(response.ToByteArray());
                }
                catch (Exception e)
                {
                    Console.WriteLine(" [.] " + e.Message);
                    responseBytes.Add(new byte[0]);
                }
                finally
                {
                    for (int i = 0; i < responseBytes.Count; i++)
                    {
                        var tag = i == responseBytes.Count() - 1 ? "end" : "transient";
                        var replyProps = Channel.CreateBasicProperties();
                        replyProps.CorrelationId = $"{props.CorrelationId}|{i}|{tag}";

                        Channel.BasicPublish(exchange: Exchange,
                            routingKey: props.ReplyTo, basicProperties: replyProps, body: responseBytes[i]);
                    }

                    Channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                }
            };

            Channel.BasicConsume(queue: QueueName, autoAck: false, consumer: consumer);

            Console.WriteLine(" [x] Awaiting RPC requests");
            _cts.Token.WaitHandle.WaitOne();
        });

        return Task.CompletedTask;
    }

}