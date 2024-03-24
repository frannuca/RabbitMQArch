using System.Collections.Concurrent;
using Google.Protobuf;

namespace rmqlib.RPC;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

public abstract class RpcServer<TRequest, TResponse>(string? hostname, string exchange, string user)
    : RpcBase<TRequest, TResponse>(hostname, exchange, user)
    where TRequest : IMessage<TRequest>, new()
    where TResponse : IMessage<TResponse>, new()
{
    private CancellationTokenSource _cts = new CancellationTokenSource();

    protected abstract TResponse Process(TRequest request);
    
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
            Channel.BasicConsume(queue: QueueName, autoAck: false, consumer: consumer);

            consumer.Received += (model, ea) =>
            {
                List<byte[]> responseBytes = new List<byte[]>();

                var props = ea.BasicProperties;
                
                try
                {
                    var body = ea.Body.ToArray();
                    TRequest request = new TRequest();
                    request.MergeFrom(body);
                    TResponse response = Process(request);

                    responseBytes = SplitByteArray(response.ToByteArray());
                }
                catch (Exception e)
                {
                    Console.WriteLine(" [.] " + e.Message);
                    var errorResponse = new Queuing.Protobuf.Messages.ErrorMgs
                    {
                        Error = e.Message
                    };
                    responseBytes.Add(errorResponse.ToByteArray());
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
            
            Console.WriteLine(" [x] Awaiting RPC requests");
            _cts.Token.WaitHandle.WaitOne();
        });

        return Task.CompletedTask;
    }

}