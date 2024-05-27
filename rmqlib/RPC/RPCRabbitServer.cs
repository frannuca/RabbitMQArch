using System.Collections.Concurrent;
using Google.Protobuf;
using Queuing.Protobuf.Messages;
using rmqlib.broadcast;

namespace rmqlib.RPC;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

public abstract class RpcServer<TRequest, TResponse>(
    string? hostname,
    string exchange,
    string user,
    Action<string>? notificationAction = null)
    : RpcBase<TRequest, TResponse>(hostname, exchange, user, notificationAction)
    where TRequest : IMessage<TRequest>, new()
    where TResponse : IMessage<TResponse>, new()
{
    private CancellationTokenSource _cts = new();

    protected abstract TResponse Process(TRequest request);

    private static List<byte[]> SplitByteArray(byte[] source)
    {
        const int chunkSize = 1024 * 1024; // 1MB
        var chunks = new List<byte[]>();
        var i = 0;
        for (; i < source.Length; i += chunkSize)
        {
            var actualChunkSize = Math.Min(chunkSize, source.Length - i);
            var chunk = new byte[actualChunkSize];
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
            Channel.BasicConsume(QueueName, false, consumer);

            consumer.Received += (model, ea) =>
            {
                List<byte[]> responseBytes = new();

                var props = ea.BasicProperties;

                try
                {
                    var body = ea.Body.ToArray();
                    var request = new TRequest();
                    request.MergeFrom(body);
                    var response = Process(request);

                    responseBytes = SplitByteArray(response.ToByteArray());
                }
                catch (Exception e)
                {
                    Console.WriteLine(" [.] " + e.Message);
                    NotificationAction?.Invoke(e.Message);
                    var errorResponse = new GenericResponse()
                    {
                        Error = new ErrorMgs() { Error = e.Message }
                    };
                    responseBytes.Add(errorResponse.ToByteArray());
                }
                finally
                {
                    for (var i = 0; i < responseBytes.Count; i++)
                    {
                        var tag = i == responseBytes.Count() - 1 ? "end" : "transient";
                        var replyProps = Channel.CreateBasicProperties();
                        replyProps.CorrelationId = $"{props.CorrelationId}|{i}|{tag}";

                        Channel.BasicPublish(Exchange,
                            props.ReplyTo, replyProps, responseBytes[i]);
                    }

                    Channel.BasicAck(ea.DeliveryTag, false);
                }
            };

            Console.WriteLine(" [x] Awaiting RPC requests");
            _cts.Token.WaitHandle.WaitOne();
        });

        return Task.CompletedTask;
    }
}