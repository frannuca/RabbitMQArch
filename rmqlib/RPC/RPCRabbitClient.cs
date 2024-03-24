using Google.Protobuf;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Text;

namespace rmqlib.RPC
{
    public  class RpcClient<TRequest, TResponse>: RpcBase<TRequest, TResponse>
        where TRequest : IMessage<TRequest>, new()
        where TResponse : IMessage<TResponse>, new()
    {
        private readonly EventingBasicConsumer _consumer;
        private readonly string _replyQueueName;
        
        protected ConcurrentDictionary<string, ConcurrentBag<(int index,string transientStatus,byte[] payload)>> _responses = 
            new ConcurrentDictionary<string, ConcurrentBag<(int index, string transientStatus, byte[] payload)>>();
        
        public RpcClient(string? hostname, string exchange, string user):base(hostname, exchange, user)
        {
            _replyQueueName = QueueName+System.Guid.NewGuid().ToString() + ".reply";
            Channel.QueueDeclare(_replyQueueName,false,true,false,null);
            Channel.QueueBind(_replyQueueName, exchange, _replyQueueName);
            _consumer = new EventingBasicConsumer(Channel);
            
            Start();
        }

        public Task Start()
        {
            return Task.Run(() =>
            {
                Channel.BasicConsume(
                    consumer: _consumer,
                    queue: _replyQueueName,
                    autoAck: false);

                _consumer.Received += (model, ea) =>
                {
                    ct.ThrowIfCancellationRequested();
                    var body = ea.Body.ToArray();
                    var responseProps = ea.BasicProperties;
                    try
                    {
                        var idparts = responseProps.CorrelationId.Split("|");
                        if (idparts.Length == 3)
                        {
                            var cid = idparts[0];
                            int partIndex = double.TryParse(idparts[1], out double d) ? (int)d : -1;
                            var transientStatus = idparts[2];
                            if (!_responses.ContainsKey(cid))
                            {
                                _responses[cid] =
                                    new ConcurrentBag<(int index, string transientStatus, byte[] payload)>();
                            }

                            _responses[cid].Add((partIndex, transientStatus, body));
                        }
                        else
                        {
                            _responses[responseProps.CorrelationId].Add((0, "end", body));
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                    finally
                    {
                        Channel.BasicAck(ea.DeliveryTag, false);

                    }


                };

            });
        }

        private Task<TResponse> GetResponse(string correlationId)
        {
            return Task.Run(() =>
            {
                while ( !_responses.ContainsKey(correlationId)
                       || _responses[correlationId].Any(x => x.transientStatus == "end") == false)
                {
                    ct.ThrowIfCancellationRequested();
                }
                var response = new TResponse();
                var payload_reconstructed =
                    _responses[correlationId].OrderBy(x =>x.index)
                        .Select(x => x.payload)
                        .SelectMany(x => x).ToArray();
                response.MergeFrom(payload_reconstructed);
                _responses.TryRemove(correlationId, out _);
                return response;
            });
        }

        public async Task<TResponse>  CallAsync(TRequest request)
        {
            
            
            var props = Channel.CreateBasicProperties();
            string correlationId = _newCorrelationId();
            props.CorrelationId = correlationId;
            props.ReplyTo = _replyQueueName;

            var messageBytes = request.ToByteArray();

            Channel.BasicPublish(
                exchange: Exchange,
                routingKey: QueueName,
                basicProperties: props,
                body: messageBytes);

            return await GetResponse(correlationId);
        }

    }
}