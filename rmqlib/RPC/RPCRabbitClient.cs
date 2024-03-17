using Google.Protobuf;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Text;

namespace rmqlib.RPC
{
    public  class RpcClient<TRequest, TResponse>: RPCBase<TRequest, TResponse>
        where TRequest : IMessage<TRequest>, new()
        where TResponse : IMessage<TResponse>, new()
    {
        private readonly EventingBasicConsumer _consumer;
        private readonly string _replyQueueName;
        
        public RpcClient(string? hostname, string exchange, string user):base(hostname, exchange, user, false)
        {
            _replyQueueName = QueueName + ".reply";
            Channel.QueueDeclare(_replyQueueName,false,true,true,null);
            
            Channel.QueueBind(_replyQueueName, exchange, _replyQueueName);
            _consumer = new EventingBasicConsumer(Channel);
        }
        
        public  async Task<TResponse> CallAsync(TRequest request)
        {
            return await Task<TResponse>.Run(() =>
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

                TResponse response = new TResponse();
                List<(int index, byte[] payload )> responses = new List<(int index, byte[] payload)>();
                _consumer.Received += (model, ea) =>
                {
                    bool continueConsuming = true;
                    while (continueConsuming)
                    {
                        var body = ea.Body.ToArray();
                        var responseProps = ea.BasicProperties;
                        if (responseProps.CorrelationId.Contains(correlationId))
                        {
                            try
                            {
                                var idparts = responseProps.CorrelationId.Split("|");
                                if (idparts.Length == 3)
                                {
                                    var cid = idparts[0];
                                    int partIndex = double.TryParse(idparts[1], out double d) ? (int)d : -1;
                                    var transientStatus = idparts[2];
                                    responses.Add((partIndex, body));
                                    if (transientStatus.ToLower() == "end")
                                    {
                                        continueConsuming = false;
                                    }
                                }
                                else
                                {
                                    continueConsuming = false;
                                    responses.Add((0, body));
                                }

                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);
                            }
                            finally
                            {
                                Channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                            }
                            
                            
                        }
                    }
                    
                    var payload_reconstructed =
                        responses.OrderBy(x =>x.index).Select(x => x.payload)
                            .SelectMany(x => x).ToArray();
                    response.MergeFrom(payload_reconstructed);
                };
                
                Channel.BasicConsume(
                    consumer: _consumer,
                    queue: _replyQueueName,
                    autoAck: false);

                return response;
            });

        }
        
    }
}