using Google.Protobuf;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace rmqlib.broadcast;

public class BroadCastServer<TMessage> : IDisposable
    where TMessage : IMessage<TMessage>, new()
{
    public string Exchange { get; }
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private CancellationTokenSource cts = new CancellationTokenSource();
    private CancellationToken ct => cts.Token;
    private readonly string _queueName = typeof(TMessage).Name.ToLower()+"_broadcast";
    public string QueueName => _queueName;
    protected Action<TMessage> _fCallback;
    public IModel Channel => _channel;
    public BroadCastServer(string host,string exchange,Action<TMessage> fCallback)
    {
        Exchange = exchange;
        _fCallback= fCallback;
        var factory = new ConnectionFactory() { HostName = host ?? "localhost" };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(exchange,ExchangeType.Fanout);
        _channel.QueueDeclare(_queueName,false,false,false,null);
        _channel.QueueBind(_queueName, exchange, "");
        
        
    }
    public Task Start()
    {
        Task.Run(() =>
        {
            var consumer = new EventingBasicConsumer(Channel);
            Channel.BasicConsume(queue: QueueName, autoAck: false, consumer: consumer);

            consumer.Received += (model, ea) =>
            {
                ct.ThrowIfCancellationRequested();
                try
                {
                    var body = ea.Body.ToArray();
                    TMessage request = new TMessage();
                    request.MergeFrom(body);
                    _fCallback(request);
                }
                catch (Exception e)
                {
                    Console.WriteLine(" [.] " + e.Message);
                }
            };
            
            Console.WriteLine(" [x] Awaiting BroadCasting requests");
            cts.Token.WaitHandle.WaitOne();
        });

        return Task.CompletedTask;
    }


    public void Dispose()
    {
        cts.Cancel();
        _connection.Dispose();
        _channel.Dispose();
    }
}