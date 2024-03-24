using Google.Protobuf;
using RabbitMQ.Client;

namespace rmqlib.RPC;

public abstract class RpcBase<TRequest, TResponse>: IDisposable
    where TRequest : IMessage<TRequest>, new()
    where TResponse : IMessage<TResponse>, new()
{
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly string _exchange;
    private readonly string _user;
    private readonly string _queueName = typeof(TRequest).Name.ToLower().Replace("request","");
    protected CancellationTokenSource cts = new CancellationTokenSource();
    protected CancellationToken ct => cts.Token;
    protected RpcBase(string? hostname, string exchange,string user)
    {
        _exchange = exchange;
        _user = user;
        var factory = new ConnectionFactory() { HostName = hostname ?? "localhost" };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(exchange,ExchangeType.Direct,true, false,null);
        _channel.QueueDeclare(_queueName,false,false,false,null);
        _channel.QueueBind(_queueName, exchange, _queueName);
        _channel.ModelShutdown += (sender, ea) =>
        {
            Console.WriteLine("Channel is being shut down. Reason: " + ea.ReplyText);
        };
    }
    
    protected string _newCorrelationId()
    {
        return Guid.NewGuid().ToString();
    }

    
    public void Close()
    {
        cts.Cancel();
        _connection.Close();
    }

    public void Dispose()
    {
        if (_connection.IsOpen)
        {
            _connection.Close();
        }
    }
    
    protected IModel Channel => _channel;
    public string User => _user;
    public string Exchange => _exchange;
    public string QueueName => _queueName;
}