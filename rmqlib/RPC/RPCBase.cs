using Google.Protobuf;
using RabbitMQ.Client;

namespace rmqlib.RPC;

public abstract class RPCBase<TRequest, TResponse>: IDisposable
    where TRequest : IMessage<TRequest>, new()
    where TResponse : IMessage<TResponse>, new()
{
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly string _exchange;
    private readonly string _user;
    private readonly string _queueName = typeof(TRequest).Name.ToLower().Replace("request","");
   
        
    public RPCBase(string? hostname, string exchange, string user,bool autodelete)
    {
        _exchange = exchange;
        _user = user;
        var factory = new ConnectionFactory() { HostName = hostname ?? "localhost" };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(exchange, ExchangeType.Direct, false, true);
        _channel.QueueDeclare(_queueName,false,false,autodelete,null);
    }
    
    protected string _newCorrelationId()
    {
        return Guid.NewGuid().ToString();
    }
    
    public void Close()
    {
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