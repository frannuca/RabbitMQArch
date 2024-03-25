using Google.Protobuf;
using RabbitMQ.Client;

namespace rmqlib.broadcast;

public class BroadCasterClient<TMessage> : IDisposable
    where TMessage : IMessage<TMessage>, new()
{
    string _exchange;
    IConnection _connection;
    IModel _channel;
    string _queueName = typeof(TMessage).Name.ToLower() + "_broadcast";
    public BroadCasterClient(string host, string exchange)
    {
        _exchange= exchange;
        var factory = new ConnectionFactory() { HostName = host ?? "localhost" };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(exchange,ExchangeType.Fanout);
        _channel.QueueDeclare(_queueName,false,false,false,null);
        _channel.QueueBind(_queueName, exchange, "");

    }

    public void SendMessage(TMessage msg)
    {
        _channel.BasicPublish(_exchange, "", null, msg.ToByteArray());
    }
    public void Dispose()
    {
        _connection.Dispose();
        _channel.Dispose();
    }
}