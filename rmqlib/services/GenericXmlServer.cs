using rmqlib.RPC;

namespace rmqlib.services;

using Queuing.Protobuf.Messages;

public class GenericXmlServer : RpcServer<GenericRequest, GenericResponse>
{
    public GenericXmlServer(string? hostname, string exchange, string user, Action<string>? notificationAction = null) :
        base(hostname, exchange, user, notificationAction)
    {
    }

    protected override GenericResponse Process(GenericRequest request)
    {
        var msg = request.Xml.XmlString;
        if (msg.Contains("9999")) throw new Exception("Invalid message 9999");
        Console.WriteLine("Processing message: " + msg);
        var response = new GenericResponse();
        response.Xml = new XmlMessage();
        response.Xml.XmlString = msg + "_processed";
        return response;
    }
}