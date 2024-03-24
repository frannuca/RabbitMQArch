using rmqlib.RPC;

namespace rmqlib.services;
using Queuing.Protobuf.Messages;

public class GenericXmlServer : RpcServer<GenericRequest, GenericResponse>
{
    public GenericXmlServer(string? hostname, string exchange, string user) : base(hostname, exchange, user)
    {
    }

    protected override GenericResponse Process(GenericRequest request)
    {
        var msg = request.Xml.XmlString;
        Console.WriteLine("Processing message: " + msg);
        var response = new GenericResponse();
        response.Xml = new XmlMessage();
        response.Xml.XmlString = msg + "_processed";
        return response;
    }
}
