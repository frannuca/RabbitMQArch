// See https://aka.ms/new-console-template for more information

using System.IO.Pipes;
using Queuing.Protobuf.Messages;
using rmqlib.broadcast;
using rmqlib.services;

Console.WriteLine("Server Starting!");
var notification = new BroadCasterClient<BroadCastMessage>("localhost","broadcast");
var servers = new List<GenericXmlServer>();
for (int i = 0; i < 10; i++)
{
    var server = new GenericXmlServer("localhost","generic_tests","fran",
        (string msg) => { notification.SendMessage(new BroadCastMessage(){Message = DateTime.Now.ToLongTimeString()+msg}); });
    await server.Start();
    servers.Add(server);
}
Console.ReadLine();
servers.ForEach(server => server.Stop()); 