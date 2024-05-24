// See https://aka.ms/new-console-template for more information

using Queuing.Protobuf.Messages;
using rmqlib.broadcast;

Console.WriteLine("Hello, World!");

var server = new BroadCastServer<BroadCastMessage>("localhost","broadcast", (msg) =>
{
    Console.WriteLine("Broad Casting Received message: " + msg.Message);
});

await server.Start();
Console.WriteLine("Press [enter] to exit");
Console.ReadLine();