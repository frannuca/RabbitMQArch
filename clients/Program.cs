// See https://aka.ms/new-console-template for more information

using Queuing.Protobuf.Messages;
using rmqlib.RPC;

Console.WriteLine("Client Starting!");



var fsender = async (int N, string clientname)=>
{
    var client = new RpcClient<GenericRequest, GenericResponse>("localhost","generic_tests",clientname);
    for(int i=0;i<N;i++)
    {
        var request = new GenericRequest();
        request.Xml = new XmlMessage();
        request.Xml.XmlString = "Request message ready from client "+clientname+"#"+i.ToString();
        try
        {
            var response = await client.CallAsync(request);
            Console.WriteLine(response?.Xml?.XmlString);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        
    }   
};
int N = 100000;
var task1 = Task.Run(() => fsender(N,"client1"));
var task2 = Task.Run(() => fsender(N,"client2"));
var task3 = Task.Run(() => fsender(N,"client3"));

Task.WaitAll([task1,task2,task3]);
Console.WriteLine("All Client done! Press any key to exit.");
Console.ReadLine();