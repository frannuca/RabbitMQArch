// See https://aka.ms/new-console-template for more information

using System.Globalization;
using rmqlib.RPC;
using rmqlib.services;

Console.WriteLine("Hello, World!");
var server = new GenericXmlServer("localhost","duper","fran");
await server.Start();
Console.ReadLine();