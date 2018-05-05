using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;
using SignalR.EventAggregatorProxy;
using SignalR.EventAggregatorProxy.Client.EventAggregation;

namespace signalr_aspnetcore
{
    public class ChatHub : Hub
    {
        private const int COORDINATES_NUMBER = 2;

        private static int CurrentNumberOfAgents = 0;
        private static ConcurrentDictionary<string, Jankiel> MyUsers = new ConcurrentDictionary<string, Jankiel>();
        private static int IdAssigner = 0;
        private static List<Point> coordinates = new List<Point>();
        private static int numberOfAgents = 0;

        public ChatHub()
        {
            string line;
            string [] stringCoordinates;
            try
            {   // Open the text file using a stream reader.
                using (StreamReader sr = new StreamReader("positions.txt"))
                {
                    int lineCounter = 0;
                    while ((line = sr.ReadLine()) != null)
                    {
                        if(lineCounter++ == 0)
                        {
                            numberOfAgents = int.Parse(line);
                        }
                        else
                        {
                            int[] singleCoordinates = new int[COORDINATES_NUMBER];
                            stringCoordinates = line.Split(" ", StringSplitOptions.None);
                            for (int i = 0; i < COORDINATES_NUMBER; i++)
                                singleCoordinates[i] = int.Parse(stringCoordinates[i]);
                            coordinates.Add(new Point(singleCoordinates[0], singleCoordinates[1]));
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("The file could not be read:");
                Console.WriteLine(e.Message);
            }
        }
        public void Send(string name, string message)
        {
            // Call the broadcastMessage method to update clients.
            Clients.All.InvokeAsync("broadcastMessage", name, message);
        }
        private string AssignName()
        {
            return "Jankiel" + IdAssigner++ + " " + coordinates[CurrentNumberOfAgents];
        }

        public void Private(string name, string message, string receiverName)
        {
            string connId = MyUsers.Values.Where(x => x.Name == receiverName).FirstOrDefault().ConnectionId;
            
            // Call the broadcastMessage method to update clients.
            Clients.Client(connId).InvokeAsync("privateMessage", name, message);
        }
        public override Task OnConnectedAsync()
        {
            string connId = Context.ConnectionId;
            string newUserName = AssignName();
            Clients.Client(connId).InvokeAsync("assignName", newUserName, coordinates[CurrentNumberOfAgents].X, coordinates[CurrentNumberOfAgents].Y);
            MyUsers.TryAdd(Context.ConnectionId, new Jankiel() { ConnectionId = connId, Name = newUserName, Coordinates = coordinates[CurrentNumberOfAgents++] });
            Clients.All.InvokeAsync("broadcastMessage", "system", $"{Context.ConnectionId} joined the conversation");
            if (MyUsers.Count == numberOfAgents)
            {
                List < Tuple < string,Point>> agentsInfo = new List<Tuple<string, Point>>();
                foreach(Jankiel j in MyUsers.Values)
                {
                    agentsInfo.Add(new Tuple<string, Point>(j.Name, j.Coordinates));
                }
                
                for (int i=0;i<agentsInfo.Count;i++)
                    Clients.All.InvokeAsync("sendAgentsData", agentsInfo[i].Item1,agentsInfo[i].Item2.X,agentsInfo[i].Item2.Y);

                Clients.All.InvokeAsync("startSimulation");
            }
            return base.OnConnectedAsync();
        }
        public override Task OnDisconnectedAsync(System.Exception exception)
        {
            Jankiel jankiel;
            MyUsers.TryRemove(Context.ConnectionId, out jankiel);
            Clients.All.InvokeAsync("broadcastMessage", "system", $"{Context.ConnectionId} left the conversation");
            return base.OnDisconnectedAsync(exception);
        }

    }
}
