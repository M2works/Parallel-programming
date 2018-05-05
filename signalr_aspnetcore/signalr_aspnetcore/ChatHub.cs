using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;

namespace signalr_aspnetcore
{
    public class ChatHub : Hub
    {
        public static ConcurrentDictionary<string, Jankiel> MyUsers = new ConcurrentDictionary<string, Jankiel>();
        private static int IdAssigner = 0;
        public void Send(string name, string message)
        {
            // Call the broadcastMessage method to update clients.
            Clients.All.InvokeAsync("broadcastMessage", name, message);
        }
        public string AssignName()
        {
            return "Jankiel" + IdAssigner++;
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
            MyUsers.TryAdd(Context.ConnectionId, new Jankiel() { ConnectionId = connId, Name = newUserName });
            Clients.Client(connId).InvokeAsync("assignName", newUserName);
            Clients.All.InvokeAsync("broadcastMessage", "system", $"{Context.ConnectionId} joined the conversation");
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
