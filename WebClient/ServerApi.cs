using System;
using SpacetimeDB;
using SpacetimeDB.Types;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace WebClient;

public record ChatMessage(string? Sender, string Text);

public class ServerApi {
	private Identity? local_identity;
	private readonly ConcurrentQueue<(string Command, string Args)> input_queue = new();
	private Timer? _timer;

	public readonly List<ChatMessage> ChatMessages = [];

	public void Run() {
		AuthToken.Init(".spacetime_csharp_quickstart");
		var conn = ConnectToDB();
		RegisterCallbacks(conn);

		_timer = new Timer(_ => { ProcessThread(conn); }, null, TimeSpan.Zero, TimeSpan.FromMinutes(1));
	}

	private const string HOST = "http://localhost:3000";
	private const string DBNAME = "quickstart-chat";

	private DbConnection ConnectToDB() {
		var conn = DbConnection.Builder()
							   .WithUri(HOST)
							   .WithModuleName(DBNAME)
							   .WithToken(AuthToken.Token)
							   .OnConnect(OnConnected)
							   .OnConnectError(OnConnectError)
							   .OnDisconnect(OnDisconnected)
							   .Build();
		return conn;
	}

	private void OnConnected(DbConnection conn, Identity identity, string authToken) {
		local_identity = identity;
		AuthToken.SaveToken(authToken);

		conn.SubscriptionBuilder()
			.OnApplied(OnSubscriptionApplied)
			.SubscribeToAllTables();
	}

	private void AddSystemMessage(string message) {
		ChatMessages.Add(new ChatMessage(null, message));
	}

	private void OnConnectError(Exception e) {
		AddSystemMessage($"Error while connecting: {e}");
	}

	private void OnDisconnected(DbConnection conn, Exception? e) {
		AddSystemMessage(e != null
							 ? $"Disconnected abnormally: {e}"
							 : "Disconnected normally.");
	}

	private void RegisterCallbacks(DbConnection conn) {
		conn.Db.Users.OnInsert += User_OnInsert;
		conn.Db.Users.OnUpdate += User_OnUpdate;

		conn.Db.Messages.OnInsert += Message_OnInsert;

		conn.Reducers.OnSetName += Reducer_OnSetNameEvent;
		conn.Reducers.OnSendMessage += Reducer_OnSendMessageEvent;
	}

	private string UserNameOrIdentity(User user) => user.Name ?? user.Identity.ToString()[..8];

	private void User_OnInsert(EventContext ctx, User insertedValue) {
		if (insertedValue.Online) {
			AddSystemMessage($"{UserNameOrIdentity(insertedValue)} is online");
		}
	}

	private void User_OnUpdate(EventContext ctx, User oldValue, User newValue) {
		if (oldValue.Name != newValue.Name) {
			AddSystemMessage($"{UserNameOrIdentity(oldValue)} renamed to {newValue.Name}");
		}

		if (oldValue.Online != newValue.Online) {
			AddSystemMessage(newValue.Online
								 ? $"{UserNameOrIdentity(newValue)} connected."
								 : $"{UserNameOrIdentity(newValue)} disconnected.");
		}
	}

	private void Message_OnInsert(EventContext ctx, Message insertedValue) {
		if (ctx.Event is not Event<Reducer>.SubscribeApplied) {
			PrintMessage(ctx.Db, insertedValue);
		}
	}

	private void PrintMessage(RemoteTables tables, Message message) {
		var sender = tables.Users.Identity.Find(message.Sender);
		var senderName = "unknown";
		if (sender != null) {
			senderName = UserNameOrIdentity(sender);
		}

		ChatMessages.Add(new ChatMessage(senderName, message.Text));
	}

	private void Reducer_OnSetNameEvent(ReducerEventContext ctx, string name) {
		var e = ctx.Event;
		if (e.CallerIdentity == local_identity && e.Status is Status.Failed(var error)) {
			AddSystemMessage($"Failed to change name to {name}: {error}");
		}
	}

	private void Reducer_OnSendMessageEvent(ReducerEventContext ctx, string text) {
		var e = ctx.Event;
		if (e.CallerIdentity == local_identity && e.Status is Status.Failed(var error)) {
			AddSystemMessage($"Failed to send message {text}: {error}");
		}
	}

	private void OnSubscriptionApplied(SubscriptionEventContext ctx) {
		AddSystemMessage("Connected");
		PrintMessagesInOrder(ctx.Db);
	}

	private void PrintMessagesInOrder(RemoteTables tables) {
		foreach (var message in tables.Messages.Iter().OrderBy(item => item.Sent)) {
			PrintMessage(tables, message);
		}
	}

	private void ProcessThread(DbConnection conn) {
		try {
			conn.FrameTick();
			ProcessCommands(conn.Reducers);
		}
		finally {
			conn.Disconnect();
			_timer?.Dispose();
			_timer = null;
		}
	}

	private void ProcessCommands(RemoteReducers reducers) {
		while (input_queue.TryDequeue(out var command)) {
			switch (command.Command) {
				case "message":
					reducers.SendMessage(command.Args);
					break;
				case "name":
					reducers.SetName(command.Args);
					break;
			}
		}
	}

	public void SetName(string name) {
		input_queue.Enqueue(("name", name));
	}

	public void SendMessage(string message) {
		input_queue.Enqueue(("message", message));
	}
}