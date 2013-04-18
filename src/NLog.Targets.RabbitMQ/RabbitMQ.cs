using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using NLog.Common;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing.v0_9_1;
using NLog.Layouts;

namespace NLog.Targets
{
	using System.Threading;
	using System.Threading.Tasks;

	using global::RabbitMQ.Client.Exceptions;

	/// <summary>
	/// A RabbitMQ-target for NLog. See https://github.com/haf/NLog.RabbitMQ for documentation in Readme.md.
	/// </summary>
	[Target("RabbitMQ")]
	public class RabbitMQ : TargetWithLayout
	{
		private IConnection _Connection;
		private IModel _Model;
		private readonly Encoding _Encoding = Encoding.UTF8;
		private readonly Queue<Tuple<byte[], IBasicProperties, string>> _UnsentMessages
			= new Queue<Tuple<byte[], IBasicProperties, string>>(512);

		public RabbitMQ()
		{
			Layout = "${message}";
		}

		#region Properties

		private string _VHost = "/";

		/// <summary>
		/// 	Gets or sets the virtual host to publish to.
		/// </summary>
		public string VHost
		{
			get { return _VHost; }
			set { if (value != null) _VHost = value; }
		}

		private string _UserName = "guest";

		/// <summary>
		/// 	Gets or sets the username to use for
		/// 	authentication with the message broker. The default
		/// 	is 'guest'
		/// </summary>
		public string UserName
		{
			get { return _UserName; }
			set { _UserName = value; }
		}

		private string _Password = "guest";

		/// <summary>
		/// 	Gets or sets the password to use for
		/// 	authentication with the message broker.
		/// 	The default is 'guest'
		/// </summary>
		public string Password
		{
			get { return _Password; }
			set { _Password = value; }
		}

		private ushort _Port = 5672;

		/// <summary>
		/// 	Gets or sets the port to use
		/// 	for connections to the message broker (this is the broker's
		/// 	listening port).
		/// 	The default is '5672'.
		/// </summary>
		public ushort Port
		{
			get { return _Port; }
			set { _Port = value; }
		}

		private Layout _Topic = "{0}";

		///<summary>
		///	Gets or sets the routing key (aka. topic) with which
		///	to send messages. Defaults to {0}, which in the end is 'error' for log.Error("..."), and
		///	so on. An example could be setting this property to 'ApplicationType.MyApp.Web.{0}'.
		///	The default is '{0}'.
		///</summary>
		public Layout Topic
		{
			get { return _Topic; }
			set { _Topic = value; }
		}

		private IProtocol _Protocol = Protocols.DefaultProtocol;

		/// <summary>
		/// 	Gets or sets the AMQP protocol (version) to use
		/// 	for communications with the RabbitMQ broker. The default 
		/// 	is the RabbitMQ.Client-library's default protocol.
		/// </summary>
		public IProtocol Protocol
		{
			get { return _Protocol; }
			set { if (value != null) _Protocol = value; }
		}

		private string _HostName = "localhost";

		/// <summary>
		/// 	Gets or sets the host name of the broker to log to.
		/// </summary>
		/// <remarks>
		/// 	Default is 'localhost'
		/// </remarks>
		public string HostName
		{
			get { return _HostName; }
			set { if (value != null) _HostName = value; }
		}

		private string _Exchange = "app-logging";

		/// <summary>
		/// 	Gets or sets the exchange to bind the logger output to.
		/// </summary>
		/// <remarks>
		/// 	Default is 'log4net-logging'
		/// </remarks>
		public string Exchange
		{
			get { return _Exchange; }
			set { if (value != null) _Exchange = value; }
		}

		private bool _Durable = true;

		/// <summary>
		/// 	Gets or sets the setting specifying whether the exchange
		///		is durable (persisted across restarts)
		/// </summary>
		/// <remarks>
		/// 	Default is true
		/// </remarks>
		public bool Durable
		{
			get { return _Durable; }
			set { _Durable = value; }
		}

		/// <summary>
		/// 	Gets or sets the application id to specify when sending. Defaults to null,
		/// 	and then IBasicProperties.AppId will be the name of the logger instead.
		/// </summary>
		public string AppId { get; set; }

		private int _MaxBuffer = 10240;

		/// <summary>
		/// Gets or sets the maximum number of messages to save in the case
		/// that the RabbitMQ instance goes down. Must be >= 1. Defaults to 10240.
		/// </summary>
		public int MaxBuffer
		{
			get { return _MaxBuffer; }
			set { if (value > 0) _MaxBuffer = value; }
		}

		ushort _HeartBeatSeconds = 3;

		/// <summary>
		/// Gets or sets the number of heartbeat seconds to have for the RabbitMQ connection.
		/// If the heartbeat times out, then the connection is closed (logically) and then
		/// re-opened the next time a log message comes along.
		/// </summary>
		public ushort HeartBeatSeconds
		{
			get { return _HeartBeatSeconds; }
			set { _HeartBeatSeconds = value; }
		}

		bool _UseJSON;

		/// <summary>
		/// Gets or sets whether to format the data in the body as a JSON structure.
		/// Having it as a JSON structure means that you can more easily interpret the data
		/// at its final resting place, than if it were a simple string - i.e. you don't
		/// have to mess with advanced parsers if you have this options for all of your
		/// applications. A product that you can use for viewing logs
		/// generated is logstash (http://logstash.net), elasticsearch (https://github.com/elasticsearch/elasticsearch)
		/// and kibana (http://rashidkpc.github.com/Kibana/)
		/// </summary>
		public bool UseJSON
		{
			get { return _UseJSON; }
			set { _UseJSON = value; }
		}

		#endregion

		protected override void Write(LogEventInfo logEvent)
		{
			var basicProperties = GetBasicProperties(logEvent);
			var message = GetMessage(logEvent);
			var routingKey = GetTopic(logEvent);

			if (_Model == null || !_Model.IsOpen)
				StartConnection();

			try
			{
				CheckUnsent();
				Publish(message, basicProperties, routingKey);
			}
			catch (OperationInterruptedException e)
			{
				// This traps network/channel errors detected by the RabbitMQ client
				// The shutdown reason could be inspected to determine if the connection is still available for use
				this.AddUnsent(routingKey, basicProperties, message);
				InternalLogger.Warn("Unable to send message: " + e.Message);
				throw;
			}
			catch (IOException e)
			{
				AddUnsent(routingKey, basicProperties, message);
				InternalLogger.Warn("Could not send to RabbitMQ instance! {0}", e.ToString());
				throw;
			}
			catch (ObjectDisposedException e)
			{
				AddUnsent(routingKey, basicProperties, message);
				//InternalLogger.Error("Could not write data to the network stream! {0}", e.ToString());
				throw;
			}
		}

		private void AddUnsent(string routingKey, IBasicProperties basicProperties, byte[] message)
		{
			if (_UnsentMessages.Count < _MaxBuffer)
				_UnsentMessages.Enqueue(Tuple.Create(message, basicProperties, routingKey));
			else
				InternalLogger.Warn("MaxBuffer {0} filled. Ignoring message.", _MaxBuffer);
		}

		private void CheckUnsent()
		{
			// using a queue so that removing and publishing is a single operation
			while (_UnsentMessages.Count > 0)
			{
				var tuple = _UnsentMessages.Dequeue();
				InternalLogger.Info("publishing unsent message: {0}.", tuple);
				Publish(tuple.Item1, tuple.Item2, tuple.Item3);
			}
		}

		private void Publish(byte[] bytes, IBasicProperties basicProperties, string routingKey)
		{
			_Model.BasicPublish(_Exchange, routingKey, true, false, basicProperties, bytes);
		}

		private string GetTopic(LogEventInfo eventInfo)
		{
			var routingKey = _Topic.Render(eventInfo);
			routingKey = routingKey.Replace("{0}", eventInfo.Level.Name);
			return routingKey;
		}

		private byte[] GetMessage(LogEventInfo info)
		{
			return _Encoding.GetBytes(MessageFormatter.GetMessageInner(_UseJSON, Layout, info));
		}

		private IBasicProperties GetBasicProperties(LogEventInfo loggingEvent)
		{
			var @event = loggingEvent;

			return new BasicProperties
				{
					ContentEncoding = "utf8",
					ContentType = _UseJSON ? "application/json" : "text/plain",
					AppId = AppId ?? @event.LoggerName,
					Timestamp = new AmqpTimestamp(MessageFormatter.GetEpochTimeStamp(@event)),
					UserId = UserName // support Validated User-ID (see http://www.rabbitmq.com/extensions.html)
				};
		}

	    private readonly object _ConnectionLock = new object();
		/// <summary>
		/// Never throws, blocks until connection is established
		/// </summary>
		private void StartConnection()
		{
			int connectionAttempts = 0;
			while (!Closed)
			{
				connectionAttempts++;
				// Shared lock with ShutdownAmqp, since they both access _Model and _Connection
				lock (this._ConnectionLock)
				{
					if (Closed) return; // double checking to make sure we don't open a connection during shutdown
					// If we already have an open Model, don't open a new one
					if (_Model != null && _Model.IsOpen) return;

					try
					{
						while (_Connection == null || !_Connection.IsOpen)
						{
							// If the connection is closed, dispose of it to make sure resources are released immediately
							if (_Connection != null)
							{
								try
								{
									_Connection.Dispose();
								}
								catch (OperationInterruptedException)
								{
								}
								catch (IOException)
								{
								}
								_Connection = null;
							}
							// If we need a new connection, we need a new model, so dispose of the existing model
							if (_Model != null)
							{
								_Model.Dispose();
								_Model = null;
							}

							// Open a new connection
							ConnectionFactory factory = GetConnectionFac();
							try
							{
								_Connection = factory.CreateConnection();
							}
							catch (BrokerUnreachableException)
							{
								InternalLogger.Error(
									"Could not reach RabbitMQ broker at {0}, attempt {1}",
									factory.HostName,
									connectionAttempts);
							}
						}

						try
						{
							_Model = _Connection.CreateModel();
							_Model.ExchangeDeclare(_Exchange, ExchangeType.Topic, _Durable);
						}
						catch (Exception e)
						{
							InternalLogger.Error("Could not create model, {0}", e);
							if (_Model != null)
							{
								_Model.Dispose();
								_Model = null;
							}
						}
					}
					catch (Exception e)
					{
						InternalLogger.Error(string.Format("Could not connect to Rabbit instance, {0}", e));
					}
				}
				// simple exponentially increasing wait time for each attempt, max at once a minute
				Thread.Sleep(Math.Min(250 * connectionAttempts * connectionAttempts, 60000));
			}
		}

		private ConnectionFactory GetConnectionFac()
		{
			return new ConnectionFactory
			{
				HostName = HostName,
				VirtualHost = VHost,
				UserName = UserName,
				Password = Password,
				RequestedHeartbeat = HeartBeatSeconds,
				Port = Port
			};
		}

		private void ShutdownAmqp()
		{
			// Shared lock with StartConnection
			lock (this._ConnectionLock)
			{
				try
				{
					if (_Model != null && _Model.IsOpen)
					{
						_Model.Abort();
						_Model = null;
					}
				}
				catch (Exception e)
				{
					InternalLogger.Error("Error closing model, {0}", e);
				}

				try
				{
					if (_Connection != null && _Connection.IsOpen)
					{
						_Connection.Close(Constants.ReplySuccess, "closing appender", 1000);
						_Connection.Abort(1000); // you get 2 seconds to shut down!
						_Connection.Dispose();
					}
					_Connection = null;
				}
				catch (Exception e)
				{
					InternalLogger.Error("Error closing connection, {0}", e);
				}
			}
		}

		// Dispose calls CloseTarget!
		protected bool Closed;
		protected override void CloseTarget()
		{
			if (!Closed)
			{
				Closed = true;
				ShutdownAmqp();
			}

			base.CloseTarget();
		}
	}
}