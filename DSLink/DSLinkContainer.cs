using System;
using System.Threading.Tasks;
using DSLink.Connection;
using Newtonsoft.Json.Linq;
using DSLink.Request;
using DSLink.Respond;
using DSLink.Logging;

namespace DSLink
{
    public class DSLinkContainer
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private Task _pingTask;
        private Handshake _handshake;
        private bool _reconnectOnFailure;
        private bool _isLinkInitialized;
        private readonly Configuration _config;
        private readonly DSLinkResponder _responder;
        private readonly DSLinkRequester _requester;
        private Connector _connector;

        public Configuration Config => _config;
        public virtual Responder Responder => _responder;
        public virtual DSLinkRequester Requester => _requester;
        public virtual Connector Connector => _connector;

        public DSLinkContainer(Configuration config)
        {
            _config = config;

            InitConnector();

            if (Config.Responder)
            {
                _responder = new DSLinkResponder(this);
                _responder.Init();
            }

            if (Config.Requester)
            {
                _requester = new DSLinkRequester(this);
                _requester.Init();
            }
        }

        /// <summary>
        /// Initializes the DSLink's node structure by building, or
        /// loading from disk when the link has been ran before.
        /// </summary>
        public async Task Initialize()
        {
            if (_isLinkInitialized)
            {
                return;
            }

            _isLinkInitialized = true;

            await _config._initKeyPair();

            _pingTask = new Task(OnPingTaskElapsed);
            _pingTask.Start();

            if (Config.Responder)
            {
                var initDefault = true;
                if (Config.LoadNodesJson)
                {
                    initDefault = !(await LoadSavedNodes());
                }

                if (initDefault)
                {
                    InitializeDefaultNodes();
                }
            }
        }

        /// <summary>
        /// Used to initialize the node structure when nodes.json does not
        /// exist yet or failed to load.
        /// </summary>
        public virtual void InitializeDefaultNodes()
        {
        }

        public async Task<ConnectionState> Connect(uint maxAttempts = 0)
        {
            await Initialize();

            _reconnectOnFailure = true;
            _handshake = new Handshake(this);
            var attemptsLeft = maxAttempts;
            uint attempts = 1;
            while (maxAttempts == 0 || attemptsLeft > 0)
            {
                _config.RemoteEndpoint = await _handshake.Shake();
                if (_config.RemoteEndpoint != null)
                {
                    await Connector.Connect();
                    return Connector.ConnectionState;
                }

                var delay = attempts;
                if (delay > Config.MaxConnectionCooldown)
                {
                    delay = Config.MaxConnectionCooldown;
                }

                Logger.Warn($"Failed to connect, delaying for {delay} seconds");
                await Task.Delay(TimeSpan.FromSeconds(delay));

                if (attemptsLeft > 0)
                {
                    attemptsLeft--;
                }

                attempts++;
            }

            Logger.Warn("Failed to connect within the allotted connection attempt limit.");
            OnConnectionFailed();
            return ConnectionState.Disconnected;
        }

        public void Disconnect()
        {
            _reconnectOnFailure = false;
            Connector.Disconnect();
        }

        public async Task<bool> LoadSavedNodes()
        {
            if (_responder == null)
            {
                throw new DSAException(this, "Responder is not enabled.");
            }

            return await Responder.DiskSerializer.DeserializeFromDisk();
        }

        public async Task SaveNodes()
        {
            if (_responder == null)
            {
                throw new DSAException(this, "Responder is not enabled.");
            }

            await Responder.DiskSerializer.SerializeToDisk();
        }

        private async void OnOpen()
        {
            await Connector.Flush();
        }

        private async void OnClose()
        {
            if (Responder != null)
            {
                Responder.SubscriptionManager.ClearAll();
                Responder.StreamManager.ClearAll();
            }

            if (_reconnectOnFailure)
            {
                InitConnector();
                await Connect();
                OnConnectorReconnected();
            }
        }

        /// <summary>
        /// Called when the connection is opened to the broker.
        /// Override when you need to do something after connection opens.
        /// </summary>
        protected virtual void OnConnectionOpen()
        {
        }

        /// <summary>
        /// Called when the connection is closed to the broker.
        /// Override when you need to do something after connection closes.
        /// </summary>
        protected virtual void OnConnectionClosed()
        {
        }

        /// <summary>
        /// Called when the connection fails to connect to the broker.
        /// Override when you need to detect a failure to connect.
        /// </summary>
        protected virtual void OnConnectionFailed()
        {
        }

        /// <summary>
        /// Called when the connection reconnects to the broker.
        /// Override when you need to resubscribe.
        /// </summary>
        protected virtual void OnConnectorReconnected()
        {
        }

        private async Task OnMessage(JObject message)
        {
            var response = new JObject();
            if (message["msg"] != null)
            {
                response["ack"] = message["msg"].Value<int>();
            }

            var write = false;

            if (message["requests"] != null)
            {
                var responses = await Responder.ProcessRequests(message["requests"].Value<JArray>());
                if (responses.Count > 0)
                {
                    response["responses"] = responses;
                }

                write = true;
            }

            if (message["responses"] != null)
            {
                await Requester.ProcessResponses(message["responses"].Value<JArray>());
                write = true;
            }

            if (write)
            {
                await Connector.Write(response);
            }
        }

        private void InitConnector()
        {
            _reconnectOnFailure = true;
            _connector = new WebSocketConnector(_config);

            // Connector events
            _connector.OnMessage += OnStringRead;
            _connector.OnBinaryMessage += OnBinaryRead;
            _connector.OnWrite += OnStringWrite;
            _connector.OnBinaryWrite += OnBinaryWrite;
            _connector.OnOpen += OnOpen;
            _connector.OnClose += OnClose;

            // Overridable events for DSLink writers
            _connector.OnOpen += OnConnectionOpen;
            _connector.OnClose += OnConnectionClosed;
        }

        private async void OnStringRead(MessageEvent messageEvent)
        {
            LogMessageString(false, messageEvent);
            await OnMessage(Connector.DataSerializer.Deserialize(messageEvent.Message));
        }

        private void OnStringWrite(MessageEvent messageEvent)
        {
            LogMessageString(true, messageEvent);
        }

        private async void OnBinaryRead(BinaryMessageEvent messageEvent)
        {
            LogMessageBytes(false, messageEvent);
            await OnMessage(Connector.DataSerializer.Deserialize(messageEvent.Message));
        }

        private void OnBinaryWrite(BinaryMessageEvent messageEvent)
        {
            LogMessageBytes(true, messageEvent);
        }

        private void LogMessageString(bool sent, MessageEvent messageEvent)
        {
            if (Logger.IsDebugEnabled())
            {
                var verb = sent ? "Sent" : "Received";
                var logString = $"Text {verb}: {messageEvent.Message}";
                Logger.Debug(logString);
            }
        }

        private void LogMessageBytes(bool sent, BinaryMessageEvent messageEvent)
        {
            if (Logger.IsDebugEnabled())
            {
                var verb = sent ? "Sent" : "Received";
                var logString = $"Binary {verb}: ";
                if (messageEvent.Message.Length < 5000)
                {
                    logString += BitConverter.ToString(messageEvent.Message);
                }
                else
                {
                    logString += "(over 5000 bytes)";
                }

                Logger.Debug(logString);
            }
        }

        private async void OnPingTaskElapsed()
        {
            while (_pingTask.Status != TaskStatus.Canceled)
            {
                if (Connector.Connected())
                {
                    // Write a blank message containing no responses/requests.
                    await Connector.Write(new JObject(), false);
                }

                // Delay thirty seconds until the next ping.
                await Task.Delay(30000);
            }
        }
    }
}