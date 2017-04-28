// Connection.cs
//
//Copyright © 2006 - 2012 Dieter Lunn
//Modified 2012 Paul Freund ( freund.paul@lvl3.org )
//
//This library is free software; you can redistribute it and/or modify it under
//the terms of the GNU Lesser General Public License as published by the Free
//Software Foundation; either version 3 of the License, or (at your option)
//any later version.
//
//This library is distributed in the hope that it will be useful, but WITHOUT
//ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
//FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
//
//You should have received a copy of the GNU Lesser General Public License along
//with this library; if not, write to the Free Software Foundation, Inc., 59
//Temple Place, Suite 330, Boston, MA 02111-1307 USA

using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Windows.Foundation;
using Windows.Networking;
using Windows.Networking.Sockets;
using Windows.Security.Cryptography;
using Windows.Storage.Streams;
using XMPP.compression;
using XMPP.extensions;
using XMPP.registries;
using XMPP.states;

namespace XMPP.common
{
    #region helper

    static class Repeat
    {
        public static Task Interval(TimeSpan pollInterval, Action action, CancellationToken token)
        {
            return Task.Factory.StartNew(() => {
                for (; ; )
                {
                    if (token.WaitCancellationRequested(pollInterval))
                        break;

                    action();
                }
            }, token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }
    }

    static class CancellationTokenExtensions
    {
        public static bool WaitCancellationRequested(
            this CancellationToken token,
            TimeSpan timeout)
        {
            return token.WaitHandle.WaitOne(timeout);
        }
    }

    #endregion

    public class Connection : IConnection
    {
        public Connection(Manager manager) { _manager = manager; }

        public void Dispose() { Dispose(true); }

        virtual protected void Dispose(bool managed)
        {
            _elevateMutex.Dispose();
        }

        #region properties

        public bool IsConnected { get; private set; }
        public string Hostname { get { return _manager.Settings.Hostname; } }
        public bool IsSSLEnabled { get; set; }

        #endregion

        #region member

        private readonly Manager _manager;

        private const int               _bufferSize = 64*1024; // This is the maximal TCP packet size
		private HostName                _hostname = null;
		private readonly UTF8Encoding   _encoding = new UTF8Encoding();
		private ICompression            _compression = null;
		private bool                    _isCompressionEnabled = false;

        private string _socketWriteMessage = string.Empty;
        private IBuffer _socketReadBuffer = new Windows.Storage.Streams.Buffer(_bufferSize);

        private readonly StreamSocket _socket = new StreamSocket();

        IAsyncAction _socketConnector = null;
        IAsyncAction _socketElevator = null;
        IAsyncOperationWithProgress<IBuffer, UInt32> _socketReader = null;
        IAsyncOperationWithProgress<UInt32, UInt32> _socketWriter = null;

        private ManualResetEvent _elevateMutex = new ManualResetEvent(true);

        #endregion

        #region actions

        public void Connect()
        {
            SocketConnect();
        }

        public void Disconnect()
        {
            SocketDisconnect();
        }

        public void Send(tags.Tag tag)
        {
            // Remove comments
            var manipulationCopy = new XElement(tag);
            var descendants = manipulationCopy.DescendantNodesAndSelf();
            var comments = descendants.Where(node => node.NodeType == System.Xml.XmlNodeType.Comment);
            while (comments.Count() > 0)
                comments.First().Remove();

            this.Send(manipulationCopy.ToString());
        }

        public void Send(string message)
        {
            SocketSend(message);
        }

        public void EnableSSL()
        {
            if( !_manager.Settings.OldSSL )
                SocketElevate();
        }

        public void EnableCompression(string algorithm)
        {
            StartCompression(algorithm);
        }

        #endregion

        #region socket operations

        private void SocketConnect()
        {
            if (IsConnected)
            {
#if DEBUG
                _manager.Events.LogMessage(this, LogType.Warn, "Already connected");
#endif
                return;
            }

            if (_socketConnector != null && _socketConnector.Status == AsyncStatus.Started)
            {
#if DEBUG
                _manager.Events.LogMessage(this, LogType.Warn, "Already connecting");
#endif
                return;
            }

            CleanupState();

            try
            {
                _hostname = new HostName(_manager.Settings.Hostname);
            }
            catch
            {
                ConnectionError(ErrorType.InvalidHostName, ErrorPolicyType.Deactivate);
                return;
            }

            _socket.Control.KeepAlive = false;

            var protection = _manager.Settings.OldSSL ? SocketProtectionLevel.SslAllowNullEncryption : SocketProtectionLevel.PlainSocket;
            _socketConnector = _socket.ConnectAsync(_hostname, _manager.Settings.Port.ToString(), protection);
            _socketConnector.Completed = OnSocketConnectorCompleted;
        }

        private void SocketDisconnect()
        {
            if (IsConnected)
            {
                SocketSend("</stream:stream>", true);
            }
#if DEBUG
            else
            {
                _manager.Events.LogMessage(this, LogType.Warn, "Already disconnected");
            }
#endif

            CleanupState();

            _manager.Events.Disconnected(this);
        }

        private void SocketSend(string message, bool synchronized = false)
        {
            if (!IsConnected || string.IsNullOrEmpty(message) ) return;

            // Prepare message
            _socketWriteMessage = message;
            byte[] writeBytes = _encoding.GetBytes(message);

            if( _isCompressionEnabled )
                writeBytes = _compression.Deflate(writeBytes);

            IBuffer sendBuffer = CryptographicBuffer.CreateFromByteArray(writeBytes);

            if (synchronized) // Wait for completion
            {
                _elevateMutex.WaitOne(-1);
                _socket.OutputStream.WriteAsync(sendBuffer).AsTask().Wait(-1);
            }
            else // wait for last task and start new one
            {
                // Wait for other reads to finish 
                if (_socketWriter != null && _socketWriter.Status == AsyncStatus.Started)
                {
                    try
                    {
                        _socketWriter.AsTask().Wait(-1);
                    }
                    catch
                    {
                        //if (_socketWriter.Status == AsyncStatus.Started)
                        //{
                        //    ConnectionError(ErrorType.WriteStateMismatch, ErrorPolicyType.Reconnect);
                        //    return;
                        //}
                    }
                }

                _elevateMutex.WaitOne(-1);

                if (IsConnected)
                {
                    _socketWriter = _socket.OutputStream.WriteAsync(sendBuffer);
                    _socketWriter.Completed = OnSocketWriterCompleted;
                }
            }
		}

        private void SocketRead()
		{
            try
            {

                if (!IsConnected) return;
                _elevateMutex.WaitOne(-1);

                _socketReader = _socket.InputStream.ReadAsync(_socketReadBuffer, _bufferSize, InputStreamOptions.Partial);
                _socketReader.Completed = OnSocketReaderCompleted;
            }
            catch
            {
                ConnectionError(ErrorType.ConnectToServerFailed, ErrorPolicyType.Reconnect);
            }
		}

        private void SocketElevate()
        {
            if (!IsConnected) return;

            _elevateMutex.Reset();

            if (_socketReader != null) 
                _socketReader.Cancel();

            // Wait for other reads to finish 
            if (_socketWriter != null && _socketWriter.Status == AsyncStatus.Started)
            {
                try
                {
                    _socketWriter.AsTask().Wait(-1);
                }
                catch
                {
                    if (_socketWriter.Status == AsyncStatus.Started)
                    {
                        ConnectionError(ErrorType.WriteStateMismatch, ErrorPolicyType.Reconnect);
                        return;
                    }
                }
            }

            _socketElevator = _socket.UpgradeToSslAsync(SocketProtectionLevel.SslAllowNullEncryption, _hostname);
            _socketElevator.Completed = OnSocketElevatorCompleted;                
        }

        private void CleanupState()
        {
            IsConnected = false;

            _manager.ProcessComplete.Set();

            _elevateMutex.Set();

            _manager.Parser.Clear();

            if (_socketConnector != null) _socketConnector.Cancel();
            if (_socketElevator != null) _socketElevator.Cancel();
            if (_socketReader != null) _socketReader.Cancel();
            if (_socketWriter != null) _socketWriter.Cancel();
        }

        private void ConnectionError(ErrorType type, ErrorPolicyType policy, string cause = "")
        {
            CleanupState();
            _manager.Events.Error(this, type, policy, cause);
        }

        private void StartCompression(string algorithm)
        {
            _compression = Static.CompressionRegistry.GetCompression(algorithm);
            _isCompressionEnabled = true;
        }

        #endregion

        #region events

        private void OnSocketConnectorCompleted(IAsyncAction action, AsyncStatus status)
        {
            if (status == AsyncStatus.Completed)
            {
                IsConnected = true;

                if (_manager.Settings.UseKeepAlive)
                {
                    // Create keepalive check
                    var cancellationTokenSource = new CancellationTokenSource();
                    var task = Repeat.Interval(
                        TimeSpan.FromSeconds(_manager.Settings.KeepAliveTime),
                        () => OnKeepAlive(),
                        cancellationTokenSource.Token
                    );
                }

                // Signal that we have a connection
                _manager.Events.Connected(this);

                // Start receiving data
                SocketRead();

                _manager.State = new ConnectedState(_manager);
                _manager.State.Execute();
            }
            else if (status == AsyncStatus.Error )
            {
                ConnectionError(ErrorType.ConnectToServerFailed, ErrorPolicyType.Reconnect, action.ErrorCode.Message);
            }
        }

        private void OnSocketReaderCompleted(IAsyncOperationWithProgress<IBuffer, uint> asyncInfo, AsyncStatus asyncStatus)
        {
            if (asyncStatus == AsyncStatus.Completed)
            {
                _manager.ProcessComplete.Reset();
                
                IBuffer readBuffer = asyncInfo.GetResults();

                if (readBuffer.Length == 0)
                {
                    // This is not neccessarily an error, it can be just fine
                    //ConnectionError(ErrorType.ServerDisconnected, "Server sent empty package");
                    return;
                }

                // Get data
                byte[] readBytes = new byte[readBuffer.Length];
                DataReader dataReader = DataReader.FromBuffer(readBuffer);
                dataReader.ReadBytes(readBytes);
                dataReader.DetachBuffer();

                // Check if it is a keepalive
                if ( !(readBytes.Length == 1 && (readBytes[0] == 0 || readBytes[0] == ' ') ) )
                {
                    // Trim
                    readBytes = readBytes.TrimNull();

                    if (readBytes == null || readBytes.Length == 0)
                    {
                        ConnectionError(ErrorType.ServerDisconnected, ErrorPolicyType.Reconnect, "Server sent empty package");
                        return;
                    }

                    // Decompress
                    if (_isCompressionEnabled)
                        readBytes = _compression.Inflate(readBytes, readBytes.Length);

                    // Encode to string
                    string data = _encoding.GetString(readBytes, 0, readBytes.Length);

                    // Add to parser
#if DEBUG
                    _manager.Events.LogMessage(this, LogType.Debug, "Incoming data: {0}", data);
#endif

                    _manager.Parser.Parse(data);
                }

                _manager.ProcessComplete.Set();

                SocketRead();
            }
            else if (asyncStatus == AsyncStatus.Error)
            {
                ConnectionError(ErrorType.SocketReadInterrupted, ErrorPolicyType.Reconnect);
                return;
            }
        }

        private void OnSocketWriterCompleted(IAsyncOperationWithProgress<uint, uint> asyncInfo, AsyncStatus asyncStatus)
        {
            if (asyncStatus == AsyncStatus.Completed)
            {
#if DEBUG
                _manager.Events.LogMessage(this, LogType.Debug, "Outgoing Message: {0}", _socketWriteMessage);
#endif
                _socketWriteMessage = "";
            }
            else if (asyncStatus == AsyncStatus.Error)
            {
                ConnectionError(ErrorType.SocketWriteInterrupted, ErrorPolicyType.Reconnect);
            }

        }

        private void OnSocketElevatorCompleted(IAsyncAction asyncInfo, AsyncStatus asyncStatus)
        {
            if (asyncStatus == AsyncStatus.Completed)
            {
                _elevateMutex.Set();
                SocketRead();
            }
            else if (asyncStatus == AsyncStatus.Error)
            {
                if (asyncInfo.ErrorCode.HResult == -2146762487) // Certificate invalid
                    ConnectionError(ErrorType.InvalidSSLCertificate, ErrorPolicyType.Deactivate);
                else if( asyncInfo.ErrorCode.HResult == -2146762481 )// CN MISMATCH 
                    ConnectionError(ErrorType.InvalidSSLCertificate, ErrorPolicyType.Deactivate, "The server sent a SSL certificate with a wrong CN entry");
                else
                    ConnectionError(ErrorType.InvalidSSLCertificate, ErrorPolicyType.Deactivate, asyncInfo.ErrorCode.Message);
            }

        } 

        private void OnKeepAlive()
        {
            Send(" ");
            System.Diagnostics.Debug.WriteLine(DateTime.Now.ToString("MM/dd/yyyy hh:mm:tt") + " KeepAlive");
        }

        #endregion
    }
}