using System;
using System.Net.Security;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;
using Couchbase.Configuration.Client;
using Couchbase.IO.Converters;
using Couchbase.IO.Utils;
using Microsoft.Extensions.Logging;

namespace Couchbase.IO
{
    /// <summary>
    /// A factory creator for <see cref="IConnection"/>s
    /// </summary>
    public static class DefaultConnectionFactory
    {


        /// <summary>
        /// Returns a functory for creating <see cref="Connection"/> objects.
        /// </summary>
        /// <returns>A <see cref="Connection"/> based off of the <see cref="PoolConfiguration"/> of the <see cref="IConnectionPool"/>.</returns>
        internal static Func<ConnectionPool<T>, IByteConverter, BufferAllocator, T> GetGeneric<T>(
            ILoggerFactory loggerFactory)
            where T : class, IConnection
        {
            Func<IConnectionPool<T>, IByteConverter, BufferAllocator, T> factory = (p, c, b) =>
            {
                System.Console.WriteLine("1.3.2.1");
                var socket = new Socket(p.EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                System.Console.WriteLine("1.3.2.2");
                var waitHandle = new ManualResetEvent(false);
                System.Console.WriteLine("1.3.2.3");
                var asyncEventArgs = new SocketAsyncEventArgs
                {
                    RemoteEndPoint = p.EndPoint
                };

                System.Console.WriteLine("1.3.2.4");
                asyncEventArgs.Completed += delegate { waitHandle.Set(); };
                System.Console.WriteLine("1.3.2.5");
                if (socket.ConnectAsync(asyncEventArgs))
                {
                    // True means the connect command is running asynchronously, so we need to wait for completion
                    System.Console.WriteLine("1.3.2.6");
                    if (!waitHandle.WaitOne(p.Configuration.ConnectTimeout))
                    {
                        System.Console.WriteLine("1.3.2.7");
                        socket.Dispose();
                        System.Console.WriteLine("1.3.2.8");
                        const int connectionTimedOut = 10060;
                        throw new SocketException(connectionTimedOut);
                    }
                }
                System.Console.WriteLine("1.3.2.9");
                if ((asyncEventArgs.SocketError != SocketError.Success) || !socket.Connected)
                {
                    System.Console.WriteLine("1.3.2.10");
                    socket.Dispose();
                    System.Console.WriteLine("1.3.2.11");
                    throw new SocketException((int) asyncEventArgs.SocketError);
                }
                System.Console.WriteLine("1.3.2.12");
                IConnection connection;
                if (p.Configuration.UseSsl)
                {
                    System.Console.WriteLine("1.3.2.13");
                    connection = new SslConnection(p, socket, c, b, loggerFactory);
                    System.Console.WriteLine("1.3.2.14");
                    connection.Authenticate();
                }
                else
                {
                    System.Console.WriteLine("1.3.2.15");
                    try
                    {
                        System.Console.WriteLine(typeof(T));
                        System.Console.WriteLine(p);
                        System.Console.WriteLine(socket);
                        System.Console.WriteLine(c);
                        System.Console.WriteLine(b);
                        System.Console.WriteLine(loggerFactory);

                        connection = Activator.CreateInstance(typeof(T), p, socket, c, b, loggerFactory) as T;

                    }
                    catch (Exception ex)
                    {
                        System.Console.WriteLine(ex);
                        throw;
                    }
                }
                System.Console.WriteLine("1.3.2.16");
                //need to be able to completely disable the feature if false - this should work
                if (p.Configuration.EnableTcpKeepAlives)
                {
                    System.Console.WriteLine("1.3.2.17");
                    socket.SetKeepAlives(p.Configuration.EnableTcpKeepAlives,
                        p.Configuration.TcpKeepAliveTime,
                        p.Configuration.TcpKeepAliveInterval);
                }
                System.Console.WriteLine("1.3.2.18");
                return connection as T;
            };
            return factory;
        }
    }
}

#region [ License information          ]

/* ************************************************************
 *
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2014 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 * ************************************************************/

#endregion
