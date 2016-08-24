using System;
using Couchbase.Core.Transcoders;
using Couchbase.IO;
using Couchbase.IO.Converters;
using Couchbase.IO.Operations.Authentication;
using Microsoft.Extensions.Logging;

namespace Couchbase.Authentication.SASL
{
    /// <summary>
    /// Creates an ISaslMechanism to use for authenticating Couchbase Clients.
    /// </summary>
    internal static class SaslFactory
    {
       // private readonly static ILogger Log = LogManager.GetLogger("SaslFactory");

        /// <summary>
        /// The default timeout for SASL-related operations.
        /// </summary>
        public const uint DefaultTimeout = 2500; //2.5sec

        public static Func<string, string, IIOService, ITypeTranscoder, ISaslMechanism> GetFactory(ILoggerFactory loggerFactory)
        {
            return (username, password, service, transcoder) =>
            {
                var logger = loggerFactory.CreateLogger("Couchbase.Authentication.SASL." + nameof(SaslFactory));

                ISaslMechanism saslMechanism = null;
                IConnection connection = null;
                try
                {
                    System.Console.WriteLine("1");
                    connection = service.ConnectionPool.Acquire();
                    System.Console.WriteLine("2");
                    var saslListResult = service.Execute(new SaslList(transcoder, DefaultTimeout), connection);
                    System.Console.WriteLine("3");
                    if (saslListResult.Success)
                    {
                        System.Console.WriteLine("4");
                        if (saslListResult.Value.Contains("CRAM-MD5"))
                        {
                            System.Console.WriteLine("5");
                            saslMechanism = new CramMd5Mechanism(service, username, password, transcoder, loggerFactory);
                        }
                        else
                        {
                            System.Console.WriteLine("6");
                            saslMechanism = new PlainTextMechanism(service, username, password, transcoder, loggerFactory);
                        }
                        System.Console.WriteLine("7");
                    }
                }
                catch (Exception e)
                {
                    logger.LogError(e.Message, e);
                }
                finally
                {
                    if (connection != null)
                    {
                        System.Console.WriteLine("8");
                        service.ConnectionPool.Release(connection);
                    }
                }
                return saslMechanism;
            };
        }
    }
}
