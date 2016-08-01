using System;
using System.Diagnostics;
using System.Net.Http;
using System.Reflection;
using Couchbase.Configuration;
using Couchbase.Configuration.Client;
using Couchbase.Configuration.Server.Providers.Streaming;
using Couchbase.Configuration.Server.Serialization;
using Couchbase.Core;
using Couchbase.Management;
using Couchbase.Utils;
using Couchbase.Views;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

#if NET45
using Couchbase.Configuration.Client.Providers;
#endif

namespace Couchbase
{
    /// <summary>
    /// The client interface to a Couchbase Server Cluster.
    /// </summary>
    public sealed class Cluster : ICluster
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger Log;
        private const string DefaultBucket = "default";
        private readonly ClientConfiguration _configuration;
        private readonly IClusterController _clusterController;
        private volatile bool _disposed;

        /// <summary>
        /// Ctor for creating Cluster instance using the default settings.
        /// </summary>
        /// <remarks>
        /// This is the default configuration and will attempt to bootstrap off of localhost.
        /// </remarks>
        public Cluster(ILoggerFactory loggerFactory)
            : this(new ClientConfiguration(loggerFactory), loggerFactory)
        {
        }


        /// <summary>
        /// Ctor for creating Cluster instance using an <see cref="ICouchbaseClientDefinition"/>.
        /// </summary>
        /// <param name="definition">The configuration definition loaded from a configuration file.</param>
        public Cluster(ICouchbaseClientDefinition definition, ILoggerFactory loggerFactory)
            : this(new ClientConfiguration(definition, loggerFactory), loggerFactory)
        {
        }

#if NET45

        /// <summary>
        /// Ctor for creating Cluster instance using an App.Config or Web.config.
        /// </summary>
        /// <param name="configurationSectionName">The name of the configuration section to use.</param>
        /// <remarks>Note that <see cref="CouchbaseClientSection"/> needs include the sectionGroup name as well: "couchbaseSection/couchbase" </remarks>
        public Cluster(string configurationSectionName)
            : this(new ClientConfiguration((CouchbaseClientSection)ConfigurationManager.GetSection(configurationSectionName)))
        {
        }

#endif

        /// <summary>
        /// Ctor for creating Cluster instance with a custom <see cref="ClientConfiguration"/> configuration.
        /// </summary>
        /// <param name="configuration">The ClientCOnfiguration to use for initialization.</param>
        public Cluster(ClientConfiguration configuration, ILoggerFactory loggerFactory)
        {
            // can't use ": this(" to call the other constructor because we need to pass "this" to the ClusterController constructor
            // so we have a bit of code duplication here
            _loggerFactory = loggerFactory;
            Log = _loggerFactory.CreateLogger<Cluster>();
            _configuration = configuration;
            _clusterController = new ClusterController(this, configuration, _loggerFactory);
            LogConfigurationAndVersion(_configuration);
        }

        /// <summary>
        /// Ctor for creating Cluster instance.
        /// </summary>
        /// <param name="configuration">The ClientCOnfiguration to use for initialization.</param>
        /// <param name="clusterController">The ClusterManager instance use.</param>
        /// <remarks>
        /// This overload is primarly added for testing.
        /// </remarks>
        internal Cluster(ClientConfiguration configuration, IClusterController clusterController)
        {
            _configuration = configuration;
            _clusterController = clusterController;
            LogConfigurationAndVersion(_configuration);
        }

        /// <summary>
        /// Opens the default bucket associated with a Couchbase Cluster.
        /// </summary>
        /// <returns>An instance which implements the IBucket interface with the
        /// default buckets configuration.</returns>
        /// <remarks>Use Cluster.CloseBucket(bucket) to release resources associated with a Bucket.</remarks>
        public IBucket OpenBucket()
        {
            return _clusterController.CreateBucket(DefaultBucket);
        }

        /// <summary>
        /// Creates a connection to a specific SASL authenticated Couchbase Bucket.
        /// </summary>
        /// <param name="bucketname">The Couchbase Bucket to connect to.</param>
        /// <param name="password">The SASL password to use.</param>
        /// <returns>An instance which implements the IBucket interface.</returns>
        /// <remarks>Use Cluster.CloseBucket(bucket) to release resources associated with a Bucket.</remarks>
        public IBucket OpenBucket(string bucketname, string password)
        {
            return _clusterController.CreateBucket(bucketname, password);
        }

        /// <summary>
        /// Creates a connection to a non-SASL Couchbase bucket.
        /// </summary>
        /// <param name="bucketname">The Couchbase Bucket to connect to.</param>
        /// <returns>An instance which implements the IBucket interface.</returns>
        /// <remarks>
        /// Use Cluster.CloseBucket(bucket) to release resources associated with a Bucket.
        /// </remarks>
        public IBucket OpenBucket(string bucketname)
        {
            if (string.IsNullOrWhiteSpace(bucketname))
            {
                if (bucketname == null)
                {
                    throw new ArgumentNullException("bucketname");
                }
                throw new ArgumentException("bucketname cannot be null, empty or whitespace.");
            }
            return _clusterController.CreateBucket(bucketname);
        }

        /// <summary>
        /// Closes and releases all resources associated with a Couchbase bucket.
        /// </summary>
        /// <param name="bucket">The Bucket to close.</param>
        public void CloseBucket(IBucket bucket)
        {
            if (bucket == null)
            {
                throw new ArgumentNullException("bucket");
            }
            _clusterController.DestroyBucket(bucket);
        }

        /// <summary>
        /// Creates a <see cref="IClusterManager"/> object that uses the current <see cref="ICluster"/> configuration settings.
        /// </summary>
        /// <returns>A <see cref="IClusterManager"/> instance that uses the current <see cref="ICluster"/> configuration settings. </returns>
        public IClusterManager CreateManager(string username, string password)
        {
            var serverConfig = new HttpServerConfig(Configuration, username, password, _loggerFactory);
            try
            {
                serverConfig.Initialize();
            }
            catch (BootstrapException e)
            {
                //if initializing a new cluster, we won't be able to bootstrap
                //so Initialize will fail; you can still use the REST API methods
                //that do not depend upon the API exposed by the config
                Log.LogInformation(e.Message);
            }

            return new ClusterManager(Configuration,
                serverConfig,
                new HttpClient(),
                new JsonDataMapper(Configuration),
                username,
                password, _loggerFactory);
        }

        /// <summary>
        /// Returns an object representing cluster status information.
        /// </summary>
        [Obsolete("Use CreateManager(user, password).ClusterInfo() instead")]
        public IClusterInfo Info
        {
            get { return _clusterController.Info(); }
        }

        /// <summary>
        /// The current client configuration being used by the <see cref="Cluster"/> object.
        /// Set this by passing in a <see cref="ClientConfiguration"/> object into <see cref="Initialize(ClientConfiguration)" /> or by
        /// providing a <see cref="CouchbaseClientSection"/> in your App.config or Web.config and calling <see cref="Initialize(string)"/>
        /// </summary>
        public ClientConfiguration Configuration
        {
            //TODO returned cloned copy?
            get { return _configuration; }
        }

        /// <summary>
        /// Returns a response indicating whether or not the <see cref="IBucket"/> instance has been opened and this <see cref="Cluster"/> instance is observing it.
        /// </summary>
        /// <param name="bucketName">The name of the bucket to check.</param>
        /// <returns>True if the <see cref="IBucket"/> has been opened and the cluster is registered as an observer.</returns>
        public bool IsOpen(string bucketName)
        {
            return _clusterController.IsObserving(bucketName);
        }

        /// <summary>
        /// Closes and releases all internal resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            Log.LogDebug("Disposing {0}", GetType().Name);
        }

        void LogConfigurationAndVersion(ClientConfiguration configuration)
        {
            var version = CurrentAssembly.Version;
            Log.LogInformation("Version: {0}", version);

            try
            {
                var config = JsonConvert.SerializeObject(configuration);
                Log.LogInformation("Configuration: {0}", config);
            }
            catch (Exception e)
            {
                //NCBC-797
                Log.LogInformation("Could not serialize ClientConfiguration.", e);
            }
        }

        /// <summary>
        /// Disposes the Cluster object, calling GC.SuppressFinalize(this) if it's not called on the finalization thread.
        /// </summary>
        /// <param name="disposing">True if called by an explicit call to Dispose by the consuming application; false if called via finalization.</param>
        void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    GC.SuppressFinalize(this);
                }
                if (_clusterController != null)
                {
                    _clusterController.Dispose();
                }
                _disposed = true;
            }
        }

#if DEBUG
        /// <summary>
        /// Cleans up any non-reclaimed resources.
        /// </summary>
        /// <remarks>will run if Dispose is not called on a Cluster instance.</remarks>
        ~Cluster()
        {
            Dispose(false);
            Log.LogDebug("Finalizing {0}", GetType().Name);
        }
#endif
    }
}

#region [ License information ]

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
