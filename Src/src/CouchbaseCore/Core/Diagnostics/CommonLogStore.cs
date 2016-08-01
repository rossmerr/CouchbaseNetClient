using Microsoft.Extensions.Logging;

namespace Couchbase.Core.Diagnostics
{
    public class CommonLogStore : ITimingStore
    {
        private readonly ILogger _log;
        public CommonLogStore(ILoggerFactory loggerFactory)
        {
            _log = loggerFactory.CreateLogger<CommonLogStore>();
        }

        public void Write(string format, params object[] args)
        {
            _log.LogInformation(format, args);
        }

        public bool Enabled
        {
            get { return _log != null && _log.IsEnabled(LogLevel.Debug); }
        }
    }
}
