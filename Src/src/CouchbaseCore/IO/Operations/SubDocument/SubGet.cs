using System.Linq;
using Couchbase.Core;
using Couchbase.Core.Serialization;
using Couchbase.Core.Transcoders;

namespace Couchbase.IO.Operations.SubDocument
{
    internal class SubGet<T> : SubDocSingularLookupBase<T>
    {
        public SubGet(LookupInBuilder<T> builder, string key, IVBucket vBucket, ITypeTranscoder transcoder, uint timeout)
            : base(builder, key, vBucket, transcoder, timeout)
        {
            CurrentSpec = builder.FirstSpec();
            Path = CurrentSpec.Path;
        }

        public override OperationCode OperationCode
        {
            get { return OperationCode.SubGet; }
        }
    }
}
