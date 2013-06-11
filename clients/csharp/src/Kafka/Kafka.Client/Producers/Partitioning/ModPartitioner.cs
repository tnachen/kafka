using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Client.Producers.Partitioning
{
    public class ModPartitioner : IPartitioner<string>
    {
        public int Partition(string key, int numPartitions)
        {
            return key.GetHashCode() % numPartitions;
        }
    }
}
