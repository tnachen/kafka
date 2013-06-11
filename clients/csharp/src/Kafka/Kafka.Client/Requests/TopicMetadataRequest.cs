/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.IO;
using Kafka.Client.Cluster;
using Kafka.Client.Exceptions;
using Kafka.Client.Producers;

namespace Kafka.Client.Requests
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Linq;
    using System.Text;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using Kafka.Client.Messages;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class TopicMetadataRequest : AbstractRequest, IWritable
    {
        private short versionId;
        private int correlationId;
        private string clientId;
        private const int DefaultNumberOfTopicsSize = 4;
        private const int DefaultDetailedMetadataSize = 2;
        private const int DefaultTimestampSize = 8;
        private const int DefaultCountSize = 4;
        private const byte DefaultHeaderSize8 = DefaultRequestSizeSize + DefaultRequestIdSize;

        public IEnumerable<string> Topics { get; private set; }

        public DetailedMetadataRequest DetailedMetadata { get; private set; }

        public long? Timestamp { get; private set; }

        private TopicMetadataRequest(IEnumerable<string> topics, short versionId, int correlationId, string clientId)
        {
            if(topics == null)
            {
                throw new ArgumentNullException("topics", "List of topics cannot be null.");
            }

            if(topics.Count() == 0)
            {
                throw new ArgumentException("List of topics cannot be empty.");
            }

            this.Topics = new List<string>(topics);
            this.versionId = versionId;
            this.correlationId = correlationId;
            this.clientId = clientId;
            int length = GetRequestLength();
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        /// <summary>
        /// Creates simple request with no segment metadata information
        /// </summary>
        /// <param name="topics">list of topics</param>
        /// <param name="versionId"></param>
        /// <param name="correlationId"></param>
        /// <param name="clientId"></param>
        /// <returns>request</returns>
        public static TopicMetadataRequest Create(IEnumerable<string> topics, short versionId, int correlationId, string clientId)
        {
            return new TopicMetadataRequest(topics, versionId, correlationId, clientId);
        }

        public override RequestTypes RequestType
        {
            get { return RequestTypes.TopicMetadataRequest; }
        }

        public void WriteTo(MemoryStream output)
        {
            Guard.NotNull(output, "output");

            using (var writer = new KafkaBinaryWriter(output))
            {
                writer.Write(this.RequestBuffer.Capacity - DefaultRequestSizeSize);
                writer.Write(this.RequestTypeId);
                this.WriteTo(writer);
            }
        }

        public void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.NotNull(writer, "writer");
            writer.Write(versionId);
            writer.Write(correlationId);
            writer.WriteShortString(clientId, AbstractRequest.DefaultEncoding);
            writer.Write(this.Topics.Count());
            foreach (var topic in Topics)
            {
                writer.WriteShortString(topic, AbstractRequest.DefaultEncoding);
            }            
        }

        public int GetRequestLength()
        {
            var size = DefaultHeaderSize8 +
                FetchRequest.DefaultVersionIdSize +
                FetchRequest.DefaultCorrelationIdSize +
                BitWorks.GetShortStringLength(this.clientId, DefaultEncoding) +
                DefaultNumberOfTopicsSize +
                this.Topics.Sum(x => BitWorks.GetShortStringLength(x, AbstractRequest.DefaultEncoding));
            
            return size;
        }

        public static IEnumerable<TopicMetadata> DeserializeTopicsMetadataResponse(KafkaBinaryReader reader)
        {
            int correlationId = reader.ReadInt32();
            int brokerCount = reader.ReadInt32();
            var brokers = new Broker[brokerCount];
            for (int i = 0; i < brokerCount; ++i)
            {
                brokers[i] = Broker.ParseFrom(reader);
            }

            var numTopics = reader.ReadInt32();
            var topicMetadata = new TopicMetadata[numTopics];
            for (int i = 0; i < numTopics; i++)
            {
                topicMetadata[i] = TopicMetadata.ParseFrom(reader);
            }
            return topicMetadata;
        }
    }

    public enum DetailedMetadataRequest : short
    {
        SegmentMetadata = (short)1,
        NoSegmentMetadata = (short)0,
    }
}
