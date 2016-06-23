using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using MsgPack.Serialization;

namespace CassandraRepository
{
    public class ObjectStore
    {
        private readonly SingletonCassandraSessionFactory _sessionFactory;
        private readonly PreparedStatement _writeMessage;
        private readonly PreparedStatement _readSingleMessage;
        private readonly PreparedStatement _readMultipleMessages;

        private static string WriteCqlStatement = @"INSERT INTO Repository.Items 
                (entity_id, message_id, message_type, message)
                VALUES(?, ?, ?, ?)";

        private static string ReadSingleCqlStatement =
                @"SELECT message_type, message from Repository.Items
                  WHERE message_id = ?";

        private static string ReadMultipleCqlStatement =
            @"SELECT message_type, message from Repository.Items
              WHERE entity_id = ?";

        public ObjectStore(SingletonCassandraSessionFactory sessionFactory)
        {
            _sessionFactory = sessionFactory;
            _writeMessage = _sessionFactory.GetSession().Prepare(WriteCqlStatement);
            _readSingleMessage = _sessionFactory.GetSession().Prepare(ReadSingleCqlStatement);
            _readMultipleMessages = _sessionFactory.GetSession().Prepare(ReadMultipleCqlStatement);
        }

        public async Task Save(Guid entityId, Guid entryId, object domainObject, ConsistencyLevel level)
        {
            var typeInformation = domainObject.GetType().AssemblyQualifiedName;

            var serializer = SerializationContext.Default.GetSerializer(domainObject.GetType());
            var serialisedDate = serializer.PackSingleObject(domainObject);

            var statement =
                _writeMessage.Bind(entityId.ToString(), entryId.ToString(), typeInformation, serialisedDate)
                .SetConsistencyLevel(level);

            await _sessionFactory.GetSession().ExecuteAsync(statement);
        }

        public async Task<object> Get(Guid entryId, ConsistencyLevel consistency)
        {
            var readSingleMessage =
                _readSingleMessage.Bind(entryId.ToString()).SetConsistencyLevel(consistency);

            var lastSequenceRows = await _sessionFactory.GetSession().ExecuteAsync(readSingleMessage);

            var singleRow = lastSequenceRows.First();

            var deserialisedData = DeserialisedDataFromRow(singleRow);
            return deserialisedData;
        }

        public async Task<IEnumerable<object>> GetAll(Guid entityId, ConsistencyLevel consistency)
        {
            var readMultipleMessagesStatement =
                _readMultipleMessages.Bind(entityId.ToString()).SetConsistencyLevel(consistency);

            var lastSequenceRows = await _sessionFactory.GetSession().ExecuteAsync(readMultipleMessagesStatement);

            return lastSequenceRows.Select(DeserialisedDataFromRow).ToList();
        }

        private static object DeserialisedDataFromRow(Row singleRow)
        {
            string type = singleRow.GetValue<string>("message_type");
            byte[] data = singleRow.GetValue<byte[]>("message");

            var typeToConvertTo = Type.GetType(type);

            var serializer = SerializationContext.Default.GetSerializer(typeToConvertTo);
            var deserialisedData = serializer.UnpackSingleObject(data);
            return deserialisedData;
        }
    }
}