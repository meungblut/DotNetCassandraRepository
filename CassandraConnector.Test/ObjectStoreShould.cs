using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using NUnit.Framework;

namespace CassandraRepository.Test
{
    public class ObjectStoreShould
    {
        private ClusterConfiguration config;
        private SingletonCassandraSessionFactory sessionFactory;
        private ObjectStore _objectStore;
        private Guid _entityId;

        [SetUp]
        public void ConfigureCluster()
        {
            config = new ClusterConfiguration("cassandra", "cassandra", "192.168.99.100", 32896);
            sessionFactory = new SingletonCassandraSessionFactory(config);
            CreateKeyspaceAndTable();
            _objectStore = new ObjectStore(sessionFactory);

            _entityId = Guid.NewGuid();
        }

        [Test]
        public async Task AllowSavingAndRetrievingOfObjectsByEntryId()
        {
            var domainObject = new DummyDomainObject(Guid.NewGuid().ToString());
            var entryId = Guid.NewGuid();

            await _objectStore.Save(_entityId, entryId, domainObject, ConsistencyLevel.Any);

            var retrievedObject = (DummyDomainObject)await _objectStore.Get(entryId, ConsistencyLevel.One);

            Assert.AreEqual(retrievedObject.Data, domainObject.Data);
        }

        [Test]
        public async Task AllowSavingAndRetrievingOfMultipleItemsByEntityId()
        {
            for (int i = 0; i < 10000; i++)
                await SaveItem1();

            var retrievedItems = await _objectStore.GetAll(_entityId, ConsistencyLevel.One);

            Assert.AreEqual(10000, retrievedItems.Count());
        }

        [Test]
        public async Task AllowSavingAndRetrievingOfItemsOfDifferentTypes()
        {
            for (int i = 0; i < 1000; i++)
                if (i%2 == 0)
                    await SaveItem1();
                        else
                    await SaveItem2();

            var retrievedItems = await _objectStore.GetAll(_entityId, ConsistencyLevel.One);

            Assert.AreEqual(500, retrievedItems.Count(x => x is DummyDomainObject));
            Assert.AreEqual(500, retrievedItems.Count(x => x is SecondDummyDomainObject));

        }

        private async Task SaveItem1()
        {
            var domainObject = new DummyDomainObject(Guid.NewGuid().ToString());
            var entryId = Guid.NewGuid();

            await _objectStore.Save(_entityId, entryId, domainObject, ConsistencyLevel.Any);
        }

        private async Task SaveItem2()
        {
            var domainObject = new SecondDummyDomainObject(Guid.NewGuid().ToString());
            var entryId = Guid.NewGuid();

            await _objectStore.Save(_entityId, entryId, domainObject, ConsistencyLevel.Any);
        }

        private void CreateKeyspaceAndTable()
        {
            var session = sessionFactory.GetSession();

            var createKeyspace = "CREATE KEYSPACE IF NOT EXISTS Repository WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
            session.Execute(createKeyspace);

            session.Execute("DROP TABLE IF EXISTS Repository.Items");

            string createTable = @"CREATE TABLE IF NOT EXISTS Repository.Items (
                entity_id text,
                message_id text,
                message_type text,
                node_written_by bigint,
                message blob,
                PRIMARY KEY((entity_id), message_id))";

            session.Execute(createTable);

            session.Execute("CREATE INDEX messageId ON Repository.Items(message_id)");
        }
    }
}