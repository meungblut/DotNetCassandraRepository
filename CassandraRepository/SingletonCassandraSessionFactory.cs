using System;
using System.Net;
using Cassandra;

namespace CassandraRepository
{
    public class SingletonCassandraSessionFactory
    {
        private readonly IClusterConfiguration _configuration;

        private readonly Lazy<ISession> cassandraSession; 

        public SingletonCassandraSessionFactory(IClusterConfiguration configuration)
        {
            _configuration = configuration;
            cassandraSession = new Lazy<ISession>(BuildSession);
        }

        public ISession GetSession()
        {
            return cassandraSession.Value;
        }

        private ISession BuildSession()
        {
            var builder = Cluster.Builder();

            var endpoints = new[] { new IPEndPoint(IPAddress.Parse(_configuration.Uri), _configuration.Port) };
            builder.AddContactPoints(endpoints);

            builder.WithCredentials(_configuration.Username, _configuration.Password);
            return builder.Build().Connect();
        }
    }
}
