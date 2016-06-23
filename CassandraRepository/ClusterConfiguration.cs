using System.ComponentModel;

namespace CassandraRepository
{
    public class ClusterConfiguration : IClusterConfiguration
    {
        public string Username { get; }
        public string Password { get; }
        public int Port { get; set; }
        public string Uri { get; }

        public ClusterConfiguration(string username, string password, string uri, int port)
        {
            Username = username;
            Password = password;
            Uri = uri;
            Port = port;
        }
    }
}
