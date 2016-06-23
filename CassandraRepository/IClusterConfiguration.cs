namespace CassandraRepository
{
    public interface IClusterConfiguration
    {
        string Password { get; }
        int Port { get; set; }
        string Uri { get; }
        string Username { get; }
    }
}