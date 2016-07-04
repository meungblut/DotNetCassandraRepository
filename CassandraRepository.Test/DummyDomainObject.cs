namespace CassandraRepository.Test
{
    public class DummyDomainObject
    {
        public DummyDomainObject(string data)
        {
            Data = data;
        }
        public string Data { get; }
    }
}
