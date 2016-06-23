namespace CassandraRepository.Test
{
    public class SecondDummyDomainObject
    {
        public SecondDummyDomainObject()
        {
        }

        public SecondDummyDomainObject(string data)
        {
            Data1 = data;
            Data2 = data;
        }

        public string Data1 { get; set; }
        public string Data2 { get; set; } 
    }
}