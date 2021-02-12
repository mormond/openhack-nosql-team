namespace Meo
{
    public class Order
    {
        public string id { get; set; }
        public string InsertRegion { get; set; }
        public string Email { get; set; }
        public string PostalCode { get; set; }
        public string OrderDateTime { get; set; }
        public decimal Total { get; set; }
        public string OrderId { get; set; }
        public string OrderDate { get; set; }
        public string OrderDateHour { get; set; }
    }

    public class OrderDetails : Order
    {
        public int ProductId { get; set; }
        public int Quantity { get; set; }
        public decimal UnitPrice { get; set; }
    }
}