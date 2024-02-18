import faust

class ProductEvent(faust.Record):
    id: int
    name: str
    price: int

class CustomerEvent(faust.Record):
    id: int
    name: str
    email: str

class OrderEvent(faust.Record):
    id: int
    customer_id: int
    order_date: str

class OrderItemEvent(faust.Record):
    id: int
    order_id: int
    product_id: int
    quantity: int
