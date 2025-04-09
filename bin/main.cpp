#include <iostream>
#include <lib/MyCoolDB.h>

int main() {
    try {
        MyCoolDB base(R"(C:\Users\bodor\CLionProjects\labwork-12-mvegorov\tests\test_bases\base_9.txt)");
        std::string request_string = "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n"
                                     "FROM Orders\n"
                                     "INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;";
        std::cout<<base.Request(request_string);
    } catch (const Exception& ex) {
        std::cout<<ex.text;
    }
}

