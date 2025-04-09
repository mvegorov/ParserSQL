#include <lib/MyCoolDB.h>
#include <gtest/gtest.h>

TEST(DBFormatTest, SimpleRequest) {

    MyCoolDB base("base_1.txt");
    std::string request = "SELECT CustomerName, City FROM Customers;";

    ASSERT_NO_THROW(base.Request(request));
}

TEST(DBFormatTest, ValidSpaces) {

    MyCoolDB base("base_1.txt");
    std::string request = R"(SELECT    CustomerName   ,  City
                                    FROM
                                    Customers;)";

    ASSERT_NO_THROW(base.Request(request));
}

TEST(DBFormatTest, InvalidSpaces) {

    MyCoolDB base("base_1.txt");
    std::string request1 = R"(SEL ECT CustomerName, City FRO M Customers;)";
    std::string request2 = R"(SELECT Custom
                                    erName, City FR OM Customers;)";

    ASSERT_ANY_THROW(base.Request(request1));
    ASSERT_ANY_THROW(base.Request(request2));
}

TEST(DBFormatTest, InvalidAveragePunctuation) {

    MyCoolDB base("base_1.txt");
    std::string request1 = "SELECT CustomerName,, City FROM Customers;";
    std::string request2 = "SELECT CustomerName, City FROM Customers";
    std::string request3 = "SELECT CustomerName City FROM Customers;";
    std::string request4 = "SELECT, CustomerName, City FROM Customers;";

    ASSERT_ANY_THROW(base.Request(request1));
    ASSERT_ANY_THROW(base.Request(request2));
    ASSERT_ANY_THROW(base.Request(request3));
    ASSERT_ANY_THROW(base.Request(request4));
}

TEST(DBFormatTest, ValidValuesPunctuation) {

    MyCoolDB base("base_1.txt");
    std::string request1 = R"(INSERT INTO Customers
VALUES (1, '2', '3', '4', '5', '6', '7');)";
    std::string request2 = R"(INSERT INTO Customers
VALUES ( 1,  '2', '3', '4'  ,'5' , '6' ,'7' ) ;)";

    ASSERT_NO_THROW(base.Request(request1));
    ASSERT_NO_THROW(base.Request(request2));
}

TEST(DBFormatTest, InvalidValuesPunctuation) {

    MyCoolDB base("base_1.txt");
    std::string request1 = R"(INSERT INTO Customers
VALUES 1, '2', '3');)";
    std::string request2 = R"(INSERT INTO Customers
VALUES (1 '2' '3');)";
    std::string request3 = R"(INSERT INTO Customers
VALUES (1,, '2', '3');)";

    ASSERT_ANY_THROW(base.Request(request1));
    ASSERT_ANY_THROW(base.Request(request2));
    ASSERT_ANY_THROW(base.Request(request3));
}

TEST(DBFormatTest, ValidCreateTablePunctuation) {

    MyCoolDB base("base_1.txt");
    std::string request1 = R"(CREATE TABLE Table1 (a int, b int, c int, d int, f int);)";
    std::string request2 = R"(CREATE TABLE Table2 (a int , b int  ,c int,d int  ,  f int );)";
    std::string request3 = R"(CREATE TABLE Table3 (a int, b int,
 c int,
 d int, f int);)";

    ASSERT_NO_THROW(base.Request(request1));
    ASSERT_NO_THROW(base.Request(request2));
    ASSERT_NO_THROW(base.Request(request3));
}

TEST(DBFormatTest, InvalidCreateTablePunctuation) {

    MyCoolDB base("base_1.txt");

    ASSERT_ANY_THROW(base.Request("CREATE TABLE Table1 (a int b int);"));
    ASSERT_ANY_THROW(base.Request("CREATE TABLE Table1 (a, b int);"));
    ASSERT_ANY_THROW(base.Request("CREATE TABLE Table1 a int, b int;"));
    ASSERT_ANY_THROW(base.Request("CREATE TABLE Table1 (a int, b int,);"));
    ASSERT_ANY_THROW(base.Request("CREATE TABLE Table1 (a int some_excess, b int);"));
    ASSERT_ANY_THROW(base.Request("CREATE TABLE Table1 (a int, b int, some_excess);"));
    ASSERT_ANY_THROW(base.Request("CREATE TABLE Table1 (a int PRMRY KY, b int);"));
    ASSERT_ANY_THROW(base.Request("CREATE TABLE Table1 (a int, b int, PRIMARY KEY () );"));
    ASSERT_ANY_THROW(base.Request("CREATE TABLE Table1 (a int, b int FOREIGN KEY REFERENCES);"));
}

TEST(DBFormatTest, ValidJoinPunctuation) {

    MyCoolDB base("base_9.txt");
    std::string request1 = R"(SELECT Orders.OrderID
FROM Orders
LEFT JOIN Customers ON Orders.CustomerID=Customers.CustomerID;)";
    std::string request2 = R"(SELECT Orders.OrderID
FROM Orders
LEFT JOIN Customers ON Orders.CustomerID = Customers.CustomerID;)";
    std::string request3 = R"(SELECT Orders.OrderID
FROM Orders
LEFT JOIN Customers   ON   Orders.CustomerID  =  Customers.CustomerID ;)";

    ASSERT_NO_THROW(base.Request(request1));
    ASSERT_NO_THROW(base.Request(request2));
    ASSERT_NO_THROW(base.Request(request3));
}

TEST(DBFormatTest, InvalidJoinPunctuation) {

    MyCoolDB base("base_9.txt");

    ASSERT_ANY_THROW(base.Request(R"(SELECT Orders.OrderID
FROM Orders
LEFT JOIN Customers ON Orders.CustomerID Customers.CustomerID ;)"));
    ASSERT_ANY_THROW(base.Request(R"(SELECT Orders.OrderID
FROM Orders
LEFT JOIN Customers ON Orders.CustomerID= ;)"));
    ASSERT_ANY_THROW(base.Request(R"(SELECT Orders.OrderID
FROM Orders
LEFT JOIN ON Orders.CustomerID=Customers.CustomerID ;)"));
    ASSERT_ANY_THROW(base.Request(R"(SELECT .OrderID
FROM Orders
LEFT JOIN Customers ON Orders.CustomerID=Customers.CustomerID ;)"));
    ASSERT_ANY_THROW(base.Request(R"(SELECT Orders.
FROM Orders
LEFT JOIN Customers ON Orders.CustomerID=Customers.CustomerID ;)"));
    ASSERT_ANY_THROW(base.Request(R"(SELECT .
FROM Orders
LEFT JOIN Customers ON Orders.CustomerID=Customers.CustomerID ;)"));
    ASSERT_ANY_THROW(base.Request(R"(SELECT Orders.OrderID
FROM Orders
LEFT RIGHT JOIN Customers ON Orders.CustomerID=Customers.CustomerID ;)"));
    ASSERT_ANY_THROW(base.Request(R"(SELECT Orders.OrderID
FROM Orders
LEFT Customers ON Orders.CustomerID.=Customers.CustomerID ;)"));
    ASSERT_ANY_THROW(base.Request(R"(SELECT Orders. OrderID
FROM Orders
LEFT JOIN Customers ON Orders. CustomerID=Customers. CustomerID ;)"));
    ASSERT_ANY_THROW(base.Request(R"(SELECT Orders .OrderID
FROM Orders
LEFT JOIN Customers ON Orders .CustomerID=Customers .CustomerID ;)"));
    ASSERT_ANY_THROW(base.Request(R"(SELECT Orders OrderID
FROM Orders
LEFT JOIN Customers ON Orders CustomerID=Customers CustomerID ;)"));
}