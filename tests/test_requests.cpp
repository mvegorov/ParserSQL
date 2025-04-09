#include <lib/MyCoolDB.h>
#include <gtest/gtest.h>

// SELECT & FROM
TEST(DBSelectFromTest, SelectAsteriskFromTable) {

    MyCoolDB base("base_2.txt");
    std::string request = "SELECT * FROM Customers;";
    std::string actual_response = base.Execute(request);
    std::cout << actual_response;
    std::string expected_response = R"(
CustomerID    CustomerName                          ContactName           Address                          City           PostalCode    Country
2             Ana Trujillo Emparedados y helados    Ana Trujillo          Avda. de la Constitucion 2222    Mexico D.F.    05021         Mexico
3             Antonio Moreno Taqueria               Antonio Moreno        Mataderos 2312                   Mexico D.F.    05023         Mexico
4             Around the Horn                       Thomas Hardy          120 Hanover Sq.                  London         WA1 1DP       UK
5             Berglunds snabbkop                    Christina Berglund    Berguvsvagen 8                   Lulea          S-958 22      Sweden
6             Blauer See Delikatessen               Hanna Moos            Forsterstr. 57                   Mannheim       68306         Germany
7             Blondel pere et fils                  Frederique Citeaux    24, place Kleber                 Strasbourg     67000         France
)";

    ASSERT_EQ(actual_response, expected_response);
}

TEST(DBSelectFromTest, SelectColumnsFromTable) {

    MyCoolDB base("base_2.txt");
    std::string request = "SELECT CustomerName, City FROM Customers;";
    std::string actual_response = base.Execute(request);
    std::string expected_response = R"(
CustomerName                          City
Ana Trujillo Emparedados y helados    Mexico D.F.
Antonio Moreno Taqueria               Mexico D.F.
Around the Horn                       London
Berglunds snabbkop                    Lulea
Blauer See Delikatessen               Mannheim
Blondel pere et fils                  Strasbourg
)";

    ASSERT_EQ(actual_response, expected_response);
}

TEST(DBSelectFromTest, InvalidSelectFrom) {

    MyCoolDB base("base_2.txt");
    ASSERT_ANY_THROW(base.Execute("SELECT CustomerName, CustomerName FROM Customers;"));
    ASSERT_ANY_THROW(base.Execute("SELECT CustomerName SELECT CustomerName FROM Customers;"));
    ASSERT_ANY_THROW(base.Execute("SELECT CustomerName FROM Customers FROM Customers;"));
    ASSERT_ANY_THROW(base.Execute("SELECT CustomerName FROM Customers some_excess;"));
    ASSERT_ANY_THROW(base.Execute("SELECT no_such_column FROM Customers;"));
    ASSERT_ANY_THROW(base.Execute("SELECT CustomerName FROM no_such_table;"));
    ASSERT_ANY_THROW(base.Execute("SELECT FROM Customers;"));
    ASSERT_ANY_THROW(base.Execute("SELECT CustomerName FROM;"));
    ASSERT_ANY_THROW(base.Execute("SELECT CustomerName;"));
    ASSERT_ANY_THROW(base.Execute("FROM Customers;"));
}

// INSERT VALUES
TEST(DBInsertValuesTest, Inserts) {

    MyCoolDB base("base_3.txt");  // В базе только пустая таблица Table1
    std::string request1 = R"(INSERT INTO Table1 (a, b, c, d, f) VALUES (0, 0, 0, 0, 0);)";
    std::string request2 = R"(INSERT INTO Table1 VALUES (0, 0, 0, 0, 0);)";
    std::string request3 = R"(INSERT INTO Table1 (a, b) VALUES (0, 0);)";

    base.Execute(request1);
    base.Execute(request2);
    base.Execute(request3);
    std::string actual_table = base.Execute("SELECT * FROM Table1;");
    std::string expected_table = R"(
a    b    c       d       f
0    0    0       0       0
0    0    0       0       0
0    0    NULL    NULL    NULL
)";
    ASSERT_EQ(actual_table, expected_table);
}

TEST(DBInsertValuesTest, InvalidInserts) {

    MyCoolDB base("base_3.txt");

    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (a) VALUES (0, 0);"));
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (a, b) VALUES (0);"));
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0);"));
}

// WHERE
TEST(DBWhereTest, WhereLogicalExpression) {

    MyCoolDB base("base_5.txt");

    std::string request = R"(SELECT * FROM Table1 WHERE (NOT a=0 AND b=1) AND (c=1 OR d<1);)";
    std::string actual_result = base.Execute(request);
    std::string expected_result = R"(
a    b    c    d    f
1    1    0    0    1
1    1    1    0    1
1    1    1    1    1
)";
    ASSERT_EQ(actual_result, expected_result);
}

TEST(DBCorrectRequestTest, InvalidWhereExpressions) {

    MyCoolDB base("base_5.txt");

    ASSERT_ANY_THROW(base.Execute("SELECT * FROM Table1 WHERE (NOT a=0 AND b=1))(;"));
    ASSERT_ANY_THROW(base.Execute("SELECT * FROM Table1 WHERE (NOT a='0' AND b='1');"));
    ASSERT_ANY_THROW(base.Execute("SELECT * FROM Table1 WHERE (NOT a=0 AND AND b=1);"));
    ASSERT_ANY_THROW(base.Execute("SELECT * FROM Table1 WHERE (NOT a=0 AND b=1 some_excess);"));
    ASSERT_ANY_THROW(base.Execute("SELECT * FROM Table1 WHERE (NOT a= AND b=1);"));
    ASSERT_ANY_THROW(base.Execute("SELECT * FROM Table1 WHERE (NOT a AND b);"));
}

// CREATE TABLE
TEST(DBCreateTableTest, ValidCreateTable) {

    MyCoolDB base("base_4.txt");

    std::string request1 = R"(CREATE TABLE Table1 (a int, b int, c int, d int, f int);)";
    std::string request2 = R"(CREATE TABLE Table2 (a int NOT NULL, b int NOT NULL PRIMARY KEY, c int, d int, f int);)";
    std::string request3 = R"(CREATE TABLE Table3 (
    a int,
    b int,
    c int,
    PRIMARY KEY (a),
    FOREIGN KEY (c) REFERENCES Table2(b));)";
    ASSERT_NO_THROW(base.Execute(request1));
    ASSERT_NO_THROW(base.Execute(request2));
    ASSERT_NO_THROW(base.Execute(request3));
}

TEST(DBCreateTableTest, InvalidCreateTable) {

    MyCoolDB base("base_3.txt");

    ASSERT_ANY_THROW(base.Execute(R"(CREATE TABLE Table1 (a int, a int);)"));
    ASSERT_ANY_THROW(base.Execute(R"(CREATE TABLE Table2 (a int, b no_such_typename);)"));
    ASSERT_ANY_THROW(base.Execute(R"(CREATE TABLE Table3 (a int , b int some_excess);)"));
}

// JOIN
TEST(DBJoinTest, ValidLeftJoin) {

    MyCoolDB base("base_9.txt");

    std::string request = R"(
SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
FROM Orders
LEFT JOIN Customers ON Orders.CustomerID=Customers.CustomerID;)";

    std::string actual_result = base.Execute(request);
    std::string expected_result = R"(
OrderID    CustomerName               OrderDate
10365      Antonio Moreno Taqueria    1996-11-27
10383      Around the Horn            1996-12-16
10384      Berglunds snabbkop         1996-12-16
10360      NULL                       1996-11-22
10362      NULL                       1996-11-25
)";
    ASSERT_EQ(actual_result, expected_result);
}

TEST(DBJoinTest, ValidInnertJoin) {

    MyCoolDB base("base_9.txt");

    std::string request = R"(
SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
FROM Orders
INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;)";

    std::string actual_result = base.Execute(request);
    std::string expected_result = R"(
OrderID    CustomerName               OrderDate
10365      Antonio Moreno Taqueria    1996-11-27
10383      Around the Horn            1996-12-16
10384      Berglunds snabbkop         1996-12-16
)";
    ASSERT_EQ(actual_result, expected_result);
}

TEST(DBJoinTest, ValidRightJoin) {

    MyCoolDB base("base_9.txt");

    std::string request = R"(
SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
FROM Orders
RIGHT JOIN Customers ON Orders.CustomerID=Customers.CustomerID;)";

    std::string actual_result = base.Execute(request);
    std::string expected_result = R"(
OrderID    CustomerName                          OrderDate
10365      Antonio Moreno Taqueria               1996-11-27
10383      Around the Horn                       1996-12-16
10384      Berglunds snabbkop                    1996-12-16
NULL       Ana Trujillo Emparedados y helados    NULL
)";
    ASSERT_EQ(actual_result, expected_result);
}

TEST(DBJoinTest, InvalidJoin) {

    MyCoolDB base("base_9.txt");

    ASSERT_ANY_THROW(base.Execute(R"(SELECT Orders.OrderID
                                                FROM Orders
                                                RIGHT JOIN no_such_table ON Orders.CustomerID=Customers.CustomerID;)"));
    ASSERT_ANY_THROW(base.Execute(R"(SELECT Orders.OrderID
                                                FROM Orders
                                                RIGHT JOIN Customers ON Orders.no_such_column=Customers.CustomerID;)"));
    ASSERT_ANY_THROW(base.Execute(R"(SELECT Orders.OrderID, Suppliers.SupplierID
                                                FROM Orders
                                                RIGHT JOIN Customers ON Orders.CustomerID=Customers.CustomerID;)"));
    ASSERT_ANY_THROW(base.Execute(R"(SELECT Orders.OrderID
                                                FROM Customers
                                                RIGHT JOIN Suppliers ON Customers.ContactName=Suppliers.ContactName;)"));
    // в последнем тесте не соблюдается условие,
    // что Customers.ContactName должен быть FOREIGN KEY от Suppliers.ContactName или наоборот
}

// DROP TABLE
TEST(DBDropTest, ValidDropTable) {

    MyCoolDB base("base_3.txt");

    ASSERT_NO_THROW(base.Execute("SELECT * FROM Table1;"));
    base.Execute("DROP TABLE Table1;");
    ASSERT_ANY_THROW(base.Execute("SELECT * FROM Table1;"));
}

TEST(DBDropTest, InvalidDropTable) {

    MyCoolDB base("base_3.txt");

    ASSERT_ANY_THROW(base.Execute("DROP TABLE no_such_table;"));
    ASSERT_ANY_THROW(base.Execute("DROP Table1;"));
}

// UPDATE
TEST(DBUpdateTest, ValidUpdate) {

    MyCoolDB base("base_6.txt");
//Table1
//
//int	bool	double	varchar	int
//a	b	c	d	f
//1	true	1.1	a	1
//2	false	1.2	b	1
//3	true	1.3	c	1

    base.Execute("UPDATE Table1 SET f = 0, d = 'e' WHERE a > 1;");
    std::string actual_result = base.Execute(R"(SELECT * FROM Table1;)");
    std::string expected_result = R"(
a    b        c      d    f
1    true     1.1    a    1
2    false    1.2    e    0
3    true     1.3    e    0
)";

    ASSERT_EQ(actual_result, expected_result);

    base.Execute("UPDATE Table1 SET f = 0, d = 'e';");
    actual_result = base.Execute(R"(SELECT * FROM Table1;)");
    expected_result = R"(
a    b        c      d    f
1    true     1.1    e    0
2    false    1.2    e    0
3    true     1.3    e    0
)";

    ASSERT_EQ(actual_result, expected_result);
}

TEST(DBUpdateTest, InvalidUpdate) {

    MyCoolDB base("base_6.txt");

    ASSERT_ANY_THROW(base.Execute("UPDATE no_such_table SET f = 0, d = 'e' WHERE a > 1;"));
    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 SET no_such_column = 0, d = 'e' WHERE a > 1;"));
    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 WHERE a > 1;"));
}

// data types
TEST(DBDataTypesTest, EveryTypeTest) {

    MyCoolDB base("base_6.txt");

    ASSERT_NO_THROW(base.Execute("UPDATE Table1 SET a = 0;"));
    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 SET a = 0.1;"));
    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 SET a = '0';"));
    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 SET a = true;"));

    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 SET b = 0;"));
    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 SET b = 0.1;"));
    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 SET b = '0';"));
    ASSERT_NO_THROW(base.Execute("UPDATE Table1 SET b = true;"));

    ASSERT_NO_THROW(base.Execute("UPDATE Table1 SET c = 0;"));
    ASSERT_NO_THROW(base.Execute("UPDATE Table1 SET c = 0.1;"));
    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 SET c = '0';"));
    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 SET c = true;"));

    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 SET d = 0;"));
    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 SET d = 0.1;"));
    ASSERT_NO_THROW(base.Execute("UPDATE Table1 SET d = '0';"));
    ASSERT_ANY_THROW(base.Execute("UPDATE Table1 SET d = true;"));

    ASSERT_NO_THROW(base.Execute("INSERT INTO Table1 (a) VALUES (0);"));
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (a) VALUES (true);"));
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (a) VALUES (0.1);"));
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (a) VALUES ('0');"));

    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (b) VALUES (0);"));
    ASSERT_NO_THROW(base.Execute("INSERT INTO Table1 (b) VALUES (true);"));
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (b) VALUES (0.1);"));
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (b) VALUES ('0');"));

    ASSERT_NO_THROW(base.Execute("INSERT INTO Table1 (c) VALUES (0);"));
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (c) VALUES (true);"));
    ASSERT_NO_THROW(base.Execute("INSERT INTO Table1 (c) VALUES (0.1);"));
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (c) VALUES ('0');"));

    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (d) VALUES (0);"));
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (d) VALUES (true);"));
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Table1 (d) VALUES (0.1);"));
    ASSERT_NO_THROW(base.Execute("INSERT INTO Table1 (d) VALUES ('0');"));
}

// DELETE
TEST(DBDeleteTest, ValidDelete) {

    MyCoolDB base("base_6.txt");

    base.Execute("DELETE FROM Table1 WHERE a > 1;");
    std::string actual_result = base.Execute(R"(SELECT * FROM Table1;)");
    std::string expected_result = R"(
a    b       c      d    f
1    true    1.1    a    1
)";
    ASSERT_EQ(actual_result, expected_result);
}

TEST(DBDeleteTest, InvalidDelete) {

    MyCoolDB base("base_6.txt");

    ASSERT_ANY_THROW(base.Execute("DELETE FROM no_such_table WHERE a > 1;"));
}

// PRIMARY / FOREIGN KEY
TEST(DBPrimaryTest, PrimaryInserts) {

    MyCoolDB base("base_10.txt");
    // Customers.CustomerID - PRIMARY KEY, Orders.CustomerID - FOREIGN KEY, Customers.ContactName - NOT NULL

    ASSERT_ANY_THROW(base.Execute("INSERT INTO Customers (ContactName) VALUES ('0');"));
    // CustomerID не может быть NULL
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Customers (CustomerID, ContactName) VALUES (4, '0');"));
    // CustomerID не может иметь повторяющиеся значения
    ASSERT_NO_THROW(base.Execute("INSERT INTO Customers (CustomerID, ContactName) VALUES (100, '0');"));
}

TEST(DBPrimaryTest, ForeignInserts) {

    MyCoolDB base("base_10.txt");

    ASSERT_ANY_THROW(base.Execute("INSERT INTO Orders (OrderID) VALUES ('0');"));
    // CustomerID не может быть NULL
    ASSERT_ANY_THROW(base.Execute("INSERT INTO Orders (CustomerID, OrderID) VALUES (100, 0);"));
    // CustomerID не может иметь значения, которого нет в Customers.CustomerID
    ASSERT_NO_THROW(base.Execute("INSERT INTO Orders (CustomerID, OrderID) VALUES (4, 0);"));
}