### **ParserSQL: Реализация упрощенной БД с поддержкой sql**  

**Класс MyCoolDB** реализует базу данных, работающую с текстовыми файлами в удобном формате (строки - записи, столбцы - через табуляцию). Основной метод Execute() принимает текстовые SQL-запросы, выполняет их парсинг, валидацию и исполнение.

### Поддерживаемый синтаксис

Ключевые слова:

- SELECT
- FROM
- WHERE
- (LEFT|RIGHT|INNER)JOIN
- CREATE TABLE
- DROP TABLE
- AND
- OR
- IS
- NOT
- NULL
- ON
- UPDATE
- INSERT
- VALUES
- DELETE
- PRIMARY KEY
- FOREIGN KEY

Поддерживаемые типы данных:

- bool
- int
- float
- double
- varchar
