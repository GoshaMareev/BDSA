--1. Создайте ключспейс library с репликацией SimpleStrategy и коэффициентом репликации 1.

CREATE KEYSPACE library WITH replication =
    {'class':'SimpleStrategy','replication_factor':1};

--2. Создайте таблицу books в ключспейсе library с полями book_id (int), title (text), author (text), year_published (int), genre (text), isbn (text) и первичным ключом book_id.

DROP TABLE IF EXISTS library.books;
CREATE TABLE library.books (
                               book_id int,
                               title text,
                               author text,
                               year_published int,
                               genre text,
                               isbn text,
                               PRIMARY KEY (book_id)
);

--3. Вставьте четыре книги в таблицу books.
-- Классика
INSERT INTO library.books (book_id, title, author, year_published, genre, isbn)
VALUES (1, 'To Kill a Mockingbird', 'Harper Lee', 1960, 'Classic', '978-0061120084');

-- Фантастика
INSERT INTO library.books (book_id, title, author, year_published, genre, isbn)
VALUES (2, 'Dune', 'Frank Herbert', 1965, 'Science Fiction', '978-0441172719');

-- Программирование
INSERT INTO library.books (book_id, title, author, year_published, genre, isbn)
VALUES (3, 'Clean Code', 'Robert C. Martin', 2008, 'Programming', '978-0132350884');

-- Детектив
INSERT INTO library.books (book_id, title, author, year_published, genre, isbn)
VALUES (4, 'The Girl with the Dragon Tattoo', 'Stieg Larsson', 2005, 'Mystery', '978-0307269751');

--4. Выберите все книги из таблицы books.
SELECT * FROM library.books;

--5. Обновите поле year_published книги с book_id = 2 на 1950.
UPDATE library.books
SET year_published = 1950
WHERE book_id = 2;

--Проверка:
SELECT title, author, year_published
FROM library.books
WHERE book_id = 2;
