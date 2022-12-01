create table if not exists users (
    id integer NOT NULL,
    user_id integer NOT NULL,
    fio varchar(250) NOT NULL,
    birth_date date,
    create_date date
);

insert into users(id, user_id, fio, birth_date, create_date)
values(1, 1, 'Иванов И.С.', '1990-03-01', '2019-09-01');
insert into users(id, user_id, fio, birth_date, create_date)
values(2, 2, 'Петров А.С.', '1990-03-23', '2019-09-01');
insert into users(id, user_id, fio, birth_date, create_date)
values(3, 3, 'Сидоров П.С.', '1991-03-01', '2019-09-01');
insert into users(id, user_id, fio, birth_date, create_date)
values(4, 4, 'Макарова С.С.', '1993-06-01', '2019-09-01');
insert into users(id, user_id, fio, birth_date, create_date)
values(5, 5, 'Сергеев И.С.', '1994-07-01', '2019-09-01');
insert into users(id, user_id, fio, birth_date, create_date)
values(6, 6, 'Матвеев А.С.', '1990-04-11', '2019-09-01');
insert into users(id, user_id, fio, birth_date, create_date)
values(7, 7, 'Николаев Р.Н.', '1997-03-11', '2019-09-01');
insert into users(id, user_id, fio, birth_date, create_date)
values(8, 8, 'Миронова С.А.', '1999-03-21', '2019-09-01');
insert into users(id, user_id, fio, birth_date, create_date)
values(9, 9, 'Чупкин М.М.', '1997-03-11', '2019-09-01');
insert into users(id, user_id, fio, birth_date, create_date)
values(10, 10, 'Васильев М.И.', '1996-03-01', '2019-09-01');
insert into users(id, user_id, fio, birth_date, create_date)
values(11, 11, 'Дагбаева П.Н.', '1995-03-01', '2019-09-01');
insert into users(id, user_id, fio, birth_date, create_date)
values(12, 12, 'Суворов А.С.', '1994-03-01', '2019-09-01');


create table if not exists user_activity (
    user_id integer NOT NULL,
    timestamp integer NOT NULL,
    type varchar(20) NOT NULL,
    page_id integer NOT NULL,
    tag varchar(100) NOT NULL,
    sign boolean NOT NULL
);

insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(1, 1667257200, 'click', 101, 'sport', false);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(1, 1667347200, 'scroll', 102, 'business', false);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(2, 1667347200, 'move', 102, 'business', true);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(3, 1667440800, 'click', 101, 'sport', true);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(3, 1667444400, 'scroll', 102, 'business', true);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(3, 1667444400, 'visit', 103, 'politics', true);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(3, 1667257200, 'click', 104, 'medic', true);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(4, 1667440800, 'move', 103, 'politics', false);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(5, 1667462400, 'scroll', 104, 'medic', true);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(6, 1667462400, 'visit', 105, 'sport', true);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(7, 1667469600, 'click', 102, 'business', true);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(8, 1667469600, 'visit', 103, 'politics', true);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(9, 1667476800, 'scroll', 102, 'business', true);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(10, 1667476800, 'click', 103, 'politics', false);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(11, 1667484000, 'click', 103, 'politics', true);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(12, 1667484000, 'move', 105, 'sport', true);
insert into user_activity(user_id, timestamp, type, page_id, tag, sign)
values(12, 1667491200, 'click', 106, 'sport', true);