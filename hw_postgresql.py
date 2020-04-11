import psycopg2 as pg
from pprint import pprint

# Напишите следующие функции для работы с таблицами:
# def create_db(): # создает таблицы
#     pass
#
# def get_students(course_id): # возвращает студентов определенного курса
#     pass
#
# def add_students(course_id, students): # создает студентов и
#                                        # записывает их на курс
#     pass
#
#
# def add_student(student): # просто создает студента
#     pass
#
# def get_student(student_id):
#     pass
# Объекты "Студент" передаются в функцию в виде словаря. Вызов функции add_students должен выполнять создание всех сущностей в транзакции.
#
# Схемы:
#
# Student:
#  id     | integer                  | not null
#  name   | character varying(100)   | not null
#  gpa    | numeric(10,2)            |
#  birth  | timestamp with time zone |
#
# Course:
#  id     | integer                  | not null
#  name   | character varying(100)   | not null

params = {
    'dbname': 'hw_sql',
    'user': 'test_user',
    'password': 'password',
    'port': '5433'
}


class PostgreSQL():
    '''Класс для работы с PostgreSQL'''

    def __init__(self, params):
        '''Инициализирует входные данные'''
        self.params = params

    def created_db(self):
        '''Создаёт 3 таблицы, если их ещё нет'''
        with pg.connect(**self.params) as conn:
            with conn.cursor() as curs:
                curs.execute('''
                    create table if not exists course(
                    id serial primary key,
                    name varchar(100)
                    );
                    ''')
                curs.execute('''
                    create table if not exists student(
                    id serial primary key,
                    name varchar(100),
                    gpa numeric(10,2) null,
                    birth date null
                    );
                    ''')

                curs.execute('''
                    create table if not exists student_course(
                    id serial primary key,
                    student_id INTEGER REFERENCES student(id),
                    course_id INTEGER REFERENCES course(id)
                    );
                    ''')

    def add_course(self, courses):
        """Добавляет курсы, как списком, так и одиночные"""
        if type(courses) == list:
            for course in list(courses):
                with pg.connect(**self.params) as conn:
                    with conn.cursor() as curs:
                        curs.execute('insert into course (name) values (%s)', (course,))
        else:
            with pg.connect(**self.params) as conn:
                with conn.cursor() as curs:
                    curs.execute('insert into course (name) values (%s)', (courses,))

    def get_course(self):
        """Отображает курсы, существующие на данный момент"""
        with pg.connect(**self.params) as conn:
            with conn.cursor() as curs:
                curs.execute('select * from course')
                result = curs.fetchall()
        return result

    def get_course_id(self, name_course):
        """Получает id курса, по его названию"""
        with pg.connect(**self.params) as conn:
            with conn.cursor() as curs:
                curs.execute('select id from course where name=%s', (name_course,))
                result = curs.fetchall()
        return result[0][0]

    def add_student(self, students):
        """Добавляет студентов, как одного, так и списком словарей"""
        if type(students) == list:
            with pg.connect(**self.params) as conn:
                with conn.cursor() as curs:
                    for student in list(students):
                        curs.execute('insert into student (name, gpa, birth) values (%s, %s, %s)', (student['name'], student['gpa'],
                                                                                                    student['birth'],))
        else:
            with pg.connect(**self.params) as conn:
                with conn.cursor() as curs:
                    curs.execute('insert into student (name, gpa, birth) values (%s, %s, %s)', (students['name'], students['gpa'],
                                                                                                students['birth'],))

    def get_student(self):
        """Отображает студентов на текущий момент"""
        with pg.connect(**self.params) as conn:
            with conn.cursor() as curs:
                curs.execute('select * from student')
                result = curs.fetchall()
        pprint(result)

    def get_student_id(self, student):
        """Получение id студента по его имени из словаря"""
        with pg.connect(**self.params) as conn:
            with conn.cursor() as curs:
                curs.execute('select id from student where name=%s', (student['name'],))
                result = curs.fetchall()
        return result[0][0]

    def list_course(self):
        """Выводит список названий курсов, нужно для проверки"""
        list_courses = []
        for course in self.get_course():
            list_courses.append(course[1])
        return list_courses

    def add_students(self, name_course, student):
        """Зачисляет студента и сразу записывает на курс"""
        with pg.connect(**self.params) as conn:
            with conn.cursor() as curs:
                if name_course in self.list_course():
                    curs.execute('insert into student (name, gpa, birth) values (%s, %s, %s)', (student['name'], student['gpa'],
                                                                                                student['birth'],))
                    conn.commit()
                    id_course = self.get_course_id(name_course)
                    id_student = self.get_student_id(student)
                    curs.execute('insert into student_course (student_id, course_id) values (%s, %s)', (id_student, id_course,))
                else:
                    print(f'Нет курса {name_course}')
                    print(f'Список доступных курсов {self.list_course()}')

    def get_students(self, name_course):
        """Выводит список студентов, зачисленный на указанный курс"""
        with pg.connect(**self.params) as conn:
            with conn.cursor() as curs:
                if name_course in self.list_course():
                    curs.execute('''select id from course where name=%s''', (name_course,))
                    conn.commit()
                    id_course = curs.fetchall()[0][0]
                    print(id_course)
                    curs.execute(
                        """select s.id, s.name, c.name from student_course sc join student s on s.id = sc.student_id join course c on 
                        c.id = sc.course_id where course_id=%s""",
                        (id_course,))
                    result = curs.fetchall()
        return result


if __name__ in '__main__':
    courses = ['basic python', 'advance python', 'django']
    student1 = {
        'name': 'Прохор Шаляпин',
        'gpa': '4.8',
        'birth': '1954-02-23'
    }
    student = {
        'name': 'Кирсан Кайфат',
        'gpa': '4.8',
        'birth': '2000-03-01'
    }
    students = list(student1 for i in range(10))

    table = PostgreSQL(params)
    table.created_db()
    table.add_course(courses)
    table.add_student(students)
    table.add_students('basic python', student)
    table.get_student()
    print(table.get_students('basic python'))