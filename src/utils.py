import psycopg2
import requests


def get_url(employee_id):
    """Поиск по названию"""
    params = {
        "per_page": 20,
        "employer_id": employee_id,
        "only_with_salary": True,
        "area": 113,
        "only_with_vacancies": True
    }
    return requests.get("https://api.hh.ru/vacancies/", params).json()['items']


def get_vacancies(employee_ids):
    """Получение списка вакансий"""
    vacancies_list = []
    for employer_id in employee_ids:
        emp_vacancies = get_url(employer_id)
        for vacancy in emp_vacancies:
            if vacancy['salary']['from'] is not None and vacancy['salary']['to'] is not None:
                vacancies_list.append({'vacancies': {'vacancy_name': vacancy['name'],
                                                     'city': vacancy['area']['name'],
                                                     'salary_from': vacancy['salary']['from'],
                                                     'salary_to': vacancy['salary']['to'],
                                                     'publish_date': vacancy['published_at'],
                                                     'vacancy_url': vacancy['alternate_url']},
                                       'companies': {'company_name': vacancy['employer']['name'],
                                                     'company_url': vacancy['employer']['alternate_url']}})
    return vacancies_list


def create_database(database_name, params):
    """Создание БД и таблиц для сохранения данных о команиях и их вакансиях"""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f'DROP DATABASE {database_name}')
    cur.execute(f'CREATE DATABASE {database_name}')

    conn.close()
    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE companies(
            company_id SERIAL PRIMARY KEY,
            company_name VARCHAR(150) NOT NULL UNIQUE ,
            url_company TEXT
            )
            ''')

    with conn.cursor() as cur:
        cur.execute('''
        CREATE TABLE vacancies(
        vacancy_id SERIAL PRIMARY KEY,
        vacancy_name VARCHAR(150) NOT NULL,
        city_name VARCHAR(100),
        publish_date DATE,
        company_id INT ,
        salary_from INTEGER,
        salary_to INTEGER,
        url_vacancy TEXT,
        
        CONSTRAINT fk_vacancies_companies 
        FOREIGN KEY(company_id) REFERENCES companies(company_id)
        )
        ''')

    conn.commit()
    conn.close()


def save_data_to_database(data, database_name, params):
    '''Сохранение полученной информации в таблицах'''
    conn = psycopg2.connect(dbname=database_name, **params)
    with conn.cursor() as cur:
        cur.execute('''
                    INSERT INTO companies(company_name, url_company)
                    VALUES 
                    ('КРОСТ', 'https://hh.ru/employer/2302207'),
                    ('МСУ-1', 'https://hh.ru/employer/159880'),
                    ('Компания Самолет', 'https://hh.ru/employer/159880'),
                    ('ЛРА Строй', 'https://hh.ru/employer/1840223'),
                    ('ГК Спутник', 'https://hh.ru/employer/1641656'),
                    ('НОАТЕК', 'https://hh.ru/employer/1277564'),
                    ('Альянс-Пресс', 'https://hh.ru/employer/1228593'),
                    ('Стройтех Групп', 'https://hh.ru/employer/9231282')
                    RETURNING company_id
                    ''', )
        company_id = cur.fetchone()[0]
        for company in data:

            vacansy_data = company['vacancies']
            cur.execute('''
            INSERT INTO vacancies(company_id, vacancy_name, city_name, publish_date, salary_from, salary_to,
            url_vacancy)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''',
                        (company_id, vacansy_data['vacancy_name'], vacansy_data['city'],
                         vacansy_data['publish_date'],
                         vacansy_data['salary_from'], vacansy_data['salary_to'], vacansy_data['vacancy_url'])
                        )

    conn.commit()
    conn.close()





