from src.config import config
from src.utils import get_vacancies, create_database, save_data_to_database


def main():
    employee_ids = [
        '2302207', #КРОСТ
        '159880',#МСУ-1
        '1102601',#Самолет
        '1840223', #ООО ЛРА Строй
        '1641656', #ГК Спутник
        '1277564', #ООО НОАТЕК
        '1228593', #Альянс-Пресс
        '9231282', #ООО Стройтех Групп
    ]
    params = config()
    data = get_vacancies(employee_ids)
    create_database('hh_company', params=params)
    save_data_to_database(data,'hh_company', params)



if __name__ == '__main__':
    main()
