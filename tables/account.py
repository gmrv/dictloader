import pandas as pd
from logger.logger import Logger


class Account:
    TRUE = 'true'
    NONE = 'NULL'
    df = None
    log = None

    def __init__(self):

        self.tablename = 'account'
        self.filepath_src = 'src/Подразделения v04.xlsx'
        self.sheet_name = 'Сотрудники'
        self.filepath_out = 'out/account.sql'

        self.log = Logger(self.tablename).log
        self.df = pd.read_excel(self.filepath_src, sheet_name=self.sheet_name)
        self.df = self.df.reset_index()
        self.log.info(f'Successfully read {len(self.df)} lines')

    @staticmethod
    def normalise_str(s):
        return str(s).strip().lower()

    @staticmethod
    def normalise_fio(fio):
        return str(fio).strip().lower().replace(' ', '')

    def run(self, subdiv_name_id_dict) -> dict:
        '''
        :param subdiv_name_id_dict: Словарь ИмяПодразделения : IdПодразделения
        :return: fio_id_dict: Словарь фио : account_id
        '''

        id = 2          # Фактически начинаем с двойки т.е. в бд уже есть пользователь с id = 1 - Sheduller
        logins = {}     # Контроль логинов на уникальность
        sql = '''INSERT INTO access.account (id, tab_num, created_at, email, login, first_name, last_name, middle_name, "position", phone_number, is_actual, subdivision_id, is_user_agreement_confirmed) VALUES'''

        fio_id_dict = {}

        self.log.info(f'File: {self.filepath_src}, Sheet: {self.sheet_name}')

        for index, row in self.df.iterrows():

            active = row.Active
            NIKA_Active = row.NIKA_Active

            # subdivision_id
            nsubdivision = self.normalise_str(row.subdivision).split('\\')[-1]
            if nsubdivision in subdiv_name_id_dict.keys():
                subdivision_id = subdiv_name_id_dict[self.normalise_str(nsubdivision)]
            else:
                subdivision_id = subdiv_name_id_dict['нпр']  # todo: Заглушка. Что с этим делать?
                self.log.error(
                    f'Index: {index}. Message: Subdivision id not found. Subdivision name: {row.subdivision}. Sets НПР subdivision.')

            # fio
            fio = str(row.fio).strip()
            try:
                first_name = fio.split(' ')[0]
                last_name = fio.split(' ')[1]
                middle_name = fio.split(' ')[2]
            except Exception as e:
                # todo: Обязательное поле. Заглушка. Что с этим делать?
                first_name = 'ERR'
                last_name = 'ERR'
                middle_name = 'ERR'
                self.log.error(f'Index: {index}. Message: Incorrect FIO format: {row.fio}')
            nfio = self.normalise_fio(fio)
            if nfio in fio_id_dict.keys():
                self.log.error(f'Index: {index}. Message: Namesake: {row.fio}')
                # raise Exception("Namesake. It's time to do something about it")
            else:
                fio_id_dict[nfio] = id

            # login
            login = self.normalise_str(row.login)
            if login == 'nan':
                login = 'nan' + str(id)
                # self.log.error(f'Index: {index}. Message: Login not found. New one has been generated: {login}')
            if login in logins.keys():
                # пропускаем дубли
                self.log.error(f'Index: {index}. Message: Duplicate login. Current: {login}({fio}) === Previous: {login}({logins[login]})')
                continue
            else:
                logins[login] = fio

            # email
            email = self.normalise_str(row.email)
            if email in ['nan', 'нет']:
                email = 'nan' + str(id) + '@nan.com'  # todo: Обязательное поле. Заглушка. Что с этим делать?
                # self.log.error(f'Index: {index}. Message: Email not found. New one has been generated : {email}')

            # tab_num
            tab_num = id  # todo: табельный номер

            # created_at
            created_at = 'current_timestamp'

            # position
            position = self.normalise_str(row.position)

            # phone
            phone = self.normalise_str(row.phone)

            # is_actual
            if int(active) == 1:
                is_actual = 'true'
            else:
                is_actual = 'false'

            # is_user_agreement_confirmed
            is_user_agreement_confirmed = 'false'

            line = f"({id}, {tab_num}, {created_at}, '{email}', '{login}', '{first_name}', '{last_name}', '{middle_name}', '{position}', '{phone}', {is_actual}, {subdivision_id}, {is_user_agreement_confirmed}),"
            sql = '\n'.join((sql, line))

            id += 1

        sql = sql[:-1]
        f = open(self.filepath_out, 'w')
        f.write(sql)
        f.close()

        self.log.info('Done')
        self.log.info('.')

        return fio_id_dict
