import pandas as pd
from logger.logger import Logger


class Account:
    TRUE = 'true'
    NONE = 'NULL'
    df = None
    log = None

    def __init__(self):

        self.tablename = 'account'
        self.filepath_src = 'src/Подразделения v01.xlsx'
        self.sheet_name = 'account'
        self.filepath_out = 'out/account.sql'

        self.log = Logger(self.tablename).log
        self.df = pd.read_excel(self.filepath_src, sheet_name=self.sheet_name)
        self.df = self.df.reset_index()
        self.log.info(f'Successfully read {len(self.df)} lines')

    @staticmethod
    def normalise_str(s):
        return str(s).strip().lower()

    #
    # subdiv_name_id_dict словарь Имя_подразделения : id подразделения сгенерированный на предыдущем шаге
    #
    def run(self, subdiv_name_id_dict):
        id = 1  # Фактически начинаем с двойки т.е. в бд уже есть пользователь с id = 1 - Sheduller
        logins = set()  # Контроль логинов на уникальность
        sql = '''INSERT INTO access.account (id, tab_num, created_at, email, login, first_name, last_name, middle_name, "position", phone_number, is_actual, subdivision_id, is_user_agreement_confirmed) VALUES'''

        for index, row in self.df.iterrows():

            subdivision_norm = self.normalise_str(row.subdivision)
            if subdivision_norm in subdiv_name_id_dict.keys():
                subdivision_id = subdiv_name_id_dict[self.normalise_str(row.subdivision)]
            else:
                subdivision_id = subdiv_name_id_dict['нпр']  # todo: Заглушка. Что с этим делать?
                self.log.error(
                    f'Index: {index}. Message: Subdivision id not found. Subdivision name: {row.subdivision}')

            login = self.normalise_str(row.login)
            if login == 'nan':
                login = 'nan' + str(id)
            if login in logins:
                # пропускаем дубли
                self.log.error(f'Index: {index}. Message: Duplicate login : {login}')
                continue
            else:
                logins.add(login)

            id += 1
            tab_num = id  # todo: табельный номер
            created_at = 'current_timestamp'
            email = self.normalise_str(row.email)

            if email in ['nan', 'нет']:
                email = 'nan' + str(id) + '@nan.com'  # todo: Заглушка. Что с этим делать?
                self.log.error(f'Index: {index}. Message: Email not found. New one has been generated : {email}')

            try:
                fio = str(row.fio).strip()
                first_name = fio.split(' ')[0]
                last_name = fio.split(' ')[1]
                middle_name = fio.split(' ')[2]
            except Exception as e:
                # todo: Заглушка. Что с этим делать?
                first_name = 'ERR'
                last_name = 'ERR'
                middle_name = 'ERR'
                self.log.error(f'Index: {index}. Message: Incorrect FIO format: {row.fio}')

            position = self.normalise_str(row.position)
            phone = self.normalise_str(row.phone)
            is_actual = 'true'
            is_user_agreement_confirmed = 'false'

            line = f"({id}, {tab_num}, {created_at}, '{email}', '{login}', '{first_name}', '{last_name}', '{middle_name}', '{position}', '{phone}', {is_actual}, {subdivision_id}, {is_user_agreement_confirmed}),"
            sql = '\n'.join((sql, line))

        sql = sql[:-1]
        f = open(self.filepath_out, 'w')
        f.write(sql)
        f.close()

        self.log.info('Done')
        self.log.info('.')
