import pandas as pd
from logger.logger import Logger

class Access:
    TRUE = 'true'
    NONE = 'NULL'
    df = None
    roles = {}
    log = None

    def __init__(self):

        self.tablename = 'access'
        self.filepath_src = 'src/Подразделения v05.xlsx'
        self.sheet_name = 'Роли'
        self.filepath_out = 'out/access.sql'

        self.log = Logger(self.tablename).log
        self.df = pd.read_excel(self.filepath_src, sheet_name=self.sheet_name)
        self.df = self.df.reset_index()
        self.log.info(f'Successfully read {len(self.df)} lines')

        role_df = pd.read_excel(self.filepath_src, sheet_name="role")
        for index, row in role_df.iterrows():
            r_id = row['id']
            r_name = row['name']
            r_name_norm = self.normalise_str(r_name)
            self.roles[r_name_norm] = r_id
        pass

    @staticmethod
    def normalise_str(s):
        return str(s).strip().lower()

    @staticmethod
    def normalise_fio(fio):
        return str(fio).strip().lower().replace(' ', '')


    def run(self, subdiv_name_id_dict, fio_id_dict):

        sql = '''INSERT INTO access.access (account_id, subdivision_id, role_id) VALUES'''

        self.log.info(f'File: {self.filepath_src}, Sheet: {self.sheet_name}')

        unique_keys = set()

        for index, row in self.df.iterrows():

            account_id = None
            subdivision_id = None
            role_id = None

            fio = row.fio
            nf = self.normalise_fio(fio)

            if nf in fio_id_dict.keys():
                account_id = fio_id_dict[nf]
            else:
                continue
                self.log.error(f'Index: {index}. Message: User not found: {fio}')

            subdivision = row.subdivision.split('\\')[-1]
            ns = self.normalise_str(subdivision)
            if ns in subdiv_name_id_dict.keys():
                subdivision_id = subdiv_name_id_dict[ns]
            else:
                continue
                self.log.error(f'Index: {index}. Message: Subdivision not found: {subdivision}')

            role = row.role
            nr = self.normalise_str(role)
            if nr in self.roles.keys():
                role_id = self.roles[nr]
            else:
                continue
                self.log.error(f'Index: {index}. Message: Role not found: {role}')

            key = f'{str(account_id)}:{str(subdivision_id)}:{str(role_id)}'

            if not (key in unique_keys):
                unique_keys.add(key)
                line = f"({account_id}, {subdivision_id}, {role_id}),"
                sql = '\n'.join((sql, line))
            else:
                self.log.error(f'Index: {index}. Message: Not unique combination fio, subdivision and role : {fio} :: {subdivision} :: {role}')

        sql = sql[:-1]

        f = open(self.filepath_out, 'w')
        f.write(sql)
        f.close()

        self.log.info('Done')
        self.log.info('.')