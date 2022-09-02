import pandas as pd
from logger.logger import Logger


class Subdivision:
    TRUE = 'true'
    NONE = 'NULL'
    df = None
    log = None

    def __init__(self):

        self.tablename = 'subdivision'
        self.filepath_src = 'src/Подразделения v05.xlsx'
        self.sheet_name = 'Подразделения'
        self.filepath_out = 'out/subdivision.sql'
        self.filepath_out_closure = 'out/subdivision_closure.sql'

        self.log = Logger(self.tablename).log
        self.df = pd.read_excel(self.filepath_src, sheet_name=self.sheet_name)
        self.df = self.df.reset_index()

        self.log.info(f'Successfully read {len(self.df)} lines')

    @staticmethod
    def normalise_str(s):
        return str(s).strip().lower()

    def run(self):
        subdivision = []
        closure = []
        subdiv_name_id_dict = {}  # Словарь {Имя_подразделения : id_подразделения} для поиска id по имени


        id = 2  # начинаем с 2 т.к. в бд уже есть подразделение с id = 1 - ROOT
        closure.append([1, 2])

        self.log.info(f'File: {self.filepath_src}, Sheet: {self.sheet_name}')

        for index, row in self.df.iterrows():
            try:
                # row.values[1] - строка вида: root_subdibision\subd1\subd2\subd3\subd4
                # берем последнее и предпоследнее поле, получаме пару parent_name : child_name
                code = row['Код']
                parent_name = row['Путь'].split('\\')[-2]
                child_name = row['Путь'].split('\\')[-1]
                path = row['Путь']
                child_id = id
                subdiv_name_id_dict[self.normalise_str(child_name)] = child_id
                subdivision.append({
                    'id': child_id,
                    'code': int(code) * 10,     # todo: Убрать когда появятся нормальные коды
                    'name': child_name,
                    'parent_name': parent_name,
                    'parent_id': subdiv_name_id_dict[self.normalise_str(parent_name)]
                })
                closure.append([subdiv_name_id_dict[self.normalise_str(parent_name)], child_id])
                id += 1
            except IndexError as e:
                code = row['Код']
                child_name = row['Путь'].split('\\')[-1]

                # Если это первая строка файла, когда еще нет родителя - обрабатываем
                if index == 0 and child_name:
                    subdiv_name_id_dict[self.normalise_str(child_name)] = id
                    self.log.debug('Except: Processing the first line')
                    subdivision.append({
                        'id': id,
                        'code': int(code) * 10,  # todo: Убрать когда появятся нормальные коды
                        'name': child_name,
                        'parent_name': 'ROOT',
                        'parent_id': 1
                    })
                    id += 1
                else:
                    self.log.error(e)

        # SUBDIVISION SQL
        result = '''INSERT INTO access.subdivision (id, code, name, is_actual, "parentId", is_visible_in_path) VALUES'''
        for item in subdivision:
            line = f"({item['id']}, {item['code']}, '{item['name']}', {self.TRUE}, {item['parent_id']}, {self.TRUE}),"
            result = '\n'.join((result, line))
        result = result[:-1]
        f = open(self.filepath_out, 'w')
        f.write(result)
        f.close()

        # SUBDIVISION_CLOSURE SQL
        result = '''INSERT INTO access.subdivision_closure(parent_subdivision_id, child_subdivision_id) VALUES'''
        for item in closure:
            line = f'({item[0]}, {item[1]}),'
            result = '\n'.join((result, line))
        result = result[:-1]
        f = open(self.filepath_out_closure, 'w')
        f.write(result)
        f.close()
        self.log.info('Done')
        self.log.info('.')

        return subdiv_name_id_dict
