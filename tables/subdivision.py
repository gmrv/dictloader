import pandas as pd
from logger.logger import Logger


class Subdivision:
    TRUE = 'true'
    NONE = 'NULL'
    df = None
    log = None

    def __init__(self):

        self.tablename = 'subdivision'
        self.filepath_src = 'src/Подразделения v01.xlsx'
        self.sheet_name = 'Подразделения'
        self.filepath_out = 'out/subdivision.sql'
        self.filepath_out_closure = 'out/subdivision_closure.sql'

        self.log = Logger(self.tablename).log
        self.df = pd.read_excel(self.filepath_src, sheet_name=self.sheet_name, header=None)
        self.df = self.df.reset_index()

        self.log.info(f'Successfully read {len(self.df)} lines')

    @staticmethod
    def normalise_str(s):
        return str(s).strip().lower()

    def run(self):
        subdivision = []
        closure = []
        subdiv_name_id_dict = {}  # Словарь {Имя_подразделения : id_подразделения} для поиска id по имени

        # начинаем с 2 т.к. в бд уже есть подразделение с id = 1 - ROOT
        id = 2
        closure.append([1, 2])

        for index, row in self.df.iterrows():
            try:
                # row.values[1] - строка вида: root_subdibision\subd1\subd2\subd3\subd4
                # берем последнее и предпоследнее поле, получаме пару parent_name : child_name
                parent_name = row.values[1].split('\\')[-2]
                child_name = row.values[1].split('\\')[-1]
                child_id = id
                subdiv_name_id_dict[self.normalise_str(child_name)] = child_id
                subdivision.append({
                    'id': child_id,
                    'code': str(child_id) + '000',
                    'name': child_name,
                    'parent_name': parent_name,
                    'parent_id': subdiv_name_id_dict[self.normalise_str(parent_name)]
                })
                closure.append([subdiv_name_id_dict[self.normalise_str(parent_name)], child_id])
                id += 1
            except IndexError as e:
                child_name = row.values[1].split('\\')[-1]

                # Если это первая строка файла, когда еще нет родителя - обрабатываем
                if index == 0 and child_name:
                    subdiv_name_id_dict[self.normalise_str(child_name)] = id
                    self.log.debug('Except: Processing the first line')
                    subdivision.append({
                        'id': id,
                        'code': 0,
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
