import pandas as pd
from yargy import Parser, rule, or_
from yargy.predicates import gram, dictionary, normalized

# адрес файла с диалогами
test = r'C:/test_data.csv'
# формулировка правил для извлечения реплик:
# приветствие, имя менеджера, название компании, прощание
R_greeting = or_(
    rule(
        dictionary({'здравствуйте', 'привет'})
    ),
    rule(
        normalized('добрый'),
        dictionary({'утро', 'день', 'вечер'})
    )
)

R_name = or_(
    rule(
        dictionary({'зовут', 'это'}),
        gram('Name')
    ),
    rule(
        gram('Name'),
        dictionary({'зовут'})
    )
)

R_company = rule(
    normalized('компания'),
    gram('NOUN').repeatable()
)

R_goodbye = or_(
    rule(
        dictionary({'всего'}),
        dictionary({'доброго'})
    ),
    rule(
        dictionary({'до'}),
        dictionary({'встечи', 'свидания', 'завтра'})
    )
)


# функция для извлечения реплик приветствия из строки, возвращает строку с текстом приветствия
def search_greeting(text):
    greeting_str = ''
    parser = Parser(R_greeting)
    for match in parser.findall(text):
        greeting_str += ' '.join([x.value for x in match.tokens]) + ' '
    return greeting_str


# функция для извлечения имени менеджера из строки
def search_name(text):
    name_str = ''
    parser = Parser(R_name)
    for match in parser.findall(text):
        for x in match.tokens:
            if x.value != 'зовут':
                name_str = ' '.join([x.value]) + ' '
    return name_str


# функция для извлечения названия компании из строки
def search_company(text):
    company_str = ''
    parser = Parser(R_company)
    for match in parser.findall(text):
        for x in match.tokens:
            if (x.value != 'компания') and (x.value != 'компании'):
                company_str += ' '.join([x.value]) + ' '
    return company_str


# функция для извлечения реплик прощания из строки
def search_goodbye(text):
    goodbye_str = ''
    parser = Parser(R_goodbye)
    for match in parser.findall(text):
        goodbye_str += ' '.join([x.value for x in match.tokens]) + ' '
    return goodbye_str


# функция для применения указанных выше функций к таблице,
# возвращает таблицу с флагами наличия реплик и таблицу с результатами парсинга для диалогов
def extract_replica(func_name, column_name):
    flag_table = pd.DataFrame(columns=['dlg_id', 'line_n', 'role', 'text'])
    pars_table = pd.DataFrame(columns=['dlg_id'])
    for dlg in range(max(data_set['dlg_id']) + 1):  # поиск применяется для каждого диалога по очереди
        data_set_1 = data_set[data_set['dlg_id'] == dlg].copy()
        pars_table.loc[dlg, 'dlg_id'] = dlg
        for line in data_set_1['line_n']:
            i = data_set_1[data_set_1['line_n'] == line].index
            replica = func_name(str(data_set_1.loc[i, 'text']).lower())
            data_set_1.loc[i, column_name] = False
            if replica != '':
                data_set_1.loc[i, column_name] = True
                pars_table.loc[dlg, column_name] = replica
        flag_table = pd.concat([flag_table] + [data_set_1]).drop(['role', 'text'], axis=1)
    return flag_table, pars_table


# функция объединения таблиц с флагами и парсингом после поиска всех реплик
def merge_tables(func_1, func_2):
    table_1, table_2 = func_1
    table_3, table_4 = func_2
    table_1_3 = table_1.merge(table_3, how='outer')
    table_2_4 = table_2.merge(table_4, how='outer')
    return table_1_3, table_2_4


data_set = pd.read_csv(test).copy()
data_set = data_set.drop(data_set[data_set['role'] != 'manager'].index)  # в датасете остаются только реплики менеджера
# объединение таблиц с флагами и результатами
table1, table2 = merge_tables(extract_replica(search_greeting, 'greeting'), extract_replica(search_name, 'name'))
table3, table4 = merge_tables(extract_replica(search_company, 'company'), extract_replica(search_goodbye, 'goodbye'))
data_flag_table = data_set.merge(table1, how='outer').merge(table3, how='outer').drop(['company'], axis=1)
pars_result_table = table2.merge(table4, how='outer')
# проверка требования к менеджеру, что в каждом диалоге нужно здороваться и прощаться
pars_result_table['greeting'] = pars_result_table['greeting'].fillna(value=' ')
pars_result_table['goodbye'] = pars_result_table['goodbye'].fillna(value=' ')
pars_result_table['rule_greeting_goodbye'] = (pars_result_table['greeting'] != ' ') & (
            pars_result_table['goodbye'] != ' ')
# удаление лишних столбцов
pars_result_table = pars_result_table.drop(['greeting', 'goodbye'], axis=1)
# сохранение таблиц с результатами
data_flag_table.to_csv('C:/Users/gesch/PycharmProjects/Kosach_data_flag_table.csv')
pars_result_table.to_csv('C:/Users/gesch/PycharmProjects/Kosach_pars_result_table.csv')