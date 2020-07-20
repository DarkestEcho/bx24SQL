from datetime import datetime, timedelta
from time import sleep

from tqdm import tqdm
import pymysql.cursors
from bitrix24 import *
from prettytable import PrettyTable

# region = Исходные данные

# Кол-во дней, за которое извлекутся данные
DAYS = 1
# Текущая дата/время
now = datetime.now()
# Дата фильтрации, после которой будут получены данные
date_ = datetime.now().date() - timedelta(DAYS)

# Название базы данных
DATABASE = "u0726932_test"
# Адрес хоста
HOST = "37.140.192.22"
# Имя пользователя
USER = "u0726932_dima"
# Пароль
PASSWORD = "qwerty123"

# https://rona.bitrix24.ru/rest/1/0v8ti9bab1yhvpr1 https://b24-64p48e.bitrix24.ru/rest/1/3p4dnufck9d7ox2x

# Входящий вебхук Битрикс24
WEBHOOK = "https://rona.bitrix24.ru/rest/1/0v8ti9bab1yhvpr1"
# Таблицы с пользовательскими полями
TABLE_WH_UF = (
    ('Сделки', 'crm.deal.list'),
    ('Лиды', 'crm.lead.list'),
    ('Контакты', 'crm.contact.list'),
    ('Компании', 'crm.company.list'),
    ('КоммерческиеПредложения', 'crm.quote.list'),
    ('Счета', 'crm.invoice.list')
)
# Таблицы без пользовательских полей
TABLE_SIMPLE = (
    ('Дела', 'crm.activity.list'),
    ('Пользователи', 'user.get'),
    ('Телефония', 'voximplant.statistic.get'),
    ('Товары', 'crm.product.list'),
    ('Валюты', 'crm.currency.list'),
    ('ЕдиницыИзмерения', 'crm.measure.list'),
    ('СтавкиНДС', 'crm.vat.list'),
    ('Направления_сделок', 'crm.dealcategory.list'),
    ('Каталоги', 'crm.catalog.list'),
    ('РазделыТоваров', 'crm.productsection.list'),
    ('СтатусыСчетов', 'crm.invoice.status.list'),
    ('Статусы', 'crm.status.list'),
    ('Подразделения', 'department.get'),
    ('ПлатежныеСистемы', 'sale.paysystem.list'),
    ('ТипыПлательщиков', 'sale.persontype.list'),
    ('ПользовательскиеПоля', 'ПользовательскиеПоля_Значения'),
    ('ТоварыСделок', 'ТоварыЛидов')

)
# Поля, которые не нужно обрабатывать
bad_user_fileds = [
    'UF_COMPANY_ID',
    'UF_CONTACT_ID',
    'UF_MYCOMPANY_ID',
    'UF_DEAL_ID',
    'UF_QUOTE_ID'
]
# Список id сделок
deal_id_list = []
# Список id лидов
lead_id_list = []
# Логи
logs = ""


# endregion

# Модуль для сущностей с пользовательскими полями
def module_uf(table, method):
    global logs
    try:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Получение данных из Битрикс24: {table}\n\n"

        all_entity = get_b24_value_uf(method)
        if all_entity != 0 and len(all_entity) > 0:
            print("\n", table + ":\n", sep='')
            # получение пользовательских полей сделки
            logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                    f"Получение пользовательских полей для: {table}\n\n"
            _user_fields = add_new_columns(table)

            th = get_name_columns(table)

            td = []
            logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                    f"Обработка и занесение в SQL данных: {table}\n\n"

            # обработка каждой сделки
            all_entity_ = tqdm(all_entity, unit='ent', ncols=100, dynamic_ncols=False)
            for entity in all_entity_:
                uf_value = get_uf_value(entity, _user_fields)
                td.extend(go_sql_query(table, entity, uf_value))

                sleep(0.01)
            # columns = len(th)
            # table_ = PrettyTable(th)
            # while td:
            #     table_.add_row(td[:columns])
            #     td = td[columns:]
            # print(table_)

            logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                    f"Работа текущего модуля: {table} завершена. Обработано сущностей: {len(all_entity)}\n\n\n"
        elif len(all_entity) == 0:
            print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Новых данных не найдено\n")
            logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Новых данных не найдено\n\n\n"
        else:
            logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                    f"Работа текущего модуля: {table} завершена с ошибками\n\n\n"
        print("\n")
        sleep(0.05)
    except BaseException as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка. Работа модуля {table} преждевременно завершена. " \
                f"Причина: {message}: {message}\n\n\n"
        print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка. Работа модуля {table} преждевременно завершена."
              f"\nПричина: {message}: {message}\n\n\n")


# Модуль для сущностей без пользовательских полей
def module_simple(table, method):
    global logs
    try:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Получение данных из Битрикс24: {table}\n\n"
        all_entity = get_b24_value_simple(method)

        if all_entity != 0 and len(all_entity) > 0:
            print("\n", table + ":\n", sep='')
            th = get_name_columns(table)
            td = []
            logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                    f"Обработка и занесение в SQL данных: {table}\n\n"
            # обработка каждой сделки
            all_entity_ = tqdm(all_entity, unit='ent', ncols=100, dynamic_ncols=False)

            for entity in all_entity_:
                td.extend(go_sql_query(table, entity))
                sleep(0.01)

            # columns = len(th)
            # table_ = PrettyTable(th)
            # while td:
            #     table_.add_row(td[:columns])
            #     td = td[columns:]
            # print(table_)

            logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                    f"Работа текущего модуля: {table} завершена. Обработано сущностей: {len(all_entity)}\n\n\n"
        elif len(all_entity) == 0:
            print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
                  f"Новых данных не найдено\n")
            logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                    f"Новых данных не найдено\n\n\n"
        else:
            logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                    f"Работа текущего модуля: {table} завершена с ошибками\n\n\n"
            print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
                  f"Работа текущего модуля: {table}завершена с ошибками\n\n\n")
        print("\n")
        sleep(0.05)
    except BaseException as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка. Работа модуля {table} преждевременно завершена. " \
                f"Причина: {message}\n\n\n"
        print(f"\n\n{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка. Работа модуля {table} преждевременно завершена."
              f"\nПричина: {message}\n\n\n")


# Выполняет SQL-запрос
def execute_sql(sql__, end_value):
    global logs
    try:
        with db:
            cur = db.cursor()

            cur.execute(sql__, end_value)

    except BaseException as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка загрузки в таблицу SQL следующих данных:" \
                f"\n{end_value}. Сообщение ошибки: {message}: {message}\n\n"
        print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка загрузки в таблицу SQL следующих данных:"
              f"\n{end_value}. Сообщение ошибки: {message}: {message}\n\n")


# Формирует SQL-запрос и выполняет его
def form_sql_query(end_value, table_name, value):
    global logs
    try:
        s = '%s, ' * len(end_value)
        find_id = find_sql(table_name, value)

        if len(find_id) == 0:
            sql = f"""INSERT INTO {table_name} VALUES({s[0:-2]})"""
            execute_sql(sql, end_value)
        else:
            if table_name == 'Валюты':
                sql = f"""DELETE FROM {table_name} WHERE `CURRENCY` = %s"""
            elif table_name == 'ПользовательскиеПоля_Значения':
                sql = f"""DELETE FROM {table_name} WHERE `ITEM_ID` = %s"""
            else:
                sql = f"""DELETE FROM {table_name} WHERE ID = %s"""
            with db:
                cur = db.cursor()
                cur.execute(sql, find_id[0][0])
            sql = f"""INSERT INTO {table_name} VALUES({s[0:-2]})"""
            execute_sql(sql, end_value)

    except BaseException as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка формирования SQL-запроса для следующих данных:\n" \
                f"{end_value}\nТаблица: {table_name}\nСообщение ошибки: {message}: {message}\n\n"
        print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка формирования SQL-запроса для следующих данных:\n"
              f"{end_value}\nТаблица: {table_name}\nСообщение ошибки: {message}: {message}\n\n")


# Находит в таблице SQL записи с указанным ID (необходим для обновления устаревших записей)
def find_sql(table, value):
    global logs
    try:
        with db:
            cur = db.cursor()

            if table == 'Валюты':
                __sql = f"""SELECT `CURRENCY` FROM {table} WHERE `CURRENCY`= %s"""
                cur.execute(__sql, value['CURRENCY'])
            elif table == 'ПользовательскиеПоля_Значения':
                __sql = f"""SELECT `ITEM_ID` FROM {table} WHERE `ITEM_ID`= %s"""
                cur.execute(__sql, value['ITEM_ID'])
            elif table == 'ТипыПлательщиков':
                __sql = f"""SELECT `ID` FROM {table} WHERE `ID`= %s"""
                cur.execute(__sql, value['id'])
            else:
                __sql = f"""SELECT `ID` FROM {table} WHERE `ID`= %s"""
                cur.execute(__sql, value['ID'])
            find_id = cur.fetchall()
        return find_id
    except BaseException as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка поиска одинаковых элементов.\nТаблица: {table}\n" \
                f"Данные: {value}\n" \
                f"Сообщение ошибки: {message}: {message}\n\n"
        print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка поиска одинаковых элементов.\nТаблица: {table}\n"
              f"Данные: {value}\n"
              f"Сообщение ошибки: {message}: {message}\n\n")
        return 0


# Получает из Битрикс24 все сущности указанного типа за указанный промежуток времени
def get_b24_value_uf(method):
    global logs
    try:
        # получение сущностей
        if method == 'crm.invoice.list':
            result = bx24.callMethod(method, select=["*", "UF_*"], filter={">DATE_UPDATE": date_,
                                                                           "<DATE_UPDATE": datetime.now().date()})
        else:
            result = bx24.callMethod(method, select=["*", "UF_*"], filter={">DATE_MODIFY": date_,
                                                                           "<DATE_MODIFY": datetime.now().date()})

        return result
    except BaseException as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка cовершения REST запроса к Битрикс24.\n" \
                f"Метод: {method}\n" \
                f"Сообщение ошибки: {message}: {message}\n\n"
        print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка cовершения REST запроса к Битрикс24.\n"
              f"Метод: {method}\n"
              f"Сообщение ошибки: {message}: {message}\n\n")
        return 0


# Получает из Битрикс24 все сущности указанного типа за указанный промежуток времени без пользовательских полей
def get_b24_value_simple(method):
    global logs
    try:
        # получение сущностей
        if method == 'crm.activity.list':
            result = bx24.callMethod(method, select=["*"], filter={">LAST_UPDATED": str(date_),
                                                                   "<LAST_UPDATED": str(datetime.now().date())})
        elif method == 'user.get':
            result = bx24.callMethod(method, select=["*"])
        elif method == 'voximplant.statistic.get':
            result = bx24.callMethod(method, select=["*"], filter={">CALL_START_DATE": str(date_),
                                                                   "<CALL_START_DATE": str(datetime.now().date())})
        elif method == 'crm.product.list':
            # ?
            result = bx24.callMethod(method, select=["*", 'PROPERTY_*'])

        else:
            result = bx24.callMethod(method)
            # ,filter={">DATE_CREATE": str(date_), "<DATE_CREATE": str(datetime.now().date())}
        if method == 'sale.persontype.list':
            return result.get('personTypes')
        else:
            return result
    except BitrixError as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка cовершения REST запроса к Битрикс24.\n" \
                f"Метод: {method}\n" \
                f"Сообщение ошибки: {message}: {message}\n\n"
        print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка cовершения REST запроса к Битрикс24.\n"
              f"Метод: {method}\n"
              f"Сообщение ошибки: {message}: {message}\n\n")
        return 0


# Возвращает названия столбов таблицы SQL
def get_name_columns(table):
    global logs
    try:
        with db:
            cur = db.cursor()
            # получение из БД заголовков столбцов
            cur.execute(f"SELECT * FROM {table}")

            all_desc = cur.description

            name_list = []  # Заголовки в таблице

            # формирование листа с названиями заголовков в таблице БД
            for desc in all_desc:
                name_list += [desc[0]]
        return name_list
    except BaseException as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка получения названий столбов в таблице: {table}\n" \
                f"Сообщение ошибки: {message}: {message}\n\n"
        print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка получения названий столбов в таблице: {table}\n"
              f"Сообщение ошибки: {message}: {message}\n\n")


# Добавляет новые пользовательские поля в таблицу сущности (не в таблицу польз. полей)
# Формирует List пользовательских полей в правильном порядке
def add_new_columns(table):
    global logs
    try:
        with db:
            cur = db.cursor()
            # получение из БД заголовков столбцов
            cur.execute(f"SELECT * FROM {table}")
            # logs += f"Select all from table '{table}' successfully\n\n"
            all_desc = cur.description

            name_list = []  # Заголовки в таблице
            new_column = []

            # формирование листа с названиями заголовков в таблице БД
            for desc in all_desc:
                name_list += [desc[0]]

            # print(name_list)
            # logs += f"Column names received: {name_list}\n\n"
            bx24_ = Bitrix24(WEBHOOK)
            # получение сущностей
            method = ''
            if table == 'Сделки' or table == 'test':
                method = 'crm.deal.userfield.list'
            elif table == 'Лиды':
                method = 'crm.lead.userfield.list'
            elif table == 'Контакты':
                method = 'crm.contact.userfield.list'
            elif table == 'Компании':
                method = 'crm.company.userfield.list'
            elif table == 'КоммерческиеПредложения':
                method = 'crm.quote.userfield.list'
            elif table == 'Счета':
                method = 'crm.invoice.userfield.list'
            result = bx24_.callMethod(method)
            user_fields_ = [uf['FIELD_NAME'] for uf in result]
            # print(user_fields)

            for x in user_fields_:
                if name_list.count(x) == 0:
                    # print("\033[31m {}".format(f"Элемент {x} отсутствует в таблице"))
                    # SQL
                    sql_1 = f"ALTER TABLE {table} ADD {x} TEXT CHARACTER SET" \
                            f" utf8 COLLATE utf8_general_ci NULL AFTER {name_list[-1]}"
                    # Выполнить команду запроса (Execute Query).
                    try:
                        cur.execute(sql_1)
                        name_list.append(x)
                        new_column.append(x)
                        print(f"New column added: {x}")
                        # logs += f"New column added: {x}\n"
                    except BaseException as message:
                        # logs += f"Error adding column '{x}' to table '{table}':" \
                        # f" \n{message} {message}\n\n"
                        print(f"Error adding column to table: \n{message} {message}")
                # else:
                # print("\033[32m {}".format(f"Элемент {x} присутствует в таблице"))
            result_uf = []
            if len(new_column) > 0:
                logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                        f"Добавлены новые столбцы:\n{new_column}\n\n"
            for n in name_list:
                if n.find("UF_") != -1 and bad_user_fileds.count(n) == 0:
                    result_uf += [n]
            return result_uf
    except BaseException as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка добавления новых столбцов.\n" \
                f"Таблица: {table}\nСообщение ошибки: {message}: {message}\n\n"
        print(f"{now.hour}:{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка добавления новых столбцов.\n"
              f"Таблица: {table}\nСообщение ошибки: {message}: {message}\n\n")
        return name_list


# Формирует List значений пользовательских полей
def get_uf_value(value, uf_):
    global logs
    uf_val = []
    try:
        for uf in uf_:
            if value.get(uf) is not None:
                uf_val += [str(value.get(uf))]
            else:
                uf_val += [None]
        return uf_val
    except BaseException as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка формирования массива польз. полей.\n" \
                f"Польз. поля: {uf_}\nДанные: {value}\nСообщение ошибки: {message}: {message}\n\n"
        print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка формирования массива польз. полей.\n"
              f"Польз. поля: {uf_}\nДанные: {value}\nСообщение ошибки: {message}: {message}\n\n")
        return uf_val


# Упорядочивает данные для запроса SQL и совершает запрос
def go_sql_query(table_name, value, uf_val=None):
    global deal_id_list, lead_id_list, logs

    try:
        end_value = []
        if table_name == 'Сделки':
            end_value = [
                int(value['ID']) if value.get('ID') is not None else None,
                value.get('TITLE'),
                value.get('TYPE_ID'),
                int(value.get('CATEGORY_ID')) if value.get('CATEGORY_ID') is not None else None,
                value.get('STAGE_ID'),
                value.get('STAGE_SEMANTIC_ID'),
                value.get('IS_NEW'),
                value.get('IS_RECURRING'),
                value.get('IS_RETURN_CUSTOMER'),
                value.get('IS_REPEATED_APPROACH'),
                int(value.get('PROBABILITY')) if value.get('PROBABILITY') is not None else None,
                value.get('CURRENCY_ID'),
                float(value.get('OPPORTUNITY')) if value.get('OPPORTUNITY') is not None else None,
                value.get('IS_MANUAL_OPPORTUNITY'),
                float(value.get('TAX_VALUE')) if value.get('TAX_VALUE') is not None else None,
                int(value.get('COMPANY_ID')) if value.get('COMPANY_ID') is not None else None,
                int(value.get('CONTACT_ID')) if value.get('CONTACT_ID') is not None else None,
                int(value.get('CONTACT_IDS')) if value.get('CONTACT_IDS') is not None else None,
                int(value.get('QUOTE_ID')) if value.get('QUOTE_ID') is not None else None,
                value.get('BEGINDATE')[0:-6].replace('T', ' ') if value.get('BEGINDATE') != '' else None,
                value.get('CLOSEDATE')[0:-6].replace('T', ' ') if value.get('CLOSEDATE') != '' else None,
                value.get('OPENED'),
                value.get('CLOSED'),
                value.get('COMMENTS'),
                int(value.get('ASSIGNED_BY_ID')) if value.get('ASSIGNED_BY_ID') is not None else None,
                int(value.get('CREATED_BY_ID')) if value.get('CREATED_BY_ID') is not None else None,
                int(value.get('MODIFY_BY_ID')) if value.get('MODIFY_BY_ID') is not None else None,
                value.get('DATE_CREATE')[0:-6].replace('T', ' ') if value.get('DATE_CREATE') != '' else None,
                value.get('DATE_MODIFY')[0:-6].replace('T', ' ') if value.get('DATE_MODIFY') != '' else None,
                value.get('SOURCE_ID'),
                value.get('SOURCE_DESCRIPTION'),
                int(value.get('LEAD_ID')) if value.get('LEAD_ID') is not None else None,
                value.get('ADDITIONAL_INFO'),
                value.get('LOCATION_ID'),
                value.get('ORIGINATOR_ID'),
                value.get('ORIGIN_ID'),
                value.get('UTM_SOURCE'),
                value.get('UTM_MEDIUM'),
                value.get('UTM_CAMPAIGN'),
                value.get('UTM_CONTENT'),
                value.get('UTM_TERM')
            ]
            deal_id_list += [int(value['ID']) if value.get('ID') is not None else None]
            end_value.extend(uf_val)
            form_sql_query(end_value, table_name, value)

        elif table_name == 'Лиды':

            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                value.get('TITLE'),
                int(value.get('HONORIFIC')) if value.get('HONORIFIC') is not None else None,
                value.get('NAME'),
                value.get('SECOND_NAME'),
                value.get('LAST_NAME'),
                value.get('BIRTHDATE')[0:-6].replace('T', ' ') if value.get('BIRTHDATE') != '' else None,
                value.get('COMPANY_TITLE'),
                value.get('SOURCE_ID'),
                value.get('SOURCE_DESCRIPTION'),
                value.get('STATUS_ID'),
                value.get('STATUS_DESCRIPTION'),
                value.get('STATUS_SEMANTIC_ID'),
                value.get('POST'),
                value.get('ADDRESS'),
                value.get('ADDRESS_2'),
                value.get('ADDRESS_CITY'),
                value.get('ADDRESS_POSTAL_CODE'),
                value.get('ADDRESS_REGION'),
                value.get('ADDRESS_PROVINCE'),
                value.get('ADDRESS_COUNTRY'),
                value.get('ADDRESS_COUNTRY_CODE'),
                value.get('CURRENCY_ID'),
                float(value.get('OPPORTUNITY')) if value.get('OPPORTUNITY') is not None else None,
                value.get('OPENED'),
                value.get('COMMENTS'),
                value.get('HAS_PHONE'),
                value.get('HAS_EMAIL'),
                value.get('HAS_IMOL'),
                int(value.get('ASSIGNED_BY_ID')) if value.get('ASSIGNED_BY_ID') is not None else None,
                int(value.get('CREATED_BY_ID')) if value.get('CREATED_BY_ID') is not None else None,
                int(value.get('MODIFY_BY_ID')) if value.get('MODIFY_BY_ID') is not None else None,
                value.get('DATE_CREATE')[0:-6].replace('T', ' ') if value.get('DATE_CREATE') != '' else None,
                value.get('DATE_MODIFY')[0:-6].replace('T', ' ') if value.get('DATE_MODIFY') != '' else None,
                int(value.get('COMPANY_ID')) if value.get('COMPANY_ID') is not None else None,
                int(value.get('CONTACT_ID')) if value.get('CONTACT_ID') is not None else None,
                int(value.get('CONTACT_IDS')) if value.get('CONTACT_IDS') is not None else None,
                value.get('IS_RETURN_CUSTOMER'),
                value.get('DATE_CLOSED')[0:-6].replace('T', ' ') if value.get('DATE_CLOSED') != '' else None,
                value.get('ORIGINATOR_ID'),
                value.get('ORIGIN_ID'),
                value.get('UTM_SOURCE'),
                value.get('UTM_MEDIUM'),
                value.get('UTM_CAMPAIGN'),
                value.get('UTM_CONTENT'),
                value.get('UTM_TERM'),
                value.get('PHONE'),
                value.get('EMAIL'),
                value.get('WEB'),
                value.get('IM')
            ]
            lead_id_list += [int(value['ID']) if value.get('ID') is not None else None]
            end_value.extend(uf_val)
            form_sql_query(end_value, table_name, value)

        elif table_name == 'Контакты':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                value.get('HONORIFIC'),
                value.get('NAME'),
                value.get('SECOND_NAME'),
                value.get('LAST_NAME'),
                value.get('PHOTO'),
                value.get('BIRTHDATE')[0:-6].replace('T', ' ') if value.get('BIRTHDATE') != '' else None,
                value.get('TYPE_ID'),
                value.get('SOURCE_ID'),
                value.get('SOURCE_DESCRIPTION'),
                value.get('POST'),
                value.get('ADDRESS'),
                value.get('ADDRESS_2'),
                value.get('ADDRESS_CITY'),
                value.get('ADDRESS_POSTAL_CODE'),
                value.get('ADDRESS_REGION'),
                value.get('ADDRESS_PROVINCE'),
                value.get('ADDRESS_COUNTRY'),
                value.get('ADDRESS_COUNTRY_CODE'),
                value.get('COMMENTS'),
                value.get('OPENED'),
                value.get('EXPORT'),
                value.get('HAS_PHONE'),
                value.get('HAS_EMAIL'),
                value.get('HAS_IMOL'),
                int(value.get('ASSIGNED_BY_ID')) if value.get('ASSIGNED_BY_ID') is not None else None,
                int(value.get('CREATED_BY_ID')) if value.get('CREATED_BY_ID') is not None else None,
                int(value.get('MODIFY_BY_ID')) if value.get('MODIFY_BY_ID') is not None else None,
                value.get('DATE_CREATE')[0:-6].replace('T', ' ') if value.get('DATE_CREATE') != '' else None,
                value.get('DATE_MODIFY')[0:-6].replace('T', ' ') if value.get('DATE_MODIFY') != '' else None,
                int(value.get('COMPANY_ID')) if value.get('COMPANY_ID') is not None else None,
                int(value.get('COMPANY_IDS')) if value.get('COMPANY_IDS') is not None else None,
                int(value.get('LEAD_ID')) if value.get('LEAD_ID') is not None else None,
                value.get('ORIGINATOR_ID'),
                value.get('ORIGIN_ID'),
                value.get('ORIGIN_VERSION'),
                int(value.get('FACE_ID')) if value.get('FACE_ID') is not None else None,
                value.get('UTM_SOURCE'),
                value.get('UTM_MEDIUM'),
                value.get('UTM_CAMPAIGN'),
                value.get('UTM_CONTENT'),
                value.get('UTM_TERM'),
                value.get('PHONE'),
                value.get('EMAIL'),
                value.get('WEB'),
                value.get('IM')
            ]

            end_value.extend(uf_val)
            form_sql_query(end_value, table_name, value)

        elif table_name == 'Компании':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                value.get('TITLE'),
                value.get('COMPANY_TYPE'),
                value.get('LOGO'),
                value.get('ADDRESS'),
                value.get('ADDRESS_2'),
                value.get('ADDRESS_CITY'),
                value.get('ADDRESS_POSTAL_CODE'),
                value.get('ADDRESS_REGION'),
                value.get('ADDRESS_PROVINCE'),
                value.get('ADDRESS_COUNTRY'),
                value.get('ADDRESS_COUNTRY_CODE'),
                value.get('ADDRESS_LEGAL'),
                value.get('REG_ADDRESS'),
                value.get('REG_ADDRESS_2'),
                value.get('REG_ADDRESS_CITY'),
                value.get('REG_ADDRESS_POSTAL_CODE'),
                value.get('REG_ADDRESS_REGION'),
                value.get('REG_ADDRESS_PROVINCE'),
                value.get('REG_ADDRESS_COUNTRY'),
                value.get('REG_ADDRESS_COUNTRY_CODE'),
                value.get('BANKING_DETAILS'),
                value.get('INDUSTRY'),
                value.get('EMPLOYEES'),
                value.get('CURRENCY_ID'),
                float(value.get('REVENUE')) if value.get('REVENUE') is not None else None,
                value.get('OPENED'),
                value.get('COMMENTS'),
                value.get('HAS_PHONE'),
                value.get('HAS_EMAIL'),
                value.get('HAS_IMOL'),
                value.get('IS_MY_COMPANY'),
                int(value.get('ASSIGNED_BY_ID')) if value.get('ASSIGNED_BY_ID') is not None else None,
                int(value.get('CREATED_BY_ID')) if value.get('CREATED_BY_ID') is not None else None,
                int(value.get('MODIFY_BY_ID')) if value.get('MODIFY_BY_ID') is not None else None,
                value.get('DATE_CREATE')[0:-6].replace('T', ' ') if value.get('DATE_CREATE') != '' else None,
                value.get('DATE_MODIFY')[0:-6].replace('T', ' ') if value.get('DATE_MODIFY') != '' else None,
                int(value.get('CONTACT_ID')) if value.get('CONTACT_ID') is not None else None,
                int(value.get('LEAD_ID')) if value.get('LEAD_ID') is not None else None,
                value.get('ORIGINATOR_ID'),
                value.get('ORIGIN_ID'),
                value.get('ORIGIN_VERSION'),
                value.get('UTM_SOURCE'),
                value.get('UTM_MEDIUM'),
                value.get('UTM_CAMPAIGN'),
                value.get('UTM_CONTENT'),
                value.get('UTM_TERM'),
                value.get('PHONE'),
                value.get('EMAIL'),
                value.get('WEB'),
                value.get('IM')
            ]

            end_value.extend(uf_val)
            form_sql_query(end_value, table_name, value)

        elif table_name == 'КоммерческиеПредложения':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                value.get('QUOTE_NUMBER'),
                value.get('TITLE'),
                value.get('STATUS_ID'),
                value.get('CURRENCY_ID'),
                float(value.get('OPPORTUNITY')) if value.get('OPPORTUNITY') is not None else None,
                float(value.get('TAX_VALUE')) if value.get('TAX_VALUE') is not None else None,
                int(value.get('COMPANY_ID')) if value.get('COMPANY_ID') is not None else None,
                int(value.get('MYCOMPANY_ID')) if value.get('MYCOMPANY_ID') is not None else None,
                int(value.get('CONTACT_ID')) if value.get('CONTACT_ID') is not None else None,
                int(value.get('CONTACT_IDS')) if value.get('CONTACT_IDS') is not None else None,
                value.get('BEGINDATE')[0:-6].replace('T', ' ') if value.get('BEGINDATE') != '' else None,
                value.get('CLOSEDATE')[0:-6].replace('T', ' ') if value.get('CLOSEDATE') != '' else None,
                value.get('OPENED'),
                value.get('CLOSED'),
                value.get('COMMENTS'),
                value.get('CONTENT'),
                value.get('TERMS'),
                value.get('CLIENT_TITLE'),
                value.get('CLIENT_ADDR'),
                value.get('CLIENT_CONTACT'),
                value.get('CLIENT_EMAIL'),
                value.get('CLIENT_PHONE'),
                value.get('CLIENT_TP_ID'),
                value.get('CLIENT_TPA_ID'),
                int(value.get('ASSIGNED_BY_ID')) if value.get('ASSIGNED_BY_ID') is not None else None,
                int(value.get('CREATED_BY_ID')) if value.get('CREATED_BY_ID') is not None else None,
                int(value.get('MODIFY_BY_ID')) if value.get('MODIFY_BY_ID') is not None else None,
                value.get('DATE_CREATE')[0:-6].replace('T', ' ') if value.get('DATE_CREATE') != '' else None,
                value.get('DATE_MODIFY')[0:-6].replace('T', ' ') if value.get('DATE_MODIFY') != '' else None,
                int(value.get('LEAD_ID')) if value.get('LEAD_ID') is not None else None,
                int(value.get('DEAL_ID')) if value.get('DEAL_ID') is not None else None,
                value.get('PERSON_TYPE_ID'),
                value.get('LOCATION_ID'),
                value.get('UTM_SOURCE'),
                value.get('UTM_MEDIUM'),
                value.get('UTM_CAMPAIGN'),
                value.get('UTM_CONTENT'),
                value.get('UTM_TERM')
            ]

            end_value.extend(uf_val)
            form_sql_query(end_value, table_name, value)

        elif table_name == 'Счета':
            end_value = [
                value.get('ACCOUNT_NUMBER'),
                value.get('COMMENTS'),
                value.get('CURRENCY'),
                value.get('DATE_BILL')[0:-6].replace('T', ' ') if value.get('DATE_BILL') != '' else None,
                value.get('DATE_INSERT')[0:-6].replace('T', ' ') if value.get('DATE_INSERT') != '' else None,
                value.get('DATE_MARKED')[0:-6].replace('T', ' ') if value.get('DATE_MARKED') != '' else None,
                value.get('DATE_PAY_BEFORE')[0:-6].replace('T', ' ') if value.get('DATE_PAY_BEFORE') != '' else None,
                value.get('DATE_PAYED')[0:-6].replace('T', ' ') if value.get('DATE_PAYED') != '' else None,
                value.get('DATE_STATUS')[0:-6].replace('T', ' ') if value.get('DATE_STATUS') != '' else None,
                value.get('DATE_UPDATE')[0:-6].replace('T', ' ') if value.get('DATE_UPDATE') != '' else None,
                int(value.get('CREATED_BY')) if value.get('CREATED_BY') is not None else None,
                int(value.get('EMP_PAYED_ID')) if value.get('EMP_PAYED_ID') is not None else None,
                int(value.get('EMP_STATUS_ID')) if value.get('EMP_STATUS_ID') is not None else None,
                int(value.get('ID')) if value.get('ID') is not None else None,
                value.get('LID'),
                value.get('XML_ID'),
                value.get('ORDER_TOPIC'),
                int(value.get('PAY_SYSTEM_ID')) if value.get('PAY_SYSTEM_ID') is not None else None,
                value.get('PAY_VOUCHER_DATE')[0:-6].replace('T', ' ') if value.get('PAY_VOUCHER_DATE') != '' else None,
                value.get('PAY_VOUCHER_NUM'),
                value.get('PAYED'),
                int(value.get('PERSON_TYPE_ID')) if value.get('PERSON_TYPE_ID') is not None else None,
                float(value.get('PRICE')) if value.get('PRICE') is not None else None,
                value.get('REASON_MARKED'),
                value.get('RESPONSIBLE_EMAIL'),
                int(value.get('RESPONSIBLE_ID')) if value.get('RESPONSIBLE_ID') is not None else None,
                value.get('RESPONSIBLE_LAST_NAME'),
                value.get('RESPONSIBLE_LOGIN'),
                value.get('RESPONSIBLE_NAME'),
                value.get('RESPONSIBLE_PERSONAL_PHOTO'),
                value.get('RESPONSIBLE_SECOND_NAME'),
                value.get('RESPONSIBLE_WORK_POSITION'),
                value.get('STATUS_ID'),
                float(value.get('TAX_VALUE')) if value.get('TAX_VALUE') is not None else None,
                value.get('IS_RECURRING'),
                int(value.get('UF_COMPANY_ID')) if value.get('UF_COMPANY_ID') is not None else None,
                int(value.get('UF_CONTACT_ID')) if value.get('UF_CONTACT_ID') is not None else None,
                int(value.get('UF_MYCOMPANY_ID')) if value.get('UF_MYCOMPANY_ID') is not None else None,
                int(value.get('UF_DEAL_ID')) if value.get('UF_DEAL_ID') is not None else None,
                int(value.get('UF_QUOTE_ID')) if value.get('UF_QUOTE_ID') is not None else None,
                value.get('USER_DESCRIPTION'),
                int(value.get('PR_LOCATION')) if value.get('PR_LOCATION') is not None else None,
                value.get('INVOICE_PROPERTIES'),
                value.get('PRODUCT_ROWS')
            ]

            end_value.extend(uf_val)
            form_sql_query(end_value, table_name, value)

        elif table_name == 'Дела':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                int(value.get('OWNER_ID')) if value.get('OWNER_ID') is not None else None,
                value.get('OWNER_TYPE_ID'),
                value.get('TYPE_ID'),
                value.get('PROVIDER_ID'),
                value.get('PROVIDER_TYPE_ID'),
                value.get('PROVIDER_GROUP_ID'),
                int(value.get('ASSOCIATED_ENTITY_ID')) if value.get('ASSOCIATED_ENTITY_ID') is not None else None,
                value.get('SUBJECT'),
                value.get('CREATED')[0:-6].replace('T', ' ') if value.get('CREATED') != '' else None,
                value.get('LAST_UPDATED')[0:-6].replace('T', ' ') if value.get('LAST_UPDATED') != '' else None,
                value.get('START_TIME')[0:-6].replace('T', ' ') if value.get('START_TIME') != '' else None,
                value.get('END_TIME')[0:-6].replace('T', ' ') if value.get('END_TIME') != '' else None,
                value.get('DEADLINE')[0:-6].replace('T', ' ') if value.get('DEADLINE') != '' else None,
                value.get('COMPLETED'),
                value.get('STATUS'),
                int(value.get('RESPONSIBLE_ID')) if value.get('RESPONSIBLE_ID') is not None else None,
                value.get('PRIORITY'),
                value.get('NOTIFY_TYPE'),
                int(value.get('NOTIFY_VALUE')) if value.get('NOTIFY_VALUE') is not None else None,
                value.get('DESCRIPTION') if value.get('PROVIDER_TYPE_ID') != 'EMAIL' else None,
                value.get('DESCRIPTION_TYPE'),
                value.get('DIRECTION'),
                value.get('LOCATION'),
                str(value.get('SETTINGS')),
                int(value.get('ORIGINATOR_ID')) if value.get('ORIGINATOR_ID') is not None else None,
                value.get('ORIGIN_ID'),
                int(value.get('AUTHOR_ID')) if value.get('AUTHOR_ID') is not None else None,
                int(value.get('EDITOR_ID')) if value.get('EDITOR_ID') is not None else None,
                str(value.get('PROVIDER_PARAMS')),
                value.get('PROVIDER_DATA'),
                int(value.get('RESULT_MARK')) if value.get('RESULT_MARK') is not None else None,
                float(value.get('RESULT_VALUE')) if value.get('RESULT_VALUE') is not None else None,
                float(value.get('RESULT_SUM')) if value.get('RESULT_SUM') is not None else None,
                value.get('RESULT_CURRENCY_ID'),
                int(value.get('RESULT_STATUS')) if value.get('RESULT_STATUS') is not None else None,
                int(value.get('RESULT_STREAM')) if value.get('RESULT_STREAM') is not None else None,
                value.get('RESULT_SOURCE_ID'),
                int(value.get('AUTOCOMPLETE_RULE')) if value.get('AUTOCOMPLETE_RULE') is not None else None
            ]

            form_sql_query(end_value, table_name, value)

        elif table_name == 'Пользователи':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                value.get('ACTIVE'),
                value.get('EMAIL'),
                value.get('NAME'),
                value.get('SECOND_NAME'),
                value.get('LAST_NAME'),
                value.get('PERSONAL_GENDER'),
                value.get('PERSONAL_PROFESSION'),
                value.get('PERSONAL_WWW'),
                value.get('PERSONAL_BIRTHDAY')[0:-6].replace('T', ' ') if value.get(
                    'PERSONAL_BIRTHDAY') != '' else None,
                value.get('PERSONAL_PHOTO'),
                value.get('PERSONAL_ICQ'),
                value.get('PERSONAL_PHONE'),
                value.get('PERSONAL_FAX'),
                value.get('PERSONAL_MOBILE'),
                value.get('PERSONAL_PAGER'),
                value.get('PERSONAL_STREET'),
                value.get('PERSONAL_CITY'),
                value.get('PERSONAL_STATE'),
                value.get('PERSONAL_ZIP'),
                value.get('PERSONAL_COUNTRY'),
                value.get('WORK_COMPANY'),
                value.get('WORK_POSITION'),
                value.get('WORK_PHONE'),
                value.get('UF_DEPARTMENT'),
                value.get('UF_INTERESTS'),
                value.get('UF_SKILLS'),
                value.get('UF_WEB_SITES'),
                value.get('UF_XING'),
                value.get('UF_LINKEDIN'),
                value.get('UF_FACEBOOK'),
                value.get('UF_TWITTER'),
                value.get('UF_SKYPE'),
                value.get('UF_DISTRICT'),
                value.get('UF_PHONE_INNER'),
                value.get('USER_TYPE')
            ]

            form_sql_query(end_value, table_name, value)

        elif table_name == 'Телефония':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                int(value.get('PORTAL_USER_ID')) if value.get('PORTAL_USER_ID') is not None else None,
                value.get('PORTAL_NUMBER'),
                value.get('PHONE_NUMBER'),
                value.get('CALL_ID'),
                value.get('CALL_CATEGORY'),
                int(value.get('CALL_DURATION')) if value.get('CALL_DURATION') is not None else None,
                value.get('CALL_START_DATE')[0:-6].replace('T', ' ') if value.get('CALL_START_DATE') != '' else None,
                value.get('CALL_VOTE'),
                float(value.get('COST')) if value.get('COST') is not None else None,
                value.get('COST_CURRENCY'),
                value.get('CALL_FAILED_CODE'),
                value.get('CALL_FAILED_REASON'),
                value.get('CRM_ENTITY_TYPE'),
                int(value.get('CRM_ENTITY_ID')) if value.get('CRM_ENTITY_ID') is not None else None,
                int(value.get('CRM_ACTIVITY_ID')) if value.get('CRM_ACTIVITY_ID') is not None else None,
                value.get('REST_APP_ID'),
                value.get('REST_APP_NAME'),
                value.get('TRANSCRIPT_ID'),
                value.get('TRANSCRIPT_PENDING'),
                value.get('SESSION_ID'),
                value.get('REDIAL_ATTEMPT'),
                value.get('COMMENT'),
                value.get('RECORD_FILE_ID'),
                value.get('CALL_TYPE')
            ]

            form_sql_query(end_value, table_name, value)

        elif table_name == 'Товары':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                int(value.get('CATALOG_ID')) if value.get('CATALOG_ID') is not None else None,
                float(value.get('PRICE')) if value.get('PRICE') is not None else None,
                value.get('CURRENCY_ID'),
                value.get('NAME'),
                value.get('CODE'),
                value.get('DESCRIPTION'),
                value.get('DESCRIPTION_TYPE'),
                value.get('ACTIVE'),
                int(value.get('SECTION_ID')) if value.get('SECTION_ID') is not None else None,
                int(value.get('SORT')) if value.get('SORT') is not None else None,
                int(value.get('VAT_ID')) if value.get('VAT_ID') is not None else None,
                value.get('VAT_INCLUDED'),
                int(value.get('MEASURE')) if value.get('MEASURE') is not None else None,
                value.get('XML_ID'),
                value.get('PREVIEW_PICTURE'),
                value.get('DETAIL_PICTURE'),
                value.get('DATE_CREATE')[0:-6].replace('T', ' ') if value.get('DATE_CREATE') != '' else None,
                value.get('TIMESTAMP_X')[0:-6].replace('T', ' ') if value.get('TIMESTAMP_X') != '' else None,
                int(value.get('MODIFIED_BY')) if value.get('MODIFIED_BY') is not None else None,
                int(value.get('CREATED_BY')) if value.get('CREATED_BY') is not None else None
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'Валюты':
            end_value = [
                value.get('CURRENCY'),
                int(value.get('AMOUNT_CNT')) if value.get('AMOUNT_CNT') is not None else None,
                float(value.get('AMOUNT')) if value.get('AMOUNT') is not None else None,
                int(value.get('SORT')) if value.get('SORT') is not None else None,
                value.get('BASE'),
                value.get('FULL_NAME'),
                value.get('LID'),
                value.get('FORMAT_STRING'),
                value.get('DEC_POINT'),
                value.get('THOUSANDS_SEP'),
                int(value.get('DECIMALS')) if value.get('DECIMALS') is not None else None,
                value.get('DATE_UPDATE')[0:-6].replace('T', ' ') if value.get('DATE_UPDATE') != '' else None
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'ЕдиницыИзмерения':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                int(value.get('CODE')) if value.get('CODE') is not None else None,
                value.get('MEASURE_TITLE'),
                value.get('SYMBOL_RUS'),
                value.get('SYMBOL_INTL'),
                value.get('SYMBOL_LETTER_INTL'),
                value.get('ACTIVE')
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'СтавкиНДС':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                value.get('TIMESTAMP_X')[0:-6].replace('T', ' ') if value.get('DATE_UPDATE') != '' else None,
                value.get('ACTIVE'),
                int(value.get('C_SORT')) if value.get('C_SORT') is not None else None,
                value.get('NAME'),
                float(value.get('RATE')) if value.get('RATE') is not None else None
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'Направления_сделок':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                value.get('CREATED_DATE')[0:-6].replace('T', ' ') if value.get('DATE_UPDATE') != '' else None,
                value.get('NAME'),
                value.get('IS_LOCKED'),
                int(value.get('SORT')) if value.get('ID') is not None else None
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'Каталоги':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                value.get('NAME'),
                value.get('ORIGINATOR_ID'),
                value.get('ORIGIN_ID'),
                value.get('XML_ID')
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'РазделыТоваров':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                int(value.get('CATALOG_ID')) if value.get('CATALOG_ID') is not None else None,
                int(value.get('SECTION_ID')) if value.get('SECTION_ID') is not None else None,
                value.get('NAME'),
                value.get('XML_ID')
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'СтатусыСчетов' or table_name == 'Статусы':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                value.get('ENTITY_ID'),
                value.get('STATUS_ID'),
                int(value.get('SORT')) if value.get('ID') is not None else None,
                value.get('NAME'),
                value.get('NAME_INIT'),
                value.get('SYSTEM'),
                str(value.get('EXTRA'))
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'Подразделения':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                value.get('NAME'),
                value.get('SORT'),
                value.get('PARENT'),
                value.get('UF_HEAD')
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'ПлатежныеСистемы':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                value.get('NAME'),
                value.get('ACTIVE'),
                int(value.get('SORT')) if value.get('SORT') is not None else None,
                value.get('DESCRIPTION'),
                int(value.get('PERSON_TYPE_ID')) if value.get('PERSON_TYPE_ID') is not None else None,
                value.get('ACTION_FILE'),
                str(value.get('HANDLER')),
                str(value.get('HANDLER_CODE')),
                str(value.get('HANDLER_NAME'))
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'ТипыПлательщиков':
            end_value = [
                int(value.get('id')) if value.get('id') is not None else None,
                value.get('name')
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'ПользовательскиеПоля':
            end_value = [
                value.get('ID'),
                value.get('NAME'),
                value.get('ENTITY')
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'ПользовательскиеПоля_Значения':
            end_value = [
                value.get('ID'),
                int(value.get('ITEM_ID')) if value.get('ITEM_ID') is not None else None,
                value.get('ITEM_NAME'),
                value.get('ENTITY')
            ]
            form_sql_query(end_value, table_name, value)

        elif table_name == 'ТоварыЛидов' or table_name == 'ТоварыСделок':
            end_value = [
                int(value.get('ID')) if value.get('ID') is not None else None,
                int(value.get('OWNER_ID')) if value.get('OWNER_ID') is not None else None,
                int(value.get('PRODUCT_ID')) if value.get('PRODUCT_ID') is not None else None,
                float(value.get('PRICE')) if value.get('PRICE') is not None else None,
                float(value.get('QUANTITY')) if value.get('QUANTITY') is not None else None,
                value.get('PRODUCT_NAME'),
                value.get('ORIGINAL_PRODUCT_NAME'),
                value.get('PRODUCT_DESCRIPTION'),
                int(value.get('MEASURE_CODE')) if value.get('MEASURE_CODE') is not None else None
            ]
            form_sql_query(end_value, table_name, value)

        return end_value
    except BaseException as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка сборки массива данных для запроса.\n" \
                f"Таблица: {table_name}\n" \
                f"Польз. поля: {uf_val}\nДанные: {value}\nСообщение ошибки: {message}: {message}\n\n\n"
        print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка сборки массива данных для запроса.\n"
              f"Таблица: {table_name}\n"
              f"Польз. поля: {uf_val}\nДанные: {value}\nСообщение ошибки: {message}: {message}\n\n")


# Возвращает красивое значение высшей сущности у UF
def _entity_to_ru(entity):
    if entity == 'CRM_DEAL':
        return 'Сделки'
    elif entity == 'CRM_LEAD':
        return 'Лиды'
    elif entity == 'CRM_CONTACT':
        return 'Контакты'
    elif entity == 'CRM_COMPANY':
        return 'Компании'
    elif entity == 'CRM_QUOTE':
        return 'Коммерческие предложения'
    elif entity == 'CRM_INVOICE':
        return 'Счета'
    else:
        return 'Не определено'


# Получает и отправляет пользовательские поля в таблицы SQL
def user_fields(tables):
    global logs
    try:

        list_user_fields = []
        sp_user_fields = []
        methods = (
            ('crm.deal.userfield.list', 'crm.deal.fields'),
            ('crm.lead.userfield.list', 'crm.lead.fields'),
            ('crm.contact.userfield.list', 'crm.contact.fields'),
            ('crm.company.userfield.list', 'crm.company.fields'),
            ('crm.quote.userfield.list', 'crm.quote.fields'),
            ('crm.invoice.userfield.list', 'crm.invoice.fields')
        )

        for _method in methods:
            res = bx24.callMethod(_method[0])
            res_name = bx24.callMethod(_method[1])
            curr_res_name = {}

            for rn in res_name:
                if rn.find('UF_') != -1:
                    curr_res_name.update([(rn, res_name.get(rn).get('listLabel'))])

            for r in res:
                if r.get('LIST') is not None:
                    sp_user_fields += [{'ID': r.get('FIELD_NAME'), 'ENTITY': _entity_to_ru(r.get('ENTITY_ID')),
                                        'NAME': curr_res_name.get(r.get('FIELD_NAME'))}]
                    for item in r.get('LIST'):
                        list_user_fields += [{'ID': r.get('FIELD_NAME'), 'ENTITY': _entity_to_ru(r.get('ENTITY_ID')),
                                              'ITEM_ID': item.get('ID'), 'ITEM_NAME': item.get('VALUE'),
                                              'NAME': curr_res_name.get(r.get('FIELD_NAME'))}]
                else:
                    sp_user_fields += [{'ID': r.get('FIELD_NAME'), 'ENTITY': _entity_to_ru(r.get('ENTITY_ID')),
                                        'NAME': curr_res_name.get(r.get('FIELD_NAME'))}]

        for _table in tables:
            print("\n", _table + ":\n", sep='')
            th = get_name_columns(_table)
            td = []

            # обработка каждой сделки
            if _table == 'ПользовательскиеПоля':
                all_user_fields_ = tqdm(sp_user_fields, unit='ent', ncols=100, dynamic_ncols=False)
            else:
                all_user_fields_ = tqdm(list_user_fields, unit='ent', ncols=100, dynamic_ncols=False)
            _i = 1

            for user_field in all_user_fields_:
                td.extend(go_sql_query(_table, user_field))

                sleep(0.01)
            print()
            # columns = len(th)
            # table_ = PrettyTable(th)
            # while td:
            #     table_.add_row(td[:columns])
            #     td = td[columns:]
            # print(table_)
            logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                    f"Добавление пользовательских полей в таблицы {tables} " \
                    f"успешно. Обработано сущностей: {len(sp_user_fields)}\n\n\n"
            print()
            sleep(0.1)
    except BaseException as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка добавления пользовательских полей.\n" \
                f"Таблицы: {tables}\n" \
                f"\nСообщение ошибки: {message}: {message}\n\n\n"
        print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка добавления пользовательских полей.\n"
              f"Таблицы: {tables}\n"
              f"\nСообщение ошибки: {message}: {message}\n\n\n")


# Заполняет таблицы SQL данными о товарах сделок и лидов
def productrows(tables):
    global logs
    deal_c = 0
    lead_c = 0
    try:

        for table in tables:
            print("\n", table + ":\n", sep='')
            sleep(0.02)
            th = get_name_columns(table)
            td = []

            if table == 'ТоварыСделок':
                deal_id_list_ = tqdm(deal_id_list, unit='ent', ncols=100, dynamic_ncols=False)

                for id in deal_id_list_:
                    deal_products = bx24.callMethod('crm.deal.productrows.get', id=id)
                    deal_c = len(deal_products)
                    if len(deal_products) > 0:
                        for deal_product in deal_products:
                            td.extend(go_sql_query(table, deal_product))

                            sleep(0.01)
                    sleep(0.25)
            else:
                lead_id_list_ = tqdm(lead_id_list, unit='ent', ncols=100, dynamic_ncols=False)

                for id in lead_id_list_:

                    lead_products = bx24.callMethod('crm.lead.productrows.get', id=id)
                    lead_c = len(lead_products)
                    if len(lead_products) > 0:
                        for lead_product in lead_products:
                            td.extend(go_sql_query(table, lead_product))
                            sleep(0.01)
                    sleep(0.25)
            sleep(0.01)
            print()
            columns = len(th)
            table_ = PrettyTable(th)
            while td:
                table_.add_row(td[:columns])
                td = td[columns:]
            # print(table_)
            logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                    f"Добавление товаров сделок/лидов в таблицы {tables} " \
                    f"успешно. Обработано сущностей: " \
                    f"{deal_c if table == 'ТоварыСделок' else lead_c}\n\n\n"

            print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
                  f"Добавление товаров сделок/лидов в таблицы {tables} "
                  f"успешно\n\n")

    except BaseException as message:
        logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
                f"Ошибка добавления товаров лидов/сделок.\n" \
                f"Таблицы: {tables}\n\n\n" \
                f"\nСообщение ошибки: {message}: {message}\n\n\n"
        print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
              f"Ошибка добавления пользовательских полей.\n"
              f"Таблицы: {tables}\n"
              f"\nСообщение ошибки: {message}: {message}\n\n\n")


# Подключение к базе данных
try:
    db = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=DATABASE)
    print("connect successful!!\n\n")
    logs = f"{'*' * 25} {datetime.now().date()} {datetime.now().hour}:{datetime.now().minute}:" \
           f"{datetime.now().second} {'*' * 25}\n\nУспешно подключение к SQL\n\n"
    # Подключение к Битрикс24 по входящему вебхуку
    bx24 = Bitrix24(WEBHOOK)

    # Модуль 1: Сделки
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 1: Сделки\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 1: Сделки\n")
    module_uf(TABLE_WH_UF[0][0], TABLE_WH_UF[0][1])

    # # Модуль 2: Лиды
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 2: Лиды\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 2: Лиды\n")
    module_uf(TABLE_WH_UF[1][0], TABLE_WH_UF[1][1])

    # # Модуль 3: Контакты
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 3: Контакты\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 3: Контакты\n")
    module_uf(TABLE_WH_UF[2][0], TABLE_WH_UF[2][1])

    # # Модуль 4: Компании
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 4: Компании\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 4: Компании\n")
    module_uf(TABLE_WH_UF[3][0], TABLE_WH_UF[3][1])

    # # Модуль 5: Коммерческие Предложения
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
            f"Запуск Модуля 5: Коммерческие Предложения\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
          f"Запуск Модуля 5: Коммерческие Предложения\n")
    module_uf(TABLE_WH_UF[4][0], TABLE_WH_UF[4][1])

    # # Модуль 6: Счета
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 6: Счета\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 6: Счета\n")
    module_uf(TABLE_WH_UF[5][0], TABLE_WH_UF[5][1])

    # # Модуль 7: Дела
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 7: Дела\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 7: Дела\n")
    module_simple(TABLE_SIMPLE[0][0], TABLE_SIMPLE[0][1])

    # # Модуль 8: Пользователи
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 8: Пользователи\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 8: Пользователи\n")
    module_simple(TABLE_SIMPLE[1][0], TABLE_SIMPLE[1][1])

    # # Модуль 9: Телефония
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 9: Телефония\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 9: Телефония\n")
    module_simple(TABLE_SIMPLE[2][0], TABLE_SIMPLE[2][1])

    # # Модуль 10: Товары
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 10: Товары\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 10: Товары\n")
    module_simple(TABLE_SIMPLE[3][0], TABLE_SIMPLE[3][1])

    # # Модуль 11: Валюты
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 11: Валюты\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 11: Валюты\n")
    module_simple(TABLE_SIMPLE[4][0], TABLE_SIMPLE[4][1])

    # # Модуль 12: Единицы Измерения
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
            f"Запуск Модуля 12: Единицы Измерения\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
          f"Запуск Модуля 12: Единицы Измерения\n")
    module_simple(TABLE_SIMPLE[5][0], TABLE_SIMPLE[5][1])

    # # Модуль 13: СтавкиНДС
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 13: СтавкиНДС\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 13: СтавкиНДС\n")
    module_simple(TABLE_SIMPLE[6][0], TABLE_SIMPLE[6][1])

    # # Модуль 14: Направления_сделок
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
            f"Запуск Модуля 14: Направления_сделок\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
          f"Запуск Модуля 14: Направления_сделок\n")
    module_simple(TABLE_SIMPLE[7][0], TABLE_SIMPLE[7][1])

    # # Модуль 15: Каталоги
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 15: Каталоги\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 15: Каталоги\n")
    module_simple(TABLE_SIMPLE[8][0], TABLE_SIMPLE[8][1])

    # # Модуль 16: РазделыТоваров
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
            f"Запуск Модуля 16: РазделыТоваров\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 16: РазделыТоваров\n")
    module_simple(TABLE_SIMPLE[9][0], TABLE_SIMPLE[9][1])

    # # Модуль 17: СтатусыСчетов
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
            f"Запуск Модуля 17: СтатусыСчетов\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 17: СтатусыСчетов\n")
    module_simple(TABLE_SIMPLE[10][0], TABLE_SIMPLE[10][1])

    # # Модуль 18: Статусы
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 18: Статусы\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 18: Статусы\n")
    module_simple(TABLE_SIMPLE[11][0], TABLE_SIMPLE[11][1])

    # # Модуль 19: Подразделения
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
            f"Запуск Модуля 19: Подразделения\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Запуск Модуля 19: Подразделения\n")
    module_simple(TABLE_SIMPLE[12][0], TABLE_SIMPLE[12][1])

    # # Модуль 20: ПлатежныеСистемы
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
            f"Запуск Модуля 20: ПлатежныеСистемы\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
          f"Запуск Модуля 20: ПлатежныеСистемы\n")
    module_simple(TABLE_SIMPLE[13][0], TABLE_SIMPLE[13][1])

    # # Модуль 21: ТипыПлательщиков
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
            f"Запуск Модуля 21: ТипыПлательщиков\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
          f"Запуск Модуля 21: ТипыПлательщиков\n")
    module_simple(TABLE_SIMPLE[14][0], TABLE_SIMPLE[14][1])

    # # Модуль 22: Пользовательские поля
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
            f"Запуск Модуля 22: Пользовательские поля\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
          f"Запуск Модуля 22: Пользовательские поля\n")
    user_fields(TABLE_SIMPLE[15])

    # Модуль 23: Товары лидов и сделок
    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | " \
            f"Запуск Модуля 23: Товары лидов и сделок\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | "
          f"Запуск Модуля 23: Товары лидов и сделок\n")
    productrows(TABLE_SIMPLE[16])

    logs += f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Завершение работы\n\n\n"
    print(f"{datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} | Завершение работы\n\n\n")
    db.close()

except BaseException as message:
    logs = f"{'*' * 25} {now.date()} {datetime.now().hour}:{datetime.now().minute}:{datetime.now().second} " \
           f"{'*' * 25}\n\nОшибка подключения к SQL:" \
           f"\n{message} {message}\n\n\n"
    print("\033[31m{}".format(logs))
    db.close()

logs_txt = open('logs.txt', 'a')
logs_txt.write(logs)
logs_txt.close()
