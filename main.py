import argparse
from datetime import datetime, timedelta
from itertools import count
from sys import exit as s_exit
from ets.ets_mysql_lib import MysqlConnection as Mc
from ets.ets_xml_worker import PROCEDURE_223_TYPES
from queries import *


PROGNAME = 'Check trade awaiting auctions Nagios plugin (223)'
DESCRIPTION = '''Плагин Nagios для проверки закупок на статусе ожидание торгов (223)'''
VERSION = '1.0'
AUTHOR = 'Belim S.'
RELEASE_DATE = '2018-09-27'

OK, WARNING, CRITICAL, UNKNOWN = range(4)
SEPARATE_LINE = '-' * 79
EXIT_DICT = {'exit_status': OK, 'ok': 0, 'warning': 0, 'critical': 0, 'all_errors': 0}
INFO_TEMPLATE = '%(p_procedure_number)s |%(p_procedure_id)s, %(p_lot_id)s|: %(error)s'
EXIT_TEMPLATE = '''Checking status: %(all_errors)s errors found\n''' + \
                SEPARATE_LINE + \
                '''\nOK: %(ok)s\nWarning: %(warning)s\nCritical: %(critical)s'''

ok_counter = count(start=1, step=1)
warning_counter = count(start=1, step=1)
critical_counter = count(start=1, step=1)
all_errors_counter = count(start=1, step=1)


# обработчик параметров командной строки
def create_parser():
    parser = argparse.ArgumentParser(description=DESCRIPTION)

    parser.add_argument('-v', '--version', action='store_true',
                        help="Показать версию программы")

    parser.add_argument('-a', '--auction', type=str, default='',
                        help="Номер процедуры")

    parser.add_argument('-t', '--type', type=str, default='', required=True,
                        choices=('223ea1', '223ea2', '223smsp_ea'),
                        help="Тип процедуры (обязательный)")

    parser.add_argument('-i', '--full_info', action='store_true',
                        help="Вывод полной информации на консоль")

    return parser

my_parser = create_parser()
namespace = my_parser.parse_args()


def show_version():
    print(PROGNAME, VERSION, '\n', DESCRIPTION, '\nAuthor:', AUTHOR, '\nRelease date:', RELEASE_DATE)


def out_printer(func):
    """Декоратор для вывода текста на консоль"""
    def wrapped(auction_data):
        info = func(auction_data)
        if info.get('error') and namespace.full_info:
            info_out = INFO_TEMPLATE % info
            print(info_out)
        return info
    return wrapped


def only_if_catalog_record_exists(func):
    """Декоратор для вывода текста на консоль"""
    def wrapped(auction_data):
        if 'c_procedure_id' in auction_data:
            info = func(auction_data)
        else:
            info = auction_data
        return info
    return wrapped


def only_if_trade_record_exists(func):
    """Декоратор для вывода текста на консоль"""
    def wrapped(auction_data):
        if 't_procedure_id' in auction_data:
            info = func(auction_data)
        else:
            info = auction_data
        return info
    return wrapped


def ignored_if_smsp(func):
    """Декоратор игнорирования функции, если не нужно выполнять для SMSP"""
    def wrapped(auction_data):
        if namespace.type == '223smsp_ea':
            info = auction_data
        else:
            info = func(auction_data)
        return info
    return wrapped


def set_warning(func):
    """Декоратор для для добавления warning"""
    def wrapped(auction_data):
        info = func(auction_data)
        if info.get('error'):
            info['error_flag'] = True
            EXIT_DICT['exit_status'] = WARNING if EXIT_DICT['exit_status'] < WARNING else EXIT_DICT['exit_status']
            EXIT_DICT['warning'] = next(warning_counter)
            EXIT_DICT['all_errors'] = next(all_errors_counter)
        info['error'] = None
        return info
    return wrapped


def set_critical(func):
    """Декоратор для для добавления critical"""
    def wrapped(auction_data):
        info = func(auction_data)
        if info.get('error'):
            info['error_flag'] = True
            EXIT_DICT['exit_status'] = CRITICAL if EXIT_DICT['exit_status'] < CRITICAL else EXIT_DICT['exit_status']
            EXIT_DICT['critical'] = next(critical_counter)
            EXIT_DICT['all_errors'] = next(all_errors_counter)
        info['error'] = None
        return info
    return wrapped


@set_critical
@out_printer
def check_catalog_procedure_exist_record_c(auction_data):
    catalog_procedure_info = cn_catalog.execute_query(get_catalog_procedure_info_query % row, dicted=True)
    if not catalog_procedure_info:
        auction_data['error'] = 'по процедуре отсутствует запись в каталоге'
    else:
        auction_data.update(catalog_procedure_info[0])
    return auction_data


@set_critical
@out_printer
def check_offer_date_p(auction_data):
    """Проверка даты окончания подачи заявок"""
    if not auction_data['p_offer_date'] or auction_data['p_offer_date'] < datetime.now():
        auction_data['error'] = 'некорректная дата начала торгов в базе процедур'
    return auction_data


@set_warning
@out_printer
@only_if_catalog_record_exists
def check_regulated_datetime_c(auction_data):
    """Проверка regulated_datetime в каталоге"""
    if auction_data['c_regulated_datetime']:
        auction_data['error'] = 'некорректная дата по текущему статусу'
    return auction_data


@set_critical
@out_printer
@only_if_catalog_record_exists
def check_offer_date_c(auction_data):
    """Проверка даты начала торгов в каталоге"""
    if not auction_data['p_offer_date'] == auction_data['c_offer_date']:
        auction_data['error'] = 'некорректная дата начала торгов в каталоге'
    return auction_data


@set_critical
@out_printer
def check_procedure_status_p(auction_data):
    """Проверка статуса процедуры в базе процедур"""
    if not auction_data['p_procedure_status'] == 'procedure.request.review':
        auction_data['error'] = 'некорректный статус процедуры в базе процедур'
    return auction_data


@set_critical
@out_printer
def check_lot_status_c(auction_data):
    """Проверка статуса лота в каталоге"""
    if not auction_data['c_lot_status_id'] == 31:
        auction_data['error'] = 'некорректный статус лота в каталоге'
    return auction_data


@set_warning
@out_printer
def check_protocol_request_status_matching_p(auction_data):
    """Проверка соответствия статуса по протоколу и по заявке"""
    error_text = ''
    protocol_request_status_matching = cn_procedures.execute_query(check_protocol_request_status_matching_query % row)
    for p_request_id, p_protocol_decision, p_request_status in protocol_request_status_matching:
        if not p_protocol_decision == p_request_status:
            error_text += '%s: protocol=%s, request=%s; ' % (str(p_request_id),
                                                             str(p_protocol_decision),
                                                             str(p_request_status))
    if error_text:
        auction_data['error'] = 'Не соответствует статус по протоколу и по заявке: ' + error_text + \
                                '%(p_procedure_number)s'
    return auction_data


@set_critical
@out_printer
def check_catalog_procedure_exist_record_p(auction_data):
    """Проверка количества допущенных заявок по лоту"""
    request_accepted_count_info = cn_procedures.execute_query(check_request_accepted_count_query_p % auction_data,
                                                              dicted=True)
    auction_data.update(request_accepted_count_info[0])
    if auction_data['request_count_p'] < 2:
        auction_data['error'] = 'по лоту допущено менее двух заявок'
    return auction_data


@set_warning
@out_printer
def check_protocol_count_p(auction_data):
    """Проверка наличия протокола"""
    protocol_count = cn_procedures.execute_query(check_protocol_count_query % auction_data)[0][0]
    if protocol_count == 0:
        auction_data['error'] = 'отсутствует протокол на статусе ожидание торгов'
    elif protocol_count > 1:
        auction_data['error'] = 'несколько протоколов на статусе ожидание торгов'
    return auction_data


@set_warning
@out_printer
def check_events_p(auction_data):
    """Проверка наличия событий не соответствующие статусу"""
    error_text = ''
    events = cn_procedures.execute_query(check_events_query_p % auction_data)
    if events:
        for p_event_id, p_event_type_code in events:
            error_text += '%s: event=%s; ' % (str(p_event_id), str(p_event_type_code))
    if error_text:
        auction_data['error'] = 'Присутствуют события не соответствующие статусу: ' + error_text + \
                                '%(p_procedure_number)s'
    return auction_data


@set_critical
@out_printer
def check_contract_p(auction_data):
    """Проверка наличия протокола"""
    contract = cn_procedures.execute_query(check_contract_query_p % auction_data)[0][0]
    if contract:
        auction_data['error'] = 'обнаружена запись контракта %s на статусе ожидание торгов' % contract[0][0]
    return auction_data


@set_critical
@out_printer
def check_trade_procedure_exist_record_t(auction_data):
    """Проверка наличия записи в торговой бд и получение данных по записи"""
    trade_procedure_info = cn_trade.execute_query(get_trade_auction_info_query % row, dicted=True)
    if not trade_procedure_info:
        auction_data['error'] = 'по процедуре отсутствует запись в торговой базе'
    else:
        auction_data.update(trade_procedure_info[0])
    return auction_data


@set_critical
@out_printer
@only_if_trade_record_exists
def check_phase_id_t(auction_data):
    """Проверка корректности фазы ожидания"""
    if not auction_data['t_phase_id'] == 0:
        auction_data['error'] = 'некорректная фаза торгов (%(t_phase_id)s)' % auction_data
    return auction_data


@set_critical
@out_printer
@only_if_trade_record_exists
def check_start_trade_datetime_t(auction_data):
    """Проверка корректности startHoldingDateTime"""
    if not auction_data['t_start_trade_datetime'] or \
            not auction_data['t_start_trade_datetime'] == auction_data['p_offer_date']:
        auction_data['error'] = 'некорректный startHoldingDateTime в торговой базе (%(t_start_trade_datetime)s)' \
                                % auction_data
    return auction_data


@set_warning
@out_printer
@only_if_trade_record_exists
def check_t_end_phase_one_datetime_t(auction_data):
    """Проверка корректности endPhaseOneDateTime"""
    if not auction_data['t_end_phase_one_datetime'] or \
            not auction_data['t_end_phase_one_datetime'] == auction_data['p_offer_date']:
        auction_data['error'] = 'некорректный endPhaseOneDateTime в торговой базе (%(t_end_phase_one_datetime)s)' \
                                % auction_data
    return auction_data


@set_warning
@out_printer
@only_if_trade_record_exists
def check_t_end_phase_two_datetime_t(auction_data):
    """Проверка корректности endPhaseTwoDateTime"""
    if not auction_data['t_end_phase_two_datetime'] or \
            not auction_data['t_end_phase_two_datetime'] == auction_data['p_offer_date']:
        auction_data['error'] = 'некорректный endPhaseTwoDateTime в торговой базе (%(t_end_phase_two_datetime)s)' \
                                % auction_data
    return auction_data


@set_critical
@out_printer
@only_if_trade_record_exists
def check_requests_exist_record_t(auction_data):
    """Проверка наличия записей о заявках в торговой БД"""
    request_info_trade = cn_trade.execute_query(get_request_info_trade_query % row, dicted=True)
    auction_data.update(request_info_trade[0])
    if not auction_data['request_count_t']:
        auction_data['error'] = 'по процедуре отсутствуют записи по заявкам в торговой бд'
    return auction_data


@set_critical
@out_printer
@only_if_trade_record_exists
def check_request_count_t(auction_data):
    """Проверка соответствия количества заявок между базами"""
    if not auction_data['request_count_p'] == auction_data['request_count_t']:
        auction_data['error'] = \
            'некорректное количество заявок в торговой бд (%(request_count_p)s/%(request_count_t)s)' % auction_data
    return auction_data


@set_critical
@out_printer
@only_if_trade_record_exists
def check_request_ids_t(auction_data):
    """Проверка отличий заявок между базами"""
    if not auction_data['request_ids_p'] == auction_data['request_ids_t']:
        auction_data['error'] = \
            'отличия по заявкам между торговой и основной бд (%(request_ids_p)s/%(request_ids_t)s)' % auction_data
    return auction_data


@set_critical
@out_printer
@only_if_trade_record_exists
@ignored_if_smsp
def check_offers_exist_record_t(auction_data):
    """Проверка отсутствия записей о ценовых предложениях в торговой БД"""
    offers_info_trade = cn_trade.execute_query(get_offers_info_trade_query % row, dicted=True)
    auction_data.update(offers_info_trade[0])
    if auction_data['offers_count_t']:
        auction_data['error'] = \
            'по процедуре присутствуют записи по ценовым предложениям в торговой бд (заявки %(offers_request_ids_t)s)' \
            % auction_data
    return auction_data


@set_critical
@out_printer
@only_if_trade_record_exists
def check_active_t(auction_data):
    """Проверка установки active за час до торгов"""
    if datetime.now() + timedelta(hours=-1) > auction_data['t_start_trade_datetime'] and not auction_data['t_active']:
        auction_data['error'] = \
            'не установлен флаг active за час до торгов' % auction_data
    return auction_data


@set_critical
@out_printer
@only_if_trade_record_exists
def check_pid_t(auction_data):
    """Проверка установки pid за час до торгов"""
    if datetime.now() + timedelta(hours=-1) > auction_data['t_start_trade_datetime'] and not auction_data['t_pid']:
        auction_data['error'] = \
            'не установлен pid за час до торгов' % auction_data
    return auction_data


if __name__ == '__main__':
    try:
        # инициализируем подключения
        cn_procedures = Mc(connection=PROCEDURE_223_TYPES[namespace.type]['connection']).connect()
        cn_catalog = Mc(connection=Mc.MS_223_CATALOG_CONNECT).connect()
        cn_trade = Mc(connection=PROCEDURE_223_TYPES[namespace.type]['connection_trade']).connect()

        if namespace.auction:
            all_published_procedures_info = cn_procedures.execute_query(get_one_trade_awaiting_procedures_info_query %
                                                                        namespace.auction, dicted=True)
        else:
            all_published_procedures_info = cn_procedures.execute_query(get_all_trade_awaiting_procedures_info_query,
                                                                        dicted=True)

        # если поиск по базе с текущими условиями ничего не дал, то указываем, что ничего не нашлось
        if namespace.auction and not all_published_procedures_info:
            print('Nothing to check')
            s_exit(UNKNOWN)

        # выполняем все проверки
        for row in all_published_procedures_info:
            row['procedure_type'] = namespace.type
            row['short_procedure_type'] = namespace.type[3:]
            check_catalog_procedure_exist_record_c(row)
            check_offer_date_p(row)
            check_regulated_datetime_c(row)
            check_procedure_status_p(row)
            check_lot_status_c(row)

            check_offer_date_c(row)
            check_protocol_request_status_matching_p(row)
            check_catalog_procedure_exist_record_p(row)
            check_protocol_count_p(row)
            check_events_p(row)
            check_contract_p(row)

            check_trade_procedure_exist_record_t(row)
            check_phase_id_t(row)
            check_start_trade_datetime_t(row)
            check_t_end_phase_one_datetime_t(row)
            check_t_end_phase_two_datetime_t(row)
            check_requests_exist_record_t(row)
            check_request_count_t(row)
            check_request_ids_t(row)
            check_offers_exist_record_t(row)
            check_active_t(row)
            check_pid_t(row)
            # если все проверки завершились успешно, то увеличиваем количество ok на единицу
            if not row.get('error_flag'):
                EXIT_DICT['ok'] = next(ok_counter)
            elif namespace.full_info:
                print(SEPARATE_LINE)

        # в режиме плагина выводим только краткую информацию
        if namespace.full_info:
            if EXIT_DICT['exit_status'] == OK:
                print('All OK!')
        else:
            print(EXIT_TEMPLATE % EXIT_DICT)

        cn_procedures.disconnect()
        cn_catalog.disconnect()
        cn_trade.disconnect()

        s_exit(EXIT_DICT['exit_status'])

    except Exception as err:
        print('Plugin error')
        print(err)
        s_exit(UNKNOWN)

    show_version()
    print('For more information run use --help')
    s_exit(UNKNOWN)
