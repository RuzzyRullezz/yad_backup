import argparse
import os
import shutil
import datetime
import logging
import socket

import yadisk
import telegram_log.handler

archive_type = 'zip'
backup_dir_pattern = '%s_%s'
time_format = '%Y_%m_%dT%H_%M_%S'


def parse_args():
    parser = argparse.ArgumentParser(
        description="Бэкап в Я.Диск",
    )
    parser.add_argument("--id", "-i", type=str,
                        help="ID приложения", required=True)
    parser.add_argument("--password", "-p", type=str,
                        help="Пароль приложения", required=True)
    parser.add_argument("--token", "-t", type=str,
                        help="Oauth token", required=True)
    parser.add_argument("--source", "-s", type=existed_path,
                        help="Что архивировать", required=True)
    parser.add_argument("--dest", "-d", type=str,
                        help="Куда складировать", required=True)
    parser.add_argument("--count", "-c", type=positive_integer,
                        help="Количество бэкапов")
    parser.add_argument("--tg_token", "-g", type=str,
                        help="Telegram token")
    parser.add_argument("--tg_chat_id", "-a", dest='tg_chat_ids', type=str, action='append',
                        help="Telegram chat id")
    return parser.parse_args()


def setup_loggers(arguments):
    token = arguments.tg_token
    chat_ids = arguments.tg_chat_ids
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    host_name = socket.gethostname()
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] [{0} %(name)s:%(lineno)s] %(message)s'.format(host_name)
    )
    if token and chat_ids:
        tg_handler = telegram_log.handler.TelegramHandler(token=token, chat_ids=chat_ids, err_log_name='')
        tg_handler.setLevel(logging.ERROR)
        tg_handler.setFormatter(formatter)
        logger.addHandler(tg_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


def positive_integer(argument: str) -> int:
    err_msg = "must be positive integer"
    try:
        as_int = int(argument)
        if as_int <= 0:
            raise argparse.ArgumentTypeError(err_msg)
        return as_int
    except ValueError:
        raise argparse.ArgumentTypeError(err_msg)


def existed_path(argument: str) -> str:
    if not os.path.exists(argument):
        raise argparse.ArgumentTypeError("%s folder does not exist" % argument)
    return argument


def backup(arguments):
    logger = logging.getLogger(__name__)
    backup_folder_name = os.path.basename(arguments.source)
    output_filename = os.path.join('/tmp', backup_folder_name)
    try:
        client = yadisk.YaDisk(id=arguments.id, secret=arguments.password, token=arguments.token)
        if not client.exists(arguments.dest):
            logger.info('Create directory %s' % arguments.dest)
            client.mkdir(arguments.dest)
        shutil.make_archive(output_filename, archive_type, arguments.source)
        output_filename = '.'.join([output_filename, archive_type])
        if arguments.count:
            exists_backups = sorted(
                filter(
                    lambda fo: backup_folder_name in fo.name,
                    list(client.listdir(arguments.dest))),
                key=lambda fo: fo.created
            )
            diff_cnt = len(exists_backups) - arguments.count
            if diff_cnt >= 0:
                remove_cnt = diff_cnt + 1
                for_remove = exists_backups[:remove_cnt]
                for rm in for_remove:
                    logger.info('Remove directory %s' % rm.path)
                    client.remove(rm.path, permanently=True)
            full_dest_dir = os.path.join(
                arguments.dest,
                backup_dir_pattern % (backup_folder_name, datetime.datetime.now().strftime(time_format))
            )
        else:
            full_dest_dir = os.path.join(
                arguments.dest,
                backup_folder_name,
            )
        full_dest_path = os.path.join(
            full_dest_dir,
            os.path.basename(output_filename)
        )
        if client.exists(full_dest_dir):
            logger.info('Remove directory %s' % full_dest_dir)
            client.remove(full_dest_dir, permanently=True)
        client.mkdir(full_dest_dir)
        client.upload(output_filename, full_dest_path, overwrite=True)
        logger.info('Upload file %s' % full_dest_path)
    except BaseException as e:
        logger.exception('ya.disk exception')
        raise
    finally:
        os.remove(output_filename)


if __name__ == '__main__':
    options = parse_args()
    setup_loggers(options)
    backup(options)
