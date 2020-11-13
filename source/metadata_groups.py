from telethon import TelegramClient, events

import asyncio
import os
import pathlib
import json
import argparse
import traceback
import datetime

class GroupMetadataCollector():
    """
    Classe que encapsula o coletor de metadados de grupos do Telegram. Possui
    o método principal que realiza a leitura da entrada e faz a coleta de
    informações como o título, integrantes, criador e administrados dos grupos
    que o usuário faz parte.

    Atributos
    -----------
    group_blacklist : list
            Lista de ids de grupos que devem ser excluídos da coleta.

    Métodos
    -----------
    run()
        Faz a coleta dos metadados de grupos de Telegram de acordo
        com os parâmetros fornecidos na criação do objeto de coleta.
    """

    def __init__(self, args):
        """
        Inicializa o objeto

        Parâmetros
        ------------
            args : argparse.Namespace()
                Objeto com atributos que contém os argumentos de linha de
                comando fornecidos.
        """

        args_dict = vars(args)

        if args.json:
            with open(args.json) as json_file:
                json_args = json.load(json_file)
                args_dict.update(json_args)
        elif args.json_string:
            json_args = json.loads(args.json_string)
            args_dict.update(json_args)

        self.group_blacklist       = args_dict["group_blacklist"]
        self.api_id                = args_dict["api_id"]
        self.api_hash              = args_dict["api_hash"]

    async def run(self):
        """
        Faz a coleta dos metadados de grupos de Telegram de acordo
        com os parâmetros fornecidos na criação do objeto de coleta.

        Parâmetros
        ------------
            profile_path : str
                Caminho para um profile alternativo do navegador
                utilizado na coleta.
        """
        now = datetime.datetime.now()
        new_folder = '/data/metadata_grupos_%s/' % (now.strftime('%Y-%m-%d_%H-%M-%S'))
        pathlib.Path(new_folder).mkdir(parents=True, exist_ok=True)
        pathlib.Path(os.path.join(new_folder, "profile_pics")).mkdir(parents=True, exist_ok=True)

        async with TelegramClient('/data/collector_local', self.api_id, self.api_hash) as client:
            async for dialog in client.iter_dialogs():
                if ((not dialog.is_group) or dialog.title in self.group_blacklist or
                        str(abs(dialog.id)) in self.group_blacklist):
                    continue
                group = {}

                creator_id = None

                participants = list()
                async for member in client.iter_participants(dialog):
                    user = dict()
                    #TODO: changed some stuff here.
                    user['id'] = member.id
                    user['username'] = member.username
                    user['first_name'] = member.first_name
                    user['last_name'] = member.last_name
                    user['number'] = member.phone
                    user['isBot'] = member.bot
                    user['profile_pic'] = os.path.join(new_folder, "profile_pics", str(member.id) + '.jpg')

                    if not os.path.isfile(user['profile_pic']):
                        await client.download_profile_photo(member, user['profile_pic'])

                    participants.append(user)

                #TODO: check what kind is and dont know how to get creator
                group['group_id'] = dialog.entity.id
                # group['creator'] = creator
                # group['kind'] = kind
                group['creation'] = dict()
                group['creation']['creation_date'] = dialog.entity.date.strftime('%Y-%m-%d %H:%M:%S')
                group['creation']['creation_timestamp'] = int(datetime.datetime.timestamp(dialog.entity.date))
                group['title'] = dialog.entity.title
                group['members'] = participants

                filename = os.path.join(new_folder, 'grupos.json')
                with open(filename, 'a') as json_file:
                    json.dump(group, json_file)
                    print('', file=json_file)


async def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--group_blacklist", nargs="+",
                        help="Lista de ids de grupos que devem ser excluídos"
                        " da coleta", default=[])

    parser.add_argument("--api_id", type=str,
                        help="ID da API de Coleta gerado em my.telegram.org (Dado sensível)")

    parser.add_argument("--api_hash", type=str,
                        help="Hash da API de Coleta gerado em my.telegram.org (Dado sensível)")

    parser.add_argument("-j", "--json", type=str,
                        help="Caminho para um arquivo json de configuração de "
                        "execução. Individualmente, as opções presentes no "
                        "arquivo sobescreveram os argumentos de linha de "
                        "comando, caso eles sejam fornecidos.")

    parser.add_argument("--json_string", type=str,
                        help="String contendo um json de configuração de"
                        " execução. Individualmente, as opções presentes no "
                        "arquivo sobescreveram os argumentos de linha de "
                        "comando, caso eles sejam fornecidos.")

    args = parser.parse_args()

    try:
        collector = GroupMetadataCollector(args)
        await collector.run()
    except Exception as e:
        #TODO: Print log file
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(main())