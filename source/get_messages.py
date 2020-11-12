from telethon import TelegramClient, events
from PIL import Image

import asyncio
import pathlib
import argparse
import datetime
import json
import traceback
import hashlib
import imagehash
import pytz
import os

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class TelegramCollector():
    """
    Classe que encapsula o coletor de grupos do Telegram. Possui
    o método principal que realiza a leitura da entrada e faz a
    coleta das mensagens, mídias e notificações.
    Atributos
    -----------
    collection_mode : str
            Modo de coleção a ser utilizado ("period" ou "unread" ou 
            "continuous").
    start_date : str
            Data de início do período de coleta (Modo "period").
    end_date : str
            Data de término do período de coleta (Modo "period").
    group_blacklist : list
            Lista de ids de grupos que devem ser excluídos da coleta.
    user_blacklist : list
            Lista de ids de usuários que devem ser excluídos da coleta.
    collect_messages : bool
            Se mensagens de texto devem ser coletadas durante a execução.
    collect_audios : bool
            Se áudios devem ser coletadas durante a execução.
    collect_videos : bool
            Se vídeos devem ser coletadas durante a execução.
    collect_images : bool
            Se imagens devem ser coletadas durante a execução.
    collect_notifications : bool
            Se notificações devem ser coletadas durante a execução.
    process_audio_hashes : bool
            Se hashes de áudios devem ser calculados durante a execução.
    process_image_hashes : bool
            Se hashes de imagens devem ser calculados durante a execução.
    process_video_hashes : bool
            Se hashes de vídeos devem ser calculados durante a execução.
    api_id : str
            ID da API de Coleta gerado em my.telegram.org (Dado sensível).
    api_hash : str
            Hash da API de Coleta gerado em my.telegram.org (Dado sensível).
    Métodos
    -----------
    Faz a coleta das mensagens de grupos de Whatsapp de acordo
    com os parâmetros fornecidos na criação do objeto de coleta.
        Parâmetros
        ------------
            profile_path : str
                Caminho para um profile alternativo do navegador
                utilizado na coleta.
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

        if (args_dict["collection_mode"] not in
                ['continuous', 'period', 'unread']):
            print('Collection mode invalid <%s>!! Using <continuous> instead' %
                  (args_dict["collection_mode"]))
            args_dict["collection_mode"] = 'continuous'
        if args_dict["write_mode"] not in ['both', 'day', 'group']:
            print('Save mode invalid <%s>!! Using <both> instead' % (
                args_dict["write_mode"]))
            args_dict["write_mode"] = 'both'

        self.collection_mode       = args_dict["collection_mode"]
        self.start_date            = args_dict["start_date"]
        self.end_date              = args_dict["end_date"]
        self.write_mode              = args_dict["write_mode"]
        self.group_blacklist       = args_dict["group_blacklist"]
        self.user_blacklist        = args_dict["user_blacklist"]
        self.collect_messages      = args_dict["collect_messages"]
        self.collect_audios        = args_dict["collect_audios"]
        self.collect_videos        = args_dict["collect_videos"]
        self.collect_images        = args_dict["collect_images"]
        self.collect_notifications = args_dict["collect_notifications"]
        self.process_audio_hashes  = args_dict["process_audio_hashes"]
        self.process_image_hashes  = args_dict["process_image_hashes"]
        self.process_video_hashes  = args_dict["process_video_hashes"]
        self.api_id                = args_dict["api_id"]
        self.api_hash              = args_dict["api_hash"]

    def _get_load_messages(self, path='./data/mid_file.txt'):
        """
        Carrega e retorna um conjunto de ids das mensagens já coletadas.

        Parâmetros
        ------------
            path : str
                Caminho para o arquivo contendo os ids das mensagens.
        """
        messagesIDs = set()
        
        if os.path.isfile(path):
            with open(path, 'r') as fin:
                for line in fin:
                    messagesIDs.add(int(line.strip()))

        return messagesIDs

    def _save_processed_ids(self, id_set, path='./data/mid_file.txt'):
        """
        Salva o conjunto de ids das mensagens já coletadas.

        Parâmetros
        ------------
            path : str
                Caminho para o arquivo contendo os ids das mensagens.
        """
        with open(path+".temp", 'w') as fmid:
            for id in id_set:
                print(str(id), file=fmid)
        
        if os.path.isfile(path):
            os.remove(path)
        os.rename(path + ".temp", path)

    def _append_processed_id(self, id, path='./data/mid_file.txt'):
        """
        Salva um novo id de uma mensagem coletada.

        Parâmetros
        ------------
            path : str
                Caminho para o arquivo contendo os ids das mensagens.
        """
        with open(path, 'a') as fmid:
            print(str(id), file=fmid)

    async def _save_message(self, message, dialog_name, day_path = "./data/mensagens/", group_path="./data/mensagens_grupo/"):
        """
        Escreve em formato json a mensagem coletada no arquivo
        referente ao grupo em que ela foi enviada. Caso o arquivo do grupo
        ainda não exista, ele será criado.
        Parâmetros
        ------------
            message : telethon.tl.custom.message.Message()
                Objeto da mensagem coletada.
            group_path : str
                Caminho da pasta em que os arquivos de mensagens por grupo
                serão escritos.
        """
        item = dict()

        #TODO: get phone number


        item["message_id"] = message.id
        item["group_id"] = message.to_id.chat_id
        item["group_name"] = dialog_name
        item["country"] = None
        item["sender"] = message.from_id
        item["data"] = message.date.strftime("%Y-%m-%d %H:%M:%S")
        item["mediatype"] = None
        item["file"] = None
        item["content"] = message.message 
        item["phash"] = None
        item["checksum"] = None

        if message.media:
            base_path = "./data/others/"
            item["mediatype"] = "other"
            if message.photo:
                base_path = "./data/image/"
                item["mediatype"] = "image"
            elif message.audio or message.voice:
                base_path = "./data/audio/"
                item["mediatype"] = "audio"
            elif message.video or message.video_note:
                base_path = "./data/video/"
                item["mediatype"] = "video"
            

            path = os.path.join(base_path, message.date.strftime("%Y-%m-%d"), str(item["message_id"]))
            file_path = await message.download_media(path)

            item["file"] = file_path.split("/")[-1]

            if file_path != None:
                item["checksum"] = md5(file_path)
                if item["mediatype"] == "image":
                    item["phash"] = str(imagehash.phash(Image.open(file_path)))

        # Save message on group ID file
        if self.write_mode == "group" or self.write_mode == "both":
            message_group_filename = os.path.join(group_path, "mensagens_grupo_" + str(item["group_id"]) + ".json" )

            # Save message on file for all messages of the group
            with open(message_group_filename, "a") as json_file:
                json.dump(item, json_file)
                print("", file=json_file)

        if self.write_mode == "day" or self.write_mode == "both":
            message_day_filename = os.path.join(day_path, "mensagens_" + message.date.strftime("%Y-%m-%d") + ".json")

            # Save message on file for all messages of the day
            with open(message_day_filename, "a") as json_file:
                json.dump(item, json_file)
                print("", file=json_file)
    
    def _save_notification(self, message, path='./data/notificacoes/'):
        """
        Escreve em formato json a notificação contida na mensagem no arquivo
        referente ao grupo em que ela foi enviada. Caso o arquivo do grupo
        ainda não exista, ele será criado.
        Parâmetros
        ------------
            message : telethon.tl.custom.message.Message()
                Objeto da mensagem coletada.
            path : str
                Caminho da pasta em que os arquivos de notificações serão
                escritos.
        """
        notification = dict()

        notification["message_id"] = message.id
        notification["group_id"] = message.to_id.chat_id
        notification["date"] = message.date.strftime("%Y-%m-%d %H:%M:%S")       
        notification["action"] = {"action_class" : type(message.action).__name__ , 
                                  "fields" : message.action.__dict__}
        notification["sender"] = message.from_id

        notification_group_filename = os.path.join(
            path, "notificacoes_grupo_" + str(notification["group_id"]) + ".json" )

        # Save message on file for all messages of the group
        with open(notification_group_filename, "a") as json_file:
            json.dump(notification, json_file)
            print("", file=json_file)

    async def _run_unread_collector(self):
        async_client = TelegramClient('collector_unread', self.api_id, self.api_hash)
        group_names = {}

        @async_client.on(events.NewMessage)
        async def event_handler(event):
            message = event.message
            if (message.to_id.chat_id and group_names[str(message.to_id.chat_id)] and 
                    str(message.from_id) not in self.user_blacklist):
                await self._save_message(message, group_names[str(message.to_id.chat_id)])
                self._append_processed_id(message.id)

        @async_client.on(events.ChatAction)
        async def event_handler(event):
            message = event.action_message
            if (message.to_id.chat_id and group_names[str(message.to_id.chat_id)] and 
                    str(message.from_id) not in self.user_blacklist):
                self._save_notification(message)
                self._append_processed_id(message.id)
                if (type(message.action).__name__ == "MessageActionChatEditTitle") :
                    #in case the title changes
                    group_names[str(message.to_id.chat_id)] = message.action.title

        await async_client.start()

        async for dialog in async_client.iter_dialogs():
            if (dialog.is_group and dialog.title not in self.group_blacklist and
                    str(abs(dialog.id)) not in self.group_blacklist):
                #TODO: Check why dialog.id is a negative number
                group_names[str(abs(dialog.id))] = dialog.title

        await async_client.run_until_disconnected()

    async def run(self):
        """
        Faz a coleta das mensagens de grupos de Telegram de acordo
        com os parâmetros fornecidos na criação do objeto de coleta.
        """

        # Create data directories
        pathlib.Path("./data/mensagens").mkdir(parents=True, exist_ok=True)
        pathlib.Path("./data/image").mkdir(parents=True, exist_ok=True)
        pathlib.Path("./data/others").mkdir(parents=True, exist_ok=True)
        pathlib.Path("./data/audio").mkdir(parents=True, exist_ok=True)
        pathlib.Path("./data/video").mkdir(parents=True, exist_ok=True)
        pathlib.Path("./data/mensagens_grupo").mkdir(parents=True, exist_ok=True)
        pathlib.Path("./data/notificacoes").mkdir(parents=True, exist_ok=True)

        # Get start and end dates
        utc = pytz.UTC
        start_date = utc.localize(datetime.datetime.strptime(self.start_date, "%Y-%m-%d"))
        end_date = utc.localize(datetime.datetime.strptime(self.end_date, "%Y-%m-%d"))

        # Load previous saved messages
        previous_ids = self._get_load_messages()
        print("Starting " + self.collection_mode + " collection.")
        try:
            if (self.collection_mode != 'unread'):
                async with TelegramClient('collector_local', self.api_id, self.api_hash) as client:
                    async for dialog in client.iter_dialogs():
                        #TODO: Check why dialog.id is a negative number
                        if (dialog.is_group and dialog.title not in self.group_blacklist and
                            str(abs(dialog.id)) not in self.group_blacklist):
                            async for message in client.iter_messages(dialog):
                                if (message.date < start_date):
                                    break
                                if (message.date > end_date and self.collection_mode == 'period'):
                                    continue
                                
                                if (message.id in previous_ids or str(message.from_id) in self.user_blacklist):
                                    continue

                                if (not message.action):
                                    await self._save_message(message, dialog.entity.title)
                                else:
                                    self._save_notification(message)

                                previous_ids.add(message.id)   

            self._save_processed_ids(previous_ids)

            print("Finished collection.")
        except Exception as e:
            self._save_processed_ids(previous_ids)
        
        print("Starting unread message collection.")
        if (self.collection_mode == 'unread' or 
                self.collection_mode == 'continuous'):
            await self._run_unread_collector()



async def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-m", "--collection_mode", type=str,
                        help="Modo de coleção a ser utilizado (\'period\'"
                        " ou \'unread\' ou \'continuous\').",
                        default='continuous')

    parser.add_argument("-s", "--start_date", type=str,
                        help="Data de início do período de coleta (Modo"
                        " \'period\').", default='2000-01-01')

    parser.add_argument("-e", "--end_date", type=str,
                        help="Data de término do período de coleta (Modo"
                        " \'period\').", default='2999-12-31')

    parser.add_argument("-w", "--write_mode", type=str,
                        help="Modo de salvamento das mensagens no arquivos de saída(\'both\', \'day\', \'group\'). ", default='both')

    parser.add_argument("--collect_messages", type=bool,
                        help="Se mensagens de texto devem ser coletadas"
                        " durante a execução.", default=True)

    parser.add_argument("--collect_audios", type=bool,
                        help="Se audios devem ser coletadas durante a"
                        " execução.", default=True)

    parser.add_argument("--collect_videos", type=bool,
                        help="Se videos devem ser coletadas durante a"
                        " execução.", default=True)

    parser.add_argument("--collect_images", type=bool,
                        help="Se imagens devem ser coletadas durante a"
                        " execução.", default=True)

    parser.add_argument("--collect_notifications", type=bool,
                        help="Se as notificações devem ser coletadas durante a"
                        " execução.", default=True)

    parser.add_argument("--process_audio_hashes", type=bool,
                        help="Se hashes de audios devem ser calculados durante"
                        " a execução.", default=False)

    parser.add_argument("--process_image_hashes", type=bool,
                        help="Se hashes de imagens devem ser calculados"
                        " durante a execução.", default=False)

    parser.add_argument("--process_video_hashes", type=bool,
                        help="Se hashes de videos devem ser calculados durante"
                        " a execução.", default=False)

    parser.add_argument("--group_blacklist", nargs="+",
                        help="Lista de ids de grupos que devem ser excluídos da"
                        " coleta", default=[])

    parser.add_argument("--user_blacklist", nargs="+",
                        help="Lista de usuários que devem ser excluídos da"
                        " coleta", default=[])

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
        collector = TelegramCollector(args)
        await collector.run()
    except Exception as e:
        #TODO: Print log file
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(main())