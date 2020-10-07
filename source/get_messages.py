import pathlib
import argparse
import json
import traceback


class TelegramCollector():
    """
    Classe que encapsula o coletor de grupos do Whatsapp. Possui
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

def main():
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
        #collector.run()
    except Exception as e:
        #TODO: Print log file
        traceback.print_exc()


if __name__ == '__main__':
    main()