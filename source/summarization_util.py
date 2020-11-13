from datetime import timedelta
from datetime import datetime
from os.path import isfile, join

import json
import argparse

# Example: python summarization_util.py -t images -m checksum -s 2020-09-18 -e 2020-11-11 


def jaccard_similarity(x, y):
    try:
        intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
        union_cardinality = len(set.union(*[set(x), set(y)]))
        return intersection_cardinality/float(union_cardinality)
    except ZeroDivisionError:
        return 0


def compare_texts(text1, text2):
    if text1 is None or text2 is None:
        return 0.0
    score = jaccard_similarity(text1, text2)
    return score


def get_days_list(start_date, end_date):

    formatter = '%Y-%m-%d'

    date1 = datetime.strptime(start_date, formatter)
    date2 = datetime.strptime(end_date, formatter)
    delta = date2 - date1       # as timedelta

    dates_list = list()
    for i in range(delta.days + 1):
        day = date1 + timedelta(days=i)
        date_string = day.strftime(formatter)
        dates_list.append(date_string)

    return dates_list


class SummarizationUtil:
    """
    Biblioteca auxiliar que compreende funções extras para realizar a
    sumarização das mídias e mensagens de um certo período.

    Atributos
    -----------
    media_type : str
            Tipo de mídia para gerar a sumarização (images, audios, videos)
    comparison_method : str
            Metódo para calcular a similaridade/igualdade entre mídias (
            checksum, phash, jaccard).
    start_date : str
            Data de início da sumarização.
    end_date : str
            Data de fim da sumarização.
    messages_path : str
            Caminho em que estão salvos os arquivos de coleta por data.

    Métodos
    -----------
    generate_media_summarization()
        Faz a sumarização das mensagens de um certo tipo de mídia. Calcula
        informações como primeira vez em que a mídia foi compartilhada,
        quantas vezes foi compartilhada, em que grupos, por quais usuários,
        etc.
    """

    def __init__(self, media_type, comparison_method, start_date, end_date,
                 messages_path="/data/mensagens/"):
        self.media_type = media_type
        self.comparison_method = comparison_method
        self.start_date = start_date
        if end_date == 'no_end_date':
            end_date = start_date
        self.end_date = end_date
        self.messages_path = messages_path

    def generate_media_summarization(self, output='default'):
        """
        Faz a sumarização das mensagens de um certo tipo de mídia. Calcula
        informações como primeira vez em que a mídia foi compartilhada,
        quantas vezes foi compartilhada, em que grupos, por quais usuários,
        etc.

        Parâmetros
        ------------
            output : str
                Caminho para o arquivo onde será escrita a sumarização.
        """
        if self.media_type == 'images':
            media = 'image'
            hash_methods = ['checksum', 'phash']
        elif self.media_type == 'videos':
            media = 'video'
            hash_methods = ['checksum']
        elif self.media_type == 'audios':
            media = 'audio'
            hash_methods = ['checksum']
        elif self.media_type == 'others':
            media = 'other'
            hash_methods = ['checksum']
        else:
            print("Type of media not supported.")
            return

        if self.comparison_method not in hash_methods:
            print("Selected method is not compatible for the type of media.")
            return

        print('Grouping %s hashes of %s from %s to %s' %
              (self.comparison_method, self.media_type, self.start_date,
               self.end_date))

        hashes = dict()
        for date in get_days_list(self.start_date, self.end_date):
            json_filename = 'mensagens_%s.json' % (date)
            if not isfile(join(self.messages_path, json_filename)):
                continue
            with open(join(self.messages_path, json_filename), 'r') as fdata:
                for line in fdata:
                    message = json.loads(line.strip())

                    kind = message['mediatype']

                    if media == kind:
                        if (media == 'image' or media == 'video' or
                                media == 'audio' or media == 'other'):
                            hash = message[self.comparison_method]

                        if hash == "":
                            continue

                        if hash not in hashes:
                            hashes[hash] = dict()
                            hashes[hash][self.comparison_method] = hash
                            hashes[hash]['first_share'] = message['data']
                            hashes[hash]['total'] = 0
                            hashes[hash]['total_groups'] = 0
                            hashes[hash]['total_users'] = 0
                            hashes[hash]['groups_shared'] = set()
                            hashes[hash]['users_shared'] = set()
                            hashes[hash]['filenames'] = set()
                            hashes[hash]['messages'] = list()

                        # ADD MESSAGE TO HASH
                        if message['data'] < hashes[hash]['first_share']:
                            hashes[hash]['first_share'] = message['data']
                        hashes[hash]['total'] += 1
                        hashes[hash]['groups_shared'].add(
                            message['group_name'])
                        hashes[hash]['users_shared'].add(message['sender'])
                        hashes[hash]['filenames'].add(message['file'])
                        hashes[hash]['messages'].append(message)
                        hashes[hash]['total_groups'] = len(
                            hashes[hash]['groups_shared'])
                        hashes[hash]['total_users'] = len(
                            hashes[hash]['users_shared'])

        # Convert sets to lists
        for hash in hashes:
            hashes[hash]["groups_shared"] = list(hashes[hash]["groups_shared"])
            hashes[hash]["users_shared"] = list(hashes[hash]["users_shared"])
            hashes[hash]["filenames"] = list(hashes[hash]["filenames"])

        if output == 'default':
            output = '/data/merged_data_%s-%s_%s-%s.json' % \
                (media, self.comparison_method, self.start_date, self.end_date)
        with open(output, 'w') as json_file:
            json.dump(hashes, json_file, indent=4)

        return hashes

    def generate_text_summarization(self, output='default', min_size=200,
                                    threshold=0.75):
        """
        Faz a sumarização das mensagens de texto. Calcula
        informações como primeira vez em que a mídia foi compartilhada,
        quantas vezes foi compartilhada, em que grupos, por quais usuários,
        etc.

        Parâmetros
        ------------
            output : str
                Caminho para o arquivo onde será escrita a sumarização.
            min_size : str
                Tamanho mínimo do texto das mensagens agrupadas.
            threshold : str
                Valor mínimo de similariade para o índice de Jaccard para
                considerar duas mensagens como iguais.
        """
        if self.media_type == 'texts':
            media = 'text'
            hash_methods = ['jaccard']
        else:
            print("Type of media not supported.")
            return

        if self.comparison_method not in hash_methods:
            print("Selected method is not compatible for the type of media. "
                  "Using Jaccard instead")

        print('Grouping %s of %s from %s to %s' %
              (self.comparison_method, self.media_type, self.start_date,
               self.end_date))

        hashes = dict()
        for date in get_days_list(self.start_date, self.end_date):
            json_filename = 'mensagens_%s.json' % (date)
            if not isfile(join(self.messages_path, json_filename)):
                continue
            with open(join(self.messages_path, json_filename), 'r') as fdata:
                for line in fdata:
                    message = json.loads(line.strip())

                    text = message['content']

                    if len(text) < min_size:
                        continue
                    isNew = True
                    mID = message['message_id']
                    hashstring = mID
                    for ID in hashes.keys():
                        text2 = hashes[ID]['text']
                        score = compare_texts(text, text2)
                        if score >= threshold:
                            isNew = False
                            hashstring = ID
                            break

                    if isNew:
                        hashes[hashstring] = dict()
                        hashes[hashstring]['first_share'] = message['data']
                        hashes[hashstring]['total'] = 0
                        hashes[hashstring]['total_groups'] = 0
                        hashes[hashstring]['total_users'] = 0
                        hashes[hashstring]['groups_shared'] = set()
                        hashes[hashstring]['users_shared'] = set()
                        hashes[hashstring]['messages_IDs'] = list()
                        hashes[hashstring]['filenames'] = list()
                        hashes[hashstring]['text'] = text
                        hashes[hashstring]['messages'] = list()

                    # ADD MESSAGE TO HASH
                    if message['data'] < hashes[hashstring]['first_share']:
                        hashes[hashstring]['first_share'] = message['data']
                    hashes[hashstring]['total'] += 1
                    hashes[hashstring]['groups_shared'].add(
                        message['group_name'])
                    hashes[hashstring]['users_shared'].add(message['sender'])
                    hashes[hashstring]['messages_IDs'].append(message['message_id'])
                    hashes[hashstring]['messages'].append(message)
                    hashes[hashstring]['total_groups'] = len(
                        hashes[hashstring]['groups_shared'])
                    hashes[hashstring]['total_users'] = len(
                        hashes[hashstring]['users_shared'])

        # Convert sets to lists
        for hash in hashes.keys():
            hashes[hash]["groups_shared"] = list(hashes[hash]["groups_shared"])
            hashes[hash]["users_shared"] = list(hashes[hash]["users_shared"])

        if output == 'default':
            output = '/data/merged_data_%s-%s_%s-%s.json' % \
                (media, self.comparison_method, self.start_date, self.end_date)
        with open(output, 'w') as json_file:
            json.dump(hashes, json_file, indent=4)

        return hashes


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-t", "--media_type", type=str,
                        help="Tipo de mídia para gerar a sumarização (images,"
                        " audios, videos, 'texts', others).", required=True)

    parser.add_argument("-m", "--comparison_method", type=str,
                        help="Metódo para calcular a similaridade/igualdade"
                        " entre mídias (checksum, phash, jaccard).",
                        required=True)

    parser.add_argument("-s", "--start_date", type=str,
                        help="Data de início da sumarização.",
                        required=True)

    parser.add_argument("-e", "--end_date", type=str,
                        help="Data de fim da sumarização. Se ausente a "
                        "sumarização ocorrerá apenas para as mensagens da data "
                        "de início",
                        default='no_end_date')

    parser.add_argument("-o", "--output", type=str,
                        help="Arquivo de saída para as mensagens salvas",
                        default='default')

    args = parser.parse_args()

    try:
        util = SummarizationUtil(args.media_type, args.comparison_method,
                                 args.start_date, args.end_date)
        if args.media_type in ['audios', 'images', 'videos', 'others']:
            util.generate_media_summarization(args.output)
        elif args.media_type in ['texts']:
            util.generate_text_summarization(args.output)

    except Exception as e:
        error_time = str(datetime.datetime.now())
        error_msg = str(e).strip()
        with open('/data/log.txt', 'w') as ferror:
            print("%s >> Error:\t%s" % (error_time, error_msg))
            print("%s >> Error:\t%s" % (error_time, error_msg), file=ferror)


if __name__ == "__main__":
    main()
