# coding: latin1

from datetime import datetime
import os
import re
import csv


class Conf:
    def __init__(self):
        # Configuration initializer
        import configparser
        self.queries = {}
        self.conf = configparser.ConfigParser()
        self.conf.read(f'{os.getcwd()}\\conf.ini', encoding='utf-8')

        # Retrieving Queries
        self.sqlqueries = self.conf['queries']
        counter = 1
        for query in self.sqlqueries.values():
            self.queries[f"query.{counter}"] = query
            counter += 1

        # Retrieving File names
        self.querynames = self.conf['names']
        self.names = {}
        testcount = 1
        for name in self.querynames.values():
            key = [k for k, v in self.querynames.items() if v == name]
            self.names[f"name.{key[0].split('.')[1]}"] = name
            testcount += 1
        print(self.names)

        # Retrieving Server variables
        self.server = self.conf['server']
        self.dbType = self.server['dbType']
        self.ip = self.server['ip']
        self.port = self.server['port']
        self.database = self.server['database']
        self.username = self.server['user']

    def __str__(self):
        return f"IP: {self.server['ip']}\nPort: {self.server['port']}\n" \
               f"Database Name: {self.server['database']}\nUsername: {self.server['user']}\n"

    def getQueries(self):
        print(self.queries)

    def _getPassword(self):
        with open(f'{os.getcwd()}\\pwd.txt') as f: return f.readline().strip('\n')

    def filterQueryField(self, raw_fieldlist):
        '''
        Get raw values between select and from and filter them into a beautifull field names list
        '''
        trimed_fieldlist = []
        fieldlist = []
        print(raw_fieldlist)
        temp_fieldlist = re.sub(',(?=((?!\().)*?\))', '@', raw_fieldlist)
        temp_fieldlist = temp_fieldlist.upper().split(',')
        for x in temp_fieldlist:
            trimed_fieldlist.append(re.sub('@(?=((?!\().)*?\))', ',', x))
        print(trimed_fieldlist)
        for x in trimed_fieldlist:
            if " as " in x.lower():
                fieldlist.append(re.search('as ([a-z_-]*)', x.lower()).group(1))
            else:
                fieldlist.append(x)
        return fieldlist

    def listing(self, query):
        '''
        Turns the query into a list of all the selected data fields
        '''
        log(f"Récupération des champs de la requête...")
        select = []
        select_raw = re.search('^select (.*?) from', query.lower())
        print(select_raw.group(1))
        select_raw = self.filterQueryField(select_raw.group(1))
        # S?lectionne les champs, puis les remet en majuscule, puis les s?parent par la virgule
        # select_raw = select_raw.group(0).upper().split(',')
        # Retire les eventuels espaces pour ?viter les erreurs
        for row in select_raw:
            select.append(row.strip())
        print(select)
        log(f"Liste des champs: {select}")
        return select

    def getTableName(self, query):
        log(f"Récupération du nom de la table...")
        select = []

        select_raw = re.search('from (.*?) ((?!where)|(?!inner)|(?!left)|(?!right))', query.lower())

        # Sélectionne les champs, puis les remet en majuscule, puis les séparent par la virgule
        select_raw = select_raw.group(1).upper().split(',')
        # Retire les éventuels espaces pour éviter les erreurs
        for row in select_raw:
            select.append(row.strip())
        print(select)
        log(f"Nom de la table: {select[0]}")
        return select


class Database:
    def __init__(self):
        configuration = Conf()
        self.dbType = configuration.dbType
        self.cnxstring = r"DRIVER={databasedriver};" \
                         f"DSN={configuration.ip}\\{configuration.database};" \
                         f"Server Name={configuration.ip};" \
                         f"Server Port={configuration.port};" \
                         f"Database={configuration.database};" \
                         f"UID={configuration.username};" \
                         f"PWD={configuration._getPassword()};"
        self.cnxstring.replace("databasedriver", self.dbType)

    def connect(self):
        if self.dbType.lower() == "hfsql":
            import pypyodbcHFSQL
            return pypyodbcHFSQL.connect(self.cnxstring)
        if self.dbType.lower() == "pervasivesql":
            import pypyodbc
            return pypyodbc.connect(self.cnxstring)
        else:
            import pypyodbc
            return pypyodbc.connect(self.cnxstring)

def checkQuery(sqlquery):
    badwords = ['UPDATE', 'DELETE', 'INSERT', 'CREATE', 'ALTER', 'DROP',
                '%', 'DATABASE', '\\', 'TABLE']
    log(f"Vérification de la conformité de la requête...")
    for word in badwords:
        if word.lower() in sqlquery.lower():
            if word == '*':
                log(f"Un astérisque a été détecté dans la requête. Par mesure de précaution, cela n'est pas autorisé")
                exit()
            log(f'La requête SQL contient un mot clé interdit : {word} \nFermeture du programme.')
            exit()
    log(f"La requête SQL est conforme.")


def log(message, big=False, separator=False, dated=False):
    rator = f'\n'
    with open(f"{os.getcwd()}\\log.txt", 'a+', encoding="utf-8") as logfile:
        now = datetime.now()
        if big:
            rator = f"\n\n\n---------------   {now.strftime('%d/%m/%Y')}   ---------------\n\n\n"
        elif separator:
            rator = f"\n\n---------------   {now.strftime('%d/%m/%Y %H:%M:%S')}   ---------------\n"
        elif dated:
            logfile.write(rator)
            logfile.write(f"{now.strftime('%d/%m/%Y %H:%M:%S')} : {message}")
            return
        logfile.write(rator)
        logfile.write(message)


def listResults(results):
    count = 0
    resultlist = []
    for result in results:
        result = list(result)
        resultlist.append(result)
        count += 1
    return resultlist, count


def createCSV(headers, values, count, queryname, tablename):
    try:
        # Permet de tester l'existence de name.{count}, malgr?s le fait qu'elle ne soit pas assign?e (?vite le crash)
        if queryname[f"name.{count}"]:
            pass
    except KeyError:
        queryname[f"name.{count}"] = None
    try:
        if queryname[f"name.{count}"]:
            filename = queryname[f"name.{count}"]
            filename_type = "personalisé"
        else:
            filename = tablename[0]
            filename_type = "automatique"

        with open(f'{os.getcwd()}\\{filename}.csv', 'w+', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=';', quoting=csv.QUOTE_ALL, lineterminator='\r', )
            writer.writerow(headers)
            writer.writerows(values)
        log(f"Fichier csv créé avec un nom {filename_type} : {os.getcwd()}\\{filename}.csv")
    except Exception as e:
        log(f'Le fichier {tablename[0]}.csv, correspondant à la query {count}, n\'a pas pu être créé.')
        log(f"Erreur: {e}\n")
        print(e)


if __name__ == "__main__":
    configuration = Conf()
    db = Database()

    with db.connect() as conn:

        log(f"Initialisation de la connexion.", separator=True)
        count1 = 0
        count2 = 1
        for query in configuration.sqlqueries.values():
            # Query Counter
            count1 += 1

            # Checking query for prohibited characters
            checkQuery(query)

            # Getting variables
            log(f"Query numéro: {count1}\nQuery: {query}")
            print(query)
            querylist = configuration.listing(query)
            tablename = configuration.getTableName(query)

            # Creation of the cursor and execution of the query
            crsr = conn.cursor()
            log(f"Curseur créé.")
            print("Curseur commande créé.")
            try:
                log(f"Lancement de la requête...")
                print(query)
                results = crsr.execute(query)
                log(f"La requête s'est bien passée")
            except Exception as e:
                log(f"La requête a échouée.")
                log(f"Erreur: {e}")
            conn.commit()
            crsr.close()
            log(f"Fermeture du curseur.\n")

            # Order the results in a list and count
            resultlist, count2 = listResults(results)

            # Create the csv file with the results of the query
            createCSV(querylist, resultlist, count1, configuration.names, tablename)
            print('create ok')



    print(f"Nombre de résultats obtenus : {count2}")
    log(f"\nNombre de résultats obtenus : {count2}")

