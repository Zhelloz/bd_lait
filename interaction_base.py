# -*- coding: utf8 -*- #
"""
Created on Tue Jan 30 11:25:50 2018

@author: Benjamin, Antoine
"""

import datetime
import mysql.connector
from mysql.connector import errorcode
import re

class Table(object):
    def __init__(self, name, entities_number):
        self.name = name
        self.hasDate = False
        self.hasSet = False
        self.entitiesNumber = entities_number
        self.data = [[] for i in range(self.entitiesNumber)]
    
    def setName(self, new_name):
        self.name = new_name
    
    def setDate(self, new_date):
        self.date = new_date
        self.hasDate = True
        
    def setSet(self, new_set):
        self.set = new_set
        self.hasSet = True
    
    def setDateName(self, date_name):
        self.dateName = date_name
    
    def appendData(self):
        for i in range(self.entitiesNumber):
            self.data[i].append({})
    
    def appendDataComponent(self, new_data_component_name, new_data_component_value, entity):
        self.data[entity-1][-1][new_data_component_name] = new_data_component_value

    def concatenateEntities(self):
        self.totalData = []
        for i in range(self.entitiesNumber):
            self.totalData = self.totalData + self.data[i]


def read_header_csv_link(link_file_name, separator):
    
    file=open(link_file_name,'r')
    lignes = file.read().splitlines()
    file.close()
    header_size = int(lignes[0].split(sep=separator)[1])
    lignes = lignes[0:header_size-1]
    header = []
    for ligne in lignes:
        header.append(ligne.split(sep=separator))
    return header
    

def lecture_csv(nom_fichier, skip, separateur):
    """
    Cette fonction permet de lire un fichier csv et recuperer les donnees
    Entrees : le nom du fichier, le nombre de ligne a sauter, le type de separateur
    Sortie : une liste de dictionnaires (cles = nom des colones du csv, valeurs = donnees)
    """
    
    fichier=open(nom_fichier,'r')
    
    lignes = fichier.read().splitlines()
    fichier.close()
    
    colones = lignes[skip].split(sep=separateur)
    
    matrice = []
    for i in range(skip+1,len(lignes)):
        ligne = dict(zip(colones, lignes[i].split(sep=separateur)))
        matrice.append(ligne)
    
    return matrice

def get_global_date(data_file_name, data_file_separator, line, column):
    
    file=open(data_file_name,'r')
    lignes = file.read().splitlines()
    file.close()
    
    ligne = lignes[line-1].split(sep=data_file_separator)
    return ligne[column-1]

def automatique_write_table(configBase, data_file_name, data_file_separator, link_file_name, link_file_separator):
    
    # read files (data file, link file)
    header_link = read_header_csv_link(link_file_name, link_file_separator)
    data_list = lecture_csv(data_file_name, int(header_link[0][2]), data_file_separator)
    link_list = lecture_csv(link_file_name, int(header_link[0][1]), link_file_separator)
    
    # creation of object tables
    table_list = []
    for i in range(1,len(header_link[1])):
        if header_link[1][i] != None and header_link[1][i] != '':
            info_table = header_link[1][i].split(sep='_')
            table = Table(info_table[0],int(info_table[1]))
            table_list.append(table)
    
    dico_link_change = {}
    
    for i in range(2, len(header_link)):
        # global date gestion
        if header_link[i][0] == 'date':
            for table in table_list:
                if header_link[i][1] == table.name:
                    position = header_link[i][3].split(sep='_')
                    date = get_global_date(data_file_name, data_file_separator, int(position[0]), int(position[1]))
                    date_transform = transformation_date_sql(date,header_link[i][4])
                    table.setDate(date_transform)
                    table.setDateName(header_link[i][2])
                    break
        # global set gestion
        if header_link[i][0] == 'set':
            for table in table_list:
                if header_link[i][1] == table.name:
                    j = 3
                    dic_set = {'column':header_link[i][2]}
                    while j < len(header_link[i]) and header_link[i][j] != '' and header_link[i][j] != None:
                        one_set = header_link[i][j].split(sep='_')
                        print(one_set)
                        dic_set[one_set[0]] = one_set[1]
                        j += 1
                    table.setSet(dic_set)
                    break
        #global link gestion
        if header_link[i][0].find('correspondance') != -1:
            dico_link_change[header_link[i][0]] = get_corres(configBase, header_link[i][1], header_link[i][2], header_link[i][3], header_link[i][4], header_link[i][5])
    
    # data transformations
    for individual_dico_data in data_list:
        for table in table_list:
            table.appendData()
        for key in individual_dico_data.keys():
            for individual_dico_link in link_list:
                if key == individual_dico_link['colonne_csv']:
                    data = ''
                    if individual_dico_link['type'] == 'date':
                        data = transformation_date_sql(individual_dico_data[key],individual_dico_link['format'])
                    elif individual_dico_link['type'] == 'time':
                        try :
                            data = transformation_time_sql(individual_dico_data[key],individual_dico_link['format'])
                        except:
                            data = individual_dico_data[key]
                    elif individual_dico_link['format'].find('correspondance') != -1:
                        for link in dico_link_change[individual_dico_link['format']]:
                            if link[0] == individual_dico_data[key]:
                                data = link[1]
                                break
                    else :
                        data = individual_dico_data[key]
                    for table in table_list : 
                        if individual_dico_link['table'] == table.name:
                            if individual_dico_link['entite'] == None or individual_dico_link['entite'] == '':
                                table.appendDataComponent(individual_dico_link['colonne_table'],data,1)
                            else :
                                entity_number = int(individual_dico_link['entite'])
                                table.appendDataComponent(individual_dico_link['colonne_table'],data,entity_number)
                            break
        for table in table_list:
            if table.hasDate:
                for i in range(table.entitiesNumber):
                    table.appendDataComponent(table.dateName,table.date,i+1)
            if table.hasSet:
                for i in range(table.entitiesNumber):
                    table.appendDataComponent(table.set['column'],table.set[str(i+1)],i+1)
        
    # data insertion
    for table in table_list:
        table.concatenateEntities()
        ecriture_table(configBase, table.totalData, table.name)

def get_corres(configBase, table, old_link_column, new_link_column, discrimination_column, discrimination_rule):
    try:
        print("tentative de connexion - base", configBase['database'], "sur", configBase['host'])
        cnx = mysql.connector.connect(** configBase)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Souci d'identification avec le compte ou le mot de passe")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Pas de base", configBase['database'])
        else:
            print("Anomalie :", err)
    else:
        print("Connexion bonne")
        
        cursor = cnx.cursor()
        
        query = "SELECT %s, %s, %s FROM %s ORDER BY %s" % (old_link_column, new_link_column, discrimination_column, table, old_link_column)
  
        cursor.execute(query)
        results = []
        for i in cursor:
            row = dict(zip(cursor.column_names, i))
            results.append(row)
        
        list_link = [[str(results[0][old_link_column]), str(results[0][new_link_column]), str(results[0][discrimination_column])]]
        
        for i in range(1, len(results)):
            if results[i][old_link_column] == list_link[-1][0]:
                if discrimination_rule == 'max' and results[i][discrimination_column] > list_link[-1][2]:
                    list_link[-1] = [str(results[i][old_link_column]), str(results[i][new_link_column]), str(results[i][discrimination_column])]
                elif discrimination_rule == 'min' and results[i][discrimination_column] < list_link[-1][discrimination_column]:
                    list_link[-1] = [str(results[i][old_link_column]), str(results[i][new_link_column]), str(results[i][discrimination_column])]
            else:
                list_link.append([str(results[i][old_link_column]), str(results[i][new_link_column]), str(results[i][discrimination_column])])
        
        cnx.close()
        return list_link
    
    


def ecriture_table(configBase, dico_donnees, table):
    """
    Cette fonction permet d'ecrire des donnees dans une base de donnees
    Entrees : acces a la base, une liste de dictionnaires (cles = nom des colones du csv, valeurs = donnees), un dictionnaire des dcorrespondances entre le csv et la base
    Sortie : ecriture dans la base
    """
    try:
        print("tentative de connexion - base", configBase['database'], "sur", configBase['host'])
        cnx = mysql.connector.connect(** configBase)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Souci d'identification avec le compte ou le mot de passe")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Pas de base", configBase['database'])
        else:
            print("Anomalie :", err)
    else:
        print("Connexion bonne")
      
        try :
            cursor = cnx.cursor()
            for ligne in dico_donnees:
                insert2 = {}
                for i in ligne:
                    if ligne[i] != '' and ligne[i] != '-':
                        insert2[i]=ligne[i]
                    
                    colones = ', '.join(insert2.keys())
                    valeurs = '\', \''.join(insert2.values())
                    valeurs = '\'' + valeurs + '\''
                try:
                    query = "INSERT INTO %s (%s) VALUES (%s)" % (table, colones ,valeurs)
                    print(query)
                    cursor.execute(query, insert2)
                    print("Bonne insertion")
                except mysql.connector.Error as err:
                    print(err)
                cnx.commit()
            
        except Exception as e:
            print("Probleme insertion")
            print(e)
      
        cnx.close()  


def ecriture_table_old(configBase, dico_donnees, dico_correspondance) :
    """
    Cette fonction permet d'ecrire des donnees dans une base de donnees
    Entrees : acces a la base, une liste de dictionnaires (cles = nom des colones du csv, valeurs = donnees), un dictionnaire des dcorrespondances entre le csv et la base
    Sortie : ecriture dans la base
    """
    
    dico_insert_table = {}
    for donnee in dico_donnees:
        insert = {}
        for cle in donnee.keys():
            for corres in dico_correspondance:
                if cle == corres['colone_csv']:
                    table = corres['table']
                    if table not in insert:
                        insert[table] = {}
                    insert[table][corres['colone_table']] = donnee[cle]
        for cle in insert.keys():
            if cle not in dico_insert_table:
                dico_insert_table[cle] = []
            dico_insert_table[cle].append(insert[cle])
    print(dico_insert_table)
    try:
        print("tentative de connexion - base", configBase['database'], "sur", configBase['host'])
        cnx = mysql.connector.connect(** configBase)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Souci d'identification avec le compte ou le mot de passe")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Pas de base", configBase['database'])
        else:
            print("Anomalie :", err)
    else:
        print("Connexion bonne")
      
        try :
            cursor = cnx.cursor()
            for table in dico_insert_table :
                for insert in dico_insert_table[table]:
                    print(insert)
                    insert2 = {}
                    for i in insert:
                        if insert[i] != '':
                            insert2[i]=insert[i]
                    
                    colones = ', '.join(insert2.keys())
                    valeurs = '\', \''.join(insert2.values())
                    valeurs = '\'' + valeurs + '\''
                    query = "INSERT INTO %s (%s) VALUES (%s)" % (table, colones ,valeurs)
                    print(query)
                    cursor.execute(query, insert2)
                    print("Bonne insertion")
                    cnx.commit()
            
        except Exception as e:
            print("Probleme insertion")
            print(e)
      
        cnx.close()
    
    
        
def lecture_table(table, configBase):
    """
    Cette fonction permet de lire les donnees d'une table dans une base de donnees
    Entrees : acces a la base, nom de la table a lire
    Sortie : une liste de dictionnaires (cles = nom des colones, valeurs = donnees)
    """
    try:
        print("tentative de connexion - base", configBase['database'], "sur", configBase['host'])
        cnx = mysql.connector.connect(** configBase)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Souci d'identification avec le compte ou le mot de passe")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Pas de base", configBase['database'])
        else:
            print("Anomalie :", err)
    else:
        print("Connexion bonne")
        
        cursor = cnx.cursor()
        
        query = "SELECT * FROM " + table
  
        cursor.execute(query)
        liste_info = []
        for i in cursor:
            row = dict(zip(cursor.column_names, i))
            liste_info.append(row)
        
        cnx.close()
        return liste_info
    

def ecriture_csv(a,file,separator):
    """
    Cette fonction permet d'ecrire des donnees dans un csv
    Entrees : une liste de dictionnaires (cles = nom des colones, valeurs = donnees), le nom du fichier a creer, le separateur a utiliser
    Sortie : fichier csv
    """
    with open(file, 'w', newline='') as f:
        fieldnames=a[0].keys()
        for i in fieldnames:
            f.write(str(i) + separator)
        f.write('\n')
        for item in a:
            for j in item.values():
                if type(j)==set:
                    j = j.pop() #j est un set avec un unique élément, on le récupère comme cela
                f.write(str(j) + separator)
            f.write('\n')
         
            
def transformation_date_old(date):
    l = date.split('.')
    l2 = [l[2],l[1],l[0]]
    return '-'.join(l2)

def transformation_date_sql(date,format_date):
    return datetime.datetime.strptime(date, format_date).strftime('%Y-%m-%d')

def transformation_time_sql(time,format_time):
    return datetime.datetime.strptime(time, format_time).strftime('%H:%M:%S')

def get_columns(configBase, table):
    """etant donnee le nom de cursor, le nom de base de donnee, le nom de table, on peut recuperer les noms de colonnes et les valeurs pre-fixed"""
    
    try:
        print("tentative de connexion - base", configBase['database'], "sur", configBase['host'])
        cnx = mysql.connector.connect(** configBase)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Souci d'identification avec le compte ou le mot de passe")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Pas de base", configBase['database'])
        else:
            print("Anomalie :", err)
    else:
        print("Connexion bonne")
        
        cursor = cnx.cursor()
        
        query = ("SELECT COLUMN_NAME,DATA_TYPE,COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}'".format(configBase['database'], table))
        cursor.execute(query)
        
        noms_columns = {}
        for (column_name, datatype, columntype) in cursor :
            noms_columns[column_name]=[datatype]
            if datatype == 'set':     
                string_set=re.findall('\((.*?)\)', columntype)[0]#enlever()
                list_item_set=re.split(",",string_set)
                pattern = re.compile("'(.*)'")#enlever ''
                list_set=[]
                for item in list_item_set:
                    list_set.append(pattern.findall(item)[0])
                noms_columns[column_name].append(list_set)

        cnx.close()
        return noms_columns
    
def get_columns_ref(configBase, table) :
    """etant donnee le nom de cursor, le nom de base de donnee, le nom de table, on peut recuperer les noms de colonnes et les valeurs pre-fixed"""
    
    noms_columns = get_columns(configBase, table)
    
    if table == 'achat_vache':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
    if table == 'aliment':
        noms_columns.pop('id_aliment', None)
    if table == 'appartenance':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
        noms_columns=gestion_ref(noms_columns,'ref_lot',"SELECT nom FROM lot",configBase)
    if table == 'avortement':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
    if table == 'chaleur':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
    if table == 'composition':
        noms_columns=gestion_ref(noms_columns,'ref_ration',"SELECT nom FROM ration",configBase)
        noms_columns=gestion_ref(noms_columns,'ref_aliment',"SELECT nom FROM aliment",configBase)
    if table == 'controle_gestation':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
    if table == 'controle_qualite_aliment':
        noms_columns.pop('id_qualite_aliment', None)
        noms_columns=gestion_ref(noms_columns,'ref_aliment',"SELECT nom FROM aliment",configBase)
    if table == 'controle_qualite_lait':
        noms_columns.pop('id_qualite_lait', None)
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
    if table == 'etat_capteur_lait':
        noms_columns=gestion_ref(noms_columns,'ref_capteur_lait',"SELECT id_capteur_lait FROM capteur_lait",configBase)
    if table == 'etat_capteur_poids':
        noms_columns=gestion_ref(noms_columns,'ref_capteur_poids',"SELECT id_capteur_poids FROM capteur_poids",configBase)
    if table == 'etat_vache':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
    if table == 'insemination':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
        noms_columns=gestion_ref(noms_columns,'ref_taureau',"SELECT nom FROM taureau",configBase)
    if table == 'lot':
        noms_columns.pop('id_lot', None)
    if table == 'medicament':
        noms_columns.pop('id_medicament', None)
    if table == 'pesee':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
        noms_columns=gestion_ref(noms_columns,'ref_capteur_poids',"SELECT id_capteur_poids FROM capteur_poids",configBase)
    if table == 'ration':
        noms_columns.pop('id_ration', None)
    if table == 'rationnement':
        noms_columns=gestion_ref(noms_columns,'ref_lot',"SELECT nom FROM lot",configBase)
        noms_columns=gestion_ref(noms_columns,'ref_ration',"SELECT nom FROM ration",configBase)
    if table == 'reforme':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
    if table == 'sante':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
        noms_columns.pop('id_traitement', None)
        noms_columns=gestion_ref(noms_columns,'ref_medicament',"SELECT nom_medicament FROM medicament",configBase)
    if table == 'tarissement':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
    if table == 'taureau':
        noms_columns.pop('id_taureau', None)
    if table == 'traite':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
        noms_columns=gestion_ref(noms_columns,'ref_capteur_lait',"SELECT id_capteur_lait FROM capteur_lait",configBase)
    if table == 'veau':
        noms_columns=gestion_ref(noms_columns,'ref_vache',"SELECT id_national_vache FROM vache",configBase)
    if table == 'vente_veau':
        noms_columns=gestion_ref(noms_columns,'ref_veau',"SELECT id_national_veau FROM veau",configBase)

    return noms_columns

def gestion_ref(noms_columns,nom_ref,query,configBase):
    
    try:
        print("tentative de connexion - base", configBase['database'], "sur", configBase['host'])
        cnx = mysql.connector.connect(** configBase)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Souci d'identification avec le compte ou le mot de passe")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Pas de base", configBase['database'])
        else:
            print("Anomalie :", err)
    else:
        print("Connexion bonne")
        
        cursor = cnx.cursor()
        noms_columns.pop(nom_ref, None)
        cursor.execute(query)
        result = cursor.fetchall()
        ref = '('
        for i in result:
            ref =  ref + '\'' + i[0] + '\'' + ','
        ref = ref[:-1] + ')'
        string_set=re.findall('\((.*?)\)', ref)[0]#enlever()
        list_item_set=re.split(",",string_set)
        pattern = re.compile("'(.*)'")#enlever ''
        list_set=[]
        for item in list_item_set:
            list_set.append(pattern.findall(item)[0])
        noms_columns[nom_ref]=['set',list_set]
        
        cnx.close()
        return noms_columns

def get_table_name(configBase): 
    
    """
    RÃ©cupÃ¨re le nom des tables prÃ©sentes dans la base bd
    """
    try:
        print("tentative de connexion - base", configBase['database'], "sur", configBase['host'])
        cnx = mysql.connector.connect(** configBase)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Souci d'identification avec le compte ou le mot de passe")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Pas de base", configBase['database'])
        else:
            print("Anomalie :", err)
    else:
        print("Connexion bonne")
        
        cursor = cnx.cursor()
        
        sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{0}'".format(configBase['database'])
  
        cursor.execute(sql)
        result = cursor.fetchall()
        noms_tables = []
        
    
        for i in range(len(result)) : 
            noms_tables.append(result[i][0])
            
        cnx.close()
        return noms_tables


def get_config_base():
    return {
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'database': 'bd_lait',
        'raise_on_warnings': True
    }

def main():
    fichier_donnees = 'ID_VACHE021217_sans_modifs.txt'
    fichier_correspondances = 'correspondance_ALPRO_traite.csv'
    separateur = ';'
    configBase = {
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'database': 'bd_lait',
        'raise_on_warnings': True
    }
    automatique_write_table(configBase, fichier_donnees, '\t', fichier_correspondances, separateur)

if __name__=="__main__":
    main()