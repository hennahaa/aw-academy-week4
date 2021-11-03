#!/usr/bin/python
import psycopg2
from config import config

''' Tämä kolossaalinen hirviö on
    harjoitus 2, 3, 4 mashup, eli
    tietokantasovellus jossa CRUD toimintoja ja muuta hauskaa'''

# 2 INSERT INTO person
def lisaa_rivi_person(nimi, ika, opiskelija_status):
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        SQL = """INSERT INTO person(name, age, student) VALUES (%s, %s, %s);"""
        record_to_insert = (nimi,ika,opiskelija_status)
        cursor.execute(SQL, record_to_insert)
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

# 1 INSERT INTO cert
def lisaa_rivi_cert(nimi, person_id):
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        SQL = """INSERT INTO certificates(name, person_id) VALUES (%s, %s);"""
        record_to_insert = (nimi,person_id)
        cursor.execute(SQL, record_to_insert)
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

# 1 SELECT Hae kaikki taulusta person ja tulosta ne
def person_kaikki():
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        SQL = """SELECT * FROM person;"""
        cursor.execute(SQL)
        for table in cursor.fetchall():
            print(table)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')                  

# 2 SELECT Hae person-taulun sarakkeiden nimet ja tulosta ne
def person_sarakkeet():
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        SQL = """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'person';"""
        cursor.execute(SQL)
        for table in cursor.fetchall():
            print(table)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')        

# 3 SELECT Hae sertti-taulun sarakkeiden nimet ja tulosta ne ja rivit
def cert_kaikki():
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        #ensimmäinen kysely -- jätetty pois tietokantasovelluksen toteutuksesta
        #SQL_1 = """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'certificates';"""
        #cursor.execute(SQL_1)
        #rows = cursor.fetchall()
        #print(rows)
        #toinen kysely
        SQL_2 = """SELECT * FROM certificates;"""
        cursor.execute(SQL_2)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

# 4 SELECT hae kaikki AWS serttejen omistajat
def hae_cert_aws():
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        SQL = """SELECT person.name AS aws_suorittaja FROM person INNER JOIN certificates ON certificates.person_id = person.id WHERE certificates.name = 'AWS';"""
        cursor.execute(SQL)
        for table in cursor.fetchall():
            print(table)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')  

# 1 UPDATE Päivitä olemassa olevaa riviä person taulussa
def paivita_person_opiskelija(opiskelija_status, person_id):
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        SQL = """UPDATE person SET student = %s WHERE id = %s"""
        record_to_insert = (opiskelija_status, person_id)
        cursor.execute(SQL, record_to_insert)
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


# 2 UPDATE Päivitä olemassa olevaa riviä certtitaulussa
def paivita_cert_nimi(sertti_nimi, sertti_id):
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        SQL = """UPDATE certificates SET name = %s WHERE id = %s"""
        record_to_insert = (sertti_nimi, sertti_id)
        cursor.execute(SQL, record_to_insert)
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

# 1 DELETE rivi person

def poista_rivi_person(person_id):
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        SQL = """DELETE FROM persons WHERE id = %s"""
        record_to_insert = (person_id,)
        cursor.execute(SQL, record_to_insert)
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

# 2 DELETE rivi certificates 

def poista_rivi_cert(sertti_id):
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        SQL = """DELETE FROM certificates WHERE id = %s"""
        record_to_insert = (sertti_id,)
        cursor.execute(SQL, record_to_insert)
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

#itse nimettyä taulua ei taida pystyä luomaan ilman että siitä tulee injektiohirviö?
def luo_table():
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        SQL = f"""CREATE TABLE uusitaulu (name VARCHAR(255), arvo VARCHAR(255))"""
        cursor.execute(SQL)
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


#tätä hirviötä voisi parannella mutta sen aika ei ole nyt
def suorita():
    while True:

        print()
        print("Komennot: CREATE DELETE UPDATE SELECT INSERT LOPETA")
        print("LOPETA sulkee ohjelman.")
        print()
        komento = input("Mikä toiminto valitaan? ")

        if komento == "CREATE":
            print("Luodaan uusi taulu nimeltä uusitaulu.")
            luo_table()
        elif komento == "DELETE":
            taulu = input("Syötä P jos poistetaan taulusta person, syötä C jos poistetaan taulusta certificates. Syötä P jos haluat poistaa taulusta person. Muulla syötteellä toiminto ohitetaan. ")
            if taulu == "C":
                rivi_id = int(input("Mikä id? "))
                poista_rivi_cert(rivi_id)
            if taulu == "P":
                rivi_id = int(input("Mikä id? "))
                poista_rivi_person(rivi_id)
            else:
                pass
        elif komento == "UPDATE":
            taulu = input("Syötä P jos päivitetään taulusta person, syötä C jos päivitetään taulusta certificates. Syötä P jos haluat päivittää taulua person. Muulla syötteellä toiminto ohitetaan. ")
            if taulu == "C":
                sertti_id = int(input("Mikä on päivitettävä id? "))
                sertti_nimi = input("Miksi se päivitetään? ")
                paivita_cert_nimi(sertti_nimi, sertti_id)
            if taulu == "P":
                person_id = int(input("Mikä on päivitettävä id? "))
                opiskelija_status = input("Mikä on uusi arvo (True vai False)? ")
                paivita_person_opiskelija(opiskelija_status, person_id)
            else:
                pass
        elif komento == "SELECT":
            taulu = input("Valitse C jos haluat hakea kaikki rivit taulusta certificates. Syötä P jos haluat hakea taulun person. Muulla syötteellä toiminto ohitetaan. ")
            if taulu == "C":
                cert_kaikki()
            if taulu == "P":
                person_kaikki()
            else:
                pass
        #TODO insert LOPPUUN
        elif komento == "INSERT":
            taulu = input("Syötä P jos lisätään tauluun person, syötä C jos lisätään tauluun certificates. Muulla syötteellä toiminto ohitetaan. ")
            if taulu == 'C':
                nimi = input("Mikä sertifikaatti? ")
                person_id = int(input("Mikä person id? (int!) "))
                lisaa_rivi_cert(nimi, person_id)
            if taulu == 'P':
                nimi = input("Henkilön nimi? ")
                ika = input("Henkilon ika? ")
                kirjain = input("Opiskelija? (K/E) ")
                
                if kirjain == "K":
                    opiskelija_status = True
                else:
                    opiskelija_status = False

                lisaa_rivi_person(nimi, ika, opiskelija_status)
            else:
                pass
        elif komento == "LOPETA":
            break;
        else:
            print()
            print("Komennot: CREATE DELETE UPDATE SELECT INSERT LOPETA")
            print("LOPETA sulkee ohjelman.")
            print()
            komento = input("Mikä toiminto valitaan? ")


if __name__ == '__main__':
    suorita()
