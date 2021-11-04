import datetime
import psycopg2
from config import config

from google.cloud import storage

'''HARJOITUS 7 - funktio, joka lataa kuvan cloud storageen ja kirjoittaa tietokantaan metadataa kuvasta
    versio jossa cloud storage-osoite on geneerinen osoite, johon ei välttämättä pääse käsiksi,
    ja sitten signed osoite jossa on rajallinen pääsy tiedostoon sen jälkeen kun se on ladattu buckettiin'''

#funktio metadatan lisäykseen metadata-tauluun
def lisaa_metadata(bucket_name, blob_name, person_id, url):
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        SQL = """INSERT INTO metadata(bucket, blob, person_id, url) VALUES (%s, %s, %s, %s);"""
        record_to_insert = (bucket_name,blob_name,person_id,url)
        cursor.execute(SQL, record_to_insert)
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

#lataa tiedoston cloud storageen ja lisää siitä metadatan tietokantaan
def lataa_tiedosto_storageen(bucket_name, file_name, blob_name,person_id):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_filename(file_name)

    url = blob.generate_signed_url(
        version="v4",
        # This URL is valid for 15 minutes
        expiration=datetime.timedelta(minutes=15),
        # Allow GET requests using this URL.
        method="GET",
    )

    #lisätään tauluun metadata jossa tässä luotu signed url
    lisaa_metadata(bucket_name,blob_name,person_id,url)

    #jos ei muodostettaisi signed urlia niin muodostettaisiin cloud storage url näin kun upload on tehty
    #gs_url = f"http://{bucket_name}.storage.googleapis.com/{blob_name}"
    #lisaa_metadata(bucket_name,blob_name,person_id,gs_url)

def hae_storageurl(blob_name):
    conn = None
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        SQL = """SELECT url FROM metadata WHERE blob = %s"""
        cursor.execute(SQL, (blob_name,))
        rows = cursor.fetchall()
        for row in rows:
            #putsataan url
            cleaned_row = ''.join(char for char in row if char not in "()',")
            print(cleaned_row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

#kovakoodattuja arvoja koska olen laiska
amparinimi = "the-cool-bucket"
tiedostonimi = "cool.png"
blobnimi = "cool"
person_id = 4
gs_url = f"http://{amparinimi}.storage.googleapis.com/{blobnimi}"

#metadatan lisäyksen testausta
#lisaa_metadata(amparinimi,blobnimi,person_id,gs_url)

#koko lautausfunktion testausta
#lataa_tiedosto_storageen(amparinimi,tiedostonimi,blobnimi,person_id)

#haun testausta
#hae_storageurl(blobnimi)