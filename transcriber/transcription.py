import boto3
from dotenv import load_dotenv
import os
import re
import producer
import traceback
import requests
import uuid
import time

load_dotenv()

salutate_speech_api_key = os.getenv("SALUTATE_SPEECH_API_KEY")


def get_token(auth_token, scope='SALUTE_SPEECH_PERS'):
    """
      Выполняет POST-запрос к эндпоинту, который выдает токен.

      Параметры:
      - auth_token (str): токен авторизации, необходимый для запроса.
      - область (str): область действия запроса API. По умолчанию — «SALUTE_SPEECH_PERS».

      Возвращает:
      - ответ API, где токен и срок его "годности".
      """
    # Создадим идентификатор UUID (36 знаков)
    rq_uid = str(uuid.uuid4())

    # API URL
    url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"

    # Заголовки
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'RqUID': rq_uid,
        'Authorization': f'Basic {auth_token}'
    }

    # Тело запроса
    payload = {
        'scope': scope
    }

    try:
        # Делаем POST запрос с отключенной SSL верификацией
        # (можно скачать сертификаты Минцифры, тогда отключать проверку не надо)
        response = requests.post(url, headers=headers, data=payload, verify=False)
        return response
    except requests.RequestException as e:
        print(f"Ошибка: {str(e)}")
        return None

def transcribe_audio(audio_data: bytes):
    access_token = get_token(salutate_speech_api_key).json()['access_token']
    # URL для распознавания речи
    upload_url = "https://smartspeech.sber.ru/rest/v1/data:upload"
    async_recognize_url = "https://smartspeech.sber.ru/rest/v1/speech:async_recognize"
    status_url = "https://smartspeech.sber.ru/rest/v1/task:get?id="
    download_url = "https://smartspeech.sber.ru/rest/v1/data:download?response_file_id="


    # Заголовки запроса
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "audio/ogg",
    }

    # Отправка POST запроса
    response = requests.post(upload_url, headers=headers, data=audio_data, verify=False)

    # Обработка ответа
    if response.status_code == 200:
        result = response.json()
        print("Весь ответ API Загрузки файла:", result)
        request_file_id = result["result"]["request_file_id"]
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        body = {
            "options": {
                "model": "general",
                "audio_encoding": "OPUS",
                "sample_rate": 16000,
                "language": "ru-RU",
                "no_speech_timeout": 1000,
                "channels_count": 1
            },
            "request_file_id": request_file_id
        }
        # Отправка POST запроса
        response = requests.post(async_recognize_url, headers=headers, json=body, verify=False)
        print("Отправили запрос на ассинхронную обработку", response)
        if response.status_code == 200:
            task_id = response.json()['result']['id']

            countdown = 20
            response_file_id = ""
            while True:
                # Отправка POST запроса
                response = requests.get(status_url + task_id, headers=headers, verify=False)
                countdown -= 1
                if response.status_code == 200:
                    is_done = response.json()['result']['status']
                    print(f"Статус={is_done}")
                    if is_done == 'DONE' or countdown < 0:
                        response_file_id = response.json()['result']['response_file_id']
                        break
                    else:
                        time.sleep(5)
            if countdown > 0:
                # Отправка POST запроса
                response = requests.get(download_url + response_file_id, headers=headers, verify=False)
                if response.status_code == 200:
                    results = response.json()[0]['results']
                    chunks = []
                    for result in results:
                        chunks.append({
                            'timestamp': (float(result['start'][:-1]), float(result['end'][:-1])),
                            'text': result['text']
                        })
                    print('chunks', chunks)
                    return chunks
            else:
                print("Ошибка слишком долго ждали")
    else:
        result = response.json()
        print("Ошибка. Весь ответ API Загрузки файла:", result)

def transcribe_audio_from_s3(bucket: str, key: str):
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_URL'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        audio_data = response['Body'].read()
        return transcribe_audio(audio_data)
    except Exception as e:
        raise Exception(f"Error fetching file from S3: {str(e)}")

def transcribe_audio_from_s3_folder(bucket: str, folder: str):
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_URL'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
        files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].startswith(f"{folder}/room")]

        transcriptions = []
        for file in files:
            print(file)
            match = re.match(r'room\.(.+?)\.(.+?)\.(\d+)\.ogg', file.split("/")[1])
            if match:
                uuid, username, timestamp = match.groups()
                timestamp = int(timestamp)
                transcription = transcribe_audio_from_s3(bucket, file)
                print("transcription", transcription)
                for chunk in transcription:
                    transcriptions.append((timestamp + chunk['timestamp'][0]*1000, username, chunk['text']))

        transcriptions.sort(key=lambda x: x[0])
        final_transcription = "\n".join([f"[{username}]:{text}" for _, username, text in transcriptions])

        producer.produce(f'{bucket}:{folder}', final_transcription)

        return final_transcription
    except Exception as e:
        print(traceback.format_exc())
        raise Exception(f"Error processing files from S3: {str(e)}")
