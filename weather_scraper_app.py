from requests_html import HTMLSession

import dbutil
00


def get_weather(html_session, city):
    url = f'https://www.google.com/search?q=weather+{city}'
    r = html_session.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'})
    temp = r.html.find('span#wob_tm', first=True).text
    unit = r.html.find('div.vk_bk.wob-unit span.wob_t', first=True).text
    desc = r.html.find('div.VQF4g', first=True).find('span#wob_dc', first=True).text
    return f'{temp} {unit} {desc}'


if __name__ == '__main__':
    con = None
    session = None
    try:
        user = 'chandu'
        passwd = 'student_123'
        host = '127.0.0.1'
        database = 'weather'
        city_list_query = 'select country_id,state_id,capital from wa_states'
        daily_weather_batch_query = 'INSERT INTO wa_daily_weather_data (country_id, state_id,celsius) ' \
                              'VALUES (%s, %s , %s)'
        con = dbutil.get_connection(user, passwd, host,"mysql", database)
        state_records=dbutil.execute_select_query(con, city_list_query)
        session = HTMLSession()
        batch_data = list()

        for state_record in state_records:
            celsius = get_weather(session,state_record['capital'])
            print("current city : "+state_record['capital']+" temp :"+celsius)
            batch_data.append((state_record['country_id'],state_record['state_id'],celsius))

        dbutil.batch_update_query(con,daily_weather_batch_query,batch_data)
        print("batch data loaded....!")

    except Exception as e:
        print("failed in main")
        print(e)
    finally:
        dbutil.close_connection(con)
        session.close()


