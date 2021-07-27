import telepot
from telepot.loop import MessageLoop

import os
import urllib.request
import requests
import datetime
import bs4
import pandas as pd
from sqlalchemy import create_engine
import json

def saveSide(df_arg):
    awsRdsDbId=os.environ['awsRdsDbId']
    awsRdsPswd=os.environ['awsRdsPswd']
    awsRdsUrl=os.environ['awsRdsUrl']
    engine = create_engine("mysql+pymysql://"+awsRdsDbId+":"+awsRdsPswd+"@"+awsRdsUrl, encoding='utf-8')
    conn = engine.connect()
 
    # df_arg의 timestamp가 
    df_side=pd.read_sql_table('side_raw',conn)
    timestamp_now=df_arg['timestamp'][-1:].values[0]
    timestamp_side_last=df_side['timestamp'][-1:].values[0]
    print (' now : ',df_arg['timestamp'][0],'\n side_last :',timestamp_side_last)
    
    # 가장 최근의 side 테이블 timestamp와 다르면 side 테이블에 신규데이터 저장
    
    if timestamp_now!=timestamp_side_last:
        df_arg.index+=len(df_side) # primary key 인 index 중복 방지
        df_arg.to_sql(name='side_raw',con=engine,if_exists='append')
        print('side_raw 테이블 저장완료')
    else: print('side_raw 테이블 시간이 같다')    
       
        
    # df_arg의 timestamp가 
    df_summary=pd.read_sql_table('side_summary',conn)
    timestamp_summary_last=df_summary['timestamp'][-1:].values[0]
    
    # 가장 최근의 summary 테이블 timestamp와 다르면 summary 테이블에 신규데이터 저장
    if timestamp_now!=timestamp_summary_last:
        
        # value_counts 저장위한 임시 df 생성
        df_tmp = pd.DataFrame([df_arg['side'].value_counts().values],
                           columns=df_arg['side'].value_counts().keys())
        df_tmp['timestamp']=timestamp_now
        
        df_tmp.rename(columns={'-':'Neutral'},inplace=True)
        df_tmp.index+=len(df_summary) # primary key 인 index 중복 방지
        df_tmp.to_sql(name='side_summary',con=engine,if_exists='append')
        print('summary 테이블 저장완료')
        
    else: print('summary 테이블 시간이 같다')
        
# 반복될 작업을 함수로 정의
def scd():
    r = requests.get('https://api.btctools.io/api/leaderboard')
    html = r.text
    tmp = json.loads(html)
    df_rankerSide = pd.DataFrame.from_dict(tmp['data'])

    # teleurl 변수에 텔레그램 botfather 한테 받은 자신의 API 넣기
    getCryptoRankerSideTelbotToken=os.environ['getCryptoRankerSideTelbotToken']
    teleurl = "https://api.telegram.org/"+getCryptoRankerSideTelbotToken+"/sendMessage"


    SubscriptID=['1785347587','1749298610']
    
    for chatid in SubscriptID:
        
        # 챗 id 와 값을 텔레그램에 보내기
        params = {'chat_id':chatid
                  ,'text': str(datetime.datetime.fromtimestamp(int(df_rankerSide['timestamp'][0])))+'\n'
                  +str(df_rankerSide['side'].value_counts())+'\n aoa:'+str(df_rankerSide[df_rankerSide['name']=='aoa']['side'].values[0])} 
    
    
        # 텔레그램으로 메시지 전송
        res = requests.get(teleurl, params=params)
    saveSide(df_rankerSide)

    
def lambda_handler(event, context):
    scd()
    print('event: ',event)
    print('context: ',context)
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
