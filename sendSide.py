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
import boto3
from io import StringIO

#### 전역변수 설정
# teleurl 변수에 텔레그램 botfather 한테 받은 자신의 API 넣기
getCryptoRankerSideTelbotToken=os.environ['getCryptoRankerSideTelbotToken']
teleurl = "https://api.telegram.org/"+getCryptoRankerSideTelbotToken+"/sendMessage"

s3_client = boto3.client('s3')
bucket = 'getcryptorankerside'
s3_recent_csv = 'df_recent_tmp.csv'

# Deploy ID
SubscriptID=['1785347587','1749298610','817356141']

# Test ID 
#SubscriptID=['1785347587']


def saveSide(df_api):
    # ---------
    # [TO-DO]
    # s3에서 df_recent_tmp 파일 읽어오기
    
    s3_data = s3_client.get_object(Bucket=bucket, Key=s3_recent_csv)
    df_side = pd.read_csv(s3_data['Body'])
    # ---------
    
    # df_api의 timestamp가
    timestamp_now=df_api['timestamp'][-1:].values[0] 
    timestamp_sideRaw_last=df_side['timestamp'][-1:].values[0]
    
    print (' now : ',df_api['timestamp'][0],'\n sideRaw_last :',timestamp_sideRaw_last)
    
    # recent의 가장 마지막 timestamp와 다르면
    if timestamp_now!=timestamp_sideRaw_last:
        
        # 데이터가 달라진게 있는지 체크하고 변경된 데이터들을 반환
        df_changedData, df_changedSide = compareRawData(df_side, df_api)
        
        # 만약 바뀐 데이터가 있으면 (changedData 길이가 1이상)
        if len(df_changedData)>0:
            # 바뀐 데이터가 있을 경우에만 DB 연결하여 AWS RDS 비용 줄이기---------
            awsRdsDbId=os.environ['dbId']
            awsRdsPswd=os.environ['dbPswd']
            awsRdsUrl=os.environ['dbUrl']
            engine = create_engine("mysql+pymysql://"+awsRdsDbId+":"+awsRdsPswd+"@"+awsRdsUrl, encoding='utf-8')
            conn = engine.connect() 
            
            
            # side_raw 테이블에 저장
            df_changedData.index += len(pd.read_sql_table('side_raw',conn)) # primary key인 index를 설정하기 위해 side_raw테이블의 lenth를 sum
            
            df_changedData.to_sql(name='side_raw',con=engine,if_exists='append')
            print('변경된 데이터 side_raw 테이블 저장완료')
            
            sendMessage('데이터 변경됨')
            
            # side가 변경된 랭커가 있으면 별도로 텔레그램 전송 및 side_summary에 저장
            if len(df_changedSide)>0:
                # 포지션 달라진 애들 telegram 전송용 STRING 생성
                str_changedSide='{: <20}'.format('NAME'+'\t\t\t'+'SIDE')+'\n'
                for index, i in df_changedSide.reset_index(drop=True).iterrows():
                    str_changedSide=str_changedSide+'\n'+'{: <20}'.format(i['name'])+"\t"+i['side']
                # 변경된 side 요약해서 텔레그램 발송
                sendMessage(str_changedSide)
            
            
            # df_api의 timestamp가 
            df_summary=pd.read_sql_table('side_summary',conn)
            timestamp_summary_last=df_summary['timestamp'][-1:].values[0]
            
            # 가장 최근의 summary 테이블 timestamp와 다르면 summary 테이블에 신규데이터 저장
            if timestamp_now!=timestamp_summary_last:
                # value_counts 저장위한 임시 df 생성
                df_tmp = pd.DataFrame([df_api['side'].value_counts().values],
                                   columns=df_api['side'].value_counts().keys())
                df_tmp['timestamp']=timestamp_now
            
                df_tmp.rename(columns={'-':'Neutral'},inplace=True)
                df_tmp.index+=len(df_summary) # primary key 인 index 중복 방지
                df_tmp.to_sql(name='side_summary',con=engine,if_exists='append')
                print('변경된 summary 테이블 저장완료')
                
            else: print('summary 테이블 시간이 같다')
            
        else : print('변경된 데이터가 없다.')
        
    else: print('side_raw 테이블 시간이 같다')
    
    

def compareRawData(df_raw, df_api):
    ## 1. raw에 있고 api에도 있는애들
    ##  -> 완료
    ## 2. raw에 있는데 api에는 없는 애들 (랭커탈락)
    ##  -> 완료
    ## 3. raw에 없는데 api에는 있는 애들 (신규랭커)
    ##  -> 완료


    # df_last => side_raw 테이블의 name 별 가장 마지막 시간에 받아온 data
    df_recent=df_raw.drop_duplicates(['name'],keep='last').drop(['Unnamed: 0'],axis=1).reset_index(drop=True)

    # hasOneData => 한때 랭커였다가 지금 api에는 없는 애들의 side_raw 마지막 데이터 + 새로 랭커 된 애들의 api 데이터
    hasOneData=pd.concat([df_recent,df_api]).drop_duplicates(['name'],keep=False)
    
    # df_recent_tmp => side_raw에 있고, api에도 있는 랭커의 side_raw데이터 + side_raw테이블에 없고 api에 있는 신규랭커 api데이터
    df_recent_tmp=pd.concat([df_recent, hasOneData]).drop_duplicates(['name'],keep=False).reset_index(drop=True)
    
    # ---------
    # [TO-DO]
    # df_recent_tmp 를 s3에 저장하는 코드를 구현하여 db에 매번 접속 안해도 되게끔 하기 
    s3 = boto3.resource('s3')
    csv_buffer = StringIO()
    df_recent_tmp.to_csv(csv_buffer)
    s3.Object(bucket, s3_recent_csv).put(Body=csv_buffer.getvalue())
    # ---------


    # 데이터 비교를 위해 어차피 다른 timestamp는 drop
    df_recent_withoutTimestamp=df_recent_tmp.drop('timestamp', axis=1)
    df_api_withoutTimestamp=df_api.drop('timestamp', axis=1)


    # 데이터 비교 후 다른게 하나라도 있으면 해당 행 concat
    df_changedData=pd.DataFrame()

    # sideDiffList = side가 달라진애들 따로 모아서 telegram 전송할거임
    df_changedSide=pd.DataFrame()

    for i in range(len(df_api)):
        # df_api_tmp => df_recent의 i번째 행의 name과 같은 df_api 데이터를 비교를 위해 선언
        df_api_tmp = df_api[ df_api['name']==df_recent_withoutTimestamp.loc[i,'name'] ].drop('timestamp', axis=1)
        isSameData = df_recent_tmp.drop('timestamp', axis=1).iloc[i]==df_api_tmp # series bool
        
        if isSameData.values.sum()!=5:
            df_changedData=pd.concat([df_changedData,df_api[df_api['name']==df_recent_withoutTimestamp.loc[i,'name']]])

            # side 달라진 랭커 메시지 보내주기 위해 side만 체크 후 달라졌으면 concat
            if isSameData['side'].values[0]==False:
                df_changedSide=pd.concat([df_changedSide,df_api[df_api['name']==df_recent_withoutTimestamp.loc[i,'name']]])

    # 신규인애들도 concat
    newRankerData=hasOneData[hasOneData['timestamp']==df_api['timestamp'][0]]

    df_changedData=pd.concat([df_changedData,newRankerData])
    df_changedSide = pd.concat([df_changedSide,newRankerData])
    
    return df_changedData.reset_index(drop=True), df_changedSide


def sendMessage(msg):
    for chatid in SubscriptID:
        # 챗 id 와 값을 텔레그램에 보내기
        params = {'chat_id':chatid
                  ,'text': msg}
        res = requests.get(teleurl, params=params)
    
    
    
# 시간별로 반복될 작업을 함수로 정의
def scd():
    r = requests.get('https://api.btctools.io/api/leaderboard')
    html = r.text
    tmp = json.loads(html)
    df_rankerSide = pd.DataFrame.from_dict(tmp['data'])
    summary = str(datetime.datetime.fromtimestamp(int(df_rankerSide['timestamp'][0])))+'\n'+str(df_rankerSide['side'].value_counts())+'\n aoa:'+str(df_rankerSide[df_rankerSide['name']=='aoa']['side'].values[0])
    
    df_rankerSide['profit']=df_rankerSide['profit'].astype(float)
    saveSide(df_rankerSide)
    sendMessage(summary)



def lambda_handler(event, context):
    scd()
    

    
    print('event: ',event)
    print('context: ',context)

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
