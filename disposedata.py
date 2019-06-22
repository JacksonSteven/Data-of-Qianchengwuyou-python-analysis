import pandas as pd

import re
df = pd.read_csv('data.csv')
df['money'].dropna(inplace=True)
# pd.set_option('display.max_columns', None)
# print(df.head())
df_money = df['money']
print(df_money.head(60))
# a = '0.8-1万/月'
# a = '25-50万/年'
# a = '300/天'
def js(a):
    if '年' in a:
        if '以上' in a or '以下' in a:
            money_sl = re.search('^(.*?)万', a).groups(1)[0]
            fgh_xg = str(int(float(money_sl) * 10000 / 12)) + '/月'
            return fgh_xg
        else:
            money_sl = re.search('^(.*?)万', a).groups(1)[0]
            fgh = money_sl.split('-')
            fgh_xg = str(int(float(fgh[0]) * 10000/12)) + '-' + str(int(float(fgh[1]) * 10000/12))+'/月'
            return fgh_xg
    elif '月' in a:
        if '以下' in a:
            money_sl = re.search('^(.*?)千', a).groups(1)[0]
            fgh_xg = str(int(float(money_sl) * 1000)) + '/月'
            return fgh_xg
        elif '以上' in a:
            money_sl = re.search('^(.*?)万', a).groups(1)[0]
            fgh_xg = str(int(float(money_sl) * 10000)) + '/月'
            return fgh_xg
        elif '千' in a:
            money_sl = re.search('^(.*?)千', a).groups(1)[0]
            fgh = money_sl.split('-')
            fgh_xg = str(int(float(fgh[0]) * 1000)) + '-' + str(int(float(fgh[1]) * 1000)) + '/月'
            return fgh_xg
        else:
            money_sl = re.search('^(.*?)万', a).groups(1)[0]
            fgh = money_sl.split('-')
            fgh_xg = str(int(float(fgh[0]) * 10000)) + '-' + str(int(float(fgh[1]) * 10000)) + '/月'
            return fgh_xg
    elif '天' in a:
        money_sl = re.search('^(.*?)[元/天]', a).groups(1)[0]
        print(money_sl)
        fgh_xg = str(int(money_sl)*30) + '/月'
        return fgh_xg
df1 = pd.read_csv('data_2.csv')
print(df1['money'].head(50))
df_money_xgh = df_money.apply(js)
df['money'] = df_money_xgh
df.to_csv('data_2.csv')
