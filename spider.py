import requests
from lxml import etree
import time
import csv
import random
import threading
from threading import settrace


headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    # 'Cookie': 'guid=295723130c4f1c2174ceb32d0b8ca44b; nsearch=jobarea%3D%26%7C%26ord_field%3D%26%7C%26recentSearch0%3D%26%7C%26recentSearch1%3D%26%7C%26recentSearch2%3D%26%7C%26recentSearch3%3D%26%7C%26recentSearch4%3D%26%7C%26collapse_expansion%3D; partner=51jobhtml5; m_search=keyword%3Dpython%26%7C%26areacode%3D000000; adv=adsnew%3D0%26%7C%26adsresume%3D1%26%7C%26adsfrom%3Dhttps%253A%252F%252Fsp0.baidu.com%252F9q9JcDHa2gU2pMbgoY3K%252Fadrc.php%253Ft%253D06KL00c00fDewkY0gPN900uiAsjuqPqT00000c6R7dC00000Ta_oZT.THLZ_Q5n1VeHksK85yF9pywd0ZnquHwBn1fvPyfsnjDYnHnkr0Kd5HnvrHm1fRnvwj7anYDkrDcYfW9DnWn1nDDLrj0YwWIa0ADqI1YhUyPGujY1nWDdrHbkrj61FMKzUvwGujYkP6K-5y9YIZK1rBtEIZF9mvR8PH7JUvc8mvqVQLwzmyP-QMKCTjq9uZP8IyYqnW0sPjc3nBu9pM0qmR9inAPcHHunXH-YmHPwIR4RwM7Bnb-dyHc4IDs1Rh4nnY4_m-n4IvN_rZ-PwDRYHAdCnAFgIzqpUbGvm-fkpN-gUAVbyDcvFh_qn1u-njRYmyDsPjR4m1F-uAfdmWfLuWRLmvcvnHKbrjc0mLFW5HnLrHTY%2526tpl%253Dtpl_11534_19347_15370%2526l%253D1511462024%2526attach%253Dlocation%25253D%252526linkName%25253D%252525E6%252525A0%25252587%252525E5%25252587%25252586%252525E5%252525A4%252525B4%252525E9%25252583%252525A8-%252525E6%252525A0%25252587%252525E9%252525A2%25252598-%252525E4%252525B8%252525BB%252525E6%252525A0%25252587%252525E9%252525A2%25252598%252526linkText%25253D%252525E3%25252580%25252590%252525E5%25252589%2525258D%252525E7%252525A8%2525258B%252525E6%25252597%252525A0%252525E5%252525BF%252525A751Job%252525E3%25252580%25252591-%25252520%252525E5%252525A5%252525BD%252525E5%252525B7%252525A5%252525E4%252525BD%2525259C%252525E5%252525B0%252525BD%252525E5%2525259C%252525A8%252525E5%25252589%2525258D%252525E7%252525A8%2525258B%252525E6%25252597%252525A0%252525E5%252525BF%252525A7%2521%252526xp%25253Did%2528%25252522m3215991883_canvas%25252522%2529%2525252FDIV%2525255B1%2525255D%2525252FDIV%2525255B1%2525255D%2525252FDIV%2525255B1%2525255D%2525252FDIV%2525255B1%2525255D%2525252FDIV%2525255B1%2525255D%2525252FH2%2525255B1%2525255D%2525252FA%2525255B1%2525255D%252526linkType%25253D%252526checksum%25253D23%2526ie%253DUTF-8%2526f%253D8%2526tn%253Dbaidu%2526wd%253D%2525E5%252589%25258D%2525E7%2525A8%25258B%2525E6%252597%2525A0%2525E5%2525BF%2525A7%2526rqlang%253Dcn%26%7C%26adsnum%3D2004282; search=jobarea%7E%60000000%7C%21ord_field%7E%600%7C%21recentSearch0%7E%601%A1%FB%A1%FA000000%2C00%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FApython%A1%FB%A1%FA2%A1%FB%A1%FA%A1%FB%A1%FA-1%A1%FB%A1%FA1558181105%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA%7C%21recentSearch1%7E%601%A1%FB%A1%FA000000%2C00%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FApython%A1%FB%A1%FA2%A1%FB%A1%FA%A1%FB%A1%FA-1%A1%FB%A1%FA1558181561%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA%D6%DC%C4%A9%CB%AB%D0%DD%7C%21',
    'Host': 'search.51job.com',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36',
}

Lock = threading.Semaphore(5)


class Thread(threading.Thread):
    def __init__(self, url, data_list):
        threading.Thread.__init__(self)
        self.url = url
        self.data_list = data_list

    def run(self) -> None:
        Lock.acquire()
        data = self.parse_url()
        if data:
            self.data_list.append(data)
        print(data)
        Lock.release()

    def parse_url(self):
        # 声明字典存储信息
        data_dict = {}
        try:
            resp = requests.get(self.url, headers=headers)
        except Exception as e:
            return None
        doc = etree.HTML(resp.content)
        div = doc.xpath('//div[@class="tHeader tHjob"]')
        if div:
            div = div[0]
            # 职位
            title = div.xpath('//h1/text()')
            if title:
                title = title[0].strip()
            else:
                title = None
            # 公司名字
            company_name = div.xpath('//p[@class="cname"]/a/text()')
            if company_name:
                company_name = company_name[0].strip()
            else:
                company_name = None
            # 薪资
            money = div.xpath('//div[@class="cn"]/strong/text()')
            if money:
                money = money[0].strip()
            else:
                money = None

            temp = div.xpath('//p[@class="msg ltype"]/@title')[0].strip().split('|')
            # 工作地点
            work_position = temp[0].strip()
            # 工作经验
            work_experience = temp[1].strip()
            # 学历
            education = temp[2].strip()
            # 招几人
            number = temp[3].strip()
            # 时间
            date_time = temp[-1].strip()
            # 详细信息
            info = div.xpath('//div[@class="bmsg job_msg inbox"]//text()')
            info = [i.strip() for i in info]
            info = ''.join(info).replace('微信分享', '').replace('\xa0', '').replace('\t', '')
            # 公司地点
            company_position = div.xpath('//div[@class="bmsg inbox"]/p[@class="fp"]/text()')
            if company_position:
                company_position = company_position[-1].strip()
            else:
                company_position = None

            data_dict['title'] = title
            data_dict['company_name'] = company_name
            data_dict['money'] = money
            data_dict['work_position'] = work_position
            data_dict['work_experience'] = work_experience
            data_dict['education'] = education
            data_dict['number'] = number
            data_dict['date_time'] = date_time
            data_dict['info'] = info
            data_dict['company_position'] = company_position
            data_dict['url'] = self.url

            return data_dict
        else:
            return None


def get_index(page):
    url = 'https://search.51job.com/list/000000,000000,0000,00,9,99,python,2,{}.html?lang=c&stype=&postchannel=0000' \
          '&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&providesalary=99&lonlat=0%2C0&radius=-1' \
          '&ord_field=0&confirmdate=9&fromType=&dibiaoid=0&address=&line=&specialarea=00&from=&welfare='.format(page)

    resp = requests.get(url, headers=headers)
    doc = etree.HTML(resp.content)
    resultList = doc.xpath('//div[@id="resultList"]/div[@class="el"]')
    for result in resultList:
        temp_url = result.xpath('.//p/span/a/@href')[0]
        yield temp_url


def main():
    page = 720

    while page <= 740:
        data_list = []
        ths = []
        for temp_url in get_index(page):
            th = Thread(temp_url, data_list)
            ths.append(th)

        for t in ths:
            t.start()
            # time.sleep(random.uniform(0.1, 0.5))
        for t in ths:
            t.join()

        if data_list:
            with open('data.csv', 'a+', encoding='utf-8', newline='') as f:
                title = data_list[0].keys()
                writer = csv.DictWriter(f, title)
                writer.writerows(data_list)
            print('第%d页写入完成++++++++++++++++++++++++' %page)
        page += 1
        time.sleep(1)


if __name__ == '__main__':
    main()