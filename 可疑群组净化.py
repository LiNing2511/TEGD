import sqlite3
import datetime


class Purity:
    def __init__(self):
        self.dataSet = 'Amazon'
        if self.dataSet == 'Amazon':
            self.con = sqlite3.connect('E:\\DB\\Books_data13.db')
        else:
            self.con = sqlite3.connect('F:\\DB\\YelpZip.db')
        self.cursor = self.con.cursor()
        self.dict_group = {}
        self.list_EXR = []
        self.list_MNR = []
        self.list_AD = []
        self.list_ATR = []
        self.list_RD = []
        self.list_AVG = []

    def EXR(self, set_for_list):
        # EXR
        for i in range(len(set_for_list)):
            if self.dataSet == 'Amazon':
                self.cursor.execute('select overall from review13 where reviewerID=\"' + set_for_list[i] + '\"')
            else:
                self.cursor.execute("select rating from reviewGraph  where user_id=%s" % (set_for_list[i]))
            rows = self.cursor.fetchall()
            sum = len(rows)
            cou = 0
            cc = 0
            for r in rows:
                cc += 1
                if r[0] == 1 or r[0] == 5:
                    cou += 1
            sum = cc
            # print(sum)
            ratio = cou / sum
            self.list_EXR.append(round(ratio, 4))
        return self.list_EXR

    def MNR(self, set_for_list):
        # MNR
        for i in range(len(set_for_list)):
            if self.dataSet == 'Amazon':
                self.cursor.execute('select count(*) from review13 where reviewerID=\"' + set_for_list[i] + '\" group by unixReviewTime')
            else:
                self.cursor.execute("select count(*) from metadata where user_id=%s group by date " % (set_for_list[i]))
            rows = self.cursor.fetchall()
            # print(rows)
            num = len(rows)
            nor = []
            # 计算平均一天评论数
            sum = 0
            for r in rows:
                sum += r[0]
                nor.append(r[0])
            avgR = num / sum
            self.list_MNR.append(round(avgR, 4))
        return self.list_MNR

    def RD(self, set_for_list):
        # RD
        for i in range(len(set_for_list)):
            if self.dataSet == 'Amazon':
                self.cursor.execute('select overall,asin from review13 where reviewerID=\"' + set_for_list[i] + '\"')
            else:
                self.cursor.execute("select rating,prod_id from reviewGraph where user_id= %s" %(set_for_list[i]))
            rows = self.cursor.fetchall()
            num = len(rows)
            a = 0
            div = 0
            for r in rows:
                rat = r[0]
                asin = r[1]
                # 计算产品平均评分
                if self.dataSet == "Amazon":
                    self.cursor.execute('select avg(overall) from review13 where asin=\"' + asin + '\"')
                else:
                    self.cursor.execute("select avg(rating) from reviewGraph where prod_id=%s"  %(asin))
                lines = self.cursor.fetchall()
                avgp = lines[0][0]
                # print('平均评分:' + str(avgp))
                divsum = abs((rat - avgp))
                if divsum >= 0.5:
                    a += 1
                div = a / num
            self.list_RD.append(round(div, 4))
        return self.list_RD

    def ATR(self, set_for_list):
        # ATR
        for i in range(len(set_for_list)):
            cursor = self.con.cursor()
            if self.dataSet == "Amazon":
                cursor.execute('select count(*),count(distinct asin) from review13 where reviewerID=\"' + set_for_list[i] + '\"')
            else:
                cursor.execute('select count(*),count(distinct prod_id) from metadata where user_id=%s' % set_for_list[i])
            rows = cursor.fetchall()
            # print(rows[0][0], rows[0][1])
            rg = len(set_for_list)
            vg = rows[0][0]
            pg = rows[0][1]
            grt = vg / (rg + pg)
            self.list_ATR.append(round(grt, 4))
        return self.list_ATR

    def AD(self, set_for_list):
        for i in range(len(set_for_list)):
            if self.dataSet == "Amazon":
                unixtime = []
                cursor_unixTime = self.con.cursor()
                cursor_unixTime.execute('select unixReviewTime from review13 where reviewerID=\'' + set_for_list[i] + '\'order by unixReviewTime desc')
                review_time = cursor_unixTime.fetchall()
                for i in range(len(review_time)):
                    unixtime.append(int(review_time[i][0]))
                ad = 1 - (max(unixtime) - min(unixtime)) / (864000 * 365)
                self.list_AD.append(round(ad, 4))
            else:
                date_min = '2005-02-16'
                date_max = '2015-01-10'
                date_min = datetime.datetime.strptime(date_min, "%Y-%m-%d")
                date_max = datetime.datetime.strptime(date_max, "%Y-%m-%d")
                cursor_rating = self.con.cursor()
                cursor_rating.execute(
                    "select user_id,date from metadata where  user_id=%s order by date desc" % (set_for_list[i]))
                rating_every_1 = cursor_rating.fetchall()
                userid_date_min = rating_every_1[-1][1]
                userid_date_min = datetime.datetime.strptime(userid_date_min, "%Y-%m-%d")
                userid_date_max = rating_every_1[0][1]
                userid_date_max = datetime.datetime.strptime(userid_date_max, "%Y-%m-%d")
                ad = (1 - (userid_date_max - userid_date_min).days / (date_max - date_min).days)
                self.list_AD.append(round(ad, 2))
        return self.list_AD

    def purity(slef, list_AVG, set_for_list, dict_group, fw):
        # fw = open(f_w, 'w')
        index_to_delete = []
        purity_group = []
        # 记录需要净化的dict_all_group中的元素的index
        for i in range(0, len(list_AVG)):
            if list_AVG[i] > 3.0:
                index_to_delete.append(i)
        print("set_for_list len =", len(set_for_list))
        print("index_to_delete = ", index_to_delete)
        # 将需要净化的元素按照上一步记录的index删除掉
        for index in index_to_delete:
            purity_group.append(set_for_list[index])
        dict_group['group'] = purity_group
        print("净化后dict_all_group" + str(dict_group))
        if len(purity_group) != 0:
            fw.write(str(dict_group) + "\n")
            fw.flush()
        else:
            print("dict_all_group全部净化掉，不写入文件")

    # 群组净化+打标签#
    # 还没写完//2022/4/7

    def read_group(self, fr, fw):
        count_num_line = 0
        while 1:
            count_num_line += 1
            print(f"第{count_num_line}次循环")
            lines = fr.readline()
            lines = lines.strip("\n")
            try:
                self.dict_group = eval(lines)
            except:
                print("ending")
            set_for_list = list(self.dict_group['group'])
            list_RD = self.RD(set_for_list)
            list_EXR = self.EXR(set_for_list)
            list_ATR = self.ATR(set_for_list)
            list_AD = self.AD(set_for_list)
            list_MNR = self.MNR(set_for_list)
            for i in range(len(set_for_list)):
                self.list_AVG.append(round((list_RD[i] + list_EXR[i] + list_ATR[i] + list_AD[i] + list_MNR[i]), 4))
            print(self.list_AVG)
            print("list_AVG len = ", len(self.list_AVG))
            self.purity(self.list_AVG, set_for_list, self.dict_group, fw)
            # self.dict_group = {}
            self.list_EXR.clear()
            self.list_MNR.clear()
            self.list_AD.clear()
            self.list_ATR.clear()
            self.list_RD.clear()
            self.list_AVG.clear()
            if not lines:
                break
            for line in lines:
                pass


if __name__ == '__main__':
    p = Purity()
    p.dataSet = 'Amazon'
    fr_Amazon = open('./output/burstSession/Amazon/Amazon_candidate_group_12.txt', 'r')
    fw_Amazon = open('./output/dynamicRuleMemory/Amazon/Amazon_candidate_group_净化.txt', 'w')
    p.read_group(fr_Amazon, fw_Amazon)
    # f_r = './output/burstSession/Amazon/Amazon_candidate_group_12.txt'
    # f_w = './output/dynamicRuleMemory/Amazon/Amazon_candidate_group_净化.txt'

