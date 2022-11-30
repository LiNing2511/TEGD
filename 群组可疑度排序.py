import datetime
import sqlite3
from math import *
import numpy as np
import os


# 最后的虚假群组


class GroupScore:
    def __init__(self):
        self.dataSet = 'Amazon'
        if self.dataSet == 'Amazon':
            self.con = sqlite3.connect('E:\\DB\\Books_data13.db')
        else:
            self.con = sqlite3.connect('F:\\DB\\YelpZip.db')
        self.cursor = self.con.cursor()
        self.community_GER = []
        self.community_RD = []
        self.community_gs = []
        self.community_GCAR = []
        self.community_AD = []
        self.community_GOR = []
        self.community_GRT = []
        self.list_AVG = []
        self.list_AVG_all = []
        self.group_all = []

    def find_str(self, str):  # 求时间片内某个值出现的次数
        counts = 0
        file = open(fr, 'r', encoding='utf-8')
        while 1:
            l = dict()
            lines = file.readline()
            lines = lines.strip("\n")
            if len(lines) > 2:
                try:
                    l = eval(lines)
                except Exception as ex:
                    print("表达式为空")
            for key in l.keys():
                time = l[key].count(str)
                counts += time
            # print("%s出现的次数：%d" %(str,counts))
            return counts

    def GER(self, set_for_list):
        community_rating_1_5 = 0
        community_rating_all = 0
        for i in range(len(set_for_list)):
            # for循环结束，某个社区结束
            cursor_rating = self.con.cursor()
            if self.dataSet == 'Amazon':
                cursor_rating.execute(
                    'select overall from review13 where overall in (1,5) and reviewerID= \'' + set_for_list[i] + '\'')
            else:
                cursor_rating.execute(
                    "select rating from reviewGraph where rating in (1,5) and user_id=%s" % (set_for_list[i]))
            rating_1_5 = cursor_rating.fetchall()
            community_rating_1_5 += len(rating_1_5)  # 这里是一个群组中的评分在1和5的所有数目（累加）
            cursor_rating_all = self.con.cursor()
            if self.dataSet == 'Amazon':
                cursor_rating_all.execute('select overall from review13 where  reviewerID= \'' + set_for_list[i] + '\'')
            else:
                cursor_rating_all.execute("select rating from reviewGraph where user_id=%s" % (set_for_list[i]))
            rating_all = cursor_rating_all.fetchall()
            community_rating_all += len(rating_all)
        if round((community_rating_1_5 / community_rating_all), 4) > 1:
            m_end = 1
        else:
            m_end = round((community_rating_1_5 / community_rating_all), 4)
        self.community_GER.append(m_end)
        return self.community_GER

    def GRD(self, set_for_list):
        n = y = x = community_rating_all = 0
        # 所有的评分community_rating_every
        # 平均分community_rating_avg
        l_set = list(set(set_for_list))
        for i in range(len(l_set)):  # for循环结束，某个社区结束
            cursor_rating = self.con.cursor()
            if self.dataSet == 'Amazon':
                cursor_rating.execute('select overall from review13 where  reviewerID= \'' + l_set[i] + '\'')
            else:
                cursor_rating.execute("select rating from reviewGraph where  user_id=%s" % (l_set[i]))
            rating_every_1 = cursor_rating.fetchone()
            # print(rating_every[0])
            community_rating_all += rating_every_1[0]  # 这里是一个群组中的评分在1和5的所有数目（累加）
            x += 1
        community_rating_avg = community_rating_all / x
        for i in range(len(l_set)):  # for循环结束，某个社区结束
            cursor_rating_all = self.con.cursor()
            if self.dataSet == 'Amazon':
                cursor_rating_all.execute('select overall from review13 where  reviewerID= \'' + l_set[i] + '\'')
            else:
                cursor_rating_all.execute("select rating from reviewGraph where user_id=%s" % (l_set[i]))

            rating_every_2 = cursor_rating_all.fetchone()
            community_rating_every = rating_every_2[0]
            n += abs(community_rating_every - community_rating_avg) / 4
            y += 1
        m = n / y
        if round(m, 4) > 1:
            m_end = 1
        else:
            m_end = round(m, 4)
        self.community_RD.append(m_end)
        return self.community_RD

    def GS(self, set_for_list):
        gs = 1 / (1 + exp(-(len(set_for_list) - 3)))
        if round(gs, 4) > 1:
            m_end = 1
        else:
            m_end = round(gs, 4)
        self.community_gs.append(m_end)
        return self.community_gs

    def GCAR(self, set_for_list, time_group):
        counts_in_time = counts_all = 0
        for i in range(len(set_for_list)):
            cursor_count = self.con.cursor()
            if self.dataSet == 'Amazon':
                cursor_count.execute('select count(*) from review13  where reviewerID = \'' + set_for_list[i] + '\'')
            else:
                cursor_count.execute('select count(*) from reviewGraph  where user_id = %s' % (set_for_list[i]))
            time = cursor_count.fetchone()
            counts_all += time[0]  # metadata中所有的评论次数#每个群组的所有节点在数据中的评论次数
            cursor_in_time = self.con.cursor()
            if self.dataSet == 'Amazon':
                cursor_in_time.execute(
                    'select unixReviewTime from review13  where reviewerID = \'' + set_for_list[i] + '\'')
            else:
                cursor_in_time.execute('select date from metadata  where user_id = %s' % (set_for_list[i]))

            user_all_time = cursor_in_time.fetchall()
            # print(user_all_time)
            if len(time_group) > 1:
                time_session_start = datetime.datetime.strptime(time_group[0], "%Y-%m-%d")
                time_session_end = datetime.datetime.strptime(time_group[1], "%Y-%m-%d")
                days = time_session_end - time_session_start
            else:
                days = 0
            for i in range(len(user_all_time)):
                timeArray = datetime.datetime.utcfromtimestamp(int(user_all_time[i][0]))
                user_time = timeArray.strftime("%Y-%m-%d")
                user_time = datetime.datetime.strptime(user_time, "%Y-%m-%d")
                # print(user_time)
                if user_time - time_session_start <= days:
                    counts_in_time += 1
        x = round(counts_in_time / counts_all, 4)  # 在时间片内出现的次数
        if x > 1:
            x = 1
        self.community_GCAR.append(x)
        return self.community_GCAR

    def GAD(self, set_for_list):  # Yelp不好算
        avg = 0
        set_l = list(set(set_for_list))
        for i in range(len(set_l)):
            # print(l[key][i])
            if self.dataSet == 'Amazon':
                unixtime = []
                cursor_unixTime = self.con.cursor()
                cursor_unixTime.execute(
                    'select unixReviewTime from review13 where reviewerID=\'' + set_l[
                        i] + '\'order by unixReviewTime desc')
                review_time = cursor_unixTime.fetchall()
                # print(review_time)
                for i in range(len(review_time)):
                    unixtime.append(int(review_time[i][0]))
                ad = 1 - (max(unixtime) - min(unixtime)) / (864000 * 365)
                avg += ad
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
                avg += ad
        avg = avg / len(set_l)
        if avg > 1:
            m_end = 1
        else:
            m_end = round(avg, 4)
        self.community_AD.append(m_end)
        return self.community_AD

    def GOR(self, set_for_list):
        sumANR = 0
        set_l = list(set(set_for_list))
        for i in range(len(set_l)):
            cursor = self.con.cursor()
            if self.dataSet == 'Amazon':
                cursor.execute(
                    'select count(*) from review13 where reviewerID=\'' + set_l[i] + '\'group by unixReviewTime')
            else:
                cursor.execute('select count(*) from metadata where user_id=%s group by date' % set_l[i])
            rows = cursor.fetchall()
            num = len(rows)
            nor = []
            for r in rows:
                nor.append(r[0])
            #  计算大于6的天数的比例
            an = 0
            for i in nor:
                if i >= 5:
                    an += 1
            anr = an / num
            sumANR += anr
        avgANR = sumANR / int(len(set_l))
        if avgANR > 1:
            m_end = 1
        else:
            m_end = round(avgANR, 4)
        self.community_GOR.append(m_end)
        return self.community_GOR

    def GRT(self, set_for_list):
        set_l = list(set(set_for_list))
        rg = len(set_l)
        pg_list = set()
        rg_list = set()
        vg = 0
        for i in range(len(set_l)):
            cursor = self.con.cursor()
            if self.dataSet == 'Amazon':
                cursor.execute('select reviewerID,asin from review13 where reviewerID=\'' + set_l[i] + '\'')
            else:
                cursor.execute('select count(*),count(distinct prod_id) from metadata where user_id=%s' % set_l[i])
            rows = cursor.fetchall()
            # print(rows)#所有群组排列
            for j in range(len(rows)):
                pg_list.add(rows[j][1])
                rg_list.add(rows[j][0])
                if len(rg_list) > 0:
                    if set_l[i] in rg_list:
                        vg += 1
        grt = vg / (rg * len(pg_list))
        if grt > 1:
            m_end = 1
        else:
            m_end = round(grt, 4)
        self.community_GRT.append(m_end)
        return self.community_GRT

    def AVG(self, fr, fw):
        file_w = open(fw, 'w', encoding='utf-8')
        file = open(fr, 'r', encoding='utf-8')
        while 1:
            dict_group = {}
            lines = file.readline()
            lines = lines.strip("\n")
            try:
                dict_group = eval(lines)
            except:
                print("ending")
                break
            print(dict_group)
            self.group_all.append(dict_group)
            set_for_list = list(dict_group['group'])
            time_group = list(dict_group['time'])
            community_RD = self.GRD(set_for_list)
            community_GER = self.GER(set_for_list)
            community_GCAR = self.GCAR(set_for_list, time_group)
            community_GRT = self.GRT(set_for_list)
            community_GOR = self.GOR(set_for_list)
            community_GS = self.GS(set_for_list)
            print("GRD = ", community_RD)
            print("GER = ", community_GER)
            print("GCAR = ", community_GCAR)
            print("GRT = ", community_GRT)
            print("GOR = ", community_GOR)
            print("GS = ", community_GS)
            self.list_AVG.append(round((community_RD[0] + community_GER[0] + community_GCAR[0] + community_GRT[0] +
                                        community_GOR[0] + community_GS[0]) / 6, 4))
            print(self.list_AVG)
            self.list_AVG_all.append(self.list_AVG[0])
            print("+++++++++++++++++")
            self.list_AVG.clear()
            self.community_RD.clear()
            self.community_GER.clear()
            self.community_GCAR.clear()
            self.community_GRT.clear()
            self.community_GOR.clear()
            self.community_gs.clear()
            if not lines:
                break
            for line in lines:
                pass
        sort_group = self.sort_group(self.list_AVG_all, self.group_all)
        for group in sort_group:
            file_w.write(str(group) + "\n")
            file_w.flush()
        file_w.close()

    def sort_group(self, list_AVG_all, group_all):
        list_AVG_all = np.array(list_AVG_all)
        list_AVG_all = list_AVG_all.argsort()
        group_all = np.array(group_all)
        sort_group = group_all[list_AVG_all]

        return sort_group

# 全局变量
if __name__ == '__main__':
    GS = GroupScore()
    GS.dataSet = 'Amazon'
    fr = './output/dynamicRuleMemory/Amazon/Amazon_spammer_group_净化.txt'
    fw = './output/dynamicRuleMemory/Amazon/Amazon_spammer_group_排序.txt'
    GS.AVG(fr, fw)
