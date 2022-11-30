import datetime
import gc
import random
import sys


class yelp_metapath:
    def __init__(self, args):

        self.output_metapath_list = []
        self.args = args
        self.data_path = './input/Yelp/'
        self.co_review_path = 'metadata.txt'
        self.metapath_type = []
        self.metapath_type = ['r_p_r', 'r_p_r_p_r']
        self.product_num = 0
        self.reviewer_num = 0
        self.node_dim = {}
        self.train_edges = {}  # {node id: {node id: time, ...}, ...}
        self.output_metapath = {'1': [], '2': []}
        # for time_random_work()
        self.r_p_r = {'metapath_type': [self.metapath_type[0]], 'metapath': [], 'time': []}
        # for time_random_work_plus()
        self.r_p_r_p_r = {'metapath_type': [self.metapath_type[1]], 'metapath': [], 'time': []}

    def reading_data(self):
        self.reviewer_product_time_dict = {}
        self.product_reviewer_time_dict = {}
        with open(self.data_path + self.co_review_path, encoding='utf-8') as f:
            data_line = f.readline()
            while (data_line):
                self.reviewer_num = self.reviewer_num + 1
                line = data_line.encode('utf-8').decode('utf-8-sig').split()  # 解决/ufeff的问题
                r_id = line[0]
                p_id = line[1]
                # cr_id = line[0]
                time = line[2]
                if r_id not in self.reviewer_product_time_dict.keys():
                    self.reviewer_product_time_dict[r_id] = []
                if p_id not in self.product_reviewer_time_dict.keys():
                    self.product_reviewer_time_dict[p_id] = []
                self.reviewer_product_time_dict[r_id].append((p_id, time))
                self.product_reviewer_time_dict[p_id].append((r_id, time))
                data_line = f.readline()
        self.node_dim['product'] = len(self.product_reviewer_time_dict.keys())
        self.node_dim['reviewer'] = len(self.reviewer_product_time_dict.keys())
        return self.reviewer_product_time_dict, self.product_reviewer_time_dict

    def time_random_work(self, reviewer_product_time_dict, product_reviewer_time_dict):

        print('r_p_r：metapath process')
        num = 0  # 进度
        for r_id in reviewer_product_time_dict.keys():
            num = num + 1
            self.process_bar(num, len(reviewer_product_time_dict))  # 进度条
            #
            self.r_p_r['metapath'].clear()
            self.r_p_r['time'].clear()
            p_id = reviewer_product_time_dict[r_id][0][0]  # a reviewer target product
            r_id_time = reviewer_product_time_dict[r_id][0][1]  # a reviewer review time
            datetime_r_id_time = datetime.datetime.strptime(r_id_time, "%Y-%m-%d")
            for cp_id in product_reviewer_time_dict.keys():  # cp_id :co_reviewer target product id
                cr_id = product_reviewer_time_dict[cp_id][0][0]  # co_reviewer id
                cr_id_time = product_reviewer_time_dict[cp_id][0][1]  # a co_reviewer review time
                datetime_cr_id_time = datetime.datetime.strptime(cr_id_time, "%Y-%m-%d")
                # print(r_id,p_id,r_id_time,cp_id,cr_id,cr_id_time)
                if cp_id == p_id and cr_id != r_id and \
                        (datetime_r_id_time - datetime_cr_id_time).days <= 2:
                    self.r_p_r['metapath'] = [r_id, p_id, cr_id]
                    self.r_p_r['time'] = [cr_id_time]
                    if self.r_p_r not in self.output_metapath['1']:
                        self.output_metapath['1'].append(self.r_p_r)
        return self.output_metapath['1']

    def time_random_work_plus(self, list_rpr):

        print('\n' + 'r_p_r_p_r：metapath process')
        for i in range(len(list_rpr) - 1):
            self.process_bar(i + 1, len(list_rpr) - 1)  # 进度条
            self.r_p_r_p_r['metapath'].clear()
            self.r_p_r_p_r['time'].clear()
            metapath_i_rpr = list_rpr[i]['metapath']
            r_id_1 = metapath_i_rpr[0]  # r_p_r No.1 r
            r_id_2 = metapath_i_rpr[2]  # r_p_r No.3 r
            p_id = metapath_i_rpr[1]  # r_p_r p
            r_id_time = list_rpr[i]['time'][0]  # r_p_r time
            datetime_r_id_time = datetime.datetime.strptime(r_id_time, "%Y-%m-%d")
            for j in range(i + 1, len(list_rpr)):
                metapath_j_rpr = list_rpr[j]['metapath']
                cr_id_1 = metapath_j_rpr[0]  # other r_p_r No.1 r
                cp_id = metapath_j_rpr[1]  # other r_p_r p
                cr_id_time = list_rpr[i]['time'][0]  # other r_p_r time
                datetime_cr_id_time = datetime.datetime.strptime(cr_id_time, "%Y-%m-%d")
                cr_id_2 = metapath_j_rpr[2]  # other rpr No.2 r
                if r_id_2 == cr_id_1 and p_id != cp_id and (datetime_r_id_time - datetime_cr_id_time).days <= 0:
                    self.r_p_r_p_r['metapath'] = [r_id_1, p_id, cr_id_1, cp_id, cr_id_2]
                    self.r_p_r_p_r['time'] = [cr_id_time]
                    if self.r_p_r_p_r not in self.output_metapath['2']:
                        self.output_metapath['2'].append(self.r_p_r_p_r)
                elif r_id_1 == cr_id_2 and p_id != cp_id and (datetime_r_id_time - datetime_cr_id_time).days <= 0:
                    self.r_p_r_p_r['metapath'] = [cr_id_1, p_id, r_id_1, cp_id, r_id_2]
                    self.r_p_r_p_r['time'] = [cr_id_time]
                    if self.r_p_r_p_r not in self.output_metapath['2']:

                        self.output_metapath['2'].append(self.r_p_r_p_r)
        return self.output_metapath['2']

    def data_generate(self, rpr_outpath, rprpr_outpath):
        self.reading_data()
        a, b = self.reading_data()
        c = self.time_random_work(a, b)
        with open(rpr_outpath, 'w', encoding='utf-8') as f1:
            f1.write(str(c))

        d = self.time_random_work_plus(c)
        with open(rprpr_outpath, 'w', encoding='utf-8') as f2:
            f2.write(str(d))

    def sort_All_Metapath(self, rpr_outpath, rprpr_outpath, sort_outpath):
        self.list_all = []
        self.sort_outpath_list = []
        with open(rpr_outpath, encoding='utf-8') as f:
            data_line = f.readline()
            list_rpr = data_line.strip('[]')
            list_rpr = list_rpr.split('},')
        with open(rprpr_outpath, encoding='utf-8') as f:
            data_line = f.readline()
            list_rprpr = data_line.strip('[]')
            list_rprpr = list_rprpr.split('},')
        for i in range(len(list_rpr) - 1):
            self.list_all.append((list_rpr[i] + '}'))
        # print(eval(self.list_all[0])['time'][0])
        for j in range(len(list_rprpr) - 1):
            self.list_all.append((list_rprpr[j]) + '}')
        self.sort_outpath_list = self.mergeSort(self.list_all)
        with open(sort_outpath, 'w', encoding='utf-8') as f:
            for k in range(len(self.sort_outpath_list) - 1):
                f.write(str(self.sort_outpath_list[k]) + '\n')

    def merge(self, left, right):
        # 合并两个有序列表
        res = []
        while len(left) > 0 and len(right) > 0:
            left_time_str = eval(left[0])['time'][0]
            right_time_str = eval(right[0])['time'][0]
            left_time = datetime.datetime.strptime(left_time_str, "%Y-%m-%d")
            right_time = datetime.datetime.strptime(right_time_str, "%Y-%m-%d")
            if (left_time - right_time).days < 0:
                res.append(left.pop(0))
            else:
                res.append(right.pop(0))
        if left:
            res.extend(left)
        if right:
            res.extend(right)
        return res

    def mergeSort(self, arr):
        # 归并函数
        n = len(arr)
        if n < 2:
            return arr
        middle = n // 2
        left = arr[:middle]  # 取序列左边部分
        right = arr[middle:]  # 取序列右边部分
        # 对左边部分序列递归调用归并函数
        left_sort = self.mergeSort(left)
        # 对右边部分序列递归调用归并函数
        right_sort = self.mergeSort(right)
        #
        return self.merge(left_sort, right_sort)

    def process_bar(self, num, total):  # 进度条
        rate = float(num) / total
        ratenum = int(100 * rate)
        r = '\r[{}{}]{}%'.format('*' * ratenum, ' ' * (100 - ratenum), ratenum)
        sys.stdout.write(r)
        sys.stdout.flush()


if __name__ == '__main__':
    args = ""
    data_temp = yelp_metapath(args)
    rpr_outpath = './output/metapath/YelpZip_metapath_r_p_r.txt'
    rprpr_outpath = './output/metapath/YelpZip_metapath_r_p_r_p_r.txt'
    sort_outpath = './output/metapath/YelpZip_all_metapath_sorted_by_time.txt'
    data_temp.data_generate(rpr_outpath, rprpr_outpath)
    data_temp.sort_All_Metapath(rpr_outpath, rprpr_outpath, sort_outpath)
