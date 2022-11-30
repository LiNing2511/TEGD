import datetime
import gc
import random
import sys


class amazon_metapath:
    def __init__(self, args):
        self.output_metapath_list = []
        self.args = args
        self.data_path = './input/Amazon/'
        self.rprt_path = 'rprt_12.txt'
        self.rprprt_path = 'rprprt_12.txt'
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

    def reading_data(self, fw_rpr, fw_rprpr):
        fw_rpr = open('./output/metapath/Amazon_metapath_rpr_12.txt', 'w', encoding='utf-8')
        fw_rprpr =  open('./output/metapath/Amazon_metapath_rprpr_12.txt', 'w', encoding='utf-8')
        print('\n' + 'r_p_r：metapath process')
        f1 = open(self.data_path + self.rprt_path,'r', encoding='utf-8')
        data_lines_1 = f1.readlines()
        data_line_1_num = 0
        f2 = open(self.data_path + self.rprprt_path, 'r', encoding='utf-8')
        data_lines_2 = f2.readlines()
        data_line_2_num = 0
        for data_line in data_lines_1:
            data_line_1_num += 1
            self.process_bar(data_line_1_num, len(data_lines_1))
            # self.r_p_r['metapath'].clear()
            # self.r_p_r['time'].clear()
            self.reviewer_num = self.reviewer_num + 1
            line = data_line.encode('utf-8').decode('utf-8-sig').split(';')  # 解决/ufeff的问题
            r_id = line[0]
            p_id = line[1]
            cr_id = line[2]
            time = line[3].strip('\n')
            time = datetime.datetime.strptime(time, '%m %d, %Y')
            time = time.date()
            self.r_p_r['metapath'] = [r_id, p_id, cr_id]
            self.r_p_r['time'] = [str(time)]
            if self.r_p_r not in self.output_metapath['1']:
                fw_rpr.write(str(self.r_p_r)+"\n")
                # self.output_metapath['1'].append(str(self.r_p_r))
            # print(self.output_metapath['1'])
        print('\n' + 'r_p_r_p_r：metapath process')
        for data_line in data_lines_2:
            data_line_2_num += 1
            self.process_bar(data_line_2_num, len(data_lines_2))
            # self.r_p_r_p_r['metapath'].clear()
            # self.r_p_r_p_r['time'].clear()
            self.reviewer_num = self.reviewer_num + 1
            line = data_line.encode('utf-8').decode('utf-8-sig').split(';')  # 解决/ufeff的问题
            r_id = line[0]
            p_id = line[1]
            cr_id1 = line[2]
            cp_id = line[3]
            cr_id2 = line[4]
            time = line[5].strip('\n')
            time = datetime.datetime.strptime(time, '%m %d, %Y')
            time = time.date()
            self.r_p_r_p_r['metapath'] = [r_id, p_id, cr_id1, cp_id, cr_id2]
            self.r_p_r_p_r['time'] = [str(time)]
            if self.r_p_r_p_r not in self.output_metapath['2']:
                fw_rprpr.write(str(self.r_p_r_p_r) + "\n")
                # self.output_metapath['2'].append(str(self.r_p_r_p_r))
            # print(self.output_metapath['2'])
            # print(self.r_p_r_p_r)
        return self.output_metapath['1'], self.output_metapath['2']

    # def data_generate(self, rpr_outpath, rprpr_outpath):
    #     a, b = self.reading_data()
    #     with open(rpr_outpath, 'w', encoding='utf-8') as f1:
    #         f1.write(str(a))
    #     with open(rprpr_outpath, 'w', encoding='utf-8') as f2:
    #         f2.write(str(b))

    def read_metapath(self, sort_outpath):
        fr_rpr = open('./output/metapath/Amazon_metapath_rpr_12.txt', 'r', encoding='utf-8')
        fr_rprpr = open('./output/metapath/Amazon_metapath_rprpr_12.txt', 'r', encoding='utf-8')
        print('\n' + 'metapath process')
        lines_rpr = fr_rpr.readlines()
        lines_rprpr = fr_rprpr.readlines()
        num1 = 0
        num2 = 0
        for line_rpr in lines_rpr:
            num1 += 1
            self.process_bar(num1, len(lines_rpr))
            line_rpr = line_rpr.strip("\n")
            line_rpr_dict = eval(line_rpr)
            self.output_metapath['1'].append(line_rpr_dict)
        for line_rprpr in lines_rprpr:
            num2 += 1
            self.process_bar(num2, len(lines_rprpr))
            line_rprpr = line_rprpr.strip("\n")
            line_rprpr_dict = eval(line_rprpr)
            self.output_metapath['1'].append(line_rprpr_dict)
        self.sort_outpath_list = self.mergeSort(self.output_metapath['1'])
        with open(sort_outpath, 'w', encoding='utf-8') as f:
            for k in range(len(self.sort_outpath_list) - 1):
                f.write(str(self.sort_outpath_list[k]) + '\n')

    # def sort_All_Metapath(self, rpr_outpath, rprpr_outpath, sort_outpath):
    #     self.list_all = []
    #     self.sort_outpath_list = []
    #     with open(rpr_outpath, encoding='utf-8') as f:
    #         data_line = f.readline()
    #         list_rpr = data_line.strip('[]')
    #         list_rpr = list_rpr.split('}",')
    #     with open(rprpr_outpath, encoding='utf-8') as f:
    #         data_line = f.readline()
    #         list_rprpr = data_line.strip('[]')
    #         list_rprpr = list_rprpr.split('}",')
    #     for i in range(len(list_rpr) - 1):
    #         self.list_all.append((list_rpr[i] + '}'))
    #     # print(eval(self.list_all[0])['time'][0])
    #     for j in range(len(list_rprpr) - 1):
    #         self.list_all.append((list_rprpr[j]) + '}')
    #     self.sort_outpath_list = self.mergeSort(self.list_all)
    #     with open(sort_outpath, 'w', encoding='utf-8') as f:
    #         for k in range(len(self.sort_outpath_list) - 1):
    #             f.write(str(self.sort_outpath_list[k]) + '\n')

    def merge(self, left, right):
        # 合并两个有序列表
        res = []
        while len(left) > 0 and len(right) > 0:
            left_time_str = left[0]['time'][0]
            right_time_str = right[0]['time'][0]
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

    data_temp = amazon_metapath(args)
    rpr_outpath = './output/metapath/Amazon_metapath_rpr_12.txt'
    rprpr_outpath = './output/metapath/Amazon_metapath_rprpr_12.txt'
    sort_outpath = './output/metapath/Amazon_all_metapath_sorted_by_time_12.txt'
    # data_temp.reading_data(rpr_outpath, rprpr_outpath)
    data_temp.read_metapath(sort_outpath)
    # data_temp.data_generate(rpr_outpath, rprpr_outpath)
    # data_temp.sort_All_Metapath(rpr_outpath, rprpr_outpath, sort_outpath)
