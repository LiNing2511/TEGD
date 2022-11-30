import datetime
import numpy as np


class BurstSession:
    def __init__(self):
        self.metapath_dict_i = dict()
        self.metapath_dict_j = dict()
        self.list_burst_session = []
        self.burstdict = {'time': [], 'metapath': []}
        self.purity_num = 0
        self.final_dict = {'time': [], 'group': [], 'product': []}
        self.product_group = set()

    def burstSession(self, inputpath, outputpath, timeSession):
        fw = open(outputpath, 'w', encoding='utf-8')
        with open(inputpath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            print(len(lines))
            i = 0
            while i < (lines.__len__() - 1):
                line_i = lines[i].split('\n')
                self.metapath_dict_i = eval(line_i[0])
                self.list_burst_session.clear()
                self.list_burst_session.append(self.metapath_dict_i)
                time_i = datetime.datetime.strptime(self.metapath_dict_i['time'][0], "%Y-%m-%d")
                for j in range(i + 1, lines.__len__()):
                    line_j = lines[j].split('\n')
                    self.metapath_dict_j = eval(line_j[0])
                    time_j = datetime.datetime.strptime(self.metapath_dict_j['time'][0], "%Y-%m-%d")
                    # print(time_j,time_i,(time_j.__sub__(time_i)).days)
                    if j == lines.__len__() - 1:
                        i = j
                    if (time_j - time_i).days <= timeSession:
                        self.list_burst_session.append(self.metapath_dict_j)
                    else:
                        i = j
                        break
                num = 0
                for dict_all in self.list_burst_session:
                    if num == 0:
                        self.burstdict['time'].append(dict_all['time'][0])
                    num += 1
                    self.burstdict['metapath'].append(tuple(dict_all['metapath']))
                    if num == len(self.list_burst_session) - 1:
                        self.burstdict['time'].append(dict_all['time'][0])
                fw.write(self.burstdict.__str__() + '\n')
                self.burstdict['time'].clear()  # 清空字典的键值
                self.burstdict['metapath'].clear()
            fw.close()

    def burstMatrix(self, burstSessionPath, WritePath):
        fw = open(WritePath, 'w', encoding='utf-8')
        with open(burstSessionPath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip('\n')
                line_dict = eval(line)
                # print(line_dict['metapath'])
                for metapath in line_dict['metapath']:
                    if len(metapath) == 3:
                        self.product_group.add(metapath[1])
                    else:
                        self.product_group.add(metapath[1])
                        self.product_group.add(metapath[3])
                print(self.product_group)
                for p in self.product_group:
                    self.Matrix(line_dict['metapath'], p)
                    self.purity_num = 0  # 每个矩阵都做一次净化，迭代
                    if len(group)!= 0 and len(product)!=0:
                        self.final_dict['group'] = list(group)  # 净化之后的group
                        self.final_dict['product'] = list(product)  # 净化之后的产品
                        self.final_dict['time'] = line_dict['time']  # time
                        print(self.final_dict)
                        fw.write(str(self.final_dict) + '\n')
                        fw.flush()
                    # self.final_dict['group'].clear()
                    # self.final_dict['product'].clear()
                    # # self.final_dict['time'].clear()
            fw.close()

    def Matrix(self, metapath_list, p):
        global group
        global product
        product = set()
        group = set()
        num = 0
        rpr_num = 0
        rprpr_num = 0
        for metapath in metapath_list:
            if len(metapath) == 3 and metapath[1] == p:
                product.add(metapath[1])
                group.add(metapath[0])
                group.add(metapath[2])
                num += 1
                rpr_num += 1
            if len(metapath) == 5 and (metapath[1] == p or metapath[2] == p):
                product.add(metapath[1])
                product.add(metapath[2])
                group.add(metapath[0])
                group.add(metapath[2])
                group.add(metapath[4])
                num += 1
                rprpr_num += 1
        matrix_metapath_rpr = np.zeros([len(group), len(group)], dtype=float)
        matrix_metapath_rprpr = np.zeros([len(group), len(group)], dtype=float)
        for metapath in metapath_list:
            if len(metapath) == 3 and metapath[1] == p:
                x = list(group).index(metapath[0])
                y = list(group).index(metapath[2])
                matrix_metapath_rpr[x][y] += 1
                # matrix_metapath_rpr[y][x] += 1
            if len(metapath) == 5 and (metapath[1] == p or metapath[2] == p):
                x1 = list(group).index(metapath[0])
                y1 = list(group).index(metapath[2])
                matrix_metapath_rprpr[x1][y1] += 1
                # matrix_metapath_rprpr[y1][x1] += 1
                x2 = list(group).index(metapath[2])
                y2 = list(group).index(metapath[4])
                matrix_metapath_rprpr[x2][y2] += 1
                # matrix_metapath_rprpr[y2][x2] += 1
        if num != 0:
            # final_matrix = (rpr_num / len(metapath_list)) * matrix_metapath_rpr + \
            #                (rprpr_num / len(metapath_list)) * matrix_metapath_rprpr
            final_matrix = (rpr_num / num) * matrix_metapath_rpr + \
                               (rprpr_num / num) * matrix_metapath_rprpr
        else:
            final_matrix = matrix_metapath_rpr + matrix_metapath_rprpr
        if self.purity_num != 1 and len(group) != 0:  # 如果没净化过就净化一次，迭代一次
            metapath_0, metapath_2 = self.purityMatrix(group, final_matrix)
            for metapath0, metapath2 in zip(metapath_0, metapath_2):
                for metapath in metapath_list:
                    if (metapath0 == metapath[0] and metapath2 == metapath[2]) or \
                            (metapath0 == metapath[2] and metapath2 == metapath[0]):
                        metapath_list.remove(metapath)
            self.Matrix(metapath_list, p)  #

    def purityMatrix(self, group, matrix):
        metapath_0 = []
        metapath_2 = []
        list_1 = []
        for i in range(len(matrix)):
            for j in range(len(matrix[i])):
                if matrix[i][j] != 0:
                    list_1.append(matrix[i][j])
        outliner = np.percentile(list_1, 80)  # 百分位取值，然后去除异常值
        for i in range(len(matrix)):
            for j in range(len(matrix[i])):
                if matrix[i][j] != 0 and matrix[i][j] <= outliner:
                    metapath_0.append(list(group)[i])
                    metapath_2.append(list(group)[j])
        self.purity_num = 1
        return metapath_0, metapath_2


if __name__ == '__main__':
    T = BurstSession()
    inputpath = './output/metapath/Amazon_all_metapath_sorted_by_time_12.txt'
    outputpath = './output/burstSession/Amazon/Amazon_burstSession_12.txt'
    fw = './output/burstSession/Amazon/Amazon_candidate_group_12.txt'
    T.burstSession(inputpath, outputpath, 0)
    T.burstMatrix(outputpath, fw)
    # def burstSession(self):
    # list_test = [1, 2, 3, 1, 1, 1, 1, 2, 1, 0.5, 0.22, 0.1]
    # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    # print(np.percentile(list_test, 10))
