class DynamicRuleMemory:
    def __init__(self):
        self.final_group = {'time': [], 'group': [], 'product': []}
        self.list_memory = []
        self.list_spam_strategy_memory_1 = []
        self.list_spam_strategy_memory_2 = []
        self.list_spam_strategy_memory_3 = []
        self.spammer_group = []
        self.all_spammer_group = [[]]
        self.spammer_group_evolution = {'birth': [], 'contraction': [], 'growth': [], 'merging_splitting': [],
                                        'death': []}

    def ruleMemory(self, final_group_path, spammer_group_path):
        fw = open(spammer_group_path, 'w', encoding='utf-8')
        with open(final_group_path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip('\n')
                self.final_group = eval(line)
                self.list_memory.append(self.final_group)
        for i in range(len(self.list_memory)):
            group_birth = eval(str(self.list_memory[i]))  # {birthtime, group, target product}
            self.spammer_group.append(str(group_birth))
            self.spammer_group_evolution['birth'].append(1)
            self.contraction(i, group_birth, self.list_memory, self.spammer_group, self.spammer_group_evolution)
            self.growth(i, group_birth, self.list_memory, self.spammer_group, self.spammer_group_evolution)
            self.merging_splitting(i, self.list_memory, self.spammer_group, self.all_spammer_group,
                                   self.spammer_group_evolution)
            if len(self.spammer_group) > 1:
                print(f"第{i}个爆发群组的演化群组分别是：")
                for group in self.spammer_group:
                    print(group)
                    fw.write(str(group) + "\n")
                    fw.flush()
                print(self.spammer_group_evolution)
                self.spammer_group_evolution['contraction'].clear()
                self.spammer_group_evolution['growth'].clear()
                self.spammer_group_evolution['merging_splitting'].clear()
            self.spammer_group.clear()
            self.spammer_group_evolution['birth'].clear()
        fw.close()

    def growth(self, location, group_birth, list_memory, spammer_group, spammer_group_evolution):
        for j in range(location, len(list_memory)):
            group_growth = eval(str(list_memory[j]))
            if group_birth['time'] != group_growth['time'] \
                    and set(group_birth['group']).issubset(set(group_growth['group'])):
                spammer_group_evolution['growth'].append(1)
                spammer_group.append(str(group_growth))
            # else:
            #     spammer_group_evolution['growth'].append(0)

    def contraction(self, location, group_birth, list_memory, spammer_group, spammer_group_evolution):
        for j in range(location, len(list_memory)):
            group_contraction = eval(str(list_memory[j]))
            if group_birth['time'] != group_contraction['time'] \
                    and set(group_contraction['group']).issubset(set(group_birth['group'])):
                spammer_group_evolution['contraction'].append(1)
                spammer_group.append(str(group_contraction))

    def merging_splitting(self, location, list_memory, spammer_group, all_spammer_group, spammer_group_evolution):
        if len(spammer_group) == 0:
            return 0
        else:
            for i in range(len(spammer_group)):
                group_i = eval(str(spammer_group[i]))
                for j in range(i, len(spammer_group)):
                    group_j = eval(str(spammer_group[j]))
                    all_spammer_group.append(group_i['group'].extend(group_j['group']))  # 怎么判断数组合并
            for m in range(location, len(list_memory)):
                group_merging = eval(str(list_memory[m]))
                if set(group_merging) in all_spammer_group:
                    spammer_group.append(str(group_merging))
                    spammer_group_evolution['merging_splitting'].append(1)
                # else:
                #     spammer_group_evolution['merging_splitting'].append(0)

    def jaccard_similarity(self, x, y):
        intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
        union_cardinality = len(set.union(*[set(x), set(y)]))
        return intersection_cardinality / float(union_cardinality)


if __name__ == '__main__':
    args = ""
    data_temp = DynamicRuleMemory()
    data_temp.ruleMemory("./output/burstSession/Amazon/Amazon_candidate_group_12.txt",
                         "./output/dynamicRuleMemory/Amazon/Amazon_spammer_group.txt")
