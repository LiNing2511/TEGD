from YelpMetapath import yelp_metapath
if __name__ == '__main__':
    args = ""
    data_temp = yelp_metapath(args)
    a, b = data_temp.reading_data()
    # print(a)
    # print(b)
    data_temp.time_random_work(a, b)
    # data_temp.metapath_generate()

