import pandas as pd
from py2neo import Node, Graph, Relationship

graph = Graph("http://localhost:7474/", auth=("neo4j", "12345678"), name="neo4j")

featnames_file_name = "0.featnames"
feat_file_name = "0.feat"
circles_file_name = "0.circles"
edge_file_name = "0.edges"
egofeat_file_name = "0.egofeat"

def read_data():
    feature_list = open(featnames_file_name, 'r').readlines()
    feature_count = len(feature_list)

    feature_data = pd.DataFrame(columns=["num", "name", "data"])
    feature_name = set()

    for line in feature_list:
        contents = line.split(";")
        if len(contents) == 2:
            first = contents[0].split(" ")
            second = contents[1].split(" ")
            temp = pd.DataFrame({"num": [int(first[0])], "name": [first[1]], "data": [int(second[2])]})
            feature_data = pd.concat([feature_data, temp], ignore_index=True)
            feature_name.add(first[1])
        elif len(contents) == 3:
            first = contents[0].split(" ")
            third = contents[2].split(" ")
            name = first[1] + "_" + contents[1]
            temp = pd.DataFrame({"num": [int(first[0])], "name": [name], "data": [int(third[2])]})
            feature_data = pd.concat([feature_data, temp], ignore_index=True)
            feature_name.add(name)
        elif len(contents) == 4:
            first = contents[0].split(" ")
            fourth = contents[3].split(" ")
            name = first[1] + "_" + contents[1] + "_" + contents[2]
            temp = pd.DataFrame({"num": [int(first[0])], "name": [name], "data": [int(fourth[2])]})
            feature_data = pd.concat([feature_data, temp], ignore_index=True)
            feature_name.add(name)

    node_feature_list = open(feat_file_name, 'r').readlines()
    node_circle_list = [[] for line in node_feature_list]

    circles = open(circles_file_name, 'r').readlines()
    for line in circles:
        contents = line.split("\t")
        for i in range(1, len(contents)):
            node_circle_list[int(contents[i]) - 1].append(contents[0])

    node_list = list()
    line_number = 0
    for line in node_feature_list:
        feature_dict = dict()
        for name in feature_name:
            feature_dict[name] = None

        contents = line.split(" ")
        for i in range(1, len(contents)):
            if contents[i - 1] == '1':
                feature_dict[feature_data.loc[i - 1, "name"]] = feature_data.loc[i - 1, "data"]

        node = Node("Person",
                    birthday=feature_dict['birthday'],
                    education_classes_id=feature_dict['education_classes_id'],
                    education_concentration_id=feature_dict['education_concentration_id'],
                    education_degree_id=feature_dict['education_degree_id'],
                    education_school_id=feature_dict['education_school_id'],
                    education_type=feature_dict['education_type'],
                    education_with_id=feature_dict['education_with_id'],
                    education_year_id=feature_dict['education_year_id'],
                    first_name=feature_dict['first_name'],
                    gender=feature_dict['gender'],
                    hometown_id=feature_dict['hometown_id'],
                    languages_id=feature_dict['languages_id'],
                    last_name=feature_dict['last_name'],
                    locale=feature_dict['locale'],
                    location_id=feature_dict['location_id'],
                    work_employer_id=feature_dict['work_employer_id'],
                    work_end_date=feature_dict['work_end_date'],
                    work_location_id=feature_dict['work_location_id'],
                    work_position_id=feature_dict['work_position_id'],
                    work_start_date=feature_dict['work_start_date'],
                    work_with_id=feature_dict['work_with_id'],
                    circle=node_circle_list[line_number])
        node_list.append(node)
        graph.create(node)
        line_number += 1

    for line in open("0.edges", "r").readlines():
        contents = line.split(" ")
        first = int(contents[0])
        second = int(contents[1])
        relationship = Relationship(node_list[first - 1], "Be_Friend_With", node_list[second - 1])
        relationship["undirected"] = True
        graph.create(relationship)

    center_data = open("0.egofeat", 'r').readline().split(" ")
    for line in node_feature_list:
        contents = line.split(" ")
        flag = True
        for i in range(1, len(contents)):
            if int(contents[i]) != int(center_data[i - 1]):
                flag = False
                break
        if flag == True:
            print("Center Node Id!")
            break

    feature_dict = dict()
    for name in feature_name:
        feature_dict[name] = None
    for i in range(1, len(center_data)):
        if center_data[i - 1] == '1':
            feature_dict[feature_data.loc[i - 1, "name"]] = feature_data.loc[i - 1, "data"]

    node = Node("Person",
                birthday=feature_dict['birthday'],
                education_classes_id=feature_dict['education_classes_id'],
                education_concentration_id=feature_dict['education_concentration_id'],
                education_degree_id=feature_dict['education_degree_id'],
                education_school_id=feature_dict['education_school_id'],
                education_type=feature_dict['education_type'],
                education_with_id=feature_dict['education_with_id'],
                education_year_id=feature_dict['education_year_id'],
                first_name=feature_dict['first_name'],
                gender=feature_dict['gender'],
                hometown_id=feature_dict['hometown_id'],
                languages_id=feature_dict['languages_id'],
                last_name=feature_dict['last_name'],
                locale=feature_dict['locale'],
                location_id=feature_dict['location_id'],
                work_employer_id=feature_dict['work_employer_id'],
                work_end_date=feature_dict['work_end_date'],
                work_location_id=feature_dict['work_location_id'],
                work_position_id=feature_dict['work_position_id'],
                work_start_date=feature_dict['work_start_date'],
                work_with_id=feature_dict['work_with_id'])

    graph.create(node)

    for i in range(len(node_feature_list)):
        relationship = Relationship(node_list[i], "Be_Friend_With", node)
        relationship["undirected"] = True
        graph.create(relationship)


if __name__ == "__main__":
    read_data()
