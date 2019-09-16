#for the sake of completeness also the Stub necessary to run gryffinpop.py in the right way is provided
from gryffinpop
import *

class ScholarlySearchEngine():
    def __init__(self, source_csv_file_path):
        self.data = process_data(source_csv_file_path)

    def search(self, query, col, is_number, partial_res=None):
        result_data = do_search(self.data, query, col, is_number, partial_res)
        return result_data

    def pretty_print(self, result_data):
        list_of_strings = do_pretty_print(self.data, result_data)
        return list_of_strings

    def publication_tree(self, author):
        pub_tree = do_publication_tree(self.data, author)
        return pub_tree

    def top_ten_authors(self):
        list_of_tuples = do_top_ten_authors(self.data)
        return list_of_tuples

    def coauthor_network(self, author):
        coauth_graph = do_coauthor_network(self.data, author)
        return coauth_graph
    
    def top_10_volume(self, data):
        list_of_tuples = do_top_10_volume(self.data, data)
        return list_of_tuples
